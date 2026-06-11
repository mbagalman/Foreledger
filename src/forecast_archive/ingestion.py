"""Forecast ingestion: push with caller-supplied identity (ADR-005/ADR-006).

Run identity is ``(model_id, model_version, origin, series-set)`` and is never
inferred from content. Appends are all-or-nothing: segments are written
invisibly first and become visible only when the run manifest commits, so a
crash mid-ingest leaves the archive at its pre-run state.

This module owns the run-identity bookkeeping (the manifest) that makes
re-ingestion idempotent and conflicts explicit.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import uuid
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from .errors import ValidationError
from .schema import compute_horizon, to_timestamp

logger = logging.getLogger("forecast_archive.ingestion")

_CANONICAL_INPUT = ("series_id", "target", "value")


@dataclass
class RunRecord:
    """One committed run in the manifest."""

    run_id: str
    model_id: str
    model_version: str
    origin: str
    series_key: str
    content_hash: str
    segment: str
    ingested_at: str
    superseded: bool = False

    @property
    def identity(self) -> tuple[str, str, str, str]:
        return (self.model_id, self.model_version, self.origin, self.series_key)


@dataclass
class RunManifest:
    """Append-style run bookkeeping; the single source of forecast visibility."""

    path: Path
    runs: list[RunRecord] = field(default_factory=list)

    @classmethod
    def load(cls, path: Path) -> RunManifest:
        if not path.exists():
            return cls(path=path)
        payload = json.loads(path.read_text(encoding="utf-8"))
        runs = [RunRecord(**entry) for entry in payload.get("runs", [])]
        return cls(path=path, runs=runs)

    def save(self) -> None:
        payload = {"runs": [asdict(run) for run in self.runs]}
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, indent=1), encoding="utf-8")
        os.replace(tmp, self.path)

    def active_run_ids(self) -> list[str]:
        return [run.run_id for run in self.runs if not run.superseded]

    def find_active(self, identity: tuple[str, str, str, str]) -> RunRecord | None:
        for run in self.runs:
            if not run.superseded and run.identity == identity:
                return run
        return None


def normalize_datetimes(series: pd.Series, field_name: str) -> pd.Series:
    """Coerce a column to naive pandas datetimes."""
    try:
        converted = pd.to_datetime(series)
    except (ValueError, TypeError) as exc:
        raise ValidationError(f"column for {field_name!r} is not datetime-like") from exc
    if converted.isna().any():
        raise ValidationError(f"column for {field_name!r} contains missing values")
    if converted.dt.tz is not None:
        converted = converted.dt.tz_localize(None)
    return converted


def _resolve_mapping(
    frame: pd.DataFrame, mapping: Mapping[str, str] | None, fields: tuple[str, ...]
) -> dict[str, str]:
    mapping = dict(mapping or {})
    resolved: dict[str, str] = {}
    for canonical in fields:
        column = mapping.get(canonical, canonical)
        if column in frame.columns:
            resolved[canonical] = column
    return resolved


def canonicalize_forecasts(
    frame: pd.DataFrame,
    mapping: Mapping[str, str] | None,
    model_id: str,
    model_version: str,
    origin: Any | None,
) -> pd.DataFrame:
    """Map a user frame onto the canonical forecast columns and derive horizons.

    ``origin`` may be a scalar (one run) or come from a mapped column (one run
    per distinct origin value). Identity fields are validated, never inferred.
    """
    if not isinstance(model_id, str) or not model_id:
        raise ValidationError("model_id must be a non-empty string (caller-supplied identity)")
    if not isinstance(model_version, str) or not model_version:
        raise ValidationError("model_version must be a non-empty string (caller-supplied identity)")
    if frame.empty:
        raise ValidationError("cannot ingest an empty frame")

    wanted = _CANONICAL_INPUT + (("origin",) if origin is None else ())
    resolved = _resolve_mapping(frame, mapping, wanted)
    missing = [f for f in wanted if f not in resolved]
    if missing:
        raise ValidationError(
            f"frame is missing columns for {missing}; supply them via mapping= "
            f"(have: {list(frame.columns)})"
        )

    canonical = pd.DataFrame(
        {
            "series_id": frame[resolved["series_id"]].astype(str),
            "target": normalize_datetimes(frame[resolved["target"]], "target"),
            "value": pd.to_numeric(frame[resolved["value"]], errors="raise").astype("float64"),
        }
    )
    if origin is None:
        canonical["origin"] = normalize_datetimes(frame[resolved["origin"]], "origin")
    else:
        canonical["origin"] = to_timestamp(origin, "origin")
    if canonical["value"].isna().any():
        raise ValidationError("value column contains missing values")

    canonical["model_id"] = model_id
    canonical["model_version"] = model_version
    canonical["horizon"] = compute_horizon(canonical["origin"], canonical["target"])

    duplicated = canonical.duplicated(
        subset=["model_id", "model_version", "series_id", "origin", "target"]
    )
    if duplicated.any():
        raise ValidationError(
            f"frame contains {int(duplicated.sum())} duplicate "
            "(series_id, origin, target) rows for this model/version; the grain "
            "requires uniqueness within a run"
        )
    return canonical.sort_values(["origin", "series_id", "target"]).reset_index(drop=True)


def series_key(series_ids: pd.Series) -> str:
    joined = "\x1f".join(sorted(series_ids.astype(str).unique()))
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def content_hash(group: pd.DataFrame) -> str:
    ordered = group.sort_values(["series_id", "target"])
    digest = hashlib.sha256()
    for row_series, row_target, row_value in zip(
        ordered["series_id"], ordered["target"], ordered["value"], strict=True
    ):
        digest.update(f"{row_series}\x1f{row_target.isoformat()}\x1f{row_value!r}\n".encode())
    return digest.hexdigest()


@dataclass(frozen=True)
class IngestResult:
    """Outcome metadata for one ingest call."""

    n_rows: int
    n_runs_written: int
    n_runs_skipped: int
    n_runs_superseded: int


@dataclass(frozen=True)
class _PlannedRun:
    origin: pd.Timestamp
    frame: pd.DataFrame
    identity: tuple[str, str, str, str]
    hash: str
    supersedes: RunRecord | None


def plan_runs(
    canonical: pd.DataFrame,
    manifest: RunManifest,
    on_conflict: str,
) -> tuple[list[_PlannedRun], int]:
    """Validate every run group against the manifest before anything is written.

    Returns the runs to write and the count skipped as idempotent replays.
    Raises on a same-identity/different-values conflict under
    ``on_conflict="error"`` — before any side effect, keeping the whole call
    all-or-nothing.
    """
    from .errors import IngestConflictError

    if on_conflict not in ("error", "overwrite"):
        raise ValidationError("on_conflict must be 'error' or 'overwrite'")

    planned: list[_PlannedRun] = []
    skipped = 0
    for origin_value, group in canonical.groupby("origin", sort=True):
        origin_ts = (
            pd.Timestamp(str(origin_value))
            if not isinstance(origin_value, pd.Timestamp)
            else origin_value
        )
        identity = (
            str(group["model_id"].iloc[0]),
            str(group["model_version"].iloc[0]),
            origin_ts.isoformat(),
            series_key(group["series_id"]),
        )
        digest = content_hash(group)
        existing = manifest.find_active(identity)
        if existing is not None and existing.content_hash == digest:
            skipped += 1
            continue
        if existing is not None and on_conflict == "error":
            raise IngestConflictError(
                "a run with this identity (model, version, origin, series) already "
                "exists with different values; pass on_conflict='overwrite' to "
                "supersede it explicitly"
            )
        planned.append(
            _PlannedRun(
                origin=origin_ts,
                frame=group.reset_index(drop=True),
                identity=identity,
                hash=digest,
                supersedes=existing,
            )
        )
    return planned, skipped


def commit_runs(
    planned: list[_PlannedRun],
    manifest: RunManifest,
    write_segment: Any,
    now: pd.Timestamp,
) -> IngestResult:
    """Write planned segments invisibly, then commit visibility in one
    atomic manifest save."""
    new_records: list[RunRecord] = []
    n_rows = 0
    for plan in planned:
        run_id = uuid.uuid4().hex
        frame = plan.frame.copy()
        frame["run_id"] = run_id
        frame["ingested_at"] = now
        segment = write_segment(frame)
        n_rows += len(frame)
        new_records.append(
            RunRecord(
                run_id=run_id,
                model_id=plan.identity[0],
                model_version=plan.identity[1],
                origin=plan.identity[2],
                series_key=plan.identity[3],
                content_hash=plan.hash,
                segment=segment,
                ingested_at=now.isoformat(),
            )
        )

    superseded = 0
    for plan in planned:
        if plan.supersedes is not None:
            plan.supersedes.superseded = True
            superseded += 1
    manifest.runs.extend(new_records)
    manifest.save()

    logger.info(
        "ingest committed: %d run(s), %d row(s), %d superseded",
        len(new_records),
        n_rows,
        superseded,
    )
    return IngestResult(
        n_rows=n_rows,
        n_runs_written=len(new_records),
        n_runs_skipped=0,
        n_runs_superseded=superseded,
    )
