"""Forecast ingestion: push with caller-supplied identity (ADR-005/ADR-006).

Run identity is ``(model_id, model_version, origin, series_id)`` and is never
inferred from content. Identity at series granularity (rather than per
series-*set*) is what guarantees grain uniqueness under partial replays:
re-ingesting a subset of an earlier batch matches the existing per-series
records instead of registering a second, overlapping run.

Appends are all-or-nothing: a segment is written invisibly first and becomes
visible only when the run manifest commits, so a crash mid-ingest leaves the
archive at its pre-run state.

This module owns the run-identity bookkeeping (the manifest) that makes
re-ingestion idempotent and conflicts explicit.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import uuid
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from .errors import ValidationError
from .jsonstore import atomic_write_json
from .schema import ALL_SERIES, compute_horizon, to_timestamp

logger = logging.getLogger("foreledger.ingestion")

_CANONICAL_INPUT = ("series_id", "target", "value")


@dataclass
class RunRecord:
    """One committed (model, version, origin, series) run in the manifest."""

    run_id: str
    model_id: str
    model_version: str
    origin: str
    series_id: str
    content_hash: str
    segment: str
    ingested_at: str
    superseded: bool = False

    @property
    def identity(self) -> tuple[str, str, str, str]:
        return (self.model_id, self.model_version, self.origin, self.series_id)


_FORECAST_SEGMENT_PATTERN = re.compile(r"^forecasts/[A-Za-z0-9._-]+\.parquet$")


def validate_forecast_segment_token(token: str, manifest_path: Path) -> None:
    """Reject any forecast segment token that is not a canonical relative
    path — absolute paths and traversal are corruption, never resolved."""
    from .errors import StoreFormatError

    if not _FORECAST_SEGMENT_PATTERN.match(token):
        raise StoreFormatError(
            f"run manifest at {manifest_path} holds an invalid segment token "
            f"{token!r}; the manifest is corrupt or was tampered with"
        )


def load_manifest_entries(path: Path) -> list[dict[str, Any]]:
    """Raw manifest entries, with corruption surfaced as a typed error.

    The run manifest is mandatory: a missing file is corruption, never an
    empty archive — treating absence as emptiness would let a deleted
    ``runs.json`` silently hide every committed forecast. New empty manifests
    are constructed and saved directly at store initialization.
    """
    from .errors import StoreFormatError

    if not path.exists():
        raise StoreFormatError(
            f"run manifest is missing from {path.parent}; the archive is corrupt "
            "or was modified externally"
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        entries = payload.get("runs", [])
        if not isinstance(entries, list):
            raise TypeError("runs is not a list")
    except (json.JSONDecodeError, TypeError, AttributeError) as exc:
        raise StoreFormatError(f"run manifest at {path} is unreadable or corrupt") from exc
    return entries


@dataclass
class RunManifest:
    """Append-style run bookkeeping; the single source of forecast visibility."""

    path: Path
    runs: list[RunRecord] = field(default_factory=list)

    @classmethod
    def from_entries(cls, path: Path, entries: list[dict[str, Any]]) -> RunManifest:
        from .errors import StoreFormatError

        try:
            runs = [RunRecord(**entry) for entry in entries]
        except TypeError as exc:
            raise StoreFormatError(
                f"run manifest at {path} has an unrecognized record shape; it may "
                "have been written by an incompatible foreledger version"
            ) from exc
        for run in runs:
            # canonical relative tokens only: a tampered manifest must not be
            # able to point forecast reads at files outside the archive
            validate_forecast_segment_token(run.segment, path)
        return cls(path=path, runs=runs)

    @classmethod
    def load(cls, path: Path) -> RunManifest:
        return cls.from_entries(path, load_manifest_entries(path))

    def payload(self) -> dict[str, Any]:
        """The exact JSON payload :meth:`save` writes — exposed so a commit
        can journal the candidate manifest's content digest before saving."""
        return {"runs": [asdict(run) for run in self.runs]}

    def save(self) -> None:
        atomic_write_json(self.path, self.payload())

    def active_run_ids(self) -> list[str]:
        return [run.run_id for run in self.runs if not run.superseded]

    def find_active(self, identity: tuple[str, str, str, str]) -> RunRecord | None:
        for run in self.runs:
            if not run.superseded and run.identity == identity:
                return run
        return None


def normalize_datetimes(series: pd.Series, field_name: str) -> pd.Series:
    """Coerce a column to naive pandas datetimes.

    Numeric columns are rejected: pandas would silently read them as epoch
    timestamps, turning a ``20260601``-style date column into 1970 dates.
    """
    if pd.api.types.is_numeric_dtype(series):
        raise ValidationError(
            f"column for {field_name!r} is numeric; datetimes are required "
            "(numbers would be read as epoch timestamps)"
        )
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


def validate_series_ids(raw: pd.Series, frame_kind: str) -> pd.Series:
    """Coerce series identifiers to non-empty strings; reject missing/blank.

    Untrusted input: without this check ``astype(str)`` would mint phantom
    series named ``"nan"``/``"None"`` into the permanent archive. The label
    ``"*"`` is reserved for the summary's pooled all-series cells — a real
    series named ``"*"`` would collide with them and make queries ambiguous.
    """
    if raw.isna().any():
        raise ValidationError(f"{frame_kind} series_id column contains missing values")
    as_str = raw.astype(str).str.strip()
    if (as_str == "").any():
        raise ValidationError(f"{frame_kind} series_id column contains blank identifiers")
    if (as_str == ALL_SERIES).any():
        raise ValidationError(
            f"{frame_kind} series_id {ALL_SERIES!r} is reserved for pooled summary cells"
        )
    return as_str


def validate_finite_values(raw: pd.Series, frame_kind: str) -> pd.Series:
    """Coerce values to float64 and reject NaN/±inf — non-finite numbers in
    the raw archive would poison every downstream metric."""
    numeric = pd.to_numeric(raw, errors="raise").astype("float64")
    if not np.isfinite(numeric.to_numpy()).all():
        raise ValidationError(
            f"{frame_kind} value column contains missing or non-finite values (NaN/inf)"
        )
    return numeric


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
            "series_id": validate_series_ids(frame[resolved["series_id"]], "forecast"),
            "target": normalize_datetimes(frame[resolved["target"]], "target"),
            "value": validate_finite_values(frame[resolved["value"]], "forecast"),
        }
    )
    if origin is None:
        canonical["origin"] = normalize_datetimes(frame[resolved["origin"]], "origin")
    else:
        canonical["origin"] = to_timestamp(origin, "origin")

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


def content_hash(group: pd.DataFrame) -> str:
    """Stable digest over a run's rows.

    The payload format is persisted (hashes live in ``runs.json`` and drive
    idempotency), so it must stay byte-identical across versions — a pinning
    test guards it. Built as one joined payload rather than per-row digest
    updates: same bytes, far fewer Python-level calls.
    """
    ordered = group.sort_values(["series_id", "target"])
    payload = "".join(
        f"{row_series}\x1f{row_target.isoformat()}\x1f{row_value!r}\n"
        for row_series, row_target, row_value in zip(
            ordered["series_id"], ordered["target"], ordered["value"], strict=True
        )
    )
    return hashlib.sha256(payload.encode()).hexdigest()


@dataclass(frozen=True)
class IngestResult:
    """Outcome metadata for one ingest call."""

    n_rows: int
    n_runs_written: int
    n_runs_skipped: int
    n_runs_superseded: int


@dataclass(frozen=True)
class _PlannedRun:
    frame: pd.DataFrame
    identity: tuple[str, str, str, str]
    hash: str
    supersedes: RunRecord | None


def plan_runs(
    canonical: pd.DataFrame,
    manifest: RunManifest,
    on_conflict: str,
) -> tuple[list[_PlannedRun], int]:
    """Validate every (origin, series) run against the manifest before
    anything is written.

    Returns the runs to write and the count skipped as idempotent replays.
    Raises on a same-identity/different-values conflict under
    ``on_conflict="error"`` — before any side effect, keeping the whole call
    all-or-nothing. Because identity is per series, partial replays (subset,
    superset, or overlapping series for the same origin) match the existing
    per-series records and can never produce duplicate grain rows.
    """
    from .errors import IngestConflictError

    if on_conflict not in ("error", "overwrite"):
        raise ValidationError("on_conflict must be 'error' or 'overwrite'")

    planned: list[_PlannedRun] = []
    skipped = 0
    for (origin_value, series_id), group in canonical.groupby(["origin", "series_id"], sort=True):
        origin_ts = pd.Timestamp(cast("Any", origin_value))
        identity = (
            str(group["model_id"].iloc[0]),
            str(group["model_version"].iloc[0]),
            origin_ts.isoformat(),
            str(series_id),
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
    stage_integrity: Any = None,
) -> tuple[IngestResult, RunManifest]:
    """Write one invisible segment for the whole call, then commit visibility
    in one atomic manifest save.

    The segment file carries one ``run_id`` per planned (origin, series) run;
    visibility is row-level via the manifest's active run_ids, so superseding
    one series later never hides its neighbours in the same file.

    ``stage_integrity(tokens, candidate_payload)`` is called after the
    segment write and before the manifest save, so the integrity journal
    holds the staged fingerprints and the candidate manifest's content digest
    before anything becomes visible; the caller confirms the commit after
    this function returns.

    ``manifest`` is never mutated: a candidate manifest is built and saved,
    and returned only after the save succeeds. If anything raises, the caller
    keeps its committed view — no uncommitted rows ever become visible on the
    live object. A failure before the manifest save leaves at most an
    invisible segment file (never scanned — reads use only manifest-listed
    segments) and a staged journal entry the next heal prunes; the file
    itself is retained because storage tokens are backend-defined and
    failed-write debris is rare and inert.
    """
    new_records: list[RunRecord] = []
    tagged_frames: list[pd.DataFrame] = []
    for plan in planned:
        run_id = uuid.uuid4().hex
        frame = plan.frame.copy()
        frame["run_id"] = run_id
        frame["ingested_at"] = now
        tagged_frames.append(frame)
        new_records.append(
            RunRecord(
                run_id=run_id,
                model_id=plan.identity[0],
                model_version=plan.identity[1],
                origin=plan.identity[2],
                series_id=plan.identity[3],
                content_hash=plan.hash,
                segment="",  # filled in below, one shared segment per call
                ingested_at=now.isoformat(),
            )
        )

    segment = write_segment(pd.concat(tagged_frames, ignore_index=True))
    for record in new_records:
        record.segment = segment

    superseded_ids = {plan.supersedes.run_id for plan in planned if plan.supersedes is not None}
    candidate_runs = [
        replace(run, superseded=True) if run.run_id in superseded_ids else run
        for run in manifest.runs
    ]
    candidate = RunManifest(path=manifest.path, runs=[*candidate_runs, *new_records])
    if stage_integrity is not None:
        # journal before visibility: a committed segment always has its
        # fingerprint, and the manifest's expected digest is recorded before
        # the file changes
        stage_integrity([segment], candidate.payload())
    candidate.save()

    n_rows = sum(len(frame) for frame in tagged_frames)
    logger.info(
        "ingest committed: %d run(s), %d row(s), %d superseded",
        len(new_records),
        n_rows,
        len(superseded_ids),
    )
    result = IngestResult(
        n_rows=n_rows,
        n_runs_written=len(new_records),
        n_runs_skipped=0,
        n_runs_superseded=len(superseded_ids),
    )
    return result, candidate
