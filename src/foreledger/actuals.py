"""Actuals intake: a model-independent, append-only revisable log (ADR-007).

Identity is ``(series_id, target, source, actual_recorded_at)``. Re-registering
appends a revision; the effective ``latest`` value per ``(series_id, target)``
is the row with the max ``actual_recorded_at``.

Same-timestamp tiebreak: equal-valued duplicates collapse; differing values
resolve by configured ``source_priority``; if unresolved, the conflict is
written to the error-log file and the target is marked ambiguous for the
latest basis — never a silent pick.

An actual can be marked official — at most one per ``(series_id, target)``,
sticky: a later non-official registration never changes or unsets it.
Designations live in their own append-only log so the actuals rows themselves
are never rewritten; the explicit ``mark_official`` path is the only way to
change a designation.

Visibility is transactional: segment files are written invisibly and become
readable only when the :class:`ActualsManifest` commits, so an actual and its
official designation appear together or not at all — a failed call can never
leave a half-finished pair that later changes results.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import pandas as pd

from .errors import OfficialConflictError, ValidationError
from .ingestion import (
    _resolve_mapping,
    normalize_datetimes,
    validate_finite_values,
    validate_series_ids,
)
from .jsonstore import atomic_write_json
from .schema import DEFAULT_SOURCE, to_timestamp

logger = logging.getLogger("foreledger.actuals")

_ACTUAL_INPUT = ("series_id", "target", "value")


def validate_source_label(source: Any) -> None:
    """Reject a non-string or blank ``source``.

    ``source`` is part of the durable actual identity: a non-string value
    would be persisted with a different Parquet dtype and poison every later
    identity merge against the log.
    """
    if source is None:
        return
    if not isinstance(source, str) or not source.strip():
        raise ValidationError(f"source must be a non-empty string feed label, got {source!r}")


@dataclass
class ActualsManifest:
    """The single source of actuals/officials visibility.

    Segment files are written invisibly; a registration's actual rows and
    official designations become readable together when (and only when) this
    manifest commits atomically. Files on disk but not listed here are the
    invisible leftovers of failed calls — harmless, and retried cleanly.
    """

    path: Path
    actuals: list[str] = field(default_factory=list)
    officials: list[str] = field(default_factory=list)

    #: A committed segment token is a canonical relative path: one known
    #: directory, one flat filename, the expected suffix. Anything else —
    #: absolute paths, traversal, nesting — is treated as store corruption,
    #: never resolved: a tampered manifest must not be able to read files
    #: outside the archive.
    _TOKEN_PATTERNS = {
        "actuals": re.compile(r"^actuals/[A-Za-z0-9._-]+\.parquet$"),
        "officials": re.compile(r"^officials/[A-Za-z0-9._-]+\.parquet$"),
    }

    @classmethod
    def _validated_tokens(cls, payload: Any, kind: str, path: Path) -> list[str]:
        from .errors import StoreFormatError

        tokens = payload.get(kind) if isinstance(payload, dict) else None
        if not isinstance(tokens, list):
            raise StoreFormatError(
                f"actuals manifest at {path} is malformed: {kind!r} is not a list"
            )
        pattern = cls._TOKEN_PATTERNS[kind]
        seen: set[str] = set()
        for token in tokens:
            if not isinstance(token, str) or not pattern.match(token):
                raise StoreFormatError(
                    f"actuals manifest at {path} holds an invalid {kind} segment "
                    f"token {token!r}; the manifest is corrupt or was tampered with"
                )
            if token in seen:
                raise StoreFormatError(f"actuals manifest at {path} lists segment {token!r} twice")
            seen.add(token)
        return list(tokens)

    @classmethod
    def load(cls, path: Path) -> ActualsManifest:
        """Load the manifest; a missing file is corruption, never emptiness.

        The manifest is mandatory at format 2 — treating its absence as "no
        actuals" would make a deleted manifest silently erase committed data
        from a live handle's view. New empty manifests are constructed
        directly, never via this loader.
        """
        from .errors import StoreFormatError

        if not path.exists():
            raise StoreFormatError(
                f"actuals visibility manifest is missing from {path.parent}; the "
                "archive is corrupt or was modified externally"
            )
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise StoreFormatError(f"actuals manifest at {path} is unreadable or corrupt") from exc
        return cls(
            path=path,
            actuals=cls._validated_tokens(payload, "actuals", path),
            officials=cls._validated_tokens(payload, "officials", path),
        )

    def payload(self) -> dict[str, Any]:
        """The exact JSON payload :meth:`save` writes — exposed so a commit
        can journal the candidate manifest's content digest before saving."""
        return {"actuals": self.actuals, "officials": self.officials}

    def save(self) -> None:
        atomic_write_json(self.path, self.payload())

    def extended(self, actuals: str | None, officials: str | None) -> ActualsManifest:
        """A new manifest with the given segment names appended (not saved)."""
        return ActualsManifest(
            path=self.path,
            actuals=[*self.actuals, *([actuals] if actuals else [])],
            officials=[*self.officials, *([officials] if officials else [])],
        )


def canonicalize_actuals(
    frame: pd.DataFrame,
    mapping: Mapping[str, str] | None,
    source: str | None,
    recorded_at: Any | None,
    official: bool,
) -> pd.DataFrame:
    """Map a user frame onto the canonical actuals columns."""
    if frame.empty:
        raise ValidationError("cannot register an empty actuals frame")
    validate_source_label(source)
    resolved = _resolve_mapping(frame, mapping, _ACTUAL_INPUT)
    missing = [f for f in _ACTUAL_INPUT if f not in resolved]
    if missing:
        raise ValidationError(
            f"actuals frame is missing columns for {missing}; supply them via "
            f"mapping= (have: {list(frame.columns)})"
        )
    recorded_ts = (
        pd.Timestamp.now() if recorded_at is None else to_timestamp(recorded_at, "recorded_at")
    )
    canonical = pd.DataFrame(
        {
            "series_id": validate_series_ids(frame[resolved["series_id"]], "actuals"),
            "target": normalize_datetimes(frame[resolved["target"]], "target"),
            "actual_value": validate_finite_values(frame[resolved["value"]], "actuals"),
        }
    )
    canonical["source"] = source if source is not None else DEFAULT_SOURCE
    canonical["actual_recorded_at"] = recorded_ts
    canonical["is_official"] = bool(official)

    conflicting = canonical.groupby(["series_id", "target"])["actual_value"].nunique() > 1
    if conflicting.any():
        raise ValidationError(
            f"actuals frame contains {int(conflicting.sum())} (series_id, target) "
            "pairs with differing values in the same batch"
        )
    # Equal-valued duplicates within the batch collapse silently.
    canonical = canonical.drop_duplicates(subset=["series_id", "target"]).reset_index(drop=True)
    return canonical.sort_values(["series_id", "target"]).reset_index(drop=True)


_IDENTITY = ["series_id", "target", "source", "actual_recorded_at"]


def dedup_against_log(batch: pd.DataFrame, existing: pd.DataFrame) -> pd.DataFrame:
    """Enforce the actual identity ``(series, target, source, recorded_at)``
    against the existing log before any append.

    Rows that exactly repeat an existing identity *and* value collapse (a
    replayed registration is a no-op). A different value at an existing
    identity is rejected loudly — appending it would create two truths for
    one identity, which downstream resolution could only pick between
    silently. Register a revision with a new ``recorded_at`` (or another
    ``source``) instead.
    """
    if existing.empty:
        return batch
    merged = batch.merge(
        existing[_IDENTITY + ["actual_value"]].drop_duplicates(_IDENTITY),
        on=_IDENTITY,
        how="left",
        suffixes=("", "_existing"),
    )
    seen = merged["actual_value_existing"].notna()
    conflicting = seen & (merged["actual_value"] != merged["actual_value_existing"])
    if conflicting.any():
        raise ValidationError(
            f"{int(conflicting.sum())} row(s) re-register an existing actual identity "
            "(series, target, source, recorded_at) with a different value; register a "
            "revision with a new recorded_at or source instead"
        )
    return batch[~seen.to_numpy()].reset_index(drop=True)


@dataclass(frozen=True)
class Conflict:
    """An unresolved same-timestamp actuals conflict (differing values)."""

    series_id: str
    target: pd.Timestamp
    recorded_at: pd.Timestamp
    sources: tuple[str, ...]
    values: tuple[float, ...]

    def key(self) -> str:
        return (
            f"{self.series_id}|{self.target.isoformat()}|{self.recorded_at.isoformat()}"
            f"|{','.join(self.sources)}|{','.join(repr(v) for v in self.values)}"
        )


@dataclass
class EffectiveActuals:
    """The resolved latest value per target, plus ambiguity bookkeeping."""

    latest: pd.DataFrame
    ambiguous: list[tuple[str, pd.Timestamp]] = field(default_factory=list)
    conflicts: list[Conflict] = field(default_factory=list)


def _empty_effective() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "series_id": pd.Series(dtype="object"),
            "target": pd.Series(dtype="datetime64[ns]"),
            "actual_value": pd.Series(dtype="float64"),
        }
    )


def resolve_effective_latest(
    actuals: pd.DataFrame, source_priority: list[str] | None
) -> EffectiveActuals:
    """Resolve the effective latest actual per (series_id, target).

    Ambiguous targets (unresolved same-timestamp conflicts) are excluded from
    the latest basis and reported, never silently picked.

    Vectorized for the overwhelmingly common case: groups whose newest rows
    agree on one value resolve in bulk; the per-group tiebreak loop runs only
    over groups with a genuine same-timestamp value conflict.
    """
    if actuals.empty:
        return EffectiveActuals(latest=_empty_effective())

    keys = ["series_id", "target"]
    newest = actuals[
        actuals["actual_recorded_at"]
        == actuals.groupby(keys)["actual_recorded_at"].transform("max")
    ]
    distinct_values = newest.groupby(keys)["actual_value"].transform("nunique")
    clean = (
        newest[distinct_values == 1]
        .drop_duplicates(subset=keys)[keys + ["actual_value"]]
        .astype({"actual_value": "float64"})
    )
    conflicted = newest[distinct_values > 1]
    if conflicted.empty:
        return EffectiveActuals(latest=clean.sort_values(keys).reset_index(drop=True))

    rows: list[dict[str, Any]] = []
    ambiguous: list[tuple[str, pd.Timestamp]] = []
    conflicts: list[Conflict] = []
    priority = list(source_priority or [])

    for (series_id, target), tied in conflicted.groupby(keys, sort=True):
        sources = list(tied["source"])
        distinct = len(set(sources)) == len(sources)
        if priority and distinct and all(src in priority for src in sources):
            best = min(sources, key=priority.index)
            chosen = tied[tied["source"] == best]["actual_value"].iloc[0]
            rows.append({"series_id": series_id, "target": target, "actual_value": float(chosen)})
            continue
        ambiguous.append((str(series_id), pd.Timestamp(cast("Any", target))))
        conflicts.append(
            Conflict(
                series_id=str(series_id),
                target=pd.Timestamp(cast("Any", target)),
                recorded_at=pd.Timestamp(tied["actual_recorded_at"].iloc[0]),
                sources=tuple(str(s) for s in tied["source"]),
                values=tuple(float(v) for v in tied["actual_value"]),
            )
        )

    resolved = pd.DataFrame(rows) if rows else _empty_effective()
    latest = (
        pd.concat([clean, resolved], ignore_index=True).sort_values(keys).reset_index(drop=True)
    )
    return EffectiveActuals(latest=latest, ambiguous=ambiguous, conflicts=conflicts)


def resolve_effective_official(actuals: pd.DataFrame, officials: pd.DataFrame) -> pd.DataFrame:
    """The official value per (series_id, target): the latest designation,
    dereferenced into the actuals log."""
    if actuals.empty or officials.empty:
        return _empty_effective()
    current = (
        officials.sort_values("designated_at").groupby(["series_id", "target"], sort=False).tail(1)
    )
    joined = current.merge(
        actuals,
        on=["series_id", "target", "source", "actual_recorded_at"],
        how="inner",
    )
    # Registration rejects differing values at one identity, so a designation
    # should dereference exactly one value. Defend old/foreign stores anyway:
    # a multi-valued dereference is excluded (insufficient downstream), never
    # silently picked.
    joined = joined.drop_duplicates(subset=["series_id", "target", "actual_value"])
    counts = joined.groupby(["series_id", "target"])["actual_value"].transform("size")
    conflicted = counts > 1
    if conflicted.any():
        logger.warning(
            "%d official designation(s) dereference conflicting values; "
            "excluded from the official basis",
            int(joined[conflicted].drop_duplicates(["series_id", "target"]).shape[0]),
        )
        joined = joined[~conflicted]
    return joined[["series_id", "target", "actual_value"]].reset_index(drop=True)


def current_designations(officials: pd.DataFrame) -> pd.DataFrame:
    """Latest designation row per (series_id, target)."""
    if officials.empty:
        return officials
    return (
        officials.sort_values("designated_at")
        .groupby(["series_id", "target"], sort=False)
        .tail(1)
        .reset_index(drop=True)
    )


def check_official_registration(batch: pd.DataFrame, officials: pd.DataFrame) -> pd.DataFrame:
    """Validate an ``official=True`` registration against existing designations.

    Returns the designation rows to append. Raises before any append if a
    target already has a *different* official row — stickiness means only the
    explicit ``mark_official`` path may change a designation. (Visibility is
    manifest-committed, so every visible designation dereferences its actual;
    inert orphans cannot exist.)
    """
    existing = current_designations(officials)
    to_append = batch[["series_id", "target", "source", "actual_recorded_at"]].copy()
    if not existing.empty:
        merged = to_append.merge(
            existing,
            on=["series_id", "target"],
            how="left",
            suffixes=("", "_existing"),
        )
        has_existing = merged["source_existing"].notna()
        same_row = (
            has_existing
            & (merged["source_existing"] == merged["source"])
            & (merged["actual_recorded_at_existing"] == merged["actual_recorded_at"])
        )
        conflicting = has_existing & ~same_row
        if conflicting.any():
            raise OfficialConflictError(
                f"{int(conflicting.sum())} target(s) already have a different "
                "official actual; the designation is sticky — use mark_official "
                "to change it explicitly"
            )
        to_append = merged.loc[
            ~has_existing, ["series_id", "target", "source", "actual_recorded_at"]
        ]
    return to_append.reset_index(drop=True)


def find_actual_row(
    actuals: pd.DataFrame,
    series: str,
    target: Any,
    source: str | None,
    recorded_at: Any | None,
) -> pd.Series:
    """Locate the single registered actual row a designation should point at."""
    target_ts = to_timestamp(target, "target")
    subset = actuals[(actuals["series_id"] == str(series)) & (actuals["target"] == target_ts)]
    if source is not None:
        subset = subset[subset["source"] == source]
    if recorded_at is not None:
        subset = subset[subset["actual_recorded_at"] == to_timestamp(recorded_at, "recorded_at")]
    if subset.empty:
        raise ValidationError(
            "no registered actual matches the given series/target/source/recorded_at; "
            "register the actual before marking it official"
        )
    newest = subset[subset["actual_recorded_at"] == subset["actual_recorded_at"].max()]
    if newest["actual_value"].nunique() > 1:
        raise ValidationError(
            "multiple differing actuals match; disambiguate with source= and/or recorded_at="
        )
    return cast("pd.Series", newest.iloc[0])
