"""Canonical schema for the raw archive, actuals log, and accuracy summary.

The raw on-disk schema and the archive format version are a one-way door
(ADR-001/ADR-006): any change is migration-class and must surface an explicit
format-version bump.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import numpy as np
import pandas as pd

from .errors import ValidationError

#: Archive on-disk format version. History:
#: 1 — initial layout: directory-scan visibility for actuals/officials.
#: 2 — actuals/officials visibility is committed through
#:     ``actuals_manifest.json`` (transactional registration).
#: 3 — ``runs.json`` and ``segment_integrity.json`` are mandatory: forecast
#:     reads scan only manifest-committed segments, every referenced segment
#:     (including superseded history) carries a recorded fingerprint, and a
#:     missing metadata file is corruption rather than emptiness.
#: Older stores migrate automatically on open (1→2→3); older readers refuse
#: newer stores rather than misreading them.
FORMAT_VERSION = 3

DEFAULT_SOURCE = "default"
#: Reserved series label for pooled (all-series) summary cells. Rejected as
#: a series_id at intake and in query scopes so the pooled cells can never
#: collide with — or silently substitute for — a real series.
ALL_SERIES = "*"
ALL_PERIOD = "all"

#: Raw forecast archive — append-only fact table.
#: Grain: one row per (model_id, model_version, series_id, origin, target).
FORECAST_COLUMNS = [
    "model_id",
    "model_version",
    "series_id",
    "origin",
    "target",
    "value",
    "horizon",
    "run_id",
    "ingested_at",
]

#: Actuals — model-independent append-only revisable log (ADR-007).
#: Grain: one row per (series_id, target, source, actual_recorded_at).
ACTUAL_COLUMNS = [
    "series_id",
    "target",
    "source",
    "actual_value",
    "actual_recorded_at",
    "is_official",
]

#: Derived accuracy summary — disposable cache (ADR-003).
#: Grain: one row per (model_id, model_version, series_id, horizon, metric,
#: period, actual_basis).
SUMMARY_COLUMNS = [
    "model_id",
    "model_version",
    "series_id",
    "horizon",
    "metric",
    "period",
    "actual_basis",
    "value",
    "n",
    "n_forecasts",
]

#: Dtype contract for the summary columns, kept beside ``SUMMARY_COLUMNS`` so
#: a column can never be added to one without the other. The backend re-imposes
#: it on read, so a stored generation always round-trips to the same types a
#: fresh recomputation produces — ``reconcile`` compares the two with
#: :meth:`pandas.DataFrame.equals`, which is dtype-sensitive.
SUMMARY_DTYPES: dict[str, str] = {
    "model_id": "object",
    "model_version": "object",
    "series_id": "object",
    "horizon": "int64",
    "metric": "object",
    "period": "object",
    "actual_basis": "object",
    "value": "float64",
    "n": "int64",
    "n_forecasts": "int64",
}


def to_timestamp(value: Any, field: str) -> pd.Timestamp:
    """Coerce a user-supplied origin/target to a pandas Timestamp.

    Bare numbers are rejected: pandas would silently read them as epoch
    nanoseconds, so ``as_of(20260601)`` would become a 1970 date instead of
    the obviously intended calendar day.
    """
    if isinstance(value, (bool, int, float, np.integer, np.floating)):
        raise ValidationError(
            f"{field} value {value!r} is a bare number; pass a datetime, date, or "
            "ISO string (a number would be read as epoch nanoseconds)"
        )
    try:
        ts = pd.Timestamp(value)
    except (ValueError, TypeError) as exc:
        raise ValidationError(f"{field} value {value!r} is not datetime-like") from exc
    if pd.isna(ts):
        raise ValidationError(f"{field} value is missing (NaT)")
    if ts.tz is not None:
        # match column normalization (normalize_datetimes): the archive is
        # tz-naive, so a tz-aware scalar drops its zone keeping local wall
        # time — otherwise it would crash naive-vs-aware arithmetic later
        ts = ts.tz_localize(None)
    return ts


def compute_horizon(origin: pd.Series, target: pd.Series) -> pd.Series:
    """Derive integer horizons (whole days) from origin/target timestamps.

    v1 supports daily-resolution cadences: ``target - origin`` must be a whole
    number of days for every row (weekly data yields horizons 7, 14, ...).
    """
    delta = pd.to_datetime(target) - pd.to_datetime(origin)
    days = delta / timedelta(days=1)
    if days.isna().any():
        raise ValidationError("origin/target contain missing values")
    if (days % 1 != 0).any():
        raise ValidationError(
            "target - origin must be a whole number of days for every row "
            "(sub-daily horizons are out of scope for v1)"
        )
    return days.astype("int64")


def empty_forecasts() -> pd.DataFrame:
    """An empty frame with the canonical forecast schema."""
    return pd.DataFrame(
        {
            "model_id": pd.Series(dtype="object"),
            "model_version": pd.Series(dtype="object"),
            "series_id": pd.Series(dtype="object"),
            "origin": pd.Series(dtype="datetime64[ns]"),
            "target": pd.Series(dtype="datetime64[ns]"),
            "value": pd.Series(dtype="float64"),
            "horizon": pd.Series(dtype="int64"),
            "run_id": pd.Series(dtype="object"),
            "ingested_at": pd.Series(dtype="datetime64[ns]"),
        }
    )


def empty_actuals() -> pd.DataFrame:
    """An empty frame with the canonical actuals schema."""
    return pd.DataFrame(
        {
            "series_id": pd.Series(dtype="object"),
            "target": pd.Series(dtype="datetime64[ns]"),
            "source": pd.Series(dtype="object"),
            "actual_value": pd.Series(dtype="float64"),
            "actual_recorded_at": pd.Series(dtype="datetime64[ns]"),
            "is_official": pd.Series(dtype="bool"),
        }
    )


def empty_summary() -> pd.DataFrame:
    """An empty frame with the canonical summary schema."""
    return pd.DataFrame(
        {column: pd.Series(dtype=SUMMARY_DTYPES[column]) for column in SUMMARY_COLUMNS}
    )
