"""Canonical schema for the raw archive, actuals log, and accuracy summary.

The raw on-disk schema and the archive format version are a one-way door
(ADR-001/ADR-006): any change is migration-class and must surface an explicit
format-version bump.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pandas as pd

from .errors import ValidationError

FORMAT_VERSION = 1

DEFAULT_SOURCE = "default"
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
]


def to_timestamp(value: Any, field: str) -> pd.Timestamp:
    """Coerce a user-supplied origin/target to a pandas Timestamp."""
    try:
        ts = pd.Timestamp(value)
    except (ValueError, TypeError) as exc:
        raise ValidationError(f"{field} value {value!r} is not datetime-like") from exc
    if pd.isna(ts):
        raise ValidationError(f"{field} value is missing (NaT)")
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
            "(sub-daily horizons are not supported in format version 1)"
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
        {
            "model_id": pd.Series(dtype="object"),
            "model_version": pd.Series(dtype="object"),
            "series_id": pd.Series(dtype="object"),
            "horizon": pd.Series(dtype="int64"),
            "metric": pd.Series(dtype="object"),
            "period": pd.Series(dtype="object"),
            "actual_basis": pd.Series(dtype="object"),
            "value": pd.Series(dtype="float64"),
            "n": pd.Series(dtype="int64"),
        }
    )
