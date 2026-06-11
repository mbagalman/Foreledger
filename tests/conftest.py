"""Shared synthetic fixture: a small multi-model, multi-version archive.

Deterministic by construction (no RNG) so reference computations in tests are
exact.
"""

from __future__ import annotations

import math
from pathlib import Path

import pandas as pd
import pytest

from foreledger import ForecastArchive

SERIES = ["S1", "S2", "S3"]
ORIGINS = pd.date_range("2026-01-01", periods=5, freq="D")
HORIZONS = [1, 2, 3]

MODELS = {
    ("alpha", "v1"): 1.0,
    ("alpha", "v2"): 0.5,
    ("beta", "v1"): 2.0,
}


def true_value(series: str, target: pd.Timestamp) -> float:
    return 50.0 + 10.0 * SERIES.index(series) + 3.0 * math.sin(target.dayofyear / 5.0)


def forecast_value(bias: float, series: str, origin: pd.Timestamp, target: pd.Timestamp) -> float:
    horizon = (target - origin).days
    return true_value(series, target) + bias * horizon + 0.1 * (target.day % 3)


def forecast_frame(
    bias: float,
    origins: pd.DatetimeIndex = ORIGINS,
    series: list[str] | None = None,
    horizons: list[int] | None = None,
) -> pd.DataFrame:
    series = series if series is not None else SERIES
    horizons = horizons if horizons is not None else HORIZONS
    rows = []
    for origin in origins:
        for sid in series:
            for h in horizons:
                target = origin + pd.Timedelta(days=h)
                rows.append(
                    {
                        "series_id": sid,
                        "origin": origin,
                        "target": target,
                        "value": forecast_value(bias, sid, origin, target),
                    }
                )
    return pd.DataFrame(rows)


def actuals_frame(
    origins: pd.DatetimeIndex = ORIGINS,
    series: list[str] | None = None,
    horizons: list[int] | None = None,
) -> pd.DataFrame:
    series = series if series is not None else SERIES
    horizons = horizons if horizons is not None else HORIZONS
    targets = sorted({o + pd.Timedelta(days=h) for o in origins for h in horizons})
    rows = [
        {"series_id": sid, "target": t, "value": true_value(sid, t)}
        for sid in series
        for t in targets
    ]
    return pd.DataFrame(rows)


@pytest.fixture
def store(tmp_path: Path) -> Path:
    return tmp_path / "store"


@pytest.fixture
def archive(store: Path) -> ForecastArchive:
    return ForecastArchive(store)


@pytest.fixture
def populated(archive: ForecastArchive) -> ForecastArchive:
    for (model_id, model_version), bias in MODELS.items():
        archive.ingest(forecast_frame(bias), model_id=model_id, model_version=model_version)
    archive.register_actuals(actuals_frame(), source="feed")
    return archive


def reference_mae(bias: float, horizon: int) -> float:
    """Hand-written reference: MAE at a horizon for one model over the fixture."""
    errors = []
    for origin in ORIGINS:
        for sid in SERIES:
            target = origin + pd.Timedelta(days=horizon)
            errors.append(abs(forecast_value(bias, sid, origin, target) - true_value(sid, target)))
    return sum(errors) / len(errors)
