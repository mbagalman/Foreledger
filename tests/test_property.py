"""Property tests (hypothesis) for the pure resolution and metric layers."""

from __future__ import annotations

import numpy as np
import pandas as pd
from hypothesis import given, settings
from hypothesis import strategies as st

from forecast_archive.actuals import resolve_effective_latest
from forecast_archive.metrics import mae, mape, rmse

finite_floats = st.floats(min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False)


@given(st.lists(finite_floats, min_size=1, max_size=50))
def test_mae_rmse_identity_and_nonnegativity(values: list[float]) -> None:
    arr = np.asarray(values, dtype="float64")
    assert mae(arr, arr) == 0.0
    assert rmse(arr, arr) == 0.0
    shifted = arr + 1.0
    assert mae(shifted, arr) >= 0.0
    assert rmse(shifted, arr) >= mae(shifted, arr) - 1e-12  # RMSE dominates MAE


@given(st.lists(finite_floats.filter(lambda v: abs(v) > 1e-6), min_size=1, max_size=50))
def test_mape_zero_for_perfect_forecast(values: list[float]) -> None:
    arr = np.asarray(values, dtype="float64")
    assert mape(arr, arr) == 0.0


revision_strategy = st.lists(
    st.tuples(
        st.integers(min_value=0, max_value=2),  # series index
        st.integers(min_value=0, max_value=3),  # target index
        st.integers(min_value=0, max_value=5),  # recorded_at offset (days)
        finite_floats,  # value
    ),
    min_size=1,
    max_size=30,
)


@settings(max_examples=50, deadline=None)
@given(revision_strategy)
def test_latest_recorded_revision_always_wins(revisions: list[tuple[int, int, int, float]]) -> None:
    base = pd.Timestamp("2026-01-01")
    frame = pd.DataFrame(
        {
            "series_id": [f"S{s}" for s, _, _, _ in revisions],
            "target": [base + pd.Timedelta(days=t) for _, t, _, _ in revisions],
            "source": "feed",
            "actual_value": [v for _, _, _, v in revisions],
            "actual_recorded_at": [base + pd.Timedelta(days=30 + r) for _, _, r, _ in revisions],
            "is_official": False,
        }
    )
    resolved = resolve_effective_latest(frame, source_priority=None)

    for (series_id, target), group in frame.groupby(["series_id", "target"]):
        newest = group[group["actual_recorded_at"] == group["actual_recorded_at"].max()]
        expected_values = set(newest["actual_value"])
        row = resolved.latest[
            (resolved.latest["series_id"] == series_id) & (resolved.latest["target"] == target)
        ]
        if len(expected_values) == 1:
            # unambiguous: exactly the newest value is effective
            assert len(row) == 1
            assert row["actual_value"].iloc[0] in expected_values
        else:
            # same source, same timestamp, differing values: ambiguous, excluded
            assert row.empty
            assert (str(series_id), pd.Timestamp(target)) in [
                (a, pd.Timestamp(b)) for a, b in resolved.ambiguous
            ]
