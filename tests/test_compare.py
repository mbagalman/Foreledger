"""Comparison: equivalence to single-model calls, champion deltas."""

from __future__ import annotations

import pandas as pd
import pytest
from tests.conftest import HORIZONS, MODELS

from foreledger import ForecastArchive

ALL_MODELS = list(MODELS)


def test_comparison_equals_single_model_calls(populated: ForecastArchive) -> None:
    frame = populated.compare_models(2, ALL_MODELS, metric="MAE")
    assert len(frame) == len(ALL_MODELS)
    for _, row in frame.iterrows():
        single = populated.accuracy_at_horizon(
            2, metric="MAE", model_id=row["model_id"], model_version=row["model_version"]
        )
        assert row["value"] == single.value
        assert row["n"] == single.n
        assert row["status"] == single.status


def test_persisted_champion_yields_deltas(populated: ForecastArchive) -> None:
    populated.set_champion("alpha", "v1")
    frame = populated.compare_models(1, [("alpha", "v1"), ("alpha", "v2"), ("beta", "v1")])

    champion_row = frame[(frame["model_id"] == "alpha") & (frame["model_version"] == "v1")].iloc[0]
    challenger = frame[(frame["model_id"] == "alpha") & (frame["model_version"] == "v2")].iloc[0]
    beta = frame[frame["model_id"] == "beta"].iloc[0]

    assert bool(champion_row["is_champion"])
    assert champion_row["delta_vs_champion"] == 0.0

    v1 = populated.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    v2 = populated.accuracy_at_horizon(1, model_id="alpha", model_version="v2")
    assert v1.value is not None and v2.value is not None
    assert challenger["delta_vs_champion"] == pytest.approx(v2.value - v1.value)
    # v2 (smaller bias) beats the champion: negative delta on an error metric
    assert challenger["delta_vs_champion"] < 0

    # beta has no champion: no delta reported
    assert pd.isna(beta["champion_version"])
    assert pd.isna(beta["delta_vs_champion"])


def test_champion_argument_overrides_persisted(populated: ForecastArchive) -> None:
    populated.set_champion("alpha", "v1")
    frame = populated.compare_models(
        1, [("alpha", "v1"), ("alpha", "v2")], champion=("alpha", "v2")
    )
    v2_row = frame[frame["model_version"] == "v2"].iloc[0]
    assert bool(v2_row["is_champion"])
    assert v2_row["delta_vs_champion"] == 0.0
    # the persisted champion is untouched
    assert populated.champions() == {"alpha": "v1"}


def test_champion_last_write_wins(populated: ForecastArchive) -> None:
    populated.set_champion("alpha", "v1")
    populated.set_champion("alpha", "v2")
    assert populated.champions() == {"alpha": "v2"}


def test_compare_curve_equals_scoped_curves(populated: ForecastArchive) -> None:
    frame = populated.compare_curve(ALL_MODELS, metric="MAE")
    assert len(frame) == len(ALL_MODELS) * len(HORIZONS)
    for model_id, model_version in ALL_MODELS:
        curve = populated.accuracy_curve(
            metric="MAE", model_id=model_id, model_version=model_version
        )
        subset = frame[
            (frame["model_id"] == model_id) & (frame["model_version"] == model_version)
        ].sort_values("horizon")
        assert list(subset["value"]) == [p.value for p in curve]


def test_comparison_over_common_scope(populated: ForecastArchive) -> None:
    frame = populated.compare_models(1, ALL_MODELS, series="S1")
    for _, row in frame.iterrows():
        single = populated.accuracy_at_horizon(
            1, model_id=row["model_id"], model_version=row["model_version"], series="S1"
        )
        assert row["value"] == single.value


def test_compare_curve_default_horizons_scope_to_listed_models(
    populated: ForecastArchive,
) -> None:
    """An unrelated model's extra horizons must not inject all-insufficient
    rows into a comparison between other models."""
    from tests.conftest import ORIGINS

    far = pd.DataFrame(
        {
            "series_id": ["S1"],
            "target": [ORIGINS[0] + pd.Timedelta(days=9)],
            "value": [100.0],
        }
    )
    populated.ingest(far, model_id="gamma", model_version="v1", origin=ORIGINS[0])

    frame = populated.compare_curve(ALL_MODELS, metric="MAE")
    assert set(frame["horizon"]) == set(HORIZONS)  # no horizon-9 rows


def test_malformed_comparison_inputs_raise_typed_errors(populated: ForecastArchive) -> None:
    from foreledger import ValidationError

    with pytest.raises(ValidationError):
        populated.compare_models(1, [("a", "b", "c")])  # not a pair
    with pytest.raises(ValidationError):
        populated.compare_models(1, ["alpha"])  # bare string entry
    with pytest.raises(ValidationError):
        populated.compare_models(1, [])
    # a 2-element LIST as champion previously built a garbage map from the
    # strings' characters — silently wrong champions
    with pytest.raises(ValidationError):
        populated.compare_models(1, [("alpha", "v1")], champion=["alpha", "v1"])
    # a length-2 bytes entry must not be accepted as a garbage ('v', '1') pair
    with pytest.raises(ValidationError):
        populated.compare_models(1, [b"v1"])


def test_compare_models_accepts_numpy_rows(populated: ForecastArchive) -> None:
    """Self-review regression: numpy (model_id, model_version) rows — e.g.
    ``df[[...]].to_numpy()`` — used to unpack fine and must keep working; the
    tightened validator must not reject them."""
    import numpy as np

    frame = populated.compare_models(1, np.array([["alpha", "v1"], ["beta", "v1"]]))
    assert set(frame["model_id"]) == {"alpha", "beta"}


def test_compare_curve_empty_scope_keeps_schema(populated: ForecastArchive) -> None:
    frame = populated.compare_curve([("nobody", "v0")], metric="MAE")
    assert frame.empty
    assert "model_id" in frame.columns and "delta_vs_champion" in frame.columns
