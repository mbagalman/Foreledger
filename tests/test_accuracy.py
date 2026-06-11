"""Evaluation correctness: reference equality, routing, curves, drill, as_of."""

from __future__ import annotations

import numpy as np
import pytest
from tests.conftest import HORIZONS, MODELS, ORIGINS, SERIES, reference_mae

from forecast_archive import ForecastArchive


def test_accuracy_equals_handwritten_reference(populated: ForecastArchive) -> None:
    for (model_id, model_version), bias in MODELS.items():
        for h in HORIZONS:
            result = populated.accuracy_at_horizon(
                h, metric="MAE", model_id=model_id, model_version=model_version
            )
            assert result.status == "ok"
            assert result.value == pytest.approx(reference_mae(bias, h), rel=1e-12)
            assert result.n == len(ORIGINS) * len(SERIES)


def test_summary_route_equals_raw_route(populated: ForecastArchive) -> None:
    kwargs = {"metric": "RMSE", "model_id": "alpha", "model_version": "v1"}
    served_summary = populated.accuracy_at_horizon(2, **kwargs)
    assert served_summary.served_from == "summary"

    # delete the disposable summary: the raw fallback must be invisible and equal
    summary_file = populated.store / "summary" / "summary.parquet"
    summary_file.unlink()
    served_raw = populated.accuracy_at_horizon(2, **kwargs)
    assert served_raw.served_from == "raw"
    assert served_raw.value == served_summary.value
    assert served_raw.n == served_summary.n

    # and the summary is rebuildable from raw on demand
    populated.rebuild_summary()
    populated.reconcile()
    assert populated.accuracy_at_horizon(2, **kwargs).served_from == "summary"


def test_per_series_scope_and_both_routes(populated: ForecastArchive) -> None:
    summary_served = populated.accuracy_at_horizon(
        1, model_id="alpha", model_version="v2", series="S2"
    )
    assert summary_served.served_from == "summary"
    raw_served = populated.accuracy_at_horizon(
        1, model_id="alpha", model_version="v2", series=["S2"]
    )
    assert raw_served.served_from == "raw"  # list-scoped queries compute from raw
    assert raw_served.value == summary_served.value


def test_official_basis_summary_and_raw_agree(populated: ForecastArchive) -> None:
    from tests.conftest import actuals_frame

    official = actuals_frame().iloc[: len(SERIES) * 6]
    populated.register_actuals(official, source="official-feed", official=True)

    result = populated.accuracy_at_horizon(1, basis="official", model_id="beta", model_version="v1")
    summary_file = populated.store / "summary" / "summary.parquet"
    summary_file.unlink()
    raw = populated.accuracy_at_horizon(1, basis="official", model_id="beta", model_version="v1")
    assert raw.value == result.value
    assert raw.n == result.n


def test_curve_equals_per_horizon_calls(populated: ForecastArchive) -> None:
    curve = populated.accuracy_curve(metric="MAE", model_id="alpha", model_version="v1")
    assert [p.horizon for p in curve] == HORIZONS
    for point in curve:
        single = populated.accuracy_at_horizon(
            point.horizon, metric="MAE", model_id="alpha", model_version="v1"
        )
        assert point.value == single.value
        assert point.n == single.n
    frame = curve.to_frame()
    assert list(frame["horizon"]) == HORIZONS


def test_period_scoping(populated: ForecastArchive) -> None:
    scoped = populated.accuracy_at_horizon(
        1,
        model_id="alpha",
        model_version="v1",
        period=(ORIGINS[0], ORIGINS[1]),
    )
    assert scoped.served_from == "raw"
    assert scoped.n == 2 * len(SERIES)


def test_as_of_no_leakage(populated: ForecastArchive) -> None:
    cutoff = ORIGINS[2]
    known = populated.as_of(cutoff)
    assert not known.empty
    assert (known["origin"] <= cutoff).all()

    full = populated.as_of(ORIGINS[-1])
    later_runs = full[full["origin"] > cutoff]
    assert len(later_runs) > 0  # the fixture does contain later runs
    assert len(known) + len(later_runs) == len(full)


def test_as_of_scoping(populated: ForecastArchive) -> None:
    scoped = populated.as_of(ORIGINS[-1], model_id="beta", model_version="v1", series="S1")
    assert set(scoped["model_id"]) == {"beta"}
    assert set(scoped["series_id"]) == {"S1"}


def test_drill_reconciles_to_summary_cell(populated: ForecastArchive) -> None:
    cell = {
        "model_id": "alpha",
        "model_version": "v1",
        "horizon": 2,
        "metric": "MAE",
        "basis": "latest",
    }
    rows = populated.drill(cell)
    assert len(rows) == len(ORIGINS) * len(SERIES)
    drilled_mae = float(np.mean(np.abs(rows["value"] - rows["actual_value"])))
    summary_value = populated.accuracy_at_horizon(
        2, metric="MAE", model_id="alpha", model_version="v1"
    )
    assert drilled_mae == pytest.approx(summary_value.value, rel=1e-12)


def test_list_models_coverage(populated: ForecastArchive) -> None:
    listing = populated.list_models()
    assert len(listing) == len(MODELS)
    assert set(zip(listing["model_id"], listing["model_version"], strict=True)) == set(MODELS)
    row = listing[(listing["model_id"] == "alpha") & (listing["model_version"] == "v1")].iloc[0]
    assert row["n_rows"] == len(ORIGINS) * len(SERIES) * len(HORIZONS)
    assert row["n_series"] == len(SERIES)
    assert row["first_origin"] == ORIGINS[0]
    assert row["last_origin"] == ORIGINS[-1]


def test_builtin_metrics_all_compute(populated: ForecastArchive) -> None:
    for metric in ("MAE", "RMSE", "MAPE", "MASE"):
        result = populated.accuracy_at_horizon(
            1, metric=metric, model_id="alpha", model_version="v1", series="S1"
        )
        assert result.status == "ok", metric
        assert result.value is not None and result.value > 0
