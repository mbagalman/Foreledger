"""Evaluation correctness: reference equality, routing, curves, drill, as_of."""

from __future__ import annotations

import dataclasses
from pathlib import Path

import numpy as np
import pytest
from tests.conftest import HORIZONS, MODELS, ORIGINS, SERIES, reference_mae, summary_data_file

from foreledger import ForecastArchive


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
    summary_file = summary_data_file(populated)
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
    assert result.served_from == "summary"
    # partial official coverage must be visible, not read as complete
    assert result.status == "partial"
    assert result.n_missing_actuals > 0
    summary_file = summary_data_file(populated)
    summary_file.unlink()
    raw = populated.accuracy_at_horizon(1, basis="official", model_id="beta", model_version="v1")
    assert raw.served_from == "raw"
    assert dataclasses.replace(raw, served_from="summary") == result


def test_routes_report_identical_results_under_partial_coverage(
    populated: ForecastArchive,
) -> None:
    """The full result object — including missing-actuals metadata — must not
    depend on whether the summary or the raw path served it."""
    from tests.conftest import forecast_frame

    # horizon-5 forecasts: targets run past the registered actuals, so
    # coverage is genuinely partial (fresh version: a run's content is fixed)
    populated.ingest(forecast_frame(1.0, horizons=[1, 5]), model_id="alpha", model_version="v9")
    kwargs = {"metric": "MAE", "model_id": "alpha", "model_version": "v9"}
    summary_res = populated.accuracy_at_horizon(5, **kwargs)
    assert summary_res.served_from == "summary"
    assert summary_res.status == "partial"  # gaps are explicit, never an "ok"
    assert summary_res.n_missing_actuals > 0

    summary_data_file(populated).unlink()
    raw_res = populated.accuracy_at_horizon(5, **kwargs)
    assert raw_res.served_from == "raw"
    assert dataclasses.replace(raw_res, served_from="summary") == summary_res


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


def test_pooled_multi_model_mase_matches_per_model(store: Path) -> None:
    """Review reproduction: a scope pooling several models repeats each
    (series, target) actual once per model. The MASE denominator factorized
    only series_id, so it walked those duplicates (including zero diffs
    between identical actuals) and inflated pooled MASE — two identical
    models scored 3x their individual value. The naive trajectory must be
    partitioned per (model, version, series)."""
    from tests.conftest import actuals_frame, forecast_frame

    archive = ForecastArchive(store)
    frame = forecast_frame(1.0)
    archive.ingest(frame, model_id="twin-a", model_version="v1")
    archive.ingest(frame, model_id="twin-b", model_version="v1")
    archive.register_actuals(actuals_frame())

    scoped = archive.accuracy_at_horizon(1, metric="MASE", model_id="twin-a", model_version="v1")
    pooled = archive.accuracy_at_horizon(1, metric="MASE")  # all models, raw path
    assert scoped.status == "ok" and scoped.value is not None
    assert pooled.served_from == "raw"
    assert pooled.value == pytest.approx(scoped.value, rel=1e-12)


def test_query_inputs_are_validated(populated: ForecastArchive) -> None:
    """Self-review hardening: malformed scopes raise typed errors instead of
    silently truncating, diverging, or crashing in pandas."""
    from foreledger import ValidationError

    # '*' is reserved: the summary route would read it as "all series
    # pooled" while the raw route filtered a literal series named '*'
    with pytest.raises(ValidationError, match="reserved"):
        populated.accuracy_at_horizon(1, series="*", model_id="alpha", model_version="v1")
    with pytest.raises(ValidationError, match="reserved"):
        populated.accuracy_at_horizon(1, series=["S1", "*"])
    # horizons are whole days, never silently truncated
    with pytest.raises(ValidationError):
        populated.accuracy_at_horizon(7.5, model_id="alpha", model_version="v1")
    with pytest.raises(ValidationError):
        populated.accuracy_curve(horizons="12", model_id="alpha", model_version="v1")
    # scalar non-string series
    with pytest.raises(ValidationError):
        populated.accuracy_at_horizon(1, series=123)
    # bare numbers are not datetimes (would be read as epoch nanoseconds)
    with pytest.raises(ValidationError, match="bare number"):
        populated.as_of(20260601)
    with pytest.raises(ValidationError, match="bare number"):
        populated.accuracy_at_horizon(
            1, model_id="alpha", model_version="v1", period=(20260101, 20260301)
        )


def test_horizon_edge_values_are_typed_errors(populated: ForecastArchive) -> None:
    """Self-review hardening: non-finite and out-of-range horizons raise a
    typed ValidationError (not an untyped OverflowError or a DuckDB engine
    error), a numpy bool is not silently accepted as horizon 1, and a scalar
    ``horizons=`` is a typed error rather than a bare TypeError."""
    import numpy as np

    from foreledger import ValidationError

    for bad in (float("inf"), float("-inf"), float("nan"), 1e300):
        with pytest.raises(ValidationError):
            populated.accuracy_at_horizon(bad, model_id="alpha", model_version="v1")
    with pytest.raises(ValidationError):
        populated.accuracy_at_horizon(np.True_, model_id="alpha", model_version="v1")
    with pytest.raises(ValidationError):
        populated.accuracy_curve(horizons=7, model_id="alpha", model_version="v1")


def test_empty_series_scope_is_a_typed_error(populated: ForecastArchive) -> None:
    """Review reproduction: series=[] previously rendered invalid SQL
    ("series_id" IN ()) and leaked a DuckDB ParserException through every
    public read API."""
    from foreledger import ValidationError

    with pytest.raises(ValidationError, match="at least one"):
        populated.accuracy_at_horizon(1, series=[], model_id="alpha", model_version="v1")
    with pytest.raises(ValidationError, match="at least one"):
        populated.accuracy_curve(series=[], model_id="alpha", model_version="v1")
    with pytest.raises(ValidationError, match="at least one"):
        populated.as_of("2100-01-01", series=[])
    with pytest.raises(ValidationError, match="at least one"):
        populated.compare_models(1, [("alpha", "v1")], series=[])


def test_predicate_builder_never_renders_empty_in_lists() -> None:
    """Dialect-portability belt and braces below the API validation: an
    explicitly empty filter list compiles to a match-nothing predicate, not
    to invalid SQL."""
    from foreledger.backend.base import Dialect, ForecastFilter, build_forecast_predicate

    dialect = Dialect(name="test", placeholder="?")
    empty_series = ForecastFilter(active_run_ids=["r1"], segments=["s"], series=[])
    assert build_forecast_predicate(dialect, empty_series) == ("1 = 0", [])
    empty_models = ForecastFilter(active_run_ids=["r1"], segments=["s"], models=[])
    assert build_forecast_predicate(dialect, empty_models) == ("1 = 0", [])
