"""Summary invariants: reconciliation, rebuildability, metric protocol."""

from __future__ import annotations

import time

import numpy as np
import pandas as pd
import pytest
from tests.conftest import actuals_frame, forecast_frame

from forecast_archive import ForecastArchive, ReconciliationError, ValidationError
from forecast_archive.metrics import FloatArray


def test_reconciliation_passes_for_both_bases(populated: ForecastArchive) -> None:
    # give the official basis some cells too
    populated.register_actuals(actuals_frame().head(6), source="official-feed", official=True)
    populated.reconcile()
    stored = populated._backend.read_summary()
    assert stored is not None
    assert set(stored["actual_basis"]) == {"latest", "official"}


def test_reconcile_detects_divergence(populated: ForecastArchive) -> None:
    stored = populated._backend.read_summary()
    assert stored is not None and not stored.empty
    tampered = stored.copy()
    tampered.loc[0, "value"] = tampered.loc[0, "value"] + 1.0
    populated._backend.replace_summary(tampered)
    with pytest.raises(ReconciliationError):
        populated.reconcile()
    populated.rebuild_summary()
    populated.reconcile()


def test_summary_recomputed_eagerly_on_writes(archive: ForecastArchive) -> None:
    archive.ingest(forecast_frame(1.0), model_id="alpha", model_version="v1")
    stored = archive._backend.read_summary()
    assert stored is not None and stored.empty  # forecasts but no actuals yet

    archive.register_actuals(actuals_frame())
    stored = archive._backend.read_summary()
    assert stored is not None and not stored.empty
    archive.reconcile()


def test_registered_summarizable_metric_is_precomputed(populated: ForecastArchive) -> None:
    def max_abs_error(forecast: FloatArray, actual: FloatArray) -> float:
        return float(np.max(np.abs(forecast - actual)))

    populated.register_metric("MaxAE", max_abs_error, summarizable=True)
    result = populated.accuracy_at_horizon(1, metric="MaxAE", model_id="alpha", model_version="v1")
    assert result.status == "ok"
    assert result.served_from == "summary"
    populated.reconcile()


def test_non_summarizable_metric_computes_from_raw(populated: ForecastArchive) -> None:
    def median_abs_error(forecast: FloatArray, actual: FloatArray) -> float:
        return float(np.median(np.abs(forecast - actual)))

    populated.register_metric("MedAE", median_abs_error, summarizable=False)
    result = populated.accuracy_at_horizon(1, metric="MedAE", model_id="alpha", model_version="v1")
    assert result.status == "ok"
    assert result.served_from == "raw"


def test_bad_registered_metric_cannot_corrupt_recompute(populated: ForecastArchive) -> None:
    def broken(forecast: FloatArray, actual: FloatArray) -> float:
        raise RuntimeError("user metric bug")

    populated.register_metric("Broken", broken, summarizable=True)
    # built-in cells unharmed; the recompute completed
    populated.reconcile()
    healthy = populated.accuracy_at_horizon(1, metric="MAE", model_id="alpha", model_version="v1")
    assert healthy.status == "ok"
    # the broken metric yields an explicit insufficient result, not a crash
    broken_result = populated.accuracy_at_horizon(
        1, metric="Broken", model_id="alpha", model_version="v1"
    )
    assert broken_result.status == "insufficient"


def test_hanging_registered_metric_times_out(tmp_path: object) -> None:
    from pathlib import Path

    archive = ForecastArchive(Path(str(tmp_path)) / "store", metric_timeout=0.1)
    archive.ingest(forecast_frame(1.0), model_id="alpha", model_version="v1")
    archive.register_actuals(actuals_frame())

    def hanging(forecast: FloatArray, actual: FloatArray) -> float:
        time.sleep(1.5)
        return 0.0

    started = time.monotonic()
    archive.register_metric("Hang", hanging, summarizable=True)
    result = archive.accuracy_at_horizon(1, metric="Hang", model_id="alpha", model_version="v1")
    assert result.status == "insufficient"
    # the recompute did not hang for the full sleep of every cell
    assert time.monotonic() - started < 3.0


def test_builtin_metrics_cannot_be_replaced(populated: ForecastArchive) -> None:
    with pytest.raises(ValidationError):
        populated.register_metric("MAE", lambda f, a: 0.0)


def test_summary_grain_includes_model_and_version(populated: ForecastArchive) -> None:
    stored = populated._backend.read_summary()
    assert stored is not None
    pairs = set(zip(stored["model_id"], stored["model_version"], strict=True))
    assert ("alpha", "v1") in pairs and ("alpha", "v2") in pairs and ("beta", "v1") in pairs
    # parallel versions of the same model coexist at the same horizon
    cell = stored[
        (stored["metric"] == "MAE")
        & (stored["actual_basis"] == "latest")
        & (stored["series_id"] == "*")
        & (stored["horizon"] == 1)
        & (stored["model_id"] == "alpha")
    ]
    assert len(cell) == 2


def test_late_actuals_fan_out_into_summary(archive: ForecastArchive) -> None:
    archive.ingest(forecast_frame(1.0), model_id="alpha", model_version="v1")
    partial = actuals_frame()
    half = len(partial) // 2
    archive.register_actuals(partial.head(half), recorded_at="2026-02-01")
    before = archive.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    archive.register_actuals(partial.tail(len(partial) - half), recorded_at="2026-02-02")
    after = archive.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    assert after.n > before.n
    archive.reconcile()


def test_summary_is_never_authoritative_over_raw(populated: ForecastArchive) -> None:
    # poison the summary; raw recomputation (reconcile) must surface it
    stored = populated._backend.read_summary()
    assert stored is not None
    poisoned = stored.copy()
    poisoned["value"] = 0.0
    populated._backend.replace_summary(poisoned)
    with pytest.raises(ReconciliationError):
        populated.reconcile()


def test_summary_parquet_is_disposable(populated: ForecastArchive) -> None:
    (populated.store / "summary" / "summary.parquet").unlink()
    result = populated.accuracy_at_horizon(2, model_id="beta", model_version="v1")
    assert result.status == "ok"
    assert result.served_from == "raw"


def test_pandas_polars_interop_return_shape(populated: ForecastArchive) -> None:
    frame = populated.compare_models(1, [("alpha", "v1"), ("beta", "v1")])
    assert isinstance(frame, pd.DataFrame)
