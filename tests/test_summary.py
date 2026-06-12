"""Summary invariants: reconciliation, rebuildability, validity, metric protocol."""

from __future__ import annotations

import time

import numpy as np
import pandas as pd
import pytest
from tests.conftest import actuals_frame, forecast_frame, summary_data_file

from foreledger import ForecastArchive, ReconciliationError, ValidationError
from foreledger.metrics import FloatArray


def stored_summary(archive: ForecastArchive) -> pd.DataFrame:
    """The stored summary frame (must exist and be valid for current raw state)."""
    frame = archive._valid_summary()
    assert frame is not None
    return frame


def test_reconciliation_passes_for_both_bases(populated: ForecastArchive) -> None:
    # give the official basis some cells too
    populated.register_actuals(actuals_frame().head(6), source="official-feed", official=True)
    populated.reconcile()
    stored = stored_summary(populated)
    assert set(stored["actual_basis"]) == {"latest", "official"}


def test_reconcile_detects_divergence(populated: ForecastArchive) -> None:
    stored = stored_summary(populated)
    tampered = stored.copy()
    tampered.loc[0, "value"] = tampered.loc[0, "value"] + 1.0
    populated._backend.replace_summary(tampered, populated._state_token())
    with pytest.raises(ReconciliationError):
        populated.reconcile()
    populated.rebuild_summary()
    populated.reconcile()


def test_summary_recomputed_eagerly_on_writes(archive: ForecastArchive) -> None:
    archive.ingest(forecast_frame(1.0), model_id="alpha", model_version="v1")
    stored = stored_summary(archive)
    assert stored.empty  # forecasts but no actuals yet

    archive.register_actuals(actuals_frame())
    stored = stored_summary(archive)
    assert not stored.empty
    archive.reconcile()


def test_failed_summary_refresh_recovers_on_retry(archive: ForecastArchive) -> None:
    """A summary-refresh failure after the durable commit must not corrupt
    anything: the stale summary is never served, queries fall back to raw,
    and an idempotent replay repairs the summary."""
    frame = forecast_frame(1.0)
    archive.ingest(frame, model_id="alpha", model_version="v1")
    archive.register_actuals(actuals_frame())
    populated_value = archive.accuracy_at_horizon(1, model_id="alpha", model_version="v1")

    backend = archive._backend
    original = backend.replace_summary

    def failing(summary: pd.DataFrame, token: str) -> None:
        raise RuntimeError("simulated summary write failure")

    backend.replace_summary = failing  # type: ignore[method-assign]
    second = forecast_frame(0.5)
    result = archive.ingest(second, model_id="alpha", model_version="v2")
    # the write itself is durable and reported as success
    assert result.n_runs_written > 0
    # the (now stale) stored summary is not served; raw fallback is correct
    after_failure = archive.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    assert after_failure.served_from == "raw"
    assert after_failure.value == populated_value.value
    new_model = archive.accuracy_at_horizon(1, model_id="alpha", model_version="v2")
    assert new_model.status == "ok"

    # an idempotent replay repairs the summary
    backend.replace_summary = original  # type: ignore[method-assign]
    replay = archive.ingest(second, model_id="alpha", model_version="v2")
    assert replay.n_runs_written == 0
    archive.reconcile()
    assert (
        archive.accuracy_at_horizon(1, model_id="alpha", model_version="v2").served_from
        == "summary"
    )


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


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_metric_result_yields_insufficient_not_stored(
    populated: ForecastArchive, bad: float
) -> None:
    """A non-finite return is the in-protocol "undefined on this data"
    signal: the cell reads as insufficient, no non-finite number is ever
    persisted in the summary, and the metric is NOT quarantined — it stays
    usable for scopes where it is defined."""

    def sometimes_undefined(forecast: FloatArray, actual: FloatArray) -> float:
        return bad

    populated.register_metric("Undef", sometimes_undefined, summarizable=True)
    result = populated.accuracy_at_horizon(1, metric="Undef", model_id="alpha", model_version="v1")
    assert result.status == "insufficient"
    assert result.value is None
    # the rebuilt summary holds finite values only
    populated.rebuild_summary()
    stored = populated._backend.read_summary()
    assert stored is not None
    assert np.isfinite(stored[0]["value"].to_numpy(dtype="float64")).all()
    # not quarantined: a now-finite re-registration evaluates normally
    populated.register_metric("Undef", lambda f, a: 1.0, summarizable=True)
    healthy = populated.accuracy_at_horizon(1, metric="Undef", model_id="alpha", model_version="v1")
    assert healthy.status == "ok"
    populated.reconcile()


def test_hanging_registered_metric_times_out_and_is_quarantined(tmp_path: object) -> None:
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
    # the first timeout quarantines the metric: the rebuild pays the timeout
    # once, not once per cell
    assert time.monotonic() - started < 1.5
    # built-ins are unaffected
    assert (
        archive.accuracy_at_horizon(1, metric="MAE", model_id="alpha", model_version="v1").status
        == "ok"
    )


def test_builtin_metrics_cannot_be_replaced(populated: ForecastArchive) -> None:
    with pytest.raises(ValidationError):
        populated.register_metric("MAE", lambda f, a: 0.0)


def test_reregistered_metric_never_serves_the_old_implementation(
    populated: ForecastArchive,
) -> None:
    """Review reproduction: replacing a summarizable metric under the same
    name must invalidate summary cells computed with the old implementation."""

    def custom_v1(forecast: FloatArray, actual: FloatArray) -> float:
        return 1.0

    populated.register_metric("Custom", custom_v1, summarizable=True)
    first = populated.accuracy_at_horizon(1, metric="Custom", model_id="alpha", model_version="v1")
    assert first.value == 1.0

    def custom_v2(forecast: FloatArray, actual: FloatArray) -> float:
        return 2.0

    populated.register_metric("Custom", custom_v2, summarizable=True)
    replaced = populated.accuracy_at_horizon(
        1, metric="Custom", model_id="alpha", model_version="v1"
    )
    assert replaced.value == 2.0  # never the stale 1.0
    populated.reconcile()


def test_reregistered_closure_metric_never_serves_old_capture(
    populated: ForecastArchive,
) -> None:
    """Review reproduction: identical code capturing different closure state
    is invisible to any bytecode hash — every registration event must
    invalidate the summary on its own."""

    def make_constant_metric(constant: float):  # type: ignore[no-untyped-def]
        def fn(forecast: FloatArray, actual: FloatArray) -> float:
            return constant

        return fn

    populated.register_metric("Closure", make_constant_metric(1.0), summarizable=True)
    assert (
        populated.accuracy_at_horizon(
            1, metric="Closure", model_id="alpha", model_version="v1"
        ).value
        == 1.0
    )

    populated.register_metric("Closure", make_constant_metric(2.0), summarizable=True)
    assert (
        populated.accuracy_at_horizon(
            1, metric="Closure", model_id="alpha", model_version="v1"
        ).value
        == 2.0
    )
    populated.reconcile()


def test_corrupt_summary_file_falls_back_to_raw(populated: ForecastArchive) -> None:
    expected = populated.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    summary_data_file(populated).write_bytes(b"not parquet at all")

    # the disposable cache is treated as absent, never as a query error
    result = populated.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    assert result.served_from == "raw"
    assert result.value == expected.value

    # and it is repairable in place
    populated.rebuild_summary()
    populated.reconcile()
    assert (
        populated.accuracy_at_horizon(1, model_id="alpha", model_version="v1").served_from
        == "summary"
    )


def test_summary_grain_includes_model_and_version(populated: ForecastArchive) -> None:
    stored = stored_summary(populated)
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
    stored = stored_summary(populated)
    poisoned = stored.copy()
    poisoned["value"] = 0.0
    populated._backend.replace_summary(poisoned, populated._state_token())
    with pytest.raises(ReconciliationError):
        populated.reconcile()


def test_summary_parquet_is_disposable(populated: ForecastArchive) -> None:
    summary_data_file(populated).unlink()
    result = populated.accuracy_at_horizon(2, model_id="beta", model_version="v1")
    assert result.status == "ok"
    assert result.served_from == "raw"


def test_pandas_polars_interop_return_shape(populated: ForecastArchive) -> None:
    frame = populated.compare_models(1, [("alpha", "v1"), ("beta", "v1")])
    assert isinstance(frame, pd.DataFrame)


def test_forecast_replay_repairs_absent_summary_promptly(populated: ForecastArchive) -> None:
    """Review reproduction: the no-op replay path must refresh the summary
    OUTSIDE the store lock — the publication re-acquires the (non-reentrant)
    lock, so an in-lock refresh self-times-out and silently skips the
    promised repair."""
    summary_data_file(populated).unlink()
    assert (
        populated.accuracy_at_horizon(1, model_id="alpha", model_version="v1").served_from == "raw"
    )

    started = time.monotonic()
    replay = populated.ingest(forecast_frame(1.0), model_id="alpha", model_version="v1")
    elapsed = time.monotonic() - started
    assert replay.n_runs_written == 0
    assert elapsed < 20  # never the 30s self-timeout
    # repaired immediately — no intervening reconcile()
    assert (
        populated.accuracy_at_horizon(1, model_id="alpha", model_version="v1").served_from
        == "summary"
    )


def test_actuals_replay_repairs_absent_summary_promptly(populated: ForecastArchive) -> None:
    summary_data_file(populated).unlink()

    started = time.monotonic()
    populated.register_actuals(actuals_frame(), recorded_at="2026-02-01")  # exact replay
    elapsed = time.monotonic() - started
    assert elapsed < 20  # never the 30s self-timeout
    assert (
        populated.accuracy_at_horizon(1, model_id="alpha", model_version="v1").served_from
        == "summary"
    )


def test_failed_pointer_publication_does_not_leak_generations(
    populated: ForecastArchive, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review reproduction: a generation whose pointer write fails is swept
    immediately — repeated refresh failures must not grow disk use."""
    import foreledger.backend.duckdb_backend as backend_module

    summary_dir = populated.store / "summary"

    def count() -> int:
        return len(list(summary_dir.glob("summary-*.parquet")))

    before = count()

    def failing_pointer(path, payload, indent=1):  # type: ignore[no-untyped-def]
        raise OSError("simulated metadata write failure")

    monkeypatch.setattr(backend_module, "atomic_write_json", failing_pointer)
    for _ in range(3):
        with pytest.raises(OSError, match="simulated"):
            populated.rebuild_summary()
    assert count() == before  # no orphaned generations accumulated

    # restored, publication works again
    monkeypatch.undo()
    populated.rebuild_summary()
    populated.reconcile()


def test_modified_summary_generation_is_never_served(populated: ForecastArchive) -> None:
    """Review reproduction: an in-place edit of the current summary
    generation (valid Parquet, pointer untouched) must be discarded by the
    published content digest and the query computed from raw — the
    disposable cache is never authoritative over raw."""
    expected = populated.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    assert expected.served_from == "summary"

    stored = populated._backend.read_summary()
    assert stored is not None
    tampered = stored[0].copy()
    tampered["value"] = 12345.0
    tampered.to_parquet(summary_data_file(populated), index=False)  # pointer untouched

    result = populated.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    assert result.served_from == "raw"
    assert result.value == expected.value
    assert result.value != 12345.0


@pytest.mark.parametrize(
    "dropped",
    [
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
    ],
)
def test_summary_missing_column_falls_back_to_raw(populated: ForecastArchive, dropped: str) -> None:
    """Review reproduction: a readable generation violating the summary
    schema contract (published with a valid digest and current token) must
    be treated as an absent cache, never crash the query."""
    expected = populated.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    stored = populated._backend.read_summary()
    assert stored is not None
    malformed = stored[0].drop(columns=[dropped])
    populated._backend.replace_summary(malformed, populated._state_token())

    result = populated.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    assert result.served_from == "raw"
    assert result.value == expected.value


def test_summary_incompatible_dtype_falls_back_to_raw(populated: ForecastArchive) -> None:
    expected = populated.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    stored = populated._backend.read_summary()
    assert stored is not None
    malformed = stored[0].copy()
    malformed["value"] = "not-a-number"
    populated._backend.replace_summary(malformed, populated._state_token())

    result = populated.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    assert result.served_from == "raw"
    assert result.value == expected.value


def test_quarantine_after_successful_build_stays_consistent(tmp_path: object) -> None:
    """A metric that builds its summary cells successfully and is quarantined
    LATER (timing out on a raw-routed query) must not leave the routes
    diverging: quarantine state is part of the summary validity token, so the
    pre-quarantine summary is invalidated, exact-cell queries agree with raw
    (insufficient), and reconcile() does not raise on a healthy store."""
    from pathlib import Path

    from tests.conftest import ORIGINS

    archive = ForecastArchive(Path(str(tmp_path)) / "store", metric_timeout=0.2)
    archive.ingest(forecast_frame(1.0), model_id="alpha", model_version="v1")
    archive.register_actuals(actuals_frame())

    hang = {"on": False}

    def flaky(forecast: FloatArray, actual: FloatArray) -> float:
        if hang["on"]:
            time.sleep(1.0)
        return float(np.mean(np.abs(forecast - actual)))

    archive.register_metric("Flaky", flaky, summarizable=True)
    served = archive.accuracy_at_horizon(1, metric="Flaky", model_id="alpha", model_version="v1")
    assert served.served_from == "summary"
    assert served.status == "ok"

    # the metric starts hanging; a raw-routed (period-scoped) query
    # quarantines it for the session
    hang["on"] = True
    raw = archive.accuracy_at_horizon(
        1,
        metric="Flaky",
        model_id="alpha",
        model_version="v1",
        period=(ORIGINS[0], ORIGINS[-1]),
    )
    assert raw.status == "insufficient"

    # the exact-cell query must NOT keep serving the pre-quarantine number
    after = archive.accuracy_at_horizon(1, metric="Flaky", model_id="alpha", model_version="v1")
    assert after.status == "insufficient"
    # no false corruption alarm from a feature working as designed
    archive.reconcile()
    # built-ins unaffected
    assert (
        archive.accuracy_at_horizon(1, metric="MAE", model_id="alpha", model_version="v1").status
        == "ok"
    )


def test_post_commit_validity_check_failure_does_not_fail_the_write(
    populated: ForecastArchive, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The summary-refresh boundary covers the validity check too: a failure
    there happens AFTER the durable commit and must not make a committed
    write report plain failure."""

    def boom() -> pd.DataFrame | None:
        raise RuntimeError("validity check failure after commit")

    monkeypatch.setattr(populated, "_valid_summary", boom)
    result = populated.ingest(forecast_frame(0.5), model_id="alpha", model_version="vNew")
    assert result.n_runs_written > 0


def test_failed_summary_data_write_leaves_no_tmp_files(
    populated: ForecastArchive, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failing generation data write must clean up its own temp file —
    repeated tolerated refresh failures cannot accumulate orphans."""
    from pathlib import Path

    def failing_atomic(frame: pd.DataFrame, path: Path) -> None:
        path.with_suffix(".parquet.tmp").write_bytes(b"partial")
        raise OSError("simulated ENOSPC")

    monkeypatch.setattr(populated._backend, "_atomic_write", failing_atomic)
    for _ in range(3):
        with pytest.raises(OSError, match="ENOSPC"):
            populated.rebuild_summary()
    assert not list((populated.store / "summary").glob("*.tmp"))


def test_metric_first_quarantined_during_reconcile_is_not_divergence(
    tmp_path: object,
) -> None:
    """Review reproduction: a summarizable metric whose FIRST timeout happens
    inside reconcile()'s recomputation changes the token mid-audit; the
    comparison must restart against the new stable state instead of raising
    a false ReconciliationError on a healthy archive."""
    from pathlib import Path

    archive = ForecastArchive(Path(str(tmp_path)) / "store", metric_timeout=0.2)
    archive.ingest(forecast_frame(1.0), model_id="alpha", model_version="v1")
    archive.register_actuals(actuals_frame())

    hang = {"on": False}

    def flaky(forecast: FloatArray, actual: FloatArray) -> float:
        if hang["on"]:
            time.sleep(1.0)
        return float(np.mean(np.abs(forecast - actual)))

    archive.register_metric("Flaky", flaky, summarizable=True)
    assert (
        archive.accuracy_at_horizon(
            1, metric="Flaky", model_id="alpha", model_version="v1"
        ).served_from
        == "summary"
    )

    # the very next metric evaluation is the one inside reconcile()
    hang["on"] = True
    archive.reconcile()  # must not raise

    # post-reconcile state is coherent: quarantined metric reads as
    # insufficient on both routes, built-ins still summary-served
    result = archive.accuracy_at_horizon(1, metric="Flaky", model_id="alpha", model_version="v1")
    assert result.status == "insufficient"
    assert (
        archive.accuracy_at_horizon(
            1, metric="MAE", model_id="alpha", model_version="v1"
        ).served_from
        == "summary"
    )
