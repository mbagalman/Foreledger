"""Actuals log invariants: revisions, tiebreaks, official stickiness."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from tests.conftest import ORIGINS, forecast_frame

from foreledger import ForecastArchive, OfficialConflictError


def one_target_frame(value: float) -> pd.DataFrame:
    target = ORIGINS[0] + pd.Timedelta(days=1)
    return pd.DataFrame({"series_id": ["S1"], "target": [target], "value": [value]})


def setup_forecasts(archive: ForecastArchive) -> None:
    archive.ingest(
        forecast_frame(1.0, origins=ORIGINS[:1], series=["S1"], horizons=[1]),
        model_id="alpha",
        model_version="v1",
    )


def mae_at_h1(archive: ForecastArchive, basis: str = "latest") -> object:
    return archive.accuracy_at_horizon(
        1, basis=basis, model_id="alpha", model_version="v1", series="S1"
    )


def test_revision_appends_and_latest_wins(archive: ForecastArchive) -> None:
    from tests.conftest import forecast_value

    setup_forecasts(archive)
    target = ORIGINS[0] + pd.Timedelta(days=1)
    predicted = forecast_value(1.0, "S1", ORIGINS[0], target)

    archive.register_actuals(one_target_frame(100.0), recorded_at="2026-02-01")
    first = mae_at_h1(archive)
    assert first.value == pytest.approx(abs(predicted - 100.0))

    archive.register_actuals(one_target_frame(200.0), recorded_at="2026-02-02")
    revised = mae_at_h1(archive)
    assert revised.value == pytest.approx(abs(predicted - 200.0))


def test_same_timestamp_equal_values_collapse(archive: ForecastArchive, store: Path) -> None:
    setup_forecasts(archive)
    ts = "2026-02-01T12:00:00"
    archive.register_actuals(one_target_frame(100.0), source="a", recorded_at=ts)
    archive.register_actuals(one_target_frame(100.0), source="b", recorded_at=ts)
    result = mae_at_h1(archive)
    assert result.status == "ok"
    assert not (store / "error_log.txt").exists()


def test_same_timestamp_conflict_resolved_by_source_priority(store: Path) -> None:
    archive = ForecastArchive(store, source_priority=["primary", "secondary"])
    setup_forecasts(archive)
    ts = "2026-02-01T12:00:00"
    archive.register_actuals(one_target_frame(100.0), source="secondary", recorded_at=ts)
    archive.register_actuals(one_target_frame(110.0), source="primary", recorded_at=ts)
    result = mae_at_h1(archive)
    assert result.status == "ok"
    # the primary feed's value (110) wins; drill confirms
    rows = archive.drill(
        {"model_id": "alpha", "model_version": "v1", "horizon": 1, "basis": "latest"}
    )
    assert rows["actual_value"].iloc[0] == 110.0
    assert not (store / "error_log.txt").exists()


def test_unresolved_conflict_logs_and_marks_ambiguous(
    archive: ForecastArchive, store: Path
) -> None:
    setup_forecasts(archive)
    ts = "2026-02-01T12:00:00"
    archive.register_actuals(one_target_frame(100.0), source="a", recorded_at=ts)
    archive.register_actuals(one_target_frame(110.0), source="b", recorded_at=ts)

    # never a silent pick: the target is ambiguous, hence insufficient
    result = mae_at_h1(archive)
    assert result.status == "insufficient"
    assert result.n_missing_actuals == 1

    error_log = store / "error_log.txt"
    assert error_log.exists()
    content = error_log.read_text(encoding="utf-8")
    assert "ambiguous-latest" in content and "S1" in content


def test_official_is_sticky_under_later_registrations(archive: ForecastArchive) -> None:
    setup_forecasts(archive)
    archive.register_actuals(
        one_target_frame(100.0), source="rev1", official=True, recorded_at="2026-02-01"
    )
    official_before = mae_at_h1(archive, basis="official")

    # a later, different, non-official registration must not disturb it
    archive.register_actuals(one_target_frame(250.0), source="rev2", recorded_at="2026-03-01")
    official_after = mae_at_h1(archive, basis="official")
    latest = mae_at_h1(archive, basis="latest")

    assert official_after.value == official_before.value
    assert latest.value != official_after.value


def test_registering_second_official_raises(archive: ForecastArchive) -> None:
    setup_forecasts(archive)
    archive.register_actuals(
        one_target_frame(100.0), source="rev1", official=True, recorded_at="2026-02-01"
    )
    with pytest.raises(OfficialConflictError):
        archive.register_actuals(
            one_target_frame(120.0), source="rev2", official=True, recorded_at="2026-02-02"
        )
    # the failed call appended nothing: latest basis still sees 100.0
    rows = archive.drill(
        {"model_id": "alpha", "model_version": "v1", "horizon": 1, "basis": "latest"}
    )
    assert rows["actual_value"].iloc[0] == 100.0


def test_mark_official_is_the_explicit_change_path(archive: ForecastArchive) -> None:
    setup_forecasts(archive)
    archive.register_actuals(one_target_frame(100.0), source="rev1", recorded_at="2026-02-01")
    archive.register_actuals(one_target_frame(120.0), source="rev2", recorded_at="2026-02-02")
    target = ORIGINS[0] + pd.Timedelta(days=1)

    archive.mark_official(series="S1", target=target, source="rev1")
    rows = archive.drill(
        {"model_id": "alpha", "model_version": "v1", "horizon": 1, "basis": "official"}
    )
    assert rows["actual_value"].iloc[0] == 100.0

    archive.mark_official(series="S1", target=target, source="rev2")
    rows = archive.drill(
        {"model_id": "alpha", "model_version": "v1", "horizon": 1, "basis": "official"}
    )
    assert rows["actual_value"].iloc[0] == 120.0


def test_official_basis_missing_is_insufficient_unless_fallback(
    archive: ForecastArchive,
) -> None:
    setup_forecasts(archive)
    archive.register_actuals(one_target_frame(100.0), recorded_at="2026-02-01")

    # no official actual exists: insufficient, never silently substituted
    strict = mae_at_h1(archive, basis="official")
    assert strict.status == "insufficient"
    assert strict.value is None
    assert strict.n_missing_actuals == 1

    # explicit opt-in fills from latest and flags it
    filled = archive.accuracy_at_horizon(
        1,
        basis="official",
        fallback="latest",
        model_id="alpha",
        model_version="v1",
        series="S1",
    )
    assert filled.status == "ok"
    assert filled.fallback_used
    assert filled.n_fallback == 1
    latest = mae_at_h1(archive, basis="latest")
    assert filled.value == latest.value


def test_missing_actuals_never_read_as_perfect_accuracy(archive: ForecastArchive) -> None:
    setup_forecasts(archive)
    result = mae_at_h1(archive)
    assert result.status == "insufficient"
    assert result.value is None
    assert result.n == 0
    assert result.n_missing_actuals == 1
