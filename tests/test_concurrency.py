"""Multiple archive handles on one store must merge, never clobber."""

from __future__ import annotations

from pathlib import Path

from tests.conftest import actuals_frame, forecast_frame

from foreledger import ForecastArchive


def test_two_handles_do_not_erase_each_others_runs(store: Path) -> None:
    """Review reproduction: handle B, opened before handle A's ingest, must
    not make A's committed run invisible when it commits its own."""
    handle_a = ForecastArchive(store)
    handle_b = ForecastArchive(store)  # loads the (empty) manifest now

    handle_a.ingest(forecast_frame(1.0), model_id="first", model_version="v1")
    handle_b.ingest(forecast_frame(2.0), model_id="second", model_version="v1")

    reopened = ForecastArchive(store)
    listing = reopened.list_models()
    assert set(listing["model_id"]) == {"first", "second"}
    rows = reopened.as_of("2100-01-01")
    assert len(rows) == 2 * len(forecast_frame(1.0))
    reopened.reconcile()


def test_interleaved_idempotent_replay_across_handles(store: Path) -> None:
    frame = forecast_frame(1.0)
    handle_a = ForecastArchive(store)
    handle_b = ForecastArchive(store)

    handle_a.ingest(frame, model_id="alpha", model_version="v1")
    # B replays the identical data from its stale view: the locked re-plan
    # sees A's commit and treats it as the no-op it is
    replay = handle_b.ingest(frame, model_id="alpha", model_version="v1")
    assert replay.n_runs_written == 0

    reopened = ForecastArchive(store)
    assert len(reopened.as_of("2100-01-01")) == len(frame)


def test_champion_updates_merge_across_handles(store: Path) -> None:
    handle_a = ForecastArchive(store)
    handle_b = ForecastArchive(store)

    handle_a.set_champion("alpha", "v1")
    handle_b.set_champion("beta", "v2")  # must not clobber alpha's entry

    assert ForecastArchive(store).champions() == {"alpha": "v1", "beta": "v2"}


def test_long_lived_handle_sees_other_handles_commits(store: Path) -> None:
    """Reads on an existing handle must reflect runs committed by another
    handle — never an indefinitely stale snapshot."""
    handle_a = ForecastArchive(store)
    handle_b = ForecastArchive(store)

    handle_a.ingest(forecast_frame(1.0), model_id="first", model_version="v1")
    assert set(handle_b.list_models()["model_id"]) == {"first"}

    handle_b.ingest(forecast_frame(2.0), model_id="second", model_version="v1")
    assert set(handle_a.list_models()["model_id"]) == {"first", "second"}


def test_handle_sees_overwrites_from_other_handle(store: Path) -> None:
    frame = forecast_frame(1.0)
    handle_a = ForecastArchive(store)
    handle_b = ForecastArchive(store)
    handle_a.ingest(frame, model_id="alpha", model_version="v1")
    handle_a.register_actuals(actuals_frame())
    before = handle_a.accuracy_at_horizon(1, model_id="alpha", model_version="v1")

    changed = frame.copy()
    changed["value"] = changed["value"] + 5.0
    handle_b.ingest(changed, model_id="alpha", model_version="v1", on_conflict="overwrite")

    after = handle_a.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    assert after.value != before.value
    fresh = ForecastArchive(store).accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    assert after.value == fresh.value


def test_concurrent_initialization_of_a_new_store(store: Path) -> None:
    """Constructors racing on an empty path must all succeed (serialized
    init), never tripping over each other's temp files or the lock file."""
    import json
    from concurrent.futures import ThreadPoolExecutor

    def construct(_: int) -> ForecastArchive:
        return ForecastArchive(store)

    with ThreadPoolExecutor(max_workers=8) as pool:
        handles = list(pool.map(construct, range(20)))
    assert len(handles) == 20
    from foreledger import FORMAT_VERSION

    meta = json.loads((store / "archive_meta.json").read_text(encoding="utf-8"))
    assert meta["format_version"] == FORMAT_VERSION


def test_actuals_appends_from_two_handles_both_land(store: Path) -> None:
    handle_a = ForecastArchive(store)
    handle_b = ForecastArchive(store)
    handle_a.ingest(forecast_frame(1.0), model_id="alpha", model_version="v1")

    actuals = actuals_frame()
    half = len(actuals) // 2
    handle_a.register_actuals(actuals.head(half), recorded_at="2026-02-01")
    handle_b.register_actuals(actuals.tail(len(actuals) - half), recorded_at="2026-02-01")

    reopened = ForecastArchive(store)
    assert len(reopened._visible_actuals()) == len(actuals)
    reopened.reconcile()
