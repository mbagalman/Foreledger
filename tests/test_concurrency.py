"""Multiple archive handles on one store must merge, never clobber."""

from __future__ import annotations

from pathlib import Path

import pytest
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


def test_one_call_cannot_mix_summary_and_raw_states(
    store: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review reproduction: a curve whose first point is summary-served and
    whose second point falls back to raw must answer entirely from ONE
    archive state, even when another handle commits a revision mid-call."""
    import pandas as pd

    handle_a = ForecastArchive(store)
    forecasts = pd.DataFrame(
        {
            "series_id": ["s1", "s1"],
            "target": pd.to_datetime(["2026-01-02", "2026-01-03"]),
            "value": [10.0, 10.0],
        }
    )
    handle_a.ingest(forecasts, model_id="alpha", model_version="v1", origin="2026-01-01")
    # horizon 1 covered (MAE 1.0); horizon 2 has no actual yet (insufficient,
    # so it has no summary cell and always computes from raw)
    first_actual = pd.DataFrame(
        {"series_id": ["s1"], "target": pd.to_datetime(["2026-01-02"]), "value": [9.0]}
    )
    handle_a.register_actuals(first_actual, recorded_at="2026-01-10")

    handle_b = ForecastArchive(store)
    revision = pd.DataFrame(
        {
            "series_id": ["s1", "s1"],
            "target": pd.to_datetime(["2026-01-02", "2026-01-03"]),
            "value": [10.0, 9.0],  # h1 error becomes 0.0; h2 becomes scorable
        }
    )

    # interleave: B's revision commits right after A reads the stored summary
    real_read = handle_a._backend.read_summary
    fired = {"done": False}

    def read_then_commit():  # type: ignore[no-untyped-def]
        stored = real_read()
        if not fired["done"]:
            fired["done"] = True
            handle_b.register_actuals(revision, recorded_at="2026-01-20")
        return stored

    monkeypatch.setattr(handle_a._backend, "read_summary", read_then_commit)
    curve = handle_a.accuracy_curve(
        metric="MAE", model_id="alpha", model_version="v1", horizons=[1, 2]
    )
    assert fired["done"]
    # entirely the BEFORE state: h1 from the old summary, h2 insufficient
    # from the snapshot's raw segments — never old h1 with new h2
    assert curve.points[0].value == 1.0
    assert curve.points[1].status == "insufficient"

    # a fresh call sees the AFTER state in full
    monkeypatch.setattr(handle_a._backend, "read_summary", real_read)
    after = handle_a.accuracy_curve(
        metric="MAE", model_id="alpha", model_version="v1", horizons=[1, 2]
    )
    assert after.points[0].value == 0.0
    assert after.points[1].value == 1.0


def test_summary_replacement_holds_the_store_lock(
    store: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review reproduction guard: the summary data + validity token must be
    replaced while holding the store lock, so two handles' rebuilds can
    never interleave one handle's data with the other's token."""
    import pytest as _pytest

    from foreledger import StoreLockTimeout
    from foreledger.locking import StoreLock

    archive = ForecastArchive(store)
    archive.ingest(forecast_frame(1.0), model_id="alpha", model_version="v1")
    archive.register_actuals(actuals_frame(), recorded_at="2026-02-01")

    real_replace = archive._backend.replace_summary
    probed = {"locked": False}

    def probing_replace(frame, token):  # type: ignore[no-untyped-def]
        with _pytest.raises(StoreLockTimeout), StoreLock(store / ".foreledger.lock", timeout=0.2):
            pass
        probed["locked"] = True
        real_replace(frame, token)

    monkeypatch.setattr(archive._backend, "replace_summary", probing_replace)
    archive.rebuild_summary()
    assert probed["locked"]


def test_reader_never_pairs_old_token_with_new_summary_data(
    store: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review reproduction: a summary replacement landing between a reader's
    metadata read and its data read must never let the reader pair the old
    validity token with the new data. With generation publication the single
    metadata read pins an immutable data file, so the reader gets the old
    coherent pair — or, if cleanup already removed that generation, no cache
    at all and a raw fallback from its own snapshot. Either way the answer
    is entirely the snapshot's state."""
    import pandas as pd

    import foreledger.backend.duckdb_backend as backend_module

    handle_a = ForecastArchive(store)
    forecasts = pd.DataFrame(
        {
            "series_id": ["s1", "s1"],
            "target": pd.to_datetime(["2026-01-02", "2026-01-03"]),
            "value": [10.0, 10.0],
        }
    )
    handle_a.ingest(forecasts, model_id="alpha", model_version="v1", origin="2026-01-01")
    first_actual = pd.DataFrame(
        {"series_id": ["s1"], "target": pd.to_datetime(["2026-01-02"]), "value": [9.0]}
    )
    handle_a.register_actuals(first_actual, recorded_at="2026-01-10")

    handle_b = ForecastArchive(store)
    revision = pd.DataFrame(
        {
            "series_id": ["s1", "s1"],
            "target": pd.to_datetime(["2026-01-02", "2026-01-03"]),
            "value": [10.0, 9.0],  # h1 error becomes 0.0; h2 becomes scorable
        }
    )

    # interleave: B commits + republishes the summary AFTER A has read the
    # summary metadata and BEFORE A reads the summary generation's bytes
    real_read_bytes = backend_module.Path.read_bytes
    fired = {"done": False}

    def interleaved(self: Path) -> bytes:
        if not fired["done"] and self.name.startswith("summary-"):
            fired["done"] = True
            handle_b.register_actuals(revision, recorded_at="2026-01-20")
        return real_read_bytes(self)

    monkeypatch.setattr(backend_module.Path, "read_bytes", interleaved)
    curve = handle_a.accuracy_curve(
        metric="MAE", model_id="alpha", model_version="v1", horizons=[1, 2]
    )
    assert fired["done"]
    # entirely the BEFORE state — never the new data blessed by the old token
    assert curve.points[0].value == 1.0
    assert curve.points[1].status == "insufficient"


def test_stale_rebuild_cannot_erase_a_newer_summary(
    store: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review reproduction: a slow rebuild computed from an older snapshot
    must be discarded at publication time, not published over (and sweep)
    the newer state's generation — otherwise two successful operations leave
    the eager summary unavailable until the next repair."""
    import pandas as pd

    handle_a = ForecastArchive(store)
    handle_a.ingest(forecast_frame(1.0), model_id="alpha", model_version="v1")
    handle_a.register_actuals(actuals_frame(), recorded_at="2026-02-01")

    handle_b = ForecastArchive(store)
    revision = actuals_frame()
    revision["value"] = revision["value"] + 1.0

    # pause A's recompute after it has captured the old snapshot: B commits
    # a revision (and eagerly publishes the new summary) in the gap
    real_recompute = handle_a._recompute_summary_from
    fired = {"done": False}

    def slow_recompute(snap):  # type: ignore[no-untyped-def]
        frame = real_recompute(snap)
        if not fired["done"]:
            fired["done"] = True
            handle_b.register_actuals(revision, recorded_at="2026-03-01")
        return frame

    monkeypatch.setattr(handle_a, "_recompute_summary_from", slow_recompute)
    handle_a.rebuild_summary()  # computes from the pre-revision snapshot
    assert fired["done"]

    # the stale result was discarded: the stored token matches the CURRENT
    # state and a normal query is still eagerly summary-served
    import json

    meta = json.loads((store / "summary" / "summary_meta.json").read_text(encoding="utf-8"))
    assert meta["state_token"] == handle_a._state_token()
    result = handle_a.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    assert result.served_from == "summary"
    # and it reflects the post-revision truth
    raw_pairs = handle_a.drill(
        {"model_id": "alpha", "model_version": "v1", "horizon": 1, "metric": "MAE"}
    )
    expected = float((raw_pairs["value"] - raw_pairs["actual_value"]).abs().mean())
    assert result.value == pd.Series([expected]).iloc[0]
    handle_a.reconcile()
