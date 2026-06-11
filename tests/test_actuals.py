"""Actuals log invariants: revisions, tiebreaks, official stickiness."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from tests.conftest import ORIGINS, forecast_frame

from foreledger import ForecastArchive, OfficialConflictError, ValidationError


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


def test_same_identity_different_value_rejected(archive: ForecastArchive) -> None:
    """Re-registering an existing (series, target, source, recorded_at)
    identity with a different value is rejected before any append."""
    setup_forecasts(archive)
    ts = "2026-02-01T12:00:00"
    archive.register_actuals(one_target_frame(100.0), source="a", recorded_at=ts)
    with pytest.raises(ValidationError, match="identity"):
        archive.register_actuals(one_target_frame(120.0), source="a", recorded_at=ts)
    rows = archive.drill(
        {"model_id": "alpha", "model_version": "v1", "horizon": 1, "basis": "latest"}
    )
    assert rows["actual_value"].iloc[0] == 100.0


def test_identical_replay_collapses(archive: ForecastArchive) -> None:
    setup_forecasts(archive)
    ts = "2026-02-01T12:00:00"
    archive.register_actuals(one_target_frame(100.0), source="a", recorded_at=ts)
    archive.register_actuals(one_target_frame(100.0), source="a", recorded_at=ts)  # replay
    log = archive._visible_actuals()
    assert len(log) == 1  # the replay appended nothing
    assert mae_at_h1(archive).status == "ok"


def test_official_cannot_hide_a_same_identity_conflict(archive: ForecastArchive) -> None:
    """Review reproduction: two different official values at the exact same
    identity must be rejected, not silently resolved to the first one."""
    setup_forecasts(archive)
    ts = "2026-02-01T12:00:00"
    archive.register_actuals(one_target_frame(8.0), source="a", recorded_at=ts, official=True)
    with pytest.raises(ValidationError):
        archive.register_actuals(one_target_frame(20.0), source="a", recorded_at=ts, official=True)
    rows = archive.drill(
        {"model_id": "alpha", "model_version": "v1", "horizon": 1, "basis": "official"}
    )
    assert list(rows["actual_value"]) == [8.0]
    assert mae_at_h1(archive, basis="official").status == "ok"


@pytest.mark.parametrize("bad_value", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_actual_values_rejected(archive: ForecastArchive, bad_value: float) -> None:
    setup_forecasts(archive)
    with pytest.raises(ValidationError):
        archive.register_actuals(one_target_frame(bad_value))


def test_summary_not_served_across_source_priority_change(store: Path) -> None:
    """The summary depends on the tiebreak configuration, so a handle opened
    with a different source_priority must never serve a summary built under
    the old one."""
    from tests.conftest import forecast_value

    first = ForecastArchive(store, source_priority=["a", "b"])
    setup_forecasts(first)
    ts = "2026-02-01T12:00:00"
    first.register_actuals(one_target_frame(100.0), source="a", recorded_at=ts)
    first.register_actuals(one_target_frame(120.0), source="b", recorded_at=ts)
    predicted = forecast_value(1.0, "S1", ORIGINS[0], ORIGINS[0] + pd.Timedelta(days=1))
    assert mae_at_h1(first).value == pytest.approx(abs(predicted - 100.0))  # "a" wins

    reopened = ForecastArchive(store, source_priority=["b", "a"])
    result = reopened.accuracy_at_horizon(1, model_id="alpha", model_version="v1", series="S1")
    assert result.value == pytest.approx(abs(predicted - 120.0))  # "b" wins now
    reopened.reconcile()


def test_failed_actuals_append_after_designation_is_recoverable(
    archive: ForecastArchive, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the actual's append fails after its official designation landed,
    nothing moves accuracy (the designation is inert without its actual), and
    a retry with the same recorded_at completes the pair."""
    setup_forecasts(archive)
    ts = "2026-02-01T12:00:00"

    def failing(frame: pd.DataFrame) -> None:
        raise OSError("simulated disk failure")

    monkeypatch.setattr(archive._backend, "append_actuals_segment", failing)
    with pytest.raises(OSError):
        archive.register_actuals(
            one_target_frame(100.0), source="rev1", official=True, recorded_at=ts
        )
    assert mae_at_h1(archive).status == "insufficient"
    assert mae_at_h1(archive, basis="official").status == "insufficient"

    monkeypatch.undo()
    archive.register_actuals(one_target_frame(100.0), source="rev1", official=True, recorded_at=ts)
    assert mae_at_h1(archive).status == "ok"
    assert mae_at_h1(archive, basis="official").status == "ok"
    archive.reconcile()


def test_priority_token_encoding_is_delimiter_safe(store: Path) -> None:
    """Review reproduction: source labels may contain the join delimiter, so
    distinct priority lists must never collide to one summary token."""
    first = ForecastArchive(store, source_priority=["a,b", "c"])
    setup_forecasts(first)
    ts = "2026-02-01T12:00:00"
    first.register_actuals(one_target_frame(100.0), source="a,b", recorded_at=ts)
    first.register_actuals(one_target_frame(120.0), source="c", recorded_at=ts)
    assert mae_at_h1(first).status == "ok"  # "a,b" outranks "c"

    # same comma-join, different semantics: neither tied source is covered,
    # so the conflict is ambiguous and the target unscorable
    reopened = ForecastArchive(store, source_priority=["a", "b,c"])
    result = reopened.accuracy_at_horizon(1, model_id="alpha", model_version="v1", series="S1")
    assert result.status == "insufficient"  # never the stale "ok" summary
    reopened.reconcile()


def test_failed_default_timestamp_official_registration_is_retryable(
    archive: ForecastArchive, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed official registration leaves only invisible segment files
    (the manifest never committed); a plain retry through the same public
    call (default recorded_at) must succeed."""
    setup_forecasts(archive)

    def failing(frame: pd.DataFrame) -> None:
        raise OSError("simulated disk failure")

    monkeypatch.setattr(archive._backend, "append_actuals_segment", failing)
    with pytest.raises(OSError):
        archive.register_actuals(one_target_frame(100.0), source="rev1", official=True)
    assert mae_at_h1(archive).status == "insufficient"
    assert mae_at_h1(archive, basis="official").status == "insufficient"

    monkeypatch.undo()
    archive.register_actuals(one_target_frame(100.0), source="rev1", official=True)
    assert mae_at_h1(archive).status == "ok"
    assert mae_at_h1(archive, basis="official").status == "ok"
    rows = archive.drill(
        {"model_id": "alpha", "model_version": "v1", "horizon": 1, "basis": "official"}
    )
    assert list(rows["actual_value"]) == [100.0]
    archive.reconcile()


def test_orphan_recovery_does_not_weaken_live_stickiness(
    archive: ForecastArchive,
) -> None:
    """Failure recovery must not let a *live* official be replaced."""
    setup_forecasts(archive)
    archive.register_actuals(
        one_target_frame(100.0), source="rev1", official=True, recorded_at="2026-02-01"
    )
    with pytest.raises(OfficialConflictError):
        archive.register_actuals(one_target_frame(120.0), source="rev2", official=True)


def test_nonofficial_registration_cannot_activate_a_failed_officials_intent(
    archive: ForecastArchive, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review reproduction: a failed official=True call must leave no durable
    official intent that a later official=False call could silently activate."""
    setup_forecasts(archive)
    ts = "2026-02-01T12:00:00"

    def failing(frame: pd.DataFrame) -> str:
        raise OSError("simulated disk failure")

    monkeypatch.setattr(archive._backend, "append_actuals_segment", failing)
    with pytest.raises(OSError):
        archive.register_actuals(
            one_target_frame(100.0), source="rev1", official=True, recorded_at=ts
        )
    monkeypatch.undo()

    # the same identity registered WITHOUT the official flag
    archive.register_actuals(one_target_frame(100.0), source="rev1", recorded_at=ts)
    assert mae_at_h1(archive).status == "ok"
    # the failed call's official intent must not have survived
    strict = mae_at_h1(archive, basis="official")
    assert strict.status == "insufficient"
    archive.reconcile()


def test_failed_visibility_commit_leaves_nothing_and_retries_cleanly(
    archive: ForecastArchive, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The manifest save is the single visibility point: if it fails, neither
    the actual nor its designation is readable, and any retry works."""
    setup_forecasts(archive)

    def failing_save(self: object) -> None:
        raise OSError("simulated crash at the visibility commit")

    monkeypatch.setattr("foreledger.actuals.ActualsManifest.save", failing_save)
    with pytest.raises(OSError):
        archive.register_actuals(one_target_frame(100.0), source="rev1", official=True)
    assert len(archive._visible_actuals()) == 0
    assert mae_at_h1(archive).status == "insufficient"

    monkeypatch.undo()
    archive.register_actuals(one_target_frame(100.0), source="rev1", official=True)
    assert mae_at_h1(archive).status == "ok"
    assert mae_at_h1(archive, basis="official").status == "ok"
    archive.reconcile()


def test_non_string_source_rejected_and_store_stays_usable(
    archive: ForecastArchive,
) -> None:
    """Review reproduction: a non-string source must be rejected before any
    write — persisting it would poison the durable identity column and block
    all later registrations."""
    setup_forecasts(archive)
    with pytest.raises(ValidationError, match="source"):
        archive.register_actuals(one_target_frame(100.0), source=123)  # type: ignore[arg-type]
    with pytest.raises(ValidationError, match="source"):
        archive.register_actuals(one_target_frame(100.0), source="   ")

    # the store remains fully usable for a valid registration
    archive.register_actuals(one_target_frame(100.0), source="feed")
    assert mae_at_h1(archive).status == "ok"


@pytest.mark.parametrize("bad_entry", [123, None, "", "   "])
def test_invalid_source_priority_entries_rejected(store: Path, bad_entry: object) -> None:
    with pytest.raises(ValidationError, match="source"):
        ForecastArchive(store, source_priority=["feed", bad_entry])  # type: ignore[list-item]


def test_failed_visibility_commit_leaves_no_conflict_audit_entries(
    archive: ForecastArchive, store: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review reproduction: audit entries are written only after the
    visibility commit — a registration that never became visible must not be
    described as a durable ambiguity."""
    setup_forecasts(archive)
    ts = "2026-02-01T12:00:00"
    archive.register_actuals(one_target_frame(100.0), source="a", recorded_at=ts)

    def failing_save(self: object) -> None:
        raise OSError("simulated crash at the visibility commit")

    monkeypatch.setattr("foreledger.actuals.ActualsManifest.save", failing_save)
    with pytest.raises(OSError):
        archive.register_actuals(one_target_frame(110.0), source="b", recorded_at=ts)
    monkeypatch.undo()

    # no entries, no dedup marker — the ambiguity never existed in the archive
    error_log = store / "error_log.txt"
    assert not error_log.exists() or "ambiguous-latest" not in error_log.read_text(encoding="utf-8")
    assert not (store / "conflicts_logged.json").exists()
    assert mae_at_h1(archive).status == "ok"

    # the successful retry commits the conflict AND its audit entry
    archive.register_actuals(one_target_frame(110.0), source="b", recorded_at=ts)
    assert "ambiguous-latest" in error_log.read_text(encoding="utf-8")
    assert mae_at_h1(archive).status == "insufficient"


def test_post_commit_audit_failure_is_typed_and_self_heals(
    archive: ForecastArchive, store: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the audit write fails after the data committed, the caller gets a
    typed partial-commit error and the next registration writes the entry."""
    from foreledger import ConflictLogError

    setup_forecasts(archive)
    ts = "2026-02-01T12:00:00"
    archive.register_actuals(one_target_frame(100.0), source="a", recorded_at=ts)

    def failing_write(conflicts: object) -> None:
        raise OSError("simulated audit write failure")

    monkeypatch.setattr(archive, "_write_conflict_records", failing_write)
    with pytest.raises(ConflictLogError):
        archive.register_actuals(one_target_frame(110.0), source="b", recorded_at=ts)
    monkeypatch.undo()

    # the data is durable and visible (the target is now ambiguous)
    assert mae_at_h1(archive).status == "insufficient"
    # ... and the next successful registration writes the missed entry
    other_target = pd.DataFrame(
        {
            "series_id": ["S1"],
            "target": [ORIGINS[0] + pd.Timedelta(days=2)],
            "value": [50.0],
        }
    )
    archive.register_actuals(other_target, source="a", recorded_at="2026-02-02")
    assert "ambiguous-latest" in (store / "error_log.txt").read_text(encoding="utf-8")


def test_rejected_official_conflict_leaves_audit_files_untouched(
    archive: ForecastArchive, store: Path
) -> None:
    """Validation precedes conflict-log mutation: a batch rejected for
    official stickiness must not leave entries in the integrity channel."""
    setup_forecasts(archive)
    ts = "2026-02-01T12:00:00"
    archive.register_actuals(one_target_frame(100.0), source="a", official=True, recorded_at=ts)
    with pytest.raises(OfficialConflictError):
        archive.register_actuals(one_target_frame(110.0), source="b", official=True, recorded_at=ts)
    assert not (store / "error_log.txt").exists()
    assert not (store / "conflicts_logged.json").exists()
    # and the rejected row was never committed
    assert len(archive._visible_actuals()) == 1


def test_unwritable_error_log_fails_registration_before_commit(store: Path) -> None:
    """The conflict log is a required integrity channel: if it cannot be
    written, the registration that needs it fails cleanly before any append
    — never a successful commit with a silently lost signal."""
    archive = ForecastArchive(store, error_log=store / "logdir")
    (store / "logdir").mkdir()  # a directory: opening it for append fails
    setup_forecasts(archive)
    ts = "2026-02-01T12:00:00"
    archive.register_actuals(one_target_frame(100.0), source="a", recorded_at=ts)

    with pytest.raises(OSError):
        archive.register_actuals(one_target_frame(110.0), source="b", recorded_at=ts)
    # the conflicting row was never committed; the call is retryable
    assert len(archive._visible_actuals()) == 1
    assert mae_at_h1(archive).status == "ok"


def test_failed_designation_append_leaves_nothing_visible(
    archive: ForecastArchive, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup_forecasts(archive)
    ts = "2026-02-01T12:00:00"

    def failing(frame: pd.DataFrame) -> None:
        raise OSError("simulated disk failure")

    monkeypatch.setattr(archive._backend, "append_officials_segment", failing)
    with pytest.raises(OSError):
        archive.register_actuals(
            one_target_frame(100.0), source="rev1", official=True, recorded_at=ts
        )
    # the designation write comes first, so the failed call left no trace
    assert len(archive._visible_actuals()) == 0
    assert mae_at_h1(archive).status == "insufficient"

    monkeypatch.undo()
    archive.register_actuals(one_target_frame(100.0), source="rev1", official=True, recorded_at=ts)
    assert mae_at_h1(archive, basis="official").status == "ok"
    archive.reconcile()
