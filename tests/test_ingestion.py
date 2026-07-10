"""Ingestion invariants: atomicity, idempotency, non-collision, conflicts."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from tests.conftest import ORIGINS, SERIES, forecast_frame

from foreledger import ForecastArchive, IngestConflictError, ValidationError

RUNS_PER_INGEST = len(ORIGINS) * len(SERIES)  # identity is per (origin, series)


def total_rows(archive: ForecastArchive) -> int:
    return len(archive.as_of("2100-01-01"))


def grain_is_unique(archive: ForecastArchive) -> bool:
    rows = archive.as_of("2100-01-01")
    return not rows.duplicated(
        subset=["model_id", "model_version", "series_id", "origin", "target"]
    ).any()


def test_ingest_is_idempotent(archive: ForecastArchive) -> None:
    frame = forecast_frame(1.0)
    first = archive.ingest(frame, model_id="alpha", model_version="v1")
    assert first.n_runs_written == RUNS_PER_INGEST
    baseline = total_rows(archive)

    for _ in range(3):
        replay = archive.ingest(frame, model_id="alpha", model_version="v1")
        assert replay.n_runs_written == 0
        assert replay.n_runs_skipped == RUNS_PER_INGEST
    assert total_rows(archive) == baseline


def test_parallel_versions_do_not_collide(archive: ForecastArchive) -> None:
    frame = forecast_frame(1.0)
    archive.ingest(frame, model_id="alpha", model_version="v1")
    archive.ingest(frame, model_id="alpha", model_version="v2")

    rows = archive.as_of("2100-01-01")
    one_cell = rows[
        (rows["series_id"] == "S1")
        & (rows["origin"] == ORIGINS[0])
        & (rows["target"] == ORIGINS[0] + pd.Timedelta(days=1))
    ]
    assert len(one_cell) == 2
    assert set(one_cell["model_version"]) == {"v1", "v2"}
    assert total_rows(archive) == 2 * len(frame)


def test_same_identity_different_values_raises_by_default(
    archive: ForecastArchive,
) -> None:
    frame = forecast_frame(1.0)
    archive.ingest(frame, model_id="alpha", model_version="v1")
    changed = frame.copy()
    changed["value"] = changed["value"] + 1.0
    with pytest.raises(IngestConflictError):
        archive.ingest(changed, model_id="alpha", model_version="v1")
    # the failed call left no trace
    assert total_rows(archive) == len(frame)


def test_on_conflict_overwrite_supersedes(archive: ForecastArchive) -> None:
    frame = forecast_frame(1.0)
    archive.ingest(frame, model_id="alpha", model_version="v1")
    changed = frame.copy()
    changed["value"] = changed["value"] + 1.0
    result = archive.ingest(changed, model_id="alpha", model_version="v1", on_conflict="overwrite")
    assert result.n_runs_superseded == RUNS_PER_INGEST
    rows = archive.as_of("2100-01-01")
    assert len(rows) == len(frame)  # superseded runs invisible
    merged = rows.merge(changed, on=["series_id", "origin", "target"], suffixes=("", "_new"))
    assert (merged["value"] == merged["value_new"]).all()


def test_crashed_ingest_leaves_pre_run_state(archive: ForecastArchive) -> None:
    frame = forecast_frame(1.0)
    backend = archive._backend
    original = backend.write_forecast_segment

    def failing(segment: pd.DataFrame) -> str:
        raise RuntimeError("simulated crash mid-append")

    backend.write_forecast_segment = failing  # type: ignore[method-assign]
    with pytest.raises(RuntimeError):
        archive.ingest(frame, model_id="alpha", model_version="v1")

    # no torn run is visible; the archive is at its pre-run state
    assert total_rows(archive) == 0
    assert archive.list_models().empty

    # and the same ingest is re-runnable to completion
    backend.write_forecast_segment = original  # type: ignore[method-assign]
    result = archive.ingest(frame, model_id="alpha", model_version="v1")
    assert result.n_runs_written == RUNS_PER_INGEST
    assert total_rows(archive) == len(frame)


def test_crash_between_segment_write_and_manifest_commit(
    archive: ForecastArchive, monkeypatch: pytest.MonkeyPatch
) -> None:
    frame = forecast_frame(1.0)

    def failing_save(self: object) -> None:
        raise RuntimeError("simulated crash before visibility commit")

    monkeypatch.setattr("foreledger.ingestion.RunManifest.save", failing_save)
    with pytest.raises(RuntimeError):
        archive.ingest(frame, model_id="alpha", model_version="v1")

    # the same live object sees no uncommitted rows: the failed commit never
    # touched its manifest (the candidate was discarded with the exception)
    assert total_rows(archive) == 0
    assert archive.list_models().empty

    monkeypatch.undo()
    result = archive.ingest(frame, model_id="alpha", model_version="v1")
    assert result.n_runs_written == RUNS_PER_INGEST
    assert total_rows(archive) == len(frame)
    assert grain_is_unique(archive)
    archive.reconcile()


def test_failed_overwrite_leaves_prior_run_active(
    archive: ForecastArchive, monkeypatch: pytest.MonkeyPatch
) -> None:
    frame = forecast_frame(1.0)
    archive.ingest(frame, model_id="alpha", model_version="v1")
    changed = frame.copy()
    changed["value"] = changed["value"] + 1.0

    def failing_save(self: object) -> None:
        raise RuntimeError("simulated crash during overwrite commit")

    monkeypatch.setattr("foreledger.ingestion.RunManifest.save", failing_save)
    with pytest.raises(RuntimeError):
        archive.ingest(changed, model_id="alpha", model_version="v1", on_conflict="overwrite")

    # the prior run is still active and serves its original values
    rows = archive.as_of("2100-01-01")
    assert len(rows) == len(frame)
    merged = rows.merge(frame, on=["series_id", "origin", "target"], suffixes=("", "_orig"))
    assert (merged["value"] == merged["value_orig"]).all()


def test_subset_replay_does_not_duplicate_grain(archive: ForecastArchive) -> None:
    full = forecast_frame(1.0)  # series S1, S2, S3
    archive.ingest(full, model_id="alpha", model_version="v1")
    baseline = total_rows(archive)

    subset = full[full["series_id"] == "S1"].reset_index(drop=True)
    replay = archive.ingest(subset, model_id="alpha", model_version="v1")
    assert replay.n_runs_written == 0
    assert total_rows(archive) == baseline
    assert grain_is_unique(archive)


def test_superset_replay_adds_only_new_series(archive: ForecastArchive) -> None:
    partial = forecast_frame(1.0, series=["S1", "S2"])
    archive.ingest(partial, model_id="alpha", model_version="v1")

    full = forecast_frame(1.0)  # adds S3; S1/S2 values identical
    result = archive.ingest(full, model_id="alpha", model_version="v1")
    assert result.n_runs_written == len(ORIGINS)  # only the S3 runs
    assert result.n_runs_skipped == len(ORIGINS) * 2
    assert total_rows(archive) == len(full)
    assert grain_is_unique(archive)


def test_overlapping_replay_with_changed_values_conflicts(archive: ForecastArchive) -> None:
    archive.ingest(forecast_frame(1.0, series=["S1", "S2"]), model_id="alpha", model_version="v1")
    changed = forecast_frame(1.0, series=["S2", "S3"])
    changed.loc[changed["series_id"] == "S2", "value"] += 1.0

    with pytest.raises(IngestConflictError):
        archive.ingest(changed, model_id="alpha", model_version="v1")
    assert grain_is_unique(archive)

    # explicit overwrite supersedes only the overlapping series' runs
    result = archive.ingest(changed, model_id="alpha", model_version="v1", on_conflict="overwrite")
    assert result.n_runs_superseded == len(ORIGINS)  # the S2 runs
    assert grain_is_unique(archive)
    rows = archive.as_of("2100-01-01")
    assert set(rows["series_id"]) == {"S1", "S2", "S3"}
    archive.reconcile()


def test_scalar_origin_kwarg(archive: ForecastArchive) -> None:
    one_run = forecast_frame(1.0, origins=ORIGINS[:1]).drop(columns=["origin"])
    result = archive.ingest(one_run, model_id="alpha", model_version="v1", origin=ORIGINS[0])
    assert result.n_runs_written == len(SERIES)
    assert total_rows(archive) == len(one_run)


def test_timezone_aware_scalar_origin_is_normalized(archive: ForecastArchive) -> None:
    """Review reproduction: column datetimes strip their timezone, but a
    tz-aware SCALAR origin kept it — horizon derivation then crashed with an
    untyped naive-vs-aware TypeError. Scalars must normalize the same way."""
    frame = pd.DataFrame(
        {
            "series_id": ["S1"],
            "target": pd.to_datetime(["2026-01-02"]),
            "value": [1.0],
        }
    )
    archive.ingest(frame, model_id="alpha", model_version="v1", origin="2026-01-01T00:00:00Z")
    rows = archive.as_of("2100-01-01T00:00:00Z")  # tz-aware cutoff works too
    assert len(rows) == 1
    assert rows["origin"].iloc[0] == pd.Timestamp("2026-01-01")
    assert rows["horizon"].iloc[0] == 1


def test_identity_is_caller_supplied_and_validated(archive: ForecastArchive) -> None:
    frame = forecast_frame(1.0)
    with pytest.raises(ValidationError):
        archive.ingest(frame, model_id="", model_version="v1")
    with pytest.raises(ValidationError):
        archive.ingest(frame.drop(columns=["value"]), model_id="m", model_version="v1")


def test_duplicate_grain_rows_rejected(archive: ForecastArchive) -> None:
    frame = forecast_frame(1.0)
    doubled = pd.concat([frame, frame.assign(value=frame["value"] + 1)], ignore_index=True)
    with pytest.raises(ValidationError):
        archive.ingest(doubled, model_id="alpha", model_version="v1")


@pytest.mark.parametrize("bad_series", [None, float("nan"), "", "   "])
def test_missing_or_blank_series_ids_rejected(archive: ForecastArchive, bad_series: object) -> None:
    frame = forecast_frame(1.0, origins=ORIGINS[:1], series=["S1"], horizons=[1])
    frame.loc[0, "series_id"] = bad_series
    with pytest.raises(ValidationError, match="series_id"):
        archive.ingest(frame, model_id="alpha", model_version="v1")


@pytest.mark.parametrize("bad_value", [float("nan"), np.inf, -np.inf, None])
def test_non_finite_forecast_values_rejected(archive: ForecastArchive, bad_value: object) -> None:
    frame = forecast_frame(1.0, origins=ORIGINS[:1], series=["S1"], horizons=[1])
    frame.loc[0, "value"] = bad_value
    with pytest.raises(ValidationError):
        archive.ingest(frame, model_id="alpha", model_version="v1")


def test_non_numeric_values_raise_the_typed_error(archive: ForecastArchive) -> None:
    """Review reproduction: pd.to_numeric's ValueError leaked past the public
    ValidationError contract for non-numeric forecast and actual values."""
    frame = forecast_frame(1.0, origins=ORIGINS[:1], series=["S1"], horizons=[1])
    frame["value"] = "not-a-number"
    with pytest.raises(ValidationError, match="non-numeric"):
        archive.ingest(frame, model_id="alpha", model_version="v1")

    actuals = pd.DataFrame(
        {"series_id": ["S1"], "target": pd.to_datetime(["2026-01-02"]), "value": ["oops"]}
    )
    with pytest.raises(ValidationError, match="non-numeric"):
        archive.register_actuals(actuals)


def test_content_hash_payload_is_pinned() -> None:
    """The hash format is persisted in runs.json and drives idempotency
    against existing archives — it must never drift across refactors."""
    import hashlib

    from foreledger.ingestion import content_hash

    group = pd.DataFrame(
        {
            "series_id": ["B", "A"],
            "target": pd.to_datetime(["2026-01-03", "2026-01-02"]),
            "value": pd.Series([2.5, 1.0], dtype="float64"),
        }
    )
    # independent reference: the original per-row formulation
    ordered = group.sort_values(["series_id", "target"])
    digest = hashlib.sha256()
    for s, t, v in zip(ordered["series_id"], ordered["target"], ordered["value"], strict=True):
        digest.update(f"{s}\x1f{t.isoformat()}\x1f{v!r}\n".encode())
    assert content_hash(group) == digest.hexdigest()


def test_nixtla_adapter_equals_explicit_ingest(tmp_path: object) -> None:
    from pathlib import Path

    base = Path(str(tmp_path))
    frame = forecast_frame(1.0)
    cv_frame = frame.rename(
        columns={"series_id": "unique_id", "target": "ds", "origin": "cutoff"}
    ).rename(columns={"value": "alpha"})

    explicit = ForecastArchive(base / "explicit")
    explicit.ingest(frame, model_id="alpha", model_version="v1")
    adapted = ForecastArchive(base / "adapted")
    adapted.ingest_nixtla(cv_frame, model_id="alpha", model_version="v1")

    key = ["model_id", "model_version", "series_id", "origin", "target"]
    left = explicit.as_of("2100-01-01")[key + ["value", "horizon"]]
    right = adapted.as_of("2100-01-01")[key + ["value", "horizon"]]
    pd.testing.assert_frame_equal(
        left.sort_values(key).reset_index(drop=True),
        right.sort_values(key).reset_index(drop=True),
    )


def test_star_series_id_is_reserved(archive: ForecastArchive) -> None:
    """'*' names the pooled summary cells; a real series called '*' would
    collide with them and make pooled queries ambiguous."""
    frame = forecast_frame(1.0)
    frame.loc[0, "series_id"] = "*"
    with pytest.raises(ValidationError, match="reserved"):
        archive.ingest(frame, model_id="alpha", model_version="v1")


def test_numeric_datetime_columns_are_rejected(archive: ForecastArchive) -> None:
    """A yyyymmdd int column would silently become 1970 epoch dates."""
    frame = forecast_frame(1.0)
    frame["target"] = 20260601
    with pytest.raises(ValidationError, match="numeric"):
        archive.ingest(frame, model_id="alpha", model_version="v1")


def test_object_column_hiding_numbers_is_rejected(archive: ForecastArchive) -> None:
    """Self-review reproduction: an OBJECT-dtype column of ints (typical from
    JSON/Excel/mixed input) slips past a dtype-only guard and pd.to_datetime
    reads it as 1970 epoch nanoseconds — it must be rejected too."""
    frame = forecast_frame(1.0)
    frame["target"] = pd.Series([20260601] * len(frame), index=frame.index, dtype=object)
    with pytest.raises(ValidationError, match="numeric"):
        archive.ingest(frame, model_id="alpha", model_version="v1")
