"""Ingestion invariants: atomicity, idempotency, non-collision, conflicts."""

from __future__ import annotations

import pandas as pd
import pytest
from tests.conftest import ORIGINS, forecast_frame

from forecast_archive import ForecastArchive, IngestConflictError, ValidationError


def total_rows(archive: ForecastArchive) -> int:
    return len(archive.as_of("2100-01-01"))


def test_ingest_is_idempotent(archive: ForecastArchive) -> None:
    frame = forecast_frame(1.0)
    first = archive.ingest(frame, model_id="alpha", model_version="v1")
    assert first.n_runs_written == len(ORIGINS)
    baseline = total_rows(archive)

    for _ in range(3):
        replay = archive.ingest(frame, model_id="alpha", model_version="v1")
        assert replay.n_runs_written == 0
        assert replay.n_runs_skipped == len(ORIGINS)
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
    assert result.n_runs_superseded == len(ORIGINS)
    rows = archive.as_of("2100-01-01")
    assert len(rows) == len(frame)  # superseded runs invisible
    merged = rows.merge(changed, on=["series_id", "origin", "target"], suffixes=("", "_new"))
    assert (merged["value"] == merged["value_new"]).all()


def test_crashed_ingest_leaves_pre_run_state(archive: ForecastArchive) -> None:
    frame = forecast_frame(1.0)
    backend = archive._backend
    original = backend.write_forecast_segment
    calls = {"n": 0}

    def failing(segment: pd.DataFrame) -> str:
        if calls["n"] >= 2:
            raise RuntimeError("simulated crash mid-append")
        calls["n"] += 1
        return original(segment)

    backend.write_forecast_segment = failing  # type: ignore[method-assign]
    with pytest.raises(RuntimeError):
        archive.ingest(frame, model_id="alpha", model_version="v1")

    # no torn run is visible; the archive is at its pre-run state
    assert total_rows(archive) == 0
    assert archive.list_models().empty

    # and the same ingest is re-runnable to completion
    backend.write_forecast_segment = original  # type: ignore[method-assign]
    result = archive.ingest(frame, model_id="alpha", model_version="v1")
    assert result.n_runs_written == len(ORIGINS)
    assert total_rows(archive) == len(frame)


def test_scalar_origin_kwarg(archive: ForecastArchive) -> None:
    one_run = forecast_frame(1.0, origins=ORIGINS[:1]).drop(columns=["origin"])
    result = archive.ingest(one_run, model_id="alpha", model_version="v1", origin=ORIGINS[0])
    assert result.n_runs_written == 1
    assert total_rows(archive) == len(one_run)


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
