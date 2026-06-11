"""Store lifecycle: format-version gate, refusal to re-initialize, persistence,
and migration of legacy manifests."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest
from tests.conftest import actuals_frame, forecast_frame

from foreledger import ForecastArchive, StoreFormatError, ValidationError


def test_newer_format_version_is_refused(store: Path) -> None:
    ForecastArchive(store)  # creates format version 1
    meta_path = store / "archive_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta["format_version"] = 99
    meta_path.write_text(json.dumps(meta), encoding="utf-8")

    with pytest.raises(StoreFormatError, match="newer"):
        ForecastArchive(store)


def test_non_archive_directory_is_refused(tmp_path: Path) -> None:
    target = tmp_path / "precious"
    target.mkdir()
    (target / "data.csv").write_text("not an archive", encoding="utf-8")
    with pytest.raises(StoreFormatError):
        ForecastArchive(target)
    # nothing was touched
    assert (target / "data.csv").read_text(encoding="utf-8") == "not an archive"


def test_directory_with_arbitrary_tmp_file_is_refused(tmp_path: Path) -> None:
    """Only the archive's own init plumbing is exempt from the non-archive
    check — a user's .tmp file is still user content."""
    target = tmp_path / "scratchpad"
    target.mkdir()
    (target / "notes.tmp").write_text("user data", encoding="utf-8")
    with pytest.raises(StoreFormatError):
        ForecastArchive(target)
    assert (target / "notes.tmp").read_text(encoding="utf-8") == "user data"
    assert not (target / "archive_meta.json").exists()


def test_lone_reserved_tmp_name_without_lock_is_refused(tmp_path: Path) -> None:
    """The reserved metadata temp name is trusted only alongside the lock
    file: by itself it is user content, not evidence of an in-progress init."""
    target = tmp_path / "userdir"
    target.mkdir()
    (target / "archive_meta.json.tmp").write_text("user data", encoding="utf-8")
    with pytest.raises(StoreFormatError):
        ForecastArchive(target)
    assert (target / "archive_meta.json.tmp").read_text(encoding="utf-8") == "user data"
    assert not (target / "archive_meta.json").exists()


def test_corrupt_metadata_is_a_typed_error(store: Path) -> None:
    ForecastArchive(store)
    (store / "archive_meta.json").write_text("{not json", encoding="utf-8")
    with pytest.raises(StoreFormatError):
        ForecastArchive(store)


def test_archive_persists_across_reopen(store: Path) -> None:
    first = ForecastArchive(store)
    first.ingest(forecast_frame(1.0), model_id="alpha", model_version="v1")
    first.register_actuals(actuals_frame())
    expected = first.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    del first

    reopened = ForecastArchive(store)
    result = reopened.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    assert result.value == expected.value
    assert result.n == expected.n
    reopened.reconcile()


def test_unknown_backend_is_refused(store: Path) -> None:
    with pytest.raises(ValidationError, match="snowflake"):
        ForecastArchive(store, backend="bigquery")


def _write_legacy_store(store: Path) -> int:
    """Hand-build a pre-57930cc store: one run record per origin batch with a
    series_key hash and a single run_id shared by every series in the segment."""
    ForecastArchive(store)  # lay down format-1 metadata and directories
    frame = forecast_frame(1.0)
    frame["model_id"] = "alpha"
    frame["model_version"] = "v1"
    frame["horizon"] = (frame["target"] - frame["origin"]).dt.days

    records = []
    for origin, group in frame.groupby("origin"):
        run_id = f"legacy{origin.strftime('%Y%m%d')}"
        tagged = group.copy()
        tagged["run_id"] = run_id
        tagged["ingested_at"] = pd.Timestamp("2026-06-01")
        tagged.to_parquet(store / "forecasts" / f"{run_id}.parquet", index=False)
        series_key = hashlib.sha256(
            "\x1f".join(sorted(group["series_id"].unique())).encode()
        ).hexdigest()
        records.append(
            {
                "run_id": run_id,
                "model_id": "alpha",
                "model_version": "v1",
                "origin": origin.isoformat(),
                "series_key": series_key,
                "content_hash": "legacy",
                "segment": f"forecasts/{run_id}.parquet",
                "ingested_at": "2026-06-01T00:00:00",
                "superseded": False,
            }
        )
    (store / "runs.json").write_text(json.dumps({"runs": records}), encoding="utf-8")
    return len(frame)


def test_legacy_series_set_manifest_is_migrated(store: Path) -> None:
    """A pre-57930cc archive (per series-set run records) opens cleanly: the
    manifest migrates to per-series records with no data loss and the store
    behaves like a modern archive afterwards."""
    n_rows = _write_legacy_store(store)

    archive = ForecastArchive(store)
    rows = archive.as_of("2100-01-01")
    assert len(rows) == n_rows
    assert not rows.duplicated(
        subset=["model_id", "model_version", "series_id", "origin", "target"]
    ).any()

    # migrated records are per-series: replaying the same data is a no-op
    replay = archive.ingest(forecast_frame(1.0), model_id="alpha", model_version="v1")
    assert replay.n_runs_written == 0
    archive.register_actuals(actuals_frame())
    archive.reconcile()

    # reopening does not re-migrate and loses nothing
    reopened = ForecastArchive(store)
    assert len(reopened.as_of("2100-01-01")) == n_rows


def test_unrecognized_manifest_shape_is_a_typed_error(store: Path) -> None:
    ForecastArchive(store)
    (store / "runs.json").write_text('{"runs": [{"unknown_field": 1}]}', encoding="utf-8")
    with pytest.raises(StoreFormatError):
        ForecastArchive(store)


def test_pre_manifest_actuals_store_is_adopted(store: Path) -> None:
    """Stores written before actuals visibility was manifest-committed adopt
    their visible segments losslessly; dangling officials (the leftovers of
    failed pre-manifest calls) stay inert."""
    archive = ForecastArchive(store)
    archive.ingest(forecast_frame(1.0), model_id="alpha", model_version="v1")
    archive.register_actuals(actuals_frame(), source="feed", recorded_at="2026-02-01")
    target = actuals_frame()["target"].iloc[0]
    archive.mark_official(series="S1", target=target, source="feed")
    expected = archive.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    officials_before = len(archive._visible_officials())

    # simulate a pre-manifest store: drop the manifest, add a dangling
    # officials segment referencing an actual that was never registered
    (store / "actuals_manifest.json").unlink()
    dangling = pd.DataFrame(
        {
            "series_id": ["S1"],
            "target": [pd.Timestamp("2030-01-01")],
            "source": ["ghost"],
            "actual_recorded_at": [pd.Timestamp("2030-01-01")],
            "designated_at": [pd.Timestamp("2030-01-01")],
        }
    )
    dangling.to_parquet(store / "officials" / "dangling.parquet", index=False)

    reopened = ForecastArchive(store)
    result = reopened.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    assert result.value == expected.value
    assert result.n == expected.n
    assert len(reopened._visible_officials()) == officials_before  # dangling excluded
    reopened.reconcile()


def test_malformed_legacy_record_is_a_typed_error(store: Path) -> None:
    ForecastArchive(store)
    (store / "runs.json").write_text('{"runs": [{"series_key": "x"}]}', encoding="utf-8")
    with pytest.raises(StoreFormatError, match="legacy"):
        ForecastArchive(store)
