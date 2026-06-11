"""Store lifecycle: format-version gate, refusal to re-initialize, persistence,
and migration of legacy manifests."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest
from tests.conftest import ORIGINS, actuals_frame, forecast_frame

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


def _downgrade_meta_to_v1(store: Path) -> None:
    meta_path = store / "archive_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta["format_version"] = 1
    meta_path.write_text(json.dumps(meta), encoding="utf-8")


def test_format1_actuals_store_is_migrated(store: Path) -> None:
    """A format-1 store (directory-scan visibility) migrates on open: visible
    segments adopt losslessly, dangling officials (the leftovers of failed
    pre-manifest calls) stay inert, and the format version is bumped so
    format-1 readers refuse the store instead of scanning uncommitted files."""
    archive = ForecastArchive(store)
    archive.ingest(forecast_frame(1.0), model_id="alpha", model_version="v1")
    archive.register_actuals(actuals_frame(), source="feed", recorded_at="2026-02-01")
    target = actuals_frame()["target"].iloc[0]
    archive.mark_official(series="S1", target=target, source="feed")
    expected = archive.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    officials_before = len(archive._visible_officials())

    # simulate format 1: no visibility manifest, directory-scan semantics,
    # plus a dangling officials segment from a failed pre-manifest call
    (store / "actuals_manifest.json").unlink()
    _downgrade_meta_to_v1(store)
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
    from foreledger import FORMAT_VERSION

    meta = json.loads((store / "archive_meta.json").read_text(encoding="utf-8"))
    assert meta["format_version"] == FORMAT_VERSION  # format-1 readers refuse this store
    reopened.reconcile()


def test_format2_store_missing_manifest_is_corrupt(store: Path) -> None:
    """At format 2 the visibility manifest is mandatory: its absence is
    corruption, never a license to adopt whatever segment files exist."""
    archive = ForecastArchive(store)
    archive.ingest(forecast_frame(1.0), model_id="alpha", model_version="v1")
    archive.register_actuals(actuals_frame(), recorded_at="2026-02-01")
    (store / "actuals_manifest.json").unlink()
    with pytest.raises(StoreFormatError, match="manifest"):
        ForecastArchive(store)


def test_failed_first_registration_is_not_resurrected_on_reopen(
    store: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review reproduction: the very first registration fails at the
    visibility commit; reopening the store must not adopt the failed call's
    segment files into visibility."""
    archive = ForecastArchive(store)
    archive.ingest(
        forecast_frame(1.0, origins=ORIGINS[:1], series=["S1"], horizons=[1]),
        model_id="alpha",
        model_version="v1",
    )
    frame = pd.DataFrame(
        {
            "series_id": ["S1"],
            "target": [ORIGINS[0] + pd.Timedelta(days=1)],
            "value": [100.0],
        }
    )

    def failing_save(self: object) -> None:
        raise OSError("simulated crash at the visibility commit")

    monkeypatch.setattr("foreledger.actuals.ActualsManifest.save", failing_save)
    with pytest.raises(OSError):
        archive.register_actuals(frame, source="rev1", official=True)
    monkeypatch.undo()

    reopened = ForecastArchive(store)
    for basis in ("latest", "official"):
        result = reopened.accuracy_at_horizon(1, basis=basis, model_id="alpha", model_version="v1")
        assert result.status == "insufficient", basis

    # and the same public call retries cleanly on the reopened handle
    reopened.register_actuals(frame, source="rev1", official=True)
    assert (
        reopened.accuracy_at_horizon(
            1, basis="official", model_id="alpha", model_version="v1"
        ).status
        == "ok"
    )
    reopened.reconcile()


def test_missing_committed_segment_is_a_typed_error(store: Path) -> None:
    """Review reproduction: deleting a committed raw segment must surface as
    a typed error on every route — the disposable summary must never stay
    authoritative over missing raw data."""
    archive = ForecastArchive(store)
    archive.ingest(forecast_frame(1.0), model_id="alpha", model_version="v1")
    archive.register_actuals(actuals_frame(), recorded_at="2026-02-01")
    assert archive.accuracy_at_horizon(1, model_id="alpha", model_version="v1").status == "ok"

    manifest = json.loads((store / "actuals_manifest.json").read_text(encoding="utf-8"))
    (store / manifest["actuals"][0]).unlink()

    with pytest.raises(StoreFormatError, match="missing"):
        archive.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    with pytest.raises(StoreFormatError, match="missing"):
        ForecastArchive(store).accuracy_at_horizon(1, model_id="alpha", model_version="v1")


def test_tampered_manifest_cannot_read_outside_the_store(store: Path, tmp_path: Path) -> None:
    """Review reproduction: manifest tokens are canonical relative paths; an
    absolute path (or any malformed shape) is a typed corruption error, never
    resolved against the filesystem."""
    archive = ForecastArchive(store)
    archive.ingest(forecast_frame(1.0), model_id="alpha", model_version="v1")
    archive.register_actuals(actuals_frame(), recorded_at="2026-02-01")

    outside = tmp_path / "outside.parquet"
    archive._visible_actuals().to_parquet(outside, index=False)
    manifest_path = store / "actuals_manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["actuals"] = [str(outside)]
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(StoreFormatError):
        ForecastArchive(store)

    manifest_path.write_text('{"actuals": "actuals", "officials": []}', encoding="utf-8")
    with pytest.raises(StoreFormatError):
        ForecastArchive(store)


def _populated(store: Path) -> ForecastArchive:
    archive = ForecastArchive(store)
    archive.ingest(forecast_frame(1.0), model_id="alpha", model_version="v1")
    archive.register_actuals(actuals_frame(), recorded_at="2026-02-01")
    return archive


def test_deleting_runs_manifest_is_a_typed_error(store: Path) -> None:
    """Review reproduction: a deleted runs.json must be corruption on the
    live handle and on reopen — never a silently empty forecast archive —
    and must not cause integrity records to be pruned."""
    archive = _populated(store)
    assert archive.accuracy_at_horizon(1, model_id="alpha", model_version="v1").status == "ok"
    integrity_before = (store / "segment_integrity.json").read_text(encoding="utf-8")

    (store / "runs.json").unlink()
    with pytest.raises(StoreFormatError, match="run manifest"):
        archive.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    with pytest.raises(StoreFormatError, match="run manifest"):
        ForecastArchive(store)
    # the failed opens never rewrote the integrity registry
    assert (store / "segment_integrity.json").read_text(encoding="utf-8") == integrity_before


def test_unmanifested_forecast_file_is_invisible(store: Path) -> None:
    """Review reproduction: a stray Parquet file carrying an active run_id
    must not be readable — the run manifest is the single visibility point."""
    archive = _populated(store)
    baseline = archive.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    rows_before = len(archive.as_of("2100-01-01"))

    runs = json.loads((store / "runs.json").read_text(encoding="utf-8"))
    committed = store / runs["runs"][0]["segment"]
    (store / "forecasts" / "stray.parquet").write_bytes(committed.read_bytes())

    assert len(archive.as_of("2100-01-01")) == rows_before
    raw_view = ForecastArchive(store)
    (raw_view.store / "summary" / "summary.parquet").unlink()
    raw = raw_view.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    assert raw.served_from == "raw"
    assert raw.n == baseline.n  # the stray file inflated nothing


def test_superseded_segment_deletion_is_detected(store: Path) -> None:
    """Review reproduction: superseded history is still part of the
    append-only record — deleting its segment file must fail loudly, on
    queries and on reconcile, even after a reopen."""
    archive = ForecastArchive(store)
    frame = forecast_frame(1.0)
    archive.ingest(frame, model_id="alpha", model_version="v1")
    old_segment = json.loads((store / "runs.json").read_text(encoding="utf-8"))["runs"][0][
        "segment"
    ]
    changed = frame.copy()
    changed["value"] = changed["value"] + 1.0
    archive.ingest(changed, model_id="alpha", model_version="v1", on_conflict="overwrite")
    archive.register_actuals(actuals_frame(), recorded_at="2026-02-01")

    reopened = ForecastArchive(store)  # reopen must not prune the old fingerprint
    (store / old_segment).unlink()
    with pytest.raises(StoreFormatError, match="missing"):
        reopened.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    with pytest.raises(StoreFormatError, match="missing"):
        reopened.reconcile()


def test_v2_store_migrates_and_rebless_invalidates_summary(store: Path) -> None:
    """Review reproduction (recovery path): adopting changed content must
    invalidate the old summary — the re-blessed bytes change the recorded
    fingerprints, which are bound into the summary token."""
    archive = _populated(store)
    stale = archive.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    assert stale.served_from == "summary"

    # tamper a committed actuals segment, then simulate a v2 store (no
    # integrity registry) so reopen runs the adopting migration
    manifest = json.loads((store / "actuals_manifest.json").read_text(encoding="utf-8"))
    token = manifest["actuals"][0]
    tampered = pd.read_parquet(store / token)
    tampered["actual_value"] = tampered["actual_value"] + 100.0
    tampered.to_parquet(store / token, index=False)
    (store / "segment_integrity.json").unlink()
    meta_path = store / "archive_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta["format_version"] = 2
    meta_path.write_text(json.dumps(meta), encoding="utf-8")

    reopened = ForecastArchive(store)
    result = reopened.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    # never the stale pre-tamper summary: the value reflects current raw
    assert result.value != stale.value
    reopened.reconcile()
    from foreledger import FORMAT_VERSION

    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    assert meta["format_version"] == FORMAT_VERSION


def test_deleting_manifest_on_a_live_handle_is_a_typed_error(store: Path) -> None:
    """Review reproduction: a live handle must treat a deleted mandatory
    manifest exactly like reopen does — typed corruption, never an
    empty-archive view that silently hides committed actuals."""
    archive = ForecastArchive(store)
    archive.ingest(forecast_frame(1.0), model_id="alpha", model_version="v1")
    archive.register_actuals(actuals_frame(), recorded_at="2026-02-01")
    assert archive.accuracy_at_horizon(1, model_id="alpha", model_version="v1").status == "ok"

    (store / "actuals_manifest.json").unlink()
    with pytest.raises(StoreFormatError, match="manifest"):
        archive.accuracy_at_horizon(1, model_id="alpha", model_version="v1")


def test_inplace_modified_segment_is_a_typed_error(store: Path) -> None:
    """Review reproduction: replacing a committed segment's content under the
    same filename must fail loudly — the stale summary must never keep
    serving over modified raw data."""
    archive = ForecastArchive(store)
    archive.ingest(forecast_frame(1.0), model_id="alpha", model_version="v1")
    archive.register_actuals(actuals_frame(), recorded_at="2026-02-01")
    assert archive.accuracy_at_horizon(1, model_id="alpha", model_version="v1").status == "ok"

    manifest = json.loads((store / "actuals_manifest.json").read_text(encoding="utf-8"))
    token = manifest["actuals"][0]
    tampered = pd.read_parquet(store / token)
    tampered["actual_value"] = tampered["actual_value"] + 100.0
    tampered.to_parquet(store / token, index=False)

    with pytest.raises(StoreFormatError, match="fingerprint"):
        archive.accuracy_at_horizon(1, model_id="alpha", model_version="v1")
    # a fresh handle fails the same way (the recorded fingerprint survives)
    with pytest.raises(StoreFormatError, match="fingerprint"):
        ForecastArchive(store).accuracy_at_horizon(1, model_id="alpha", model_version="v1")


def test_inplace_modified_forecast_segment_is_a_typed_error(store: Path) -> None:
    archive = ForecastArchive(store)
    archive.ingest(forecast_frame(1.0), model_id="alpha", model_version="v1")
    archive.register_actuals(actuals_frame(), recorded_at="2026-02-01")

    runs = json.loads((store / "runs.json").read_text(encoding="utf-8"))
    token = runs["runs"][0]["segment"]
    tampered = pd.read_parquet(store / token)
    tampered["value"] = tampered["value"] + 1.0
    tampered.to_parquet(store / token, index=False)

    with pytest.raises(StoreFormatError, match="fingerprint"):
        archive.accuracy_at_horizon(1, model_id="alpha", model_version="v1")


def test_reconcile_verifies_full_content_hash(store: Path) -> None:
    """Queries probe size/mtime; reconcile() audits the sha256 — a registry
    whose recorded hash disagrees with the bytes on disk must fail."""
    archive = ForecastArchive(store)
    archive.ingest(forecast_frame(1.0), model_id="alpha", model_version="v1")
    archive.register_actuals(actuals_frame(), recorded_at="2026-02-01")
    archive.reconcile()

    registry_path = store / "segment_integrity.json"
    registry = json.loads(registry_path.read_text(encoding="utf-8"))
    token = next(iter(registry["segments"]))
    registry["segments"][token]["sha256"] = "0" * 64
    registry_path.write_text(json.dumps(registry), encoding="utf-8")

    with pytest.raises(StoreFormatError, match="hash"):
        archive.reconcile()


@pytest.mark.parametrize("bad_version", [0, -1, True, 1.5, "1"])
def test_unknown_format_versions_are_refused(store: Path, bad_version: object) -> None:
    """Only the current version opens and only version 1 migrates; unknown,
    boolean, or non-integral values are corruption, never reinterpreted."""
    ForecastArchive(store)
    meta_path = store / "archive_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta["format_version"] = bad_version
    meta_path.write_text(json.dumps(meta), encoding="utf-8")
    with pytest.raises(StoreFormatError):
        ForecastArchive(store)
    # the metadata was not rewritten
    assert json.loads(meta_path.read_text(encoding="utf-8"))["format_version"] == bad_version


def test_malformed_legacy_record_is_a_typed_error(store: Path) -> None:
    ForecastArchive(store)
    (store / "runs.json").write_text('{"runs": [{"series_key": "x"}]}', encoding="utf-8")
    with pytest.raises(StoreFormatError, match="legacy"):
        ForecastArchive(store)
