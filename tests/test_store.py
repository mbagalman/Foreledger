"""Store lifecycle: format-version gate, refusal to re-initialize, persistence."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from tests.conftest import actuals_frame, forecast_frame

from forecast_archive import ForecastArchive, StoreFormatError, ValidationError


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
