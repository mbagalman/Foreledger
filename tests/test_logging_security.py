"""Security: no forecast/actual values, series, or model identifiers in logs
at default (INFO) verbosity — a series or model name can itself be sensitive.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import pytest

from forecast_archive import ForecastArchive

SENSITIVE_SERIES = "SECRET_REVENUE_SERIES"
SENSITIVE_MODEL = "SECRET_MODEL_NAME"
SENSITIVE_VALUE = 123456.789


def test_no_identifiers_or_values_at_info(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    origin = pd.Timestamp("2026-01-01")
    frame = pd.DataFrame(
        {
            "series_id": [SENSITIVE_SERIES],
            "origin": [origin],
            "target": [origin + pd.Timedelta(days=1)],
            "value": [SENSITIVE_VALUE],
        }
    )
    actuals = pd.DataFrame(
        {
            "series_id": [SENSITIVE_SERIES],
            "target": [origin + pd.Timedelta(days=1)],
            "value": [SENSITIVE_VALUE + 1],
        }
    )

    with caplog.at_level(logging.INFO):
        archive = ForecastArchive(tmp_path / "store")
        archive.ingest(frame, model_id=SENSITIVE_MODEL, model_version="vSECRET")
        archive.register_actuals(actuals, official=True)
        archive.set_champion(SENSITIVE_MODEL, "vSECRET")
        archive.accuracy_at_horizon(1, model_id=SENSITIVE_MODEL, model_version="vSECRET")
        archive.list_models()

    text = caplog.text
    assert SENSITIVE_SERIES not in text
    assert SENSITIVE_MODEL not in text
    assert "vSECRET" not in text
    assert str(SENSITIVE_VALUE) not in text
