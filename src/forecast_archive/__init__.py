"""Production Forecast Archive.

Ingests recurring forecast runs from multiple models and versions, stores them
as a durable append-only Parquet archive alongside a revisable actuals log,
and answers horizon-keyed accuracy, cross-model/version comparison, and
bitemporal ``as_of`` queries through a dialect-aware backend seam.

See AGENTS.md and docs/ for architecture, decisions (ADR-001..007), and the
phased implementation plan.
"""

from .archive import ForecastArchive
from .errors import (
    ForecastArchiveError,
    IngestConflictError,
    OfficialConflictError,
    ReconciliationError,
    StoreFormatError,
    UnknownMetricError,
    ValidationError,
)
from .ingestion import IngestResult
from .results import AccuracyCurve, AccuracyResult
from .schema import FORMAT_VERSION

__version__ = "0.1.0.dev0"

__all__ = [
    "FORMAT_VERSION",
    "AccuracyCurve",
    "AccuracyResult",
    "ForecastArchive",
    "ForecastArchiveError",
    "IngestConflictError",
    "IngestResult",
    "OfficialConflictError",
    "ReconciliationError",
    "StoreFormatError",
    "UnknownMetricError",
    "ValidationError",
    "__version__",
]
