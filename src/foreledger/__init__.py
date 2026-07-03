"""Foreledger — a durable ledger for your forecasts.

Foreledger ingests recurring forecast runs from multiple models and versions,
stores them as an append-only Parquet archive alongside a revisable actuals
log, and answers the questions production forecasting teams actually ask:

- How accurate are we at each horizon? (``accuracy_at_horizon`` / ``accuracy_curve``)
- Which model/version wins, and by how much vs. the champion? (``compare_models``)
- What did we know at the time? (``as_of`` — no hindsight leakage)

Start with :class:`ForecastArchive`; everything else hangs off it. See
``docs/quickstart.md`` for a guided tour and ``AGENTS.md`` +
``docs/internal/`` for architecture decisions (ADR-001..007) and the
phased plan.
"""

from .archive import ForecastArchive
from .errors import (
    ConflictLogError,
    ForecastArchiveError,
    IngestConflictError,
    OfficialConflictError,
    PartialCommitError,
    ReconciliationConflict,
    ReconciliationError,
    StoreFormatError,
    StoreLockTimeout,
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
    "ConflictLogError",
    "ForecastArchive",
    "ForecastArchiveError",
    "IngestConflictError",
    "IngestResult",
    "OfficialConflictError",
    "PartialCommitError",
    "ReconciliationConflict",
    "ReconciliationError",
    "StoreFormatError",
    "StoreLockTimeout",
    "UnknownMetricError",
    "ValidationError",
    "__version__",
]
