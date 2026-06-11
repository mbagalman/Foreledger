"""Production Forecast Archive.

Ingests recurring forecast runs from multiple models and versions, stores them
as a durable append-only Parquet archive alongside a revisable actuals log,
and answers horizon-keyed accuracy, cross-model/version comparison, and
bitemporal ``as_of`` queries through a dialect-aware backend seam.

See AGENTS.md and docs/ for architecture, decisions (ADR-001..007), and the
phased implementation plan.
"""

__version__ = "0.1.0.dev0"
