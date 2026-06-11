"""Typed errors raised by Foreledger.

Every failure mode the library promises to surface loudly (format
incompatibility, identity conflicts, summary divergence) has its own class,
so callers can handle each without string-matching messages.
"""

from __future__ import annotations


class ForecastArchiveError(Exception):
    """Base class for all foreledger errors."""


class StoreFormatError(ForecastArchiveError):
    """The store is corrupt, incompatible, or written by a newer format version.

    Raised on open; the archive never silently re-initializes an existing
    directory (that would discard history).
    """


class ValidationError(ForecastArchiveError):
    """User-supplied frame, mapping, or argument failed validation."""


class IngestConflictError(ForecastArchiveError):
    """A run with the same identity but different values already exists.

    Raised under ``on_conflict="error"`` (the default). Pass
    ``on_conflict="overwrite"`` to supersede the prior run explicitly.
    """


class OfficialConflictError(ForecastArchiveError):
    """A different actual is already designated official for this target.

    The official designation is sticky; change it only via the explicit
    ``mark_official`` call.
    """


class UnknownMetricError(ForecastArchiveError):
    """The requested metric name is not built in or registered."""


class ReconciliationError(ForecastArchiveError):
    """The stored summary disagrees with a recomputation from raw.

    This is a defect, never a tolerance (ADR-003).
    """
