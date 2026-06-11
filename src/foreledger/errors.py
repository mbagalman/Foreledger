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

    Raised at open (the archive never silently re-initializes an existing
    directory — that would discard history) and from any later read or write
    whose integrity probe finds committed data deleted or modified externally.
    """


class StoreLockTimeout(ForecastArchiveError):
    """The cross-process store lock could not be acquired in time.

    Another writer is holding it (or crashed while holding it on a platform
    where locks outlive the process — OS locks normally release on exit).
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


class ConflictLogError(ForecastArchiveError):
    """A registration committed durably, but writing its required conflict
    audit records failed afterwards.

    The data is safe and visible; the audit entries are written by the next
    successful registration (the deduplication marker only advances with the
    entries). Raised so the missing integrity signal is never silent.
    """


class UnknownMetricError(ForecastArchiveError):
    """The requested metric name is not built in or registered."""


class ReconciliationError(ForecastArchiveError):
    """The stored summary disagrees with a recomputation from raw.

    This is a defect, never a tolerance (ADR-003).
    """
