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


class PartialCommitError(ForecastArchiveError):
    """A write's data committed durably and is visible, but the post-commit
    integrity-journal confirmation failed.

    The archive is not corrupt: the journal still holds the commit in its
    staged/pending form, and the next locked write — including an exact
    retry of this call, which is then a no-op — or the next open completes
    the bookkeeping automatically. Raised instead of the underlying error so
    the caller is never told a committed write failed outright.
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

    This is a defect, never a tolerance (ADR-003). A *transient* inability to
    capture a stable state under concurrent writers is a different condition
    and raises :class:`ReconciliationConflict` instead, so a caller keying on
    the exception type never mistakes contention for a data defect.
    """


class ReconciliationConflict(ForecastArchiveError):
    """``reconcile()`` could not verify against a stable state because
    concurrent writers kept committing.

    Not a defect and not caught by ``except ReconciliationError``: the store
    is not known to disagree with raw — simply retry when writes settle.
    """
