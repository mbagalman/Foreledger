"""The public ``ForecastArchive`` facade — Foreledger's one entry point.

Open or create an archive at a local path (v1: DuckDB-over-Parquet behind the
dialect-aware seam), push forecast runs and actuals into it, and ask the
questions the archive exists to answer: accuracy by horizon, model-vs-model
comparison, and what-did-we-know-when (``as_of``) slices.

Everything observable goes through this class; the modules behind it
(ingestion, actuals, summary, query, backend) are implementation layers.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import logging
import os
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pandas as pd

from .actuals import (
    canonicalize_actuals,
    check_official_registration,
    dedup_against_log,
    find_actual_row,
    resolve_effective_latest,
    resolve_effective_official,
)
from .backend import Backend, ForecastFilter, create_backend
from .errors import ReconciliationError, StoreFormatError, ValidationError
from .ingestion import (
    IngestResult,
    RunManifest,
    RunRecord,
    canonicalize_forecasts,
    commit_runs,
    content_hash,
    load_manifest_entries,
    plan_runs,
)
from .locking import StoreLock
from .metrics import DEFAULT_METRIC_TIMEOUT, MetricFn, MetricRegistry
from .query import Evaluator, Period
from .results import AccuracyCurve, AccuracyResult
from .schema import FORMAT_VERSION
from .summary import build_summary

logger = logging.getLogger("foreledger")

_META_FILE = "archive_meta.json"


class ForecastArchive:
    """A durable archive of forecast runs with horizon-keyed evaluation.

    Parameters
    ----------
    store:
        Directory for the archive (created if missing/empty). An existing
        non-archive directory raises :class:`StoreFormatError` — the library
        never silently re-initializes.
    backend:
        Only ``"duckdb"`` ships in v1; ``"snowflake"`` is the v1.1 fast-follow.
    source_priority:
        Ordered source labels (highest first) used to resolve same-timestamp
        actual conflicts.
    error_log:
        Destination file for unresolved-conflict errors; defaults to
        ``<store>/error_log.txt``.
    metric_timeout:
        Wall-clock budget (seconds) for one registered-metric evaluation;
        built-in metrics are not subject to it.

    Example
    -------
    >>> archive = ForecastArchive("./my_archive")
    >>> archive.ingest(runs_df, model_id="prophet", model_version="2.1")
    >>> archive.register_actuals(actuals_df, source="warehouse")
    >>> archive.accuracy_curve(metric="MAE", model_id="prophet", model_version="2.1")
    """

    def __init__(
        self,
        store: str | Path,
        backend: str = "duckdb",
        source_priority: list[str] | None = None,
        error_log: str | Path | None = None,
        metric_timeout: float = DEFAULT_METRIC_TIMEOUT,
    ) -> None:
        self.store = Path(store)
        # cheap refusal before creating anything (incl. the lock file) inside
        # a directory that is not ours
        self._refuse_non_archive_dir()
        self.store.mkdir(parents=True, exist_ok=True)
        with self._lock():
            # serialized: concurrent constructors of a new store would race
            # on the shared metadata temp file otherwise
            self._check_or_init_store()
        self._backend: Backend = create_backend(backend, self.store)
        self._manifest_path = self.store / "runs.json"
        self._manifest = self._load_manifest()
        self._manifest_key = self._manifest_file_key()
        self._champions_path = self.store / "champions.json"
        self._conflicts_logged_path = self.store / "conflicts_logged.json"
        self._error_log = (
            Path(error_log) if error_log is not None else (self.store / "error_log.txt")
        )
        self._source_priority = list(source_priority) if source_priority else None
        self._registry = MetricRegistry(timeout=metric_timeout)
        self._evaluator = Evaluator(
            backend=self._backend,
            # late-bound and freshness-checked: reads see other handles' commits
            active_run_ids=lambda: self._current_manifest().active_run_ids(),
            registry=self._registry,
            source_priority=self._source_priority,
            champions=self.champions,
            summary_provider=self._valid_summary,
        )

    def _lock(self) -> StoreLock:
        """The cross-process lock serializing all read-modify-replace metadata
        updates (manifest, champions, conflict bookkeeping). Acquired only by
        the public write methods — helpers never re-acquire it."""
        return StoreLock(self.store / ".foreledger.lock")

    # -- store lifecycle ---------------------------------------------------

    #: The only files a concurrent constructor may legitimately observe in a
    #: store before the metadata lands. Anything else — including arbitrary
    #: ``*.tmp`` files — is user content and triggers the non-archive refusal.
    _INIT_PLUMBING = frozenset({".foreledger.lock", "archive_meta.json.tmp"})

    def _foreign_entries(self) -> bool:
        """True when the store directory holds anything that is not ours."""
        return any(entry.name not in self._INIT_PLUMBING for entry in self.store.iterdir())

    def _refuse_non_archive_dir(self) -> None:
        if not self.store.exists():
            return
        if not self.store.is_dir():
            raise StoreFormatError(f"store path {self.store} is not a directory")
        if self._foreign_entries() and not (self.store / _META_FILE).exists():
            raise StoreFormatError(
                f"{self.store} is not empty and has no archive metadata; refusing "
                "to initialize over existing contents"
            )

    def _check_or_init_store(self) -> None:
        meta_path = self.store / _META_FILE
        self._refuse_non_archive_dir()
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                stored_version = int(meta["format_version"])
            except (ValueError, KeyError, json.JSONDecodeError) as exc:
                raise StoreFormatError(
                    f"archive metadata at {meta_path} is unreadable or corrupt"
                ) from exc
            if stored_version > FORMAT_VERSION:
                raise StoreFormatError(
                    f"archive format version {stored_version} is newer than this "
                    f"library supports ({FORMAT_VERSION}); upgrade foreledger "
                    "to open this store"
                )
            return
        self.store.mkdir(parents=True, exist_ok=True)
        payload = {
            "format_version": FORMAT_VERSION,
            "created_at": pd.Timestamp.now().isoformat(),
        }
        tmp = meta_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, indent=1), encoding="utf-8")
        os.replace(tmp, meta_path)

    def _manifest_file_key(self) -> tuple[int, int] | None:
        """A cheap change marker (mtime_ns, size) for the manifest file."""
        try:
            stat = self._manifest_path.stat()
        except FileNotFoundError:
            return None
        return (stat.st_mtime_ns, stat.st_size)

    def _current_manifest(self) -> RunManifest:
        """The manifest, reloaded if another handle has committed since this
        one last read it — long-lived handles must not serve superseded or
        incomplete run sets indefinitely."""
        key = self._manifest_file_key()
        if key != self._manifest_key:
            self._manifest = RunManifest.load(self._manifest_path)
            self._manifest_key = key
        return self._manifest

    def _load_manifest(self) -> RunManifest:
        """Load the run manifest, migrating legacy (per series-set) records.

        Corruption or an unrecognized shape raises :class:`StoreFormatError`;
        the archive never guesses at run visibility.
        """
        entries = load_manifest_entries(self._manifest_path)
        if any("series_key" in entry for entry in entries):
            with self._lock():
                # re-read under the lock: another handle may have migrated
                entries = load_manifest_entries(self._manifest_path)
                if any("series_key" in entry for entry in entries):
                    return self._migrate_legacy_manifest(entries)
        return RunManifest.from_entries(self._manifest_path, entries)

    def _migrate_legacy_manifest(self, entries: list[dict[str, Any]]) -> RunManifest:
        """Deterministically migrate per-series-set run records (pre-57930cc)
        to per-series records, preserving format version 1.

        Each active legacy run's rows are re-tagged with per-series run_ids in
        a new segment; the legacy segment files stay on disk (never deleted)
        but are no longer referenced. Crash-safe: the manifest is replaced
        atomically last, so an interrupted migration simply reruns.
        """
        legacy = [e for e in entries if "series_key" in e]
        modern = [e for e in entries if "series_key" not in e]
        required = ("run_id", "model_id", "model_version", "origin", "ingested_at")
        for entry in legacy:
            missing = [key for key in required if key not in entry]
            if missing:
                raise StoreFormatError(
                    f"legacy run manifest record is missing fields {missing}; the "
                    "manifest may be corrupt or written by an incompatible version"
                )
        records = RunManifest.from_entries(self._manifest_path, modern).runs

        migrated: list[RunRecord] = []
        tagged_frames: list[pd.DataFrame] = []
        for entry in legacy:
            if entry.get("superseded"):
                continue  # invisible before the migration, invisible after
            rows = self._backend.read_forecasts(
                ForecastFilter(active_run_ids=[str(entry["run_id"])])
            )
            for series_id, group in rows.groupby("series_id", sort=True):
                run_id = uuid.uuid4().hex
                frame = group.copy()
                frame["run_id"] = run_id
                tagged_frames.append(frame)
                migrated.append(
                    RunRecord(
                        run_id=run_id,
                        model_id=str(entry["model_id"]),
                        model_version=str(entry["model_version"]),
                        origin=str(entry["origin"]),
                        series_id=str(series_id),
                        content_hash=content_hash(group),
                        segment="",
                        ingested_at=str(entry["ingested_at"]),
                    )
                )
        if tagged_frames:
            segment = self._backend.write_forecast_segment(
                pd.concat(tagged_frames, ignore_index=True)
            )
            for record in migrated:
                record.segment = segment

        manifest = RunManifest(path=self._manifest_path, runs=[*records, *migrated])
        manifest.save()
        logger.info(
            "migrated %d legacy run record(s) to %d per-series record(s)",
            len(legacy),
            len(migrated),
        )
        return manifest

    # -- write surface -------------------------------------------------------

    def ingest(
        self,
        frame: pd.DataFrame,
        mapping: Mapping[str, str] | None = None,
        *,
        model_id: str,
        model_version: str,
        origin: Any | None = None,
        on_conflict: str = "error",
    ) -> IngestResult:
        """Atomically append one or more forecast runs (one per distinct origin).

        Parameters
        ----------
        frame:
            Forecast rows. Must provide (directly or via ``mapping``) the
            columns ``series_id``, ``target``, ``value``, and — unless the
            scalar ``origin`` argument is given — ``origin``.
        mapping:
            Optional ``{canonical_name: your_column_name}`` translation, e.g.
            ``{"series_id": "sku", "value": "yhat"}``.
        model_id, model_version:
            Caller-supplied run identity (opaque strings; never inferred,
            ordered, or validated as semver/dates).
        origin:
            Scalar run date for single-run frames that carry no origin column.
        on_conflict:
            What to do when the same identity already exists with *different*
            values: ``"error"`` (default) raises; ``"overwrite"`` supersedes
            the prior run explicitly. Never a silent merge.

        Returns
        -------
        IngestResult with rows written, runs written/skipped/superseded.

        Notes
        -----
        The append is all-or-nothing: a failure mid-call leaves the archive at
        its pre-call state. Re-ingesting identical data is a no-op, and a
        different model/version always adds rows — parallel versions coexist.
        """
        canonical = canonicalize_forecasts(frame, mapping, model_id, model_version, origin)
        with self._lock():
            # plan against a fresh snapshot so concurrent handles merge
            # instead of clobbering each other's committed runs
            self._manifest = RunManifest.load(self._manifest_path)
            self._manifest_key = self._manifest_file_key()
            planned, skipped = plan_runs(canonical, self._manifest, on_conflict)
            if not planned:
                logger.info("ingest was a no-op: %d run(s) already present", skipped)
                # An idempotent replay is also the retry path after a failed
                # summary refresh — repair the summary before returning.
                self._refresh_summary_after_write()
                return IngestResult(
                    n_rows=0, n_runs_written=0, n_runs_skipped=skipped, n_runs_superseded=0
                )
            result, committed = commit_runs(
                planned,
                self._manifest,
                self._backend.write_forecast_segment,
                now=pd.Timestamp.now(),
            )
            # swap only after the durable save succeeded; a failed commit
            # leaves this handle's view at its pre-call state
            self._manifest = committed
            self._manifest_key = self._manifest_file_key()
        self._refresh_summary_after_write()
        return dataclasses.replace(result, n_runs_skipped=skipped)

    def ingest_nixtla(
        self,
        cv_frame: pd.DataFrame,
        *,
        model_id: str,
        model_version: str,
        value_column: str | None = None,
        on_conflict: str = "error",
    ) -> IngestResult:
        """Ingest a Nixtla cross-validation frame through the same push path.

        Maps ``unique_id``→series_id, ``ds``→target, ``cutoff``→origin; the
        prediction column defaults to the column named after ``model_id``.
        """
        column = value_column if value_column is not None else model_id
        if column not in cv_frame.columns:
            raise ValidationError(
                f"prediction column {column!r} not found in the Nixtla frame; "
                "pass value_column= explicitly"
            )
        mapping = {
            "series_id": "unique_id",
            "target": "ds",
            "origin": "cutoff",
            "value": column,
        }
        return self.ingest(
            cv_frame,
            mapping,
            model_id=model_id,
            model_version=model_version,
            on_conflict=on_conflict,
        )

    def register_actuals(
        self,
        frame: pd.DataFrame,
        mapping: Mapping[str, str] | None = None,
        *,
        source: str | None = None,
        official: bool = False,
        recorded_at: Any | None = None,
    ) -> None:
        """Append a batch of actuals as a revision of the model-independent log.

        Parameters
        ----------
        frame:
            Actual observations with ``series_id``, ``target``, ``value``
            columns (renameable via ``mapping``).
        mapping:
            Optional ``{canonical_name: your_column_name}`` translation.
        source:
            Feed label distinguishing providers/revisions registered at the
            same instant; defaults to a single shared label.
        official:
            Also designate these rows as the official actuals for their
            targets. The designation is sticky: if a *different* official
            already exists for any target, the whole call raises
            :class:`OfficialConflictError` before anything is written — use
            :meth:`mark_official` to change a designation explicitly.
        recorded_at:
            Knowledge timestamp for the batch; defaults to now. Earlier
            registrations are never overwritten — the effective ``latest``
            value per target is simply the newest ``recorded_at``.

        Notes
        -----
        For retry-safe pipelines pass an explicit ``recorded_at``: a retried
        call is then an exact replay (no-op for rows already appended, and it
        completes a half-finished official registration). With the default
        ``recorded_at=now`` each attempt is a new revision.
        """
        batch = canonicalize_actuals(frame, mapping, source, recorded_at, official)
        with self._lock():
            existing = self._backend.read_actuals()
            # Enforce the (series, target, source, recorded_at) identity
            # against the existing log: exact replays collapse, differing
            # values raise — both before anything is appended.
            new_rows = dedup_against_log(batch, existing)
            # Conflict bookkeeping runs against the would-be log BEFORE any
            # append: if the required error-log destination cannot be
            # written, the whole registration fails cleanly and retryably
            # instead of committing data whose integrity signal was lost.
            if new_rows.empty:
                combined = existing
            elif existing.empty:
                combined = new_rows
            else:
                combined = pd.concat([existing, new_rows], ignore_index=True)
            self._log_new_conflicts(combined)
            if official:
                # Designations reference the full canonical batch: a replayed
                # row already in the log may still need its designation.
                designations = check_official_registration(
                    batch, self._backend.read_officials(), existing
                )
                if not designations.empty:
                    designations = designations.copy()
                    designations["designated_at"] = pd.Timestamp.now()
                    # Designation BEFORE actual: a designation without its
                    # actual is inert (the official join finds nothing and
                    # latest is untouched), whereas an actual without its
                    # designation would already move latest-basis accuracy.
                    # A retry — any timestamp — completes or supersedes it.
                    self._backend.append_officials_segment(designations)
            self._backend.append_actuals_segment(new_rows)
            logger.info(
                "registered %d actual(s)%s", len(new_rows), " as official" if official else ""
            )
        self._refresh_summary_after_write()

    def mark_official(
        self,
        *,
        series: str,
        target: Any,
        source: str | None = None,
        recorded_at: Any | None = None,
    ) -> None:
        """Explicitly designate which registered actual is official for a target.

        This is the only way to *change* an official designation (at most one
        per ``(series, target)``; the latest designation wins). The actual must
        already be registered; identify it by ``series``/``target`` plus, when
        several revisions exist, ``source`` and/or ``recorded_at``.
        """
        with self._lock():
            actuals = self._backend.read_actuals()
            row = find_actual_row(actuals, series, target, source, recorded_at)
            designation = pd.DataFrame(
                {
                    "series_id": [row["series_id"]],
                    "target": [row["target"]],
                    "source": [row["source"]],
                    "actual_recorded_at": [row["actual_recorded_at"]],
                    "designated_at": [pd.Timestamp.now()],
                }
            )
            self._backend.append_officials_segment(designation)
        logger.info("official designation recorded")
        self._refresh_summary_after_write()

    def set_champion(self, model_id: str, model_version: str) -> None:
        """Persist the champion version for a model (one per model_id,
        last-write-wins; comparison metadata only, not a registry)."""
        if not model_id or not model_version:
            raise ValidationError("set_champion requires a model_id and model_version")
        with self._lock():
            champions = self.champions()  # re-read inside the lock: merge, not clobber
            champions[model_id] = model_version
            tmp = self._champions_path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(champions, indent=1), encoding="utf-8")
            os.replace(tmp, self._champions_path)
        logger.info("champion updated")

    def champions(self) -> dict[str, str]:
        """The persisted champion version per model_id."""
        if not self._champions_path.exists():
            return {}
        loaded = json.loads(self._champions_path.read_text(encoding="utf-8"))
        return {str(k): str(v) for k, v in loaded.items()}

    def register_metric(self, name: str, fn: MetricFn, summarizable: bool = True) -> None:
        """Register a custom metric per the protocol (ADR-004); summarizable
        metrics are precomputed into the summary like built-ins."""
        self._registry.register(name, fn, summarizable=summarizable)
        self._refresh_summary_after_write()

    # -- summary maintenance -------------------------------------------------

    def _raw_state(self) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        forecasts = self._backend.read_forecasts(
            ForecastFilter(active_run_ids=self._current_manifest().active_run_ids())
        )
        actuals = self._backend.read_actuals()
        officials = self._backend.read_officials()
        return forecasts, actuals, officials

    def _recompute_summary(self) -> pd.DataFrame:
        forecasts, actuals, officials = self._raw_state()
        latest = resolve_effective_latest(actuals, self._source_priority).latest
        official = resolve_effective_official(actuals, officials)
        return build_summary(forecasts, latest, official, self._registry)

    def _state_token(self) -> str:
        """Fingerprint of everything the summary is derived from: the active
        run set, the stored actuals/officials, the summarizable metric set,
        and the configured source priority (which steers same-timestamp
        tiebreaks). A summary stamped with a different token is never served.
        """
        digest = hashlib.sha256()
        for run_id in sorted(self._current_manifest().active_run_ids()):
            digest.update(f"run:{run_id}\n".encode())
        for component in sorted(self._backend.raw_state_components()):
            digest.update(f"{component}\n".encode())
        for component in sorted(self._registry.token_components()):
            digest.update(f"{component}\n".encode())
        # JSON, not a join: source labels are opaque user strings and may
        # contain any delimiter, so only a structured encoding is collision-free
        digest.update(f"priority:{json.dumps(self._source_priority or [])}\n".encode())
        return digest.hexdigest()

    def _valid_summary(self) -> pd.DataFrame | None:
        """The stored summary, only if it matches the current raw state."""
        stored = self._backend.read_summary()
        if stored is None:
            return None
        frame, token = stored
        if token != self._state_token() or "n_forecasts" not in frame.columns:
            return None
        return frame

    def rebuild_summary(self) -> None:
        """Recompute the disposable summary from raw and store it."""
        token = self._state_token()
        self._backend.replace_summary(self._recompute_summary(), token)

    def _refresh_summary_after_write(self) -> None:
        """Eagerly refresh the summary after a raw write, without letting a
        refresh failure fail the (already durable) write.

        If the rebuild fails, the stored summary's token no longer matches the
        raw state, so it is never served — queries fall back to raw, and the
        next write or idempotent replay repairs it.
        """
        if self._valid_summary() is not None:
            return
        try:
            self.rebuild_summary()
        except Exception:
            logger.warning(
                "summary refresh failed; queries will compute from raw until the "
                "next successful write",
                exc_info=True,
            )

    def reconcile(self) -> None:
        """Assert the stored summary equals a fresh recomputation from raw.

        Divergence is a defect (ADR-003); raises :class:`ReconciliationError`.
        A summary that is merely absent or stale (e.g. after a crashed
        refresh) is rebuilt instead — staleness is recoverable by design;
        disagreement at the same raw state is not.
        """
        recomputed = self._recompute_summary()
        stored = self._valid_summary()
        if stored is None:
            # absent or stale: rebuild rather than diagnose — it was never
            # being served
            self._backend.replace_summary(recomputed, self._state_token())
            return
        key = [
            "actual_basis",
            "metric",
            "model_id",
            "model_version",
            "series_id",
            "horizon",
            "period",
        ]
        stored_sorted = stored.sort_values(key, kind="mergesort").reset_index(drop=True)
        recomputed_sorted = recomputed.sort_values(key, kind="mergesort").reset_index(drop=True)
        if not stored_sorted.equals(recomputed_sorted):
            raise ReconciliationError(
                f"stored summary ({len(stored_sorted)} cells) does not equal the raw "
                f"recomputation ({len(recomputed_sorted)} cells)"
            )

    def _log_new_conflicts(self, actuals: pd.DataFrame) -> None:
        """Write newly observed unresolved same-timestamp conflicts to the
        dedicated error-log file (a data-integrity channel, not app logging).

        Called with the *would-be* log before the rows are appended, so a
        failure here aborts the registration instead of committing data whose
        integrity signal was lost. The flip side: if the registration crashes
        after this point, an entry may describe a conflict that only
        materializes on retry — a false positive in the loud channel, which
        beats a silent miss.
        """
        resolved = resolve_effective_latest(actuals, self._source_priority)
        if not resolved.conflicts:
            return
        logged: set[str] = set()
        if self._conflicts_logged_path.exists():
            logged = set(json.loads(self._conflicts_logged_path.read_text(encoding="utf-8")))
        new = [c for c in resolved.conflicts if c.key() not in logged]
        if not new:
            return
        self._error_log.parent.mkdir(parents=True, exist_ok=True)
        with self._error_log.open("a", encoding="utf-8") as handle:
            for conflict in new:
                handle.write(
                    f"{pd.Timestamp.now().isoformat()} ambiguous-latest "
                    f"series={conflict.series_id} target={conflict.target.isoformat()} "
                    f"recorded_at={conflict.recorded_at.isoformat()} "
                    f"sources={list(conflict.sources)} values={list(conflict.values)}\n"
                )
                logged.add(conflict.key())
        tmp = self._conflicts_logged_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(sorted(logged)), encoding="utf-8")
        os.replace(tmp, self._conflicts_logged_path)
        logger.warning(
            "%d unresolved same-timestamp actual conflict(s) written to the error log",
            len(new),
        )

    # -- read surface ----------------------------------------------------------

    def accuracy_at_horizon(
        self,
        h: int,
        metric: str = "MAE",
        basis: str = "latest",
        fallback: str | None = None,
        model_id: str | None = None,
        model_version: str | None = None,
        series: str | Sequence[str] | None = None,
        period: Period = None,
    ) -> AccuracyResult:
        """The accuracy metric at horizon ``h`` (days ahead) for a scope.

        Parameters
        ----------
        h:
            Horizon in days (``target - origin``).
        metric:
            A built-in (``MAE``/``RMSE``/``MAPE``/``MASE``) or registered name.
        basis:
            Which actuals to score against: ``"latest"`` (default, newest
            revision per target) or ``"official"`` (only explicitly designated
            values).
        fallback:
            With ``basis="official"`` only: ``"latest"`` fills targets that
            lack an official actual from the latest value — the result flags
            how many were filled. Without it such targets count as missing.
        model_id, model_version, series, period:
            Optional scope. Unscoped over model/version aggregates across all
            models; ``period`` is a ``(start, end)`` window on the run date.

        Returns
        -------
        AccuracyResult with a three-state status (ADR-007 amendment
        2026-06-11): ``"ok"`` — every forecast in scope was scored;
        ``"partial"`` — a value over the covered pairs, with unscored
        forecasts counted in ``n_missing_actuals``; ``"insufficient"`` —
        nothing in scope could be scored (``value`` is ``None``). Missing
        actuals never read as a silent zero/NaN.
        """
        return self._evaluator.accuracy_at_horizon(
            h,
            metric=metric,
            basis=basis,
            fallback=fallback,
            model_id=model_id,
            model_version=model_version,
            series=series,
            period=period,
        )

    def accuracy_curve(
        self,
        metric: str = "MAE",
        basis: str = "latest",
        fallback: str | None = None,
        horizons: Sequence[int] | None = None,
        model_id: str | None = None,
        model_version: str | None = None,
        series: str | Sequence[str] | None = None,
        period: Period = None,
    ) -> AccuracyCurve:
        """Accuracy vs. horizon as an :class:`AccuracyCurve`.

        One point per horizon (all horizons in scope when ``horizons`` is
        omitted); each point equals the corresponding
        :meth:`accuracy_at_horizon` call. The curve object offers
        ``to_frame()`` and, with matplotlib installed, ``plot()``.
        """
        return self._evaluator.accuracy_curve(
            metric=metric,
            basis=basis,
            fallback=fallback,
            horizons=horizons,
            model_id=model_id,
            model_version=model_version,
            series=series,
            period=period,
        )

    def compare_models(
        self,
        h: int,
        models: Sequence[tuple[str, str]],
        metric: str = "MAE",
        basis: str = "latest",
        fallback: str | None = None,
        champion: Mapping[str, str] | tuple[str, str] | None = None,
        series: str | Sequence[str] | None = None,
        period: Period = None,
    ) -> pd.DataFrame:
        """Compare listed ``(model_id, model_version)`` pairs at one horizon.

        All pairs are evaluated over the same scope, so each row's value
        equals the scoped single-model :meth:`accuracy_at_horizon`. For any
        listed version whose model has a champion — persisted via
        :meth:`set_champion` or passed via ``champion=`` — the row carries
        ``delta_vs_champion`` (negative means better on error metrics).

        Returns a DataFrame with one row per pair: value, n, status,
        champion_version, is_champion, delta_vs_champion.
        """
        return self._evaluator.compare_models(
            h,
            models,
            metric=metric,
            basis=basis,
            fallback=fallback,
            champion=champion,
            series=series,
            period=period,
        )

    def compare_curve(
        self,
        models: Sequence[tuple[str, str]],
        metric: str = "MAE",
        basis: str = "latest",
        fallback: str | None = None,
        champion: Mapping[str, str] | tuple[str, str] | None = None,
        horizons: Sequence[int] | None = None,
        series: str | Sequence[str] | None = None,
        period: Period = None,
    ) -> pd.DataFrame:
        """Accuracy-vs-horizon curves for several model/versions.

        Long-form DataFrame: one row per (model, version, horizon), with the
        same columns as :meth:`compare_models`. Each model's rows equal its
        scoped :meth:`accuracy_curve`.
        """
        return self._evaluator.compare_curve(
            models,
            metric=metric,
            basis=basis,
            fallback=fallback,
            champion=champion,
            horizons=horizons,
            series=series,
            period=period,
        )

    def as_of(
        self,
        origin: Any,
        model_id: str | None = None,
        model_version: str | None = None,
        series: str | Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """Every forecast known as of ``origin`` — what you would have seen then.

        Returns the raw rows with run date ``<= origin``, optionally scoped to
        a model/version/series. No leakage: rows from later runs can never
        appear, which makes this safe for honest backtests and audits.
        """
        return self._evaluator.as_of(
            origin, model_id=model_id, model_version=model_version, series=series
        )

    def drill(self, summary_cell: Mapping[str, Any]) -> pd.DataFrame:
        """The raw forecast/actual pairs behind one summary cell.

        ``summary_cell`` needs ``model_id``, ``model_version``, and
        ``horizon``; ``basis`` defaults to ``"latest"`` and ``series_id`` to
        all series. Recomputing the cell's metric over the returned rows
        reproduces the summary value exactly — the drill-down is the audit
        trail for any headline number.
        """
        return self._evaluator.drill(summary_cell)

    def list_models(self) -> pd.DataFrame:
        """The (model_id, model_version) pairs present and their coverage."""
        return self._evaluator.list_models()
