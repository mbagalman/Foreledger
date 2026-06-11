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
import json
import logging
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pandas as pd

from .actuals import (
    canonicalize_actuals,
    check_official_registration,
    find_actual_row,
    resolve_effective_latest,
    resolve_effective_official,
)
from .backend import Backend, ForecastFilter, create_backend
from .errors import ReconciliationError, StoreFormatError, ValidationError
from .ingestion import IngestResult, RunManifest, canonicalize_forecasts, commit_runs, plan_runs
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
        self._check_or_init_store()
        self._backend: Backend = create_backend(backend, self.store)
        self._manifest = RunManifest.load(self.store / "runs.json")
        self._champions_path = self.store / "champions.json"
        self._conflicts_logged_path = self.store / "conflicts_logged.json"
        self._error_log = (
            Path(error_log) if error_log is not None else (self.store / "error_log.txt")
        )
        self._source_priority = list(source_priority) if source_priority else None
        self._registry = MetricRegistry(timeout=metric_timeout)
        self._evaluator = Evaluator(
            backend=self._backend,
            active_run_ids=self._manifest.active_run_ids,
            registry=self._registry,
            source_priority=self._source_priority,
            champions=self.champions,
        )

    # -- store lifecycle ---------------------------------------------------

    def _check_or_init_store(self) -> None:
        meta_path = self.store / _META_FILE
        if self.store.exists():
            if not self.store.is_dir():
                raise StoreFormatError(f"store path {self.store} is not a directory")
            has_entries = any(self.store.iterdir())
            if has_entries and not meta_path.exists():
                raise StoreFormatError(
                    f"{self.store} is not empty and has no archive metadata; refusing "
                    "to initialize over existing contents"
                )
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
        planned, skipped = plan_runs(canonical, self._manifest, on_conflict)
        if not planned:
            logger.info("ingest was a no-op: %d run(s) already present", skipped)
            return IngestResult(
                n_rows=0, n_runs_written=0, n_runs_skipped=skipped, n_runs_superseded=0
            )
        result = commit_runs(
            planned,
            self._manifest,
            self._backend.write_forecast_segment,
            now=pd.Timestamp.now(),
        )
        self.rebuild_summary()
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
        """
        batch = canonicalize_actuals(frame, mapping, source, recorded_at, official)
        designations: pd.DataFrame | None = None
        if official:
            designations = check_official_registration(batch, self._backend.read_officials())
        self._backend.append_actuals_segment(batch)
        if designations is not None and not designations.empty:
            designations = designations.copy()
            designations["designated_at"] = pd.Timestamp.now()
            self._backend.append_officials_segment(designations)
        logger.info("registered %d actual(s)%s", len(batch), " as official" if official else "")
        self._log_new_conflicts()
        self.rebuild_summary()

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
        self.rebuild_summary()

    def set_champion(self, model_id: str, model_version: str) -> None:
        """Persist the champion version for a model (one per model_id,
        last-write-wins; comparison metadata only, not a registry)."""
        if not model_id or not model_version:
            raise ValidationError("set_champion requires a model_id and model_version")
        champions = self.champions()
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
        self.rebuild_summary()

    # -- summary maintenance -------------------------------------------------

    def _raw_state(self) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        forecasts = self._backend.read_forecasts(
            ForecastFilter(active_run_ids=self._manifest.active_run_ids())
        )
        actuals = self._backend.read_actuals()
        officials = self._backend.read_officials()
        return forecasts, actuals, officials

    def _recompute_summary(self) -> pd.DataFrame:
        forecasts, actuals, officials = self._raw_state()
        latest = resolve_effective_latest(actuals, self._source_priority).latest
        official = resolve_effective_official(actuals, officials)
        return build_summary(forecasts, latest, official, self._registry)

    def rebuild_summary(self) -> None:
        """Recompute the disposable summary from raw and store it."""
        self._backend.replace_summary(self._recompute_summary())

    def reconcile(self) -> None:
        """Assert the stored summary equals a fresh recomputation from raw.

        Divergence is a defect (ADR-003); raises :class:`ReconciliationError`.
        """
        recomputed = self._recompute_summary()
        stored = self._backend.read_summary()
        if stored is None:
            if recomputed.empty:
                return
            raise ReconciliationError("summary is absent but raw data yields cells")
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

    def _log_new_conflicts(self) -> None:
        """Write newly observed unresolved same-timestamp conflicts to the
        dedicated error-log file (a data-integrity channel, not app logging)."""
        actuals = self._backend.read_actuals()
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
        AccuracyResult — value and sample count when computable, or an
        explicit ``status="insufficient"`` with the missing-actuals count.
        Missing actuals never read as a silent zero/NaN.
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
