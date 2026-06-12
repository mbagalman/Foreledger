"""Evaluation & query engine — the read side of the public surface.

Routes to the precomputed summary when the request matches a summary cell
exactly and falls back to raw computation otherwise, invisibly; the two paths
share :func:`foreledger.summary.metric_over_pairs`, so they can never
silently diverge.

Missing actuals are always explicit: forecasts without a usable actual are
counted in ``n_missing_actuals`` and downgrade the status to ``partial``; a
scope where nothing can be scored is ``insufficient``. Under
``basis="official"``, targets with no official actual count as missing —
never silently substituted — unless the caller opts into
``fallback="latest"``, which fills them from the latest value and flags them
in the result.

Each public call works against one :class:`QuerySnapshot`: integrity is
verified once and the run manifest, actuals manifest, and integrity journal
are captured coherently (the archive retries the capture if a concurrent
commit lands mid-read). All raw reads scan only the snapshot's immutable
segment lists, and the stored summary is served only when its validity token
matches the token derived from the snapshot — so a fan-out call (a curve, a
comparison) does the expensive work once instead of once per point, and a
concurrent writer can never make a single call mix two archive states (for
example, old summary cells with newly revised actuals).
"""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

import pandas as pd

from .actuals import resolve_effective_latest, resolve_effective_official
from .backend.base import Backend, ForecastFilter
from .errors import ValidationError
from .metrics import MetricRegistry
from .results import AccuracyCurve, AccuracyResult
from .schema import ALL_PERIOD, ALL_SERIES, to_timestamp
from .summary import metric_over_pairs

Period = tuple[Any, Any] | None

#: Column contract of the comparison frames (compare_models / compare_curve).
_COMPARE_COLUMNS = [
    "model_id",
    "model_version",
    "horizon",
    "metric",
    "basis",
    "status",
    "value",
    "n",
    "champion_version",
    "is_champion",
    "delta_vs_champion",
]


def _parse_period(period: Period) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    if period is None:
        return None, None
    if not isinstance(period, (tuple, list)) or len(period) != 2:
        raise ValidationError("period must be a (start, end) pair of datetimes or None")
    start, end = period
    return (
        to_timestamp(start, "period start") if start is not None else None,
        to_timestamp(end, "period end") if end is not None else None,
    )


def _series_list(series: str | Sequence[str] | None) -> list[str] | None:
    if series is None:
        return None
    if isinstance(series, str):
        values = [series]
    else:
        try:
            values = [str(s) for s in series]
        except TypeError as exc:
            raise ValidationError("series must be a string or an iterable of strings") from exc
    if not values:
        # almost certainly an upstream filter that came up empty — explicit
        # beats a silent empty scope (and an empty SQL IN list is invalid on
        # some dialects)
        raise ValidationError(
            "series lists at least one series id; pass series=None for all series"
        )
    if ALL_SERIES in values:
        # '*' names the pooled summary cells; accepting it here would make
        # the summary route answer "all series pooled" while the raw route
        # filters for a literal series named '*' — silent divergence
        raise ValidationError(
            "series '*' is reserved for pooled summary cells; pass series=None to query all series"
        )
    return values


def _validate_horizon(value: Any) -> int:
    """Horizons are whole days; reject silent truncation (7.5 -> 7) and
    string digits masquerading as numbers."""
    if isinstance(value, bool):
        raise ValidationError("horizon must be an integer number of days")
    try:
        as_int = int(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"horizon {value!r} is not an integer number of days") from exc
    if as_int != value:
        raise ValidationError(f"horizon {value!r} is not a whole number of days")
    return as_int


def _validate_models(models: Sequence[tuple[str, str]]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for entry in models:
        if isinstance(entry, str) or not isinstance(entry, Sequence) or len(entry) != 2:
            raise ValidationError("models must be (model_id, model_version) pairs")
        pairs.append((str(entry[0]), str(entry[1])))
    if not pairs:
        raise ValidationError("models must list at least one (model_id, model_version)")
    return pairs


def _coverage_status(usable: bool, n_missing: int) -> Literal["ok", "partial", "insufficient"]:
    """Status reflects both computability and coverage: a value over an
    incompletely covered scope is explicitly ``partial``, never a quiet
    ``ok`` that reads as complete."""
    if not usable:
        return "insufficient"
    return "partial" if n_missing > 0 else "ok"


@dataclass
class QuerySnapshot:
    """One coherent capture of the archive's visible state for a public call.

    Built by the archive after one integrity verification, from one stable
    read of the run manifest, actuals manifest, and integrity journal.
    Segment files are immutable, so the captured token lists stay a faithful
    point-in-time view however long the call takes; ``summary_token`` is the
    validity token *derived from this snapshot*, so the stored summary can
    serve only queries whose raw fallback would read the same state. Lazy
    fields are populated at most once per call.
    """

    run_ids: list[str]
    segments: list[str]
    actuals_segments: list[str]
    officials_segments: list[str]
    summary_token: str
    summary: pd.DataFrame | None = None
    summary_loaded: bool = False
    actuals: pd.DataFrame | None = None
    officials: pd.DataFrame | None = None
    effective: dict[tuple[str, str | None], pd.DataFrame] = field(default_factory=dict)


class Evaluator:
    """Read-only evaluation over the backend seam, summary, and metrics."""

    def __init__(
        self,
        backend: Backend,
        registry: MetricRegistry,
        source_priority: list[str] | None,
        champions: Callable[[], dict[str, str]],
        snapshot_provider: Callable[[], QuerySnapshot],
    ) -> None:
        self._backend = backend
        self._registry = registry
        self._source_priority = source_priority
        self._champions = champions
        # One coherent, integrity-verified capture of the archive state per
        # public call; raises a typed error on externally modified data.
        self._snapshot_provider = snapshot_provider

    # -- snapshot plumbing ---------------------------------------------------

    def _snapshot(self) -> QuerySnapshot:
        return self._snapshot_provider()

    def _summary(self, snap: QuerySnapshot) -> pd.DataFrame | None:
        """The stored summary, only if its validity token matches the
        snapshot — a summary built before or after the captured state is
        never mixed into this call; the query computes from the snapshot's
        raw segments instead."""
        if not snap.summary_loaded:
            stored = self._backend.read_summary()
            snap.summary = None
            if stored is not None:
                # the backend guarantees the schema contract; only the
                # snapshot-token gate is decided here
                frame, token = stored
                if token == snap.summary_token:
                    snap.summary = frame
            snap.summary_loaded = True
        return snap.summary

    def _actuals(self, snap: QuerySnapshot) -> pd.DataFrame:
        if snap.actuals is None:
            snap.actuals = self._backend.read_actuals(snap.actuals_segments)
        return snap.actuals

    def _officials(self, snap: QuerySnapshot) -> pd.DataFrame:
        if snap.officials is None:
            snap.officials = self._backend.read_officials(snap.officials_segments)
        return snap.officials

    def _read_forecasts(
        self,
        snap: QuerySnapshot,
        *,
        horizon: int | None = None,
        model_id: str | None = None,
        model_version: str | None = None,
        models: Sequence[tuple[str, str]] | None = None,
        series: str | Sequence[str] | None = None,
        period: Period = None,
        origin_max: pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        start, end = _parse_period(period)
        if origin_max is not None:
            end = origin_max if end is None else min(end, origin_max)
        flt = ForecastFilter(
            active_run_ids=snap.run_ids,
            segments=snap.segments,
            model_id=model_id,
            model_version=model_version,
            models=models,
            series=_series_list(series),
            horizon=horizon,
            origin_min=start,
            origin_max=end,
        )
        return self._backend.read_forecasts(flt)

    def _effective(self, snap: QuerySnapshot, basis: str, fallback: str | None) -> pd.DataFrame:
        """Resolved actuals for a basis, with an ``is_fallback`` flag column;
        resolved once per public call and cached on the snapshot."""
        if basis not in ("latest", "official"):
            raise ValidationError("basis must be 'latest' or 'official'")
        if fallback not in (None, "latest"):
            raise ValidationError("fallback must be None or 'latest'")
        if fallback is not None and basis != "official":
            raise ValidationError("fallback='latest' only applies to basis='official'")

        cache_key = (basis, fallback)
        cached = snap.effective.get(cache_key)
        if cached is not None:
            return cached

        latest = resolve_effective_latest(self._actuals(snap), self._source_priority).latest.copy()
        latest["is_fallback"] = False
        if basis == "latest":
            snap.effective[cache_key] = latest
            return latest

        official = resolve_effective_official(self._actuals(snap), self._officials(snap)).copy()
        official["is_fallback"] = False
        if fallback == "latest" and not latest.empty:
            merged = latest.merge(
                official[["series_id", "target"]],
                on=["series_id", "target"],
                how="left",
                indicator=True,
            )
            extra = merged[merged["_merge"] == "left_only"].drop(columns="_merge").copy()
            extra["is_fallback"] = True
            official = pd.concat([official, extra], ignore_index=True)
        snap.effective[cache_key] = official
        return official

    def _evaluate_raw(
        self,
        snap: QuerySnapshot,
        *,
        horizon: int,
        metric: str,
        basis: str,
        fallback: str | None,
        model_id: str | None,
        model_version: str | None,
        series: str | Sequence[str] | None,
        period: Period,
    ) -> AccuracyResult:
        """Compute one accuracy point from raw rows (the fallback path).

        Forecasts without a matching actual are counted, never dropped
        silently: they downgrade the status via ``n_missing_actuals``.
        """
        forecasts = self._read_forecasts(
            snap,
            horizon=horizon,
            model_id=model_id,
            model_version=model_version,
            series=series,
            period=period,
        )
        effective = self._effective(snap, basis, fallback)
        merged = forecasts.merge(effective, on=["series_id", "target"], how="left")
        matched = merged["actual_value"].notna()
        pairs = merged[matched]
        n_missing = int((~matched).sum())
        n_fallback = int(pairs["is_fallback"].sum()) if not pairs.empty else 0
        value, n = metric_over_pairs(self._registry, metric, pairs)
        usable = n > 0 and value is not None and math.isfinite(value)
        return AccuracyResult(
            metric=metric,
            horizon=int(horizon),
            basis=basis,
            status=_coverage_status(usable, n_missing),
            value=float(value) if usable and value is not None else None,
            n=n,
            n_missing_actuals=n_missing,
            fallback_used=fallback is not None and n_fallback > 0,
            n_fallback=n_fallback,
            served_from="raw",
        )

    def _summary_lookup(
        self,
        snap: QuerySnapshot,
        *,
        horizon: int,
        metric: str,
        basis: str,
        model_id: str,
        model_version: str,
        series_cell: str,
    ) -> AccuracyResult | None:
        """Serve one exact summary cell, or None when the request does not
        match a stored cell (the caller then falls back to raw)."""
        stored = self._summary(snap)
        if stored is None or stored.empty:
            return None
        row = stored[
            (stored["metric"] == metric)
            & (stored["actual_basis"] == basis)
            & (stored["horizon"] == int(horizon))
            & (stored["model_id"] == model_id)
            & (stored["model_version"] == model_version)
            & (stored["series_id"] == series_cell)
            & (stored["period"] == ALL_PERIOD)
        ]
        if len(row) != 1:
            return None
        value = float(row["value"].iloc[0])
        n = int(row["n"].iloc[0])
        n_forecasts = int(row["n_forecasts"].iloc[0])
        usable = n > 0 and math.isfinite(value)
        return AccuracyResult(
            metric=metric,
            horizon=int(horizon),
            basis=basis,
            status=_coverage_status(usable, n_forecasts - n),
            value=value if usable else None,
            n=n,
            n_missing_actuals=n_forecasts - n,
            served_from="summary",
        )

    def _accuracy(
        self,
        snap: QuerySnapshot,
        h: int,
        *,
        metric: str,
        basis: str,
        fallback: str | None,
        model_id: str | None,
        model_version: str | None,
        series: str | Sequence[str] | None,
        period: Period,
    ) -> AccuracyResult:
        """Route one accuracy request: the summary serves it only when the
        scope matches a precomputed cell exactly (no fallback, no period,
        single model/version, single-or-all series); anything else — and any
        summary miss — computes from raw, invisibly."""
        self._registry.get(metric)  # raises UnknownMetricError early
        h = _validate_horizon(h)
        _series_list(series)  # validate (incl. the reserved '*') on BOTH routes
        summary_servable = (
            fallback is None
            and period is None
            and model_id is not None
            and model_version is not None
            and (series is None or isinstance(series, str))
        )
        if summary_servable:
            assert model_id is not None and model_version is not None
            result = self._summary_lookup(
                snap,
                horizon=h,
                metric=metric,
                basis=basis,
                model_id=model_id,
                model_version=model_version,
                series_cell=series if isinstance(series, str) else ALL_SERIES,
            )
            if result is not None:
                return result
        return self._evaluate_raw(
            snap,
            horizon=h,
            metric=metric,
            basis=basis,
            fallback=fallback,
            model_id=model_id,
            model_version=model_version,
            series=series,
            period=period,
        )

    def _horizons(
        self,
        snap: QuerySnapshot,
        *,
        model_id: str | None = None,
        model_version: str | None = None,
        models: Sequence[tuple[str, str]] | None = None,
        series: str | Sequence[str] | None = None,
        period: Period = None,
    ) -> list[int]:
        """The sorted distinct horizons present in the scoped forecasts."""
        forecasts = self._read_forecasts(
            snap,
            model_id=model_id,
            model_version=model_version,
            models=models,
            series=series,
            period=period,
        )
        return sorted(int(h) for h in forecasts["horizon"].unique())

    def _compare_at(
        self,
        snap: QuerySnapshot,
        h: int,
        models: Sequence[tuple[str, str]],
        champions: dict[str, str],
        *,
        metric: str,
        basis: str,
        fallback: str | None,
        series: str | Sequence[str] | None,
        period: Period,
    ) -> list[dict[str, Any]]:
        """Comparison rows for the listed models at one horizon, sharing the
        caller's snapshot; each champion is evaluated at most once."""
        champion_results: dict[str, AccuracyResult] = {}

        def scoped(mid: str, mv: str) -> AccuracyResult:
            return self._accuracy(
                snap,
                h,
                metric=metric,
                basis=basis,
                fallback=fallback,
                model_id=mid,
                model_version=mv,
                series=series,
                period=period,
            )

        rows: list[dict[str, Any]] = []
        for model_id, model_version in models:
            result = scoped(model_id, model_version)
            champ_version = champions.get(model_id)
            delta: float | None = None
            if champ_version is not None:
                if model_id not in champion_results:
                    champion_results[model_id] = scoped(model_id, champ_version)
                champ = champion_results[model_id]
                if result.ok and champ.ok and result.value is not None and champ.value is not None:
                    delta = result.value - champ.value
            rows.append(
                {
                    "model_id": model_id,
                    "model_version": model_version,
                    "horizon": int(h),
                    "metric": metric,
                    "basis": basis,
                    "status": result.status,
                    "value": result.value,
                    "n": result.n,
                    "champion_version": champ_version,
                    "is_champion": champ_version == model_version,
                    "delta_vs_champion": delta,
                }
            )
        return rows

    def _champion_map(self, override: Mapping[str, str] | tuple[str, str] | None) -> dict[str, str]:
        champions = dict(self._champions())
        if override is None:
            return champions
        if isinstance(override, tuple):
            if len(override) != 2:
                raise ValidationError("champion must be a (model_id, model_version) pair")
            champions[str(override[0])] = str(override[1])
        elif isinstance(override, Mapping):
            champions.update({str(k): str(v) for k, v in override.items()})
        else:
            # a 2-element list would otherwise build a garbage dict from the
            # strings' characters — silently wrong champions, never an error
            raise ValidationError(
                "champion must be a (model_id, model_version) tuple or a "
                "{model_id: model_version} mapping"
            )
        return champions

    # -- public operations ---------------------------------------------------

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
        """One accuracy point at horizon ``h`` (full parameter semantics on
        :meth:`ForecastArchive.accuracy_at_horizon`, which delegates here)."""
        return self._accuracy(
            self._snapshot(),
            h,
            metric=metric,
            basis=basis,
            fallback=fallback,
            model_id=model_id,
            model_version=model_version,
            series=series,
            period=period,
        )

    def horizons_in_scope(
        self,
        *,
        model_id: str | None = None,
        model_version: str | None = None,
        series: str | Sequence[str] | None = None,
        period: Period = None,
    ) -> list[int]:
        """The sorted distinct horizons present in the scoped forecasts."""
        return self._horizons(
            self._snapshot(),
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
        """One point per horizon over a single shared snapshot; each point
        equals the corresponding standalone :meth:`accuracy_at_horizon`."""
        snap = self._snapshot()
        if horizons is None:
            horizons = self._horizons(
                snap, model_id=model_id, model_version=model_version, series=series, period=period
            )
        else:
            horizons = [_validate_horizon(h) for h in horizons]
        points = tuple(
            self._accuracy(
                snap,
                h,
                metric=metric,
                basis=basis,
                fallback=fallback,
                model_id=model_id,
                model_version=model_version,
                series=series,
                period=period,
            )
            for h in horizons
        )
        return AccuracyCurve(metric=metric, basis=basis, points=points)

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
        """The metric per listed (model_id, model_version) at horizon ``h``
        over a common scope; each value equals the scoped single-model call.
        Versions whose model has a champion get a delta vs. that champion."""
        pairs = _validate_models(models)
        snap = self._snapshot()
        rows = self._compare_at(
            snap,
            h,
            pairs,
            self._champion_map(champion),
            metric=metric,
            basis=basis,
            fallback=fallback,
            series=series,
            period=period,
        )
        return pd.DataFrame(rows)

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
        """One accuracy-vs-horizon curve per listed model/version (long form);
        each equals the scoped ``accuracy_curve``."""
        pairs = _validate_models(models)
        snap = self._snapshot()
        if horizons is None:
            # scoped to the LISTED models: an unrelated model's extra
            # horizons must not inject all-insufficient rows into this curve
            horizons = self._horizons(snap, models=pairs, series=series, period=period)
        else:
            horizons = [_validate_horizon(h) for h in horizons]
        champions = self._champion_map(champion)
        rows: list[dict[str, Any]] = []
        for h in horizons:
            rows.extend(
                self._compare_at(
                    snap,
                    h,
                    pairs,
                    champions,
                    metric=metric,
                    basis=basis,
                    fallback=fallback,
                    series=series,
                    period=period,
                )
            )
        if not rows:
            return pd.DataFrame(columns=_COMPARE_COLUMNS)
        return pd.DataFrame(rows)

    def as_of(
        self,
        origin: Any,
        model_id: str | None = None,
        model_version: str | None = None,
        series: str | Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """The current record of runs with origin on or before the cutoff.

        Origin-time filter (tech spec FR-4.1): later-origin rows never appear.
        Not a transaction-time replay — explicit overwrites revise this view
        for past origins."""
        cutoff = to_timestamp(origin, "origin")
        forecasts = self._read_forecasts(
            self._snapshot(),
            model_id=model_id,
            model_version=model_version,
            series=series,
            origin_max=cutoff,
        )
        return forecasts.drop(columns=["run_id"]).reset_index(drop=True)

    def drill(self, cell: Mapping[str, Any]) -> pd.DataFrame:
        """Raw aligned rows behind one summary cell; they reconcile to it."""
        required = ("model_id", "model_version", "horizon")
        missing = [k for k in required if k not in cell]
        if missing:
            raise ValidationError(f"drill cell is missing keys: {missing}")
        basis = str(cell.get("basis", cell.get("actual_basis", "latest")))
        fallback = cell.get("fallback")
        series_cell = cell.get("series_id")
        series = None if series_cell in (None, ALL_SERIES) else str(series_cell)
        snap = self._snapshot()
        forecasts = self._read_forecasts(
            snap,
            horizon=_validate_horizon(cell["horizon"]),
            model_id=str(cell["model_id"]),
            model_version=str(cell["model_version"]),
            series=series,
            period=cell.get("period_range"),
        )
        effective = self._effective(snap, basis, fallback)
        pairs = forecasts.merge(effective, on=["series_id", "target"], how="inner")
        pairs = pairs.sort_values(["series_id", "target"], kind="mergesort")
        columns = [
            "model_id",
            "model_version",
            "series_id",
            "origin",
            "target",
            "horizon",
            "value",
            "actual_value",
            "is_fallback",
        ]
        return pairs[columns].reset_index(drop=True)

    def list_models(self) -> pd.DataFrame:
        """The (model_id, model_version) pairs present and their coverage."""
        forecasts = self._read_forecasts(self._snapshot())
        champions = self._champions()
        if forecasts.empty:
            return pd.DataFrame(
                columns=[
                    "model_id",
                    "model_version",
                    "n_rows",
                    "n_series",
                    "first_origin",
                    "last_origin",
                    "first_target",
                    "last_target",
                    "is_champion",
                ]
            )
        grouped = forecasts.groupby(["model_id", "model_version"], sort=True)
        listing = grouped.agg(
            n_rows=("value", "size"),
            n_series=("series_id", "nunique"),
            first_origin=("origin", "min"),
            last_origin=("origin", "max"),
            first_target=("target", "min"),
            last_target=("target", "max"),
        ).reset_index()
        listing["is_champion"] = [
            champions.get(mid) == mv
            for mid, mv in zip(listing["model_id"], listing["model_version"], strict=True)
        ]
        return listing
