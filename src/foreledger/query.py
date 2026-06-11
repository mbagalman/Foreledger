"""Evaluation & query engine — the read side of the public surface.

Routes to the precomputed summary when the request matches a summary cell
exactly and falls back to raw computation otherwise, invisibly; the two paths
share :func:`foreledger.summary.metric_over_pairs`, so they can never
silently diverge.

Missing actuals are an explicit insufficient result. Under
``basis="official"``, targets with no official actual are reported
insufficient unless the caller opts into ``fallback="latest"``, which fills
them from the latest value and flags them in the result.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import pandas as pd

from .actuals import resolve_effective_latest, resolve_effective_official
from .backend.base import Backend, ForecastFilter
from .errors import ValidationError
from .metrics import MetricRegistry
from .results import AccuracyCurve, AccuracyResult
from .schema import ALL_PERIOD, ALL_SERIES, to_timestamp
from .summary import metric_over_pairs

Period = tuple[Any, Any] | None


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
        return [series]
    return [str(s) for s in series]


class Evaluator:
    """Read-only evaluation over the backend seam, summary, and metrics."""

    def __init__(
        self,
        backend: Backend,
        active_run_ids: Callable[[], list[str]],
        registry: MetricRegistry,
        source_priority: list[str] | None,
        champions: Callable[[], dict[str, str]],
        summary_provider: Callable[[], pd.DataFrame | None],
    ) -> None:
        self._backend = backend
        self._active_run_ids = active_run_ids
        self._registry = registry
        self._source_priority = source_priority
        self._champions = champions
        # Returns the stored summary only when it matches the current raw
        # state (validity token); a stale or absent summary yields None and
        # every query falls back to raw computation invisibly.
        self._summary_provider = summary_provider

    # -- shared plumbing ---------------------------------------------------

    def _read_forecasts(
        self,
        *,
        horizon: int | None = None,
        model_id: str | None = None,
        model_version: str | None = None,
        series: str | Sequence[str] | None = None,
        period: Period = None,
        origin_max: pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        start, end = _parse_period(period)
        if origin_max is not None:
            end = origin_max if end is None else min(end, origin_max)
        flt = ForecastFilter(
            active_run_ids=self._active_run_ids(),
            model_id=model_id,
            model_version=model_version,
            series=_series_list(series),
            horizon=horizon,
            origin_min=start,
            origin_max=end,
        )
        return self._backend.read_forecasts(flt)

    def _effective(self, basis: str, fallback: str | None) -> pd.DataFrame:
        """Resolved actuals for a basis, with an ``is_fallback`` flag column."""
        if basis not in ("latest", "official"):
            raise ValidationError("basis must be 'latest' or 'official'")
        if fallback not in (None, "latest"):
            raise ValidationError("fallback must be None or 'latest'")
        if fallback is not None and basis != "official":
            raise ValidationError("fallback='latest' only applies to basis='official'")

        actuals = self._backend.read_actuals()
        latest = resolve_effective_latest(actuals, self._source_priority).latest.copy()
        latest["is_fallback"] = False
        if basis == "latest":
            return latest

        official = resolve_effective_official(actuals, self._backend.read_officials()).copy()
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
        return official

    def _evaluate_raw(
        self,
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
        forecasts = self._read_forecasts(
            horizon=horizon,
            model_id=model_id,
            model_version=model_version,
            series=series,
            period=period,
        )
        effective = self._effective(basis, fallback)
        merged = forecasts.merge(effective, on=["series_id", "target"], how="left")
        matched = merged["actual_value"].notna()
        pairs = merged[matched]
        n_missing = int((~matched).sum())
        n_fallback = int(pairs["is_fallback"].sum()) if not pairs.empty else 0
        value, n = metric_over_pairs(self._registry, metric, pairs)
        ok = n > 0 and value is not None and math.isfinite(value)
        return AccuracyResult(
            metric=metric,
            horizon=int(horizon),
            basis=basis,
            status="ok" if ok else "insufficient",
            value=float(value) if ok and value is not None else None,
            n=n,
            n_missing_actuals=n_missing,
            fallback_used=fallback is not None and n_fallback > 0,
            n_fallback=n_fallback,
            served_from="raw",
        )

    def _summary_lookup(
        self,
        *,
        horizon: int,
        metric: str,
        basis: str,
        model_id: str,
        model_version: str,
        series_cell: str,
    ) -> AccuracyResult | None:
        stored = self._summary_provider()
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
        ok = n > 0 and math.isfinite(value)
        return AccuracyResult(
            metric=metric,
            horizon=int(horizon),
            basis=basis,
            status="ok" if ok else "insufficient",
            value=value if ok else None,
            n=n,
            n_missing_actuals=n_forecasts - n,
            served_from="summary",
        )

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
        self._registry.get(metric)  # raises UnknownMetricError early
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
            horizon=h,
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
        forecasts = self._read_forecasts(
            model_id=model_id, model_version=model_version, series=series, period=period
        )
        return sorted(int(h) for h in forecasts["horizon"].unique())

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
        if horizons is None:
            horizons = self.horizons_in_scope(
                model_id=model_id, model_version=model_version, series=series, period=period
            )
        points = tuple(
            self.accuracy_at_horizon(
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

    def _champion_map(self, override: Mapping[str, str] | tuple[str, str] | None) -> dict[str, str]:
        champions = dict(self._champions())
        if override is None:
            return champions
        if isinstance(override, tuple):
            if len(override) != 2:
                raise ValidationError("champion must be a (model_id, model_version) pair")
            champions[override[0]] = override[1]
        else:
            champions.update(dict(override))
        return champions

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
        if not models:
            raise ValidationError("models must list at least one (model_id, model_version)")
        champions = self._champion_map(champion)

        champion_results: dict[str, AccuracyResult] = {}

        def scoped(mid: str, mv: str) -> AccuracyResult:
            return self.accuracy_at_horizon(
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
        if horizons is None:
            horizons = self.horizons_in_scope(series=series, period=period)
        frames = [
            self.compare_models(
                h,
                models,
                metric=metric,
                basis=basis,
                fallback=fallback,
                champion=champion,
                series=series,
                period=period,
            )
            for h in horizons
        ]
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def as_of(
        self,
        origin: Any,
        model_id: str | None = None,
        model_version: str | None = None,
        series: str | Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """Forecasts known as of ``origin`` — rows with ``origin <= origin``;
        no leakage from later runs by construction."""
        cutoff = to_timestamp(origin, "origin")
        forecasts = self._read_forecasts(
            model_id=model_id, model_version=model_version, series=series, origin_max=cutoff
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
        forecasts = self._read_forecasts(
            horizon=int(cell["horizon"]),
            model_id=str(cell["model_id"]),
            model_version=str(cell["model_version"]),
            series=series,
            period=cell.get("period_range"),
        )
        effective = self._effective(basis, fallback)
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
        forecasts = self._read_forecasts()
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
