"""Derived accuracy summary: eager, disposable, always rebuildable (ADR-003).

One row per (model_id, model_version, series_id, horizon, metric, period,
actual_basis), recomputed eagerly on every write and reconciling exactly to a
raw recomputation. ``series_id == "*"`` rows pool all series for the cell so
model-scoped queries can be summary-served.

Both the summary builder and the raw query path compute through
:func:`metric_over_pairs`, so summary↔raw equality is structural, not
approximate.
"""

from __future__ import annotations

import logging
from typing import Any, cast

import pandas as pd

from .metrics import MetricRegistry
from .schema import ALL_PERIOD, ALL_SERIES, SUMMARY_COLUMNS, empty_summary

logger = logging.getLogger("foreledger.summary")


#: The columns that delimit one actual trajectory within a scope. A pooled
#: scope (multiple models or versions) repeats each (series_id, target)
#: actual once per model, so a trajectory keyed on series alone would walk
#: duplicate actuals and corrupt lag-based denominators (MASE).
_TRAJECTORY_KEYS = ["model_id", "model_version", "series_id"]


def metric_over_pairs(
    registry: MetricRegistry, metric: str, pairs: pd.DataFrame
) -> tuple[float | None, int]:
    """Evaluate one metric over aligned forecast/actual pairs.

    Pairs are sorted by (series_id, target) deterministically — the metric
    protocol's documented input order — so the same scope always yields
    bit-identical results on both the summary and raw paths. Trajectory codes
    identify each (model, version, series) trajectory for lag-based
    denominators (MASE); in pooled multi-model scopes equal codes are NOT
    contiguous under this ordering, and consumers must not assume they are.
    """
    if pairs.empty:
        return None, 0
    ordered = pairs.sort_values(["series_id", "target"], kind="mergesort")
    forecast = ordered["value"].to_numpy(dtype="float64")
    actual = ordered["actual_value"].to_numpy(dtype="float64")
    codes = pd.MultiIndex.from_frame(ordered[_TRAJECTORY_KEYS]).factorize()[0].astype("float64")
    return registry.evaluate(metric, forecast, actual, codes), len(ordered)


def build_summary(
    forecasts: pd.DataFrame,
    latest_effective: pd.DataFrame,
    official_effective: pd.DataFrame,
    registry: MetricRegistry,
) -> pd.DataFrame:
    """Recompute the full summary from raw forecasts and resolved actuals.

    The ``latest`` basis is always materialized; ``official`` rows exist only
    where at least one target has an official actual. Only summarizable
    metrics are precomputed (ADR-004).

    Each cell also stores ``n_forecasts`` — the total forecast rows in scope,
    matched or not — so summary-served results report missing-actuals
    coverage exactly as a raw computation would.
    """
    metric_names = registry.names(summarizable_only=True)
    records: list[dict[str, object]] = []

    for basis, effective in (("latest", latest_effective), ("official", official_effective)):
        if forecasts.empty or effective.empty:
            continue
        scoped = forecasts.merge(effective, on=["series_id", "target"], how="left")
        groupings: list[tuple[list[str], str | None]] = [
            (["model_id", "model_version", "series_id", "horizon"], None),
            (["model_id", "model_version", "horizon"], ALL_SERIES),
        ]
        for keys, pooled_series in groupings:
            for group_key, group in scoped.groupby(keys, sort=True):
                matched = group[group["actual_value"].notna()]
                if matched.empty:
                    continue
                for metric in metric_names:
                    value, n = metric_over_pairs(registry, metric, matched)
                    if value is None:
                        continue
                    records.append(
                        {
                            "model_id": group_key[0],
                            "model_version": group_key[1],
                            "series_id": pooled_series
                            if pooled_series is not None
                            else group_key[2],
                            "horizon": int(cast("Any", group_key[-1])),
                            "metric": metric,
                            "period": ALL_PERIOD,
                            "actual_basis": basis,
                            "value": float(value),
                            "n": int(n),
                            "n_forecasts": int(len(group)),
                        }
                    )

    if not records:
        return empty_summary()
    summary = pd.DataFrame.from_records(records)[SUMMARY_COLUMNS]
    summary = summary.sort_values(
        ["actual_basis", "metric", "model_id", "model_version", "series_id", "horizon"],
        kind="mergesort",
    ).reset_index(drop=True)
    logger.info("summary rebuilt: %d cell(s)", len(summary))
    return summary
