"""Accuracy metrics: built-ins and the registerable metric protocol (ADR-004).

A metric is a callable over aligned ``(forecast, actual)`` float arrays,
sorted by (series_id, target), returning a float. Built-ins (MAE/RMSE/MAPE/
MASE) are implemented *as* protocol-conforming metrics — one code path.

Metrics registered with ``summarizable=True`` are precomputed into the
accuracy summary like built-ins; others compute over raw only. Registered
user code runs behind an error/timeout guard so a bad metric cannot corrupt
or hang a recompute.

MASE note: the denominator is the in-window naive (lag-1) absolute error of
the actuals, computed over the aligned pairs in target order. When a scope
pools multiple series, differences are taken within each series (boundaries
excluded). Pairs are the evaluation window, not a training set — scope MASE
per series/period for the textbook reading.
"""

from __future__ import annotations

import concurrent.futures
import logging
from collections.abc import Callable
from dataclasses import dataclass

import numpy as np
import numpy.typing as npt

from ..errors import UnknownMetricError, ValidationError

logger = logging.getLogger("foreledger.metrics")

FloatArray = npt.NDArray[np.float64]

#: The metric protocol: aligned (forecast, actual) arrays -> float.
MetricFn = Callable[[FloatArray, FloatArray], float]

#: Wall-clock budget for one registered-metric evaluation.
DEFAULT_METRIC_TIMEOUT = 10.0


def mae(forecast: FloatArray, actual: FloatArray) -> float:
    return float(np.mean(np.abs(forecast - actual)))


def rmse(forecast: FloatArray, actual: FloatArray) -> float:
    return float(np.sqrt(np.mean((forecast - actual) ** 2)))


def mape(forecast: FloatArray, actual: FloatArray) -> float:
    """Mean absolute percentage error over pairs with a nonzero actual.

    Zero-actual pairs are excluded (the ratio is undefined); if every actual
    is zero the result is NaN, which the eval layer reports as insufficient.
    """
    nonzero = actual != 0
    if not nonzero.any():
        return float("nan")
    return float(np.mean(np.abs((forecast[nonzero] - actual[nonzero]) / actual[nonzero])))


def _mase_denominator(actual: FloatArray, series_breaks: FloatArray | None) -> float:
    diffs = np.abs(np.diff(actual))
    if series_breaks is not None and len(series_breaks) == len(actual):
        within = series_breaks[1:] == series_breaks[:-1]
        diffs = diffs[within]
    if len(diffs) == 0:
        return float("nan")
    return float(np.mean(diffs))


def make_mase(series_breaks: FloatArray | None = None) -> MetricFn:
    """Build a MASE metric, optionally aware of series boundaries.

    ``series_breaks`` is a per-pair series code array used to exclude
    cross-series differences from the naive-error denominator.
    """

    def mase(forecast: FloatArray, actual: FloatArray) -> float:
        scale = _mase_denominator(actual, series_breaks)
        if not np.isfinite(scale) or scale == 0:
            return float("nan")
        return float(np.mean(np.abs(forecast - actual)) / scale)

    return mase


#: Built-in metric names. MASE is constructed per evaluation because its
#: denominator depends on series boundaries; the others are pure.
BUILTIN_SIMPLE: dict[str, MetricFn] = {"MAE": mae, "RMSE": rmse, "MAPE": mape}
BUILTIN_NAMES = ("MAE", "RMSE", "MAPE", "MASE")


@dataclass(frozen=True)
class RegisteredMetric:
    name: str
    fn: MetricFn
    summarizable: bool
    builtin: bool


class MetricRegistry:
    """Built-in plus user-registered metrics; one evaluation code path."""

    def __init__(self, timeout: float = DEFAULT_METRIC_TIMEOUT) -> None:
        self._timeout = timeout
        self._metrics: dict[str, RegisteredMetric] = {
            name: RegisteredMetric(name, fn, summarizable=True, builtin=True)
            for name, fn in BUILTIN_SIMPLE.items()
        }
        # MASE is summarizable; its callable is bound per evaluation.
        self._metrics["MASE"] = RegisteredMetric(
            "MASE", make_mase(None), summarizable=True, builtin=True
        )

    def register(self, name: str, fn: MetricFn, summarizable: bool = True) -> None:
        if not name or not isinstance(name, str):
            raise ValidationError("metric name must be a non-empty string")
        if name in self._metrics and self._metrics[name].builtin:
            raise ValidationError(f"cannot replace built-in metric {name!r}")
        self._metrics[name] = RegisteredMetric(name, fn, summarizable=summarizable, builtin=False)
        logger.info("registered metric (summarizable=%s)", summarizable)

    def get(self, name: str) -> RegisteredMetric:
        try:
            return self._metrics[name]
        except KeyError:
            raise UnknownMetricError(
                f"unknown metric {name!r}; built-ins are {', '.join(BUILTIN_NAMES)}"
            ) from None

    def names(self, summarizable_only: bool = False) -> list[str]:
        return [m.name for m in self._metrics.values() if m.summarizable or not summarizable_only]

    def evaluate(
        self,
        name: str,
        forecast: FloatArray,
        actual: FloatArray,
        series_codes: FloatArray | None = None,
    ) -> float | None:
        """Evaluate a metric over aligned arrays.

        Built-ins run inline. Registered user metrics run behind an
        error-isolation/timeout guard: a raising or hanging metric yields
        ``None`` (the cell is skipped) instead of corrupting the recompute.
        """
        metric = self.get(name)
        if len(forecast) == 0:
            return None
        fn = metric.fn
        if metric.builtin and name == "MASE":
            fn = make_mase(series_codes)
        if metric.builtin:
            return fn(forecast, actual)
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        try:
            future = pool.submit(fn, forecast, actual)
            return float(future.result(timeout=self._timeout))
        except concurrent.futures.TimeoutError:
            logger.warning("registered metric timed out after %.1fs; cell skipped", self._timeout)
            return None
        except Exception:
            logger.warning("registered metric raised; cell skipped", exc_info=True)
            return None
        finally:
            # Never wait on a possibly-hung user metric thread.
            pool.shutdown(wait=False, cancel_futures=True)
