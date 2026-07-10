"""Accuracy metrics: built-ins and the registerable metric protocol (ADR-004).

A metric is a callable over aligned ``(forecast, actual)`` float arrays,
sorted by (series_id, target), returning a float. Built-ins (MAE/RMSE/MAPE/
MASE) are implemented *as* protocol-conforming metrics — one code path.

Metrics registered with ``summarizable=True`` are precomputed into the
accuracy summary like built-ins; others compute over raw only. Registered
user code runs behind an error/timeout guard so a bad metric cannot corrupt
or hang a recompute: a raising metric skips its cell, and a metric that
exceeds the timeout is skipped and quarantined for the session. This is
failure containment, not a security sandbox — registered code runs
in-process with the caller's privileges.

MASE note: the denominator is the in-window naive (lag-1) absolute error of
the actuals, computed over the aligned pairs in target order. When a scope
pools multiple series — or multiple models/versions, which repeat each
series' actuals once per model — differences are taken within each
(model, version, series) trajectory (boundaries excluded), so duplicated
actuals never enter the denominator. Pairs are the evaluation window, not a
training set — scope MASE per series/period for the textbook reading.
"""

from __future__ import annotations

import logging
import math
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass

import numpy as np
import numpy.typing as npt

from ..errors import UnknownMetricError, ValidationError

logger = logging.getLogger("foreledger.metrics")

FloatArray = npt.NDArray[np.float64]

#: The metric protocol: aligned (forecast, actual) arrays -> float. A
#: non-finite return (NaN/inf) means "undefined on this data" and yields an
#: insufficient cell rather than a stored number.
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
    """Build a MASE metric, optionally aware of trajectory boundaries.

    ``series_breaks`` is a per-pair trajectory code array (one code per
    (model, version, series) trajectory) used to exclude cross-trajectory
    differences from the naive-error denominator.
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
    #: Unique per registration event for custom metrics. No static analysis
    #: can reliably equate two arbitrary callables (closures, partials, bound
    #: methods, defaults all carry hidden state), so every (re-)registration
    #: gets a fresh identity and forces one summary rebuild — cheap, and it
    #: can never serve a stale implementation's numbers.
    fingerprint: str = "builtin"


class MetricRegistry:
    """Built-in plus user-registered metrics; one evaluation code path."""

    def __init__(self, timeout: float = DEFAULT_METRIC_TIMEOUT) -> None:
        self._timeout = timeout
        # A metric that times out once is quarantined for the rest of the
        # session: Python cannot kill its (daemonized, leaked) thread, so the
        # only safe containment is to never start another one.
        self._quarantined: set[str] = set()
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
        self._metrics[name] = RegisteredMetric(
            name,
            fn,
            summarizable=summarizable,
            builtin=False,
            fingerprint=uuid.uuid4().hex,
        )
        self._quarantined.discard(name)
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

    def token_components(self) -> list[str]:
        """Identity strings for the summarizable metric set, including each
        implementation's fingerprint — replacing a metric under the same name
        must invalidate any summary computed with the old implementation.

        Quarantine state participates too: once a summarizable metric is
        quarantined, every evaluation of it returns ``None``, so a summary
        built before the quarantine would silently disagree with raw (and
        make ``reconcile()`` raise on a healthy store) if it kept serving.
        Including the quarantined names invalidates that summary instead.
        """
        components = [
            f"metric:{m.name}:{m.fingerprint}" for m in self._metrics.values() if m.summarizable
        ]
        # quarantined names are always a subset of registered metrics (only a
        # metric that resolved can be quarantined, and metrics are never
        # removed), so this just filters that subset down to the summarizable
        # ones — the ones whose disappearance would move a served summary
        components.extend(
            f"quarantined:{name}"
            for name in sorted(self._quarantined)
            if self._metrics[name].summarizable
        )
        return components

    def evaluate(
        self,
        name: str,
        forecast: FloatArray,
        actual: FloatArray,
        series_codes: FloatArray | None = None,
    ) -> float | None:
        """Evaluate a metric over aligned arrays; ``None`` means "no value".

        A non-finite return (NaN/inf) is normalized to ``None`` for built-ins
        and registered metrics alike: it is the in-protocol way to say the
        metric is undefined on this data (MAPE with all-zero actuals, MASE
        with a flat naive denominator), so the cell reads as insufficient and
        non-finite numbers never reach the stored summary. It is not treated
        as a defect — no quarantine — because one degenerate scope must not
        disable a metric that is valid everywhere else.

        Registered user metrics additionally run behind an error/timeout
        guard: a raising metric yields ``None`` (the cell is skipped); one
        that exceeds the timeout is skipped *and quarantined* for the rest of
        the session so a hung cell cannot multiply.

        This is failure containment, not a security sandbox: registered code
        runs in-process with the caller's privileges, and a hung metric's
        daemon thread keeps running until the process exits (it cannot block
        interpreter shutdown, but it is not killed either).
        """
        metric = self.get(name)
        if len(forecast) == 0:
            return None
        if name in self._quarantined:
            return None
        fn = metric.fn
        if metric.builtin and name == "MASE":
            fn = make_mase(series_codes)
        if metric.builtin:
            value = fn(forecast, actual)
            return value if math.isfinite(value) else None

        outcome: list[float] = []
        failure: list[BaseException] = []

        def runner() -> None:
            try:
                outcome.append(float(fn(forecast, actual)))
            except BaseException as exc:  # noqa: BLE001 - isolating user code
                failure.append(exc)

        worker = threading.Thread(target=runner, daemon=True, name=f"foreledger-metric-{name}")
        worker.start()
        worker.join(self._timeout)
        if worker.is_alive():
            self._quarantined.add(name)
            logger.warning(
                "registered metric timed out after %.1fs; quarantined for this session",
                self._timeout,
            )
            return None
        if failure:
            logger.warning("registered metric raised; cell skipped", exc_info=failure[0])
            return None
        if not outcome or not math.isfinite(outcome[0]):
            return None
        return outcome[0]
