"""Typed result objects returned by the evaluation API.

A missing-actuals outcome is an explicit ``status == "insufficient"`` result
with the covered sample count — never a silent zero/NaN that reads as perfect
accuracy.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, Literal

import pandas as pd

Status = Literal["ok", "insufficient"]
Basis = Literal["latest", "official"]


@dataclass(frozen=True)
class AccuracyResult:
    """The metric value for one scope at one horizon, or an explicit shortfall.

    Attributes
    ----------
    metric, horizon, basis:
        What was asked: the metric name, horizon in days, and actuals basis.
    status:
        ``"ok"`` when a finite value was computed over at least one pair;
        ``"insufficient"`` when actuals are missing/ambiguous or the metric
        is undefined on the data.
    value:
        The metric value, or ``None`` when insufficient.
    n:
        Number of forecast/actual pairs the value covers.
    n_missing_actuals:
        Forecast rows in scope that had no usable actual (includes targets
        marked ambiguous by an unresolved same-timestamp conflict).
    fallback_used, n_fallback:
        Whether/how many targets were filled from the latest basis under the
        explicit ``fallback="latest"`` opt-in (official basis only).
    served_from:
        ``"summary"`` (precomputed) or ``"raw"`` (computed on the fly) —
        the two are test-guaranteed to agree; this is latency provenance.
    """

    metric: str
    horizon: int
    basis: str
    status: Status
    value: float | None
    n: int
    n_missing_actuals: int = 0
    fallback_used: bool = False
    n_fallback: int = 0
    served_from: Literal["summary", "raw"] = "raw"

    @property
    def ok(self) -> bool:
        return self.status == "ok"


@dataclass(frozen=True)
class AccuracyCurve:
    """Accuracy vs. horizon: one :class:`AccuracyResult` per horizon."""

    metric: str
    basis: str
    points: tuple[AccuracyResult, ...] = field(default_factory=tuple)

    def __iter__(self) -> Iterator[AccuracyResult]:
        return iter(self.points)

    def __len__(self) -> int:
        return len(self.points)

    def to_frame(self) -> pd.DataFrame:
        """The curve as a DataFrame (one row per horizon)."""
        return pd.DataFrame(
            {
                "horizon": [p.horizon for p in self.points],
                "metric": [p.metric for p in self.points],
                "basis": [p.basis for p in self.points],
                "status": [p.status for p in self.points],
                "value": [p.value for p in self.points],
                "n": [p.n for p in self.points],
                "n_missing_actuals": [p.n_missing_actuals for p in self.points],
                "n_fallback": [p.n_fallback for p in self.points],
            }
        )

    def plot(self, ax: Any = None) -> Any:
        """Render the curve with matplotlib if it is installed."""
        import matplotlib.pyplot as plt

        if ax is None:
            _, ax = plt.subplots()
        frame = self.to_frame()
        usable = frame[frame["status"] == "ok"]
        ax.plot(usable["horizon"], usable["value"], marker="o")
        ax.set_xlabel("horizon (days)")
        ax.set_ylabel(self.metric)
        ax.set_title(f"{self.metric} vs. horizon (basis={self.basis})")
        return ax
