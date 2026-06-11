"""Engine-neutral backend seam (amended ADR-002).

The seam exposes scans/filters over the canonical schema in engine-neutral,
dialect-parameterized terms. Predicates are built here as ANSI SQL with
positional placeholders; a concrete backend supplies the physical table
expressions and execution. Consumers (ingestion, actuals, summary, eval)
never see the engine or its SQL dialect.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class Dialect:
    """SQL-dialect parameters used when building engine-neutral predicates."""

    name: str
    placeholder: str = "?"

    def quote(self, identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'


@dataclass(frozen=True)
class ForecastFilter:
    """Engine-neutral filter over the canonical forecast columns."""

    active_run_ids: Sequence[str]
    model_id: str | None = None
    model_version: str | None = None
    models: Sequence[tuple[str, str]] | None = None
    series: Sequence[str] | None = None
    horizon: int | None = None
    origin_min: pd.Timestamp | None = None
    origin_max: pd.Timestamp | None = None


def build_forecast_predicate(dialect: Dialect, flt: ForecastFilter) -> tuple[str, list[Any]]:
    """Build an ANSI WHERE clause + parameters for a forecast scan."""
    ph = dialect.placeholder
    q = dialect.quote
    clauses: list[str] = []
    params: list[Any] = []

    run_ids = list(flt.active_run_ids)
    if not run_ids:
        return "1 = 0", []
    clauses.append(f"{q('run_id')} IN ({', '.join([ph] * len(run_ids))})")
    params.extend(run_ids)

    if flt.model_id is not None:
        clauses.append(f"{q('model_id')} = {ph}")
        params.append(flt.model_id)
    if flt.model_version is not None:
        clauses.append(f"{q('model_version')} = {ph}")
        params.append(flt.model_version)
    if flt.models is not None:
        pair = f"({q('model_id')} = {ph} AND {q('model_version')} = {ph})"
        clauses.append("(" + " OR ".join([pair] * len(flt.models)) + ")")
        for model_id, model_version in flt.models:
            params.extend([model_id, model_version])
    if flt.series is not None:
        series = list(flt.series)
        clauses.append(f"{q('series_id')} IN ({', '.join([ph] * len(series))})")
        params.extend(series)
    if flt.horizon is not None:
        clauses.append(f"{q('horizon')} = {ph}")
        params.append(int(flt.horizon))
    if flt.origin_min is not None:
        clauses.append(f"{q('origin')} >= {ph}")
        params.append(flt.origin_min.to_pydatetime())
    if flt.origin_max is not None:
        clauses.append(f"{q('origin')} <= {ph}")
        params.append(flt.origin_max.to_pydatetime())

    return " AND ".join(clauses), params


class Backend(ABC):
    """Storage/query backend behind the dialect-aware seam.

    DuckDB-over-Parquet is the only v1 implementation; a warehouse backend
    (Snowflake, v1.1) implements the same surface additively.
    """

    dialect: Dialect

    # -- writes ----------------------------------------------------------

    @abstractmethod
    def write_forecast_segment(self, frame: pd.DataFrame) -> str:
        """Persist one ingest call's rows atomically (the frame may carry
        several run_ids); invisible until those run_ids are activated in the
        run manifest. Returns a storage token for the segment (e.g. a
        relative file path)."""

    @abstractmethod
    def append_actuals_segment(self, frame: pd.DataFrame) -> str:
        """Persist a non-empty actuals batch atomically; invisible until its
        returned segment token is committed in the actuals manifest."""

    @abstractmethod
    def append_officials_segment(self, frame: pd.DataFrame) -> str:
        """Persist non-empty official-designation rows atomically; invisible
        until the returned segment token is committed in the actuals
        manifest."""

    @abstractmethod
    def replace_summary(self, frame: pd.DataFrame, state_token: str) -> None:
        """Replace the disposable accuracy summary atomically, stamped with
        the raw-state token it was computed from. The summary data is written
        before the token, so a crash in between leaves a stale token — and a
        summary with a stale token is simply never served."""

    # -- reads -----------------------------------------------------------

    @abstractmethod
    def read_forecasts(self, flt: ForecastFilter) -> pd.DataFrame:
        """Scan the raw archive under an engine-neutral filter."""

    @abstractmethod
    def read_actuals(self, segments: Sequence[str]) -> pd.DataFrame:
        """Read the listed actuals segments (canonical actuals schema)."""

    @abstractmethod
    def read_officials(self, segments: Sequence[str]) -> pd.DataFrame:
        """Read the listed official-designation segments."""

    @abstractmethod
    def list_segments(self) -> tuple[list[str], list[str]]:
        """All stored (actuals, officials) segment tokens — committed or not.
        Used once at open to adopt pre-manifest stores."""

    @abstractmethod
    def read_summary(self) -> tuple[pd.DataFrame, str] | None:
        """Read the stored summary and its raw-state token, or None if absent
        (the summary is always rebuildable from raw)."""
