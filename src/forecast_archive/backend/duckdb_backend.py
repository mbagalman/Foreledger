"""DuckDB-over-Parquet backend — the only v1 engine (amended ADR-002).

Engine specifics (DuckDB connection handling, ``read_parquet`` table
expressions, the physical Parquet layout) live here and never leak past the
seam. Raw segments are written append-only via temp-file + atomic rename;
forecast visibility is governed by the run manifest's active run_ids, so a
crashed ingest leaves the archive at its pre-run state.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ..schema import empty_actuals, empty_forecasts
from .base import Backend, Dialect, ForecastFilter, build_forecast_predicate

DUCKDB_DIALECT = Dialect(name="duckdb", placeholder="?")

_OFFICIAL_COLUMNS = ["series_id", "target", "source", "actual_recorded_at", "designated_at"]

_FORECAST_SELECT = (
    "model_id, model_version, series_id, origin, target, value, horizon, run_id, ingested_at"
)


def _empty_officials() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "series_id": pd.Series(dtype="object"),
            "target": pd.Series(dtype="datetime64[ns]"),
            "source": pd.Series(dtype="object"),
            "actual_recorded_at": pd.Series(dtype="datetime64[ns]"),
            "designated_at": pd.Series(dtype="datetime64[ns]"),
        }
    )


class DuckDBBackend(Backend):
    """Local Parquet store queried through DuckDB."""

    def __init__(self, store: Path) -> None:
        self.dialect = DUCKDB_DIALECT
        self.store = store
        self.forecasts_dir = store / "forecasts"
        self.actuals_dir = store / "actuals"
        self.officials_dir = store / "officials"
        self.summary_dir = store / "summary"
        for directory in (
            self.forecasts_dir,
            self.actuals_dir,
            self.officials_dir,
            self.summary_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)
        self._conn: duckdb.DuckDBPyConnection | None = None

    # -- internals ---------------------------------------------------------

    def _connection(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            self._conn = duckdb.connect(":memory:")
        return self._conn

    @staticmethod
    def _files(directory: Path) -> list[str]:
        return sorted(p.as_posix() for p in directory.glob("*.parquet"))

    @staticmethod
    def _source_expr(files: list[str]) -> str:
        quoted = ", ".join("'" + f.replace("'", "''") + "'" for f in files)
        return f"read_parquet([{quoted}], union_by_name=true)"

    @staticmethod
    def _atomic_write(frame: pd.DataFrame, path: Path) -> None:
        tmp = path.with_suffix(".parquet.tmp")
        frame.to_parquet(tmp, engine="pyarrow", index=False)
        os.replace(tmp, path)

    def _query(self, sql: str, params: list[Any]) -> pd.DataFrame:
        return self._connection().execute(sql, params).df()

    # -- writes --------------------------------------------------------------

    def write_forecast_segment(self, frame: pd.DataFrame) -> str:
        run_ids = frame["run_id"].unique()
        if len(run_ids) != 1:
            raise ValueError("a forecast segment must hold exactly one run")
        path = self.forecasts_dir / f"{run_ids[0]}.parquet"
        self._atomic_write(frame, path)
        return path.relative_to(self.store).as_posix()

    def append_actuals_segment(self, frame: pd.DataFrame) -> None:
        if frame.empty:
            return
        self._atomic_write(frame, self.actuals_dir / f"{uuid.uuid4().hex}.parquet")

    def append_officials_segment(self, frame: pd.DataFrame) -> None:
        if frame.empty:
            return
        self._atomic_write(frame, self.officials_dir / f"{uuid.uuid4().hex}.parquet")

    def replace_summary(self, frame: pd.DataFrame) -> None:
        self._atomic_write(frame, self.summary_dir / "summary.parquet")

    # -- reads ---------------------------------------------------------------

    def read_forecasts(self, flt: ForecastFilter) -> pd.DataFrame:
        files = self._files(self.forecasts_dir)
        if not files or not list(flt.active_run_ids):
            return empty_forecasts()
        predicate, params = build_forecast_predicate(self.dialect, flt)
        sql = (
            f"SELECT {_FORECAST_SELECT} FROM {self._source_expr(files)} "
            f"WHERE {predicate} ORDER BY model_id, model_version, series_id, target"
        )
        frame = self._query(sql, params)
        frame["horizon"] = frame["horizon"].astype("int64")
        return frame

    def read_actuals(self) -> pd.DataFrame:
        files = self._files(self.actuals_dir)
        if not files:
            return empty_actuals()
        sql = (
            f"SELECT series_id, target, source, actual_value, actual_recorded_at, is_official "
            f"FROM {self._source_expr(files)} ORDER BY actual_recorded_at, series_id, target"
        )
        return self._query(sql, [])

    def read_officials(self) -> pd.DataFrame:
        files = self._files(self.officials_dir)
        if not files:
            return _empty_officials()
        sql = (
            f"SELECT {', '.join(_OFFICIAL_COLUMNS)} "
            f"FROM {self._source_expr(files)} ORDER BY designated_at"
        )
        return self._query(sql, [])

    def read_summary(self) -> pd.DataFrame | None:
        path = self.summary_dir / "summary.parquet"
        if not path.exists():
            return None
        frame = pd.read_parquet(path)
        frame["horizon"] = frame["horizon"].astype("int64")
        frame["n"] = frame["n"].astype("int64")
        return frame
