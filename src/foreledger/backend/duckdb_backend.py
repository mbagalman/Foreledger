"""DuckDB-over-Parquet backend — the only v1 engine (amended ADR-002).

Engine specifics (DuckDB connection handling, ``read_parquet`` table
expressions, the physical Parquet layout) live here and never leak past the
seam. Raw segments are written append-only via temp-file + atomic rename;
forecast visibility is governed by the run manifest's active run_ids, so a
crashed ingest leaves the archive at its pre-run state.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ..errors import StoreFormatError
from ..schema import empty_actuals, empty_forecasts
from .base import Backend, Dialect, ForecastFilter, build_forecast_predicate

logger = logging.getLogger("foreledger.backend")

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
        path = self.forecasts_dir / f"{uuid.uuid4().hex}.parquet"
        self._atomic_write(frame, path)
        return path.relative_to(self.store).as_posix()

    def append_actuals_segment(self, frame: pd.DataFrame) -> str:
        path = self.actuals_dir / f"{uuid.uuid4().hex}.parquet"
        self._atomic_write(frame, path)
        return path.relative_to(self.store).as_posix()

    def append_officials_segment(self, frame: pd.DataFrame) -> str:
        path = self.officials_dir / f"{uuid.uuid4().hex}.parquet"
        self._atomic_write(frame, path)
        return path.relative_to(self.store).as_posix()

    def replace_summary(self, frame: pd.DataFrame, state_token: str) -> None:
        # Data first, token second: a crash in between leaves a mismatched
        # token, so the half-replaced summary is never served.
        self._atomic_write(frame, self.summary_dir / "summary.parquet")
        meta_path = self.summary_dir / "summary_meta.json"
        tmp = meta_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps({"state_token": state_token}), encoding="utf-8")
        os.replace(tmp, meta_path)

    # -- reads ---------------------------------------------------------------

    def read_forecasts(self, flt: ForecastFilter) -> pd.DataFrame:
        # scan only manifest-committed segments — never the directory
        files = self._segment_files(flt.segments)
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

    def missing_segments(self, segments: Sequence[str]) -> list[str]:
        return [name for name in segments if not (self.store / name).exists()]

    def stat_segments(self, segments: Sequence[str]) -> dict[str, tuple[int, int]]:
        stats: dict[str, tuple[int, int]] = {}
        for name in segments:
            try:
                stat = (self.store / name).stat()
            except FileNotFoundError:
                continue
            stats[name] = (stat.st_size, stat.st_mtime_ns)
        return stats

    def fingerprint_segment(self, segment: str) -> dict[str, Any]:
        path = self.store / segment
        stat = path.stat()
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        return {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns, "sha256": digest}

    def _segment_files(self, segments: Sequence[str]) -> list[str]:
        # A committed segment that is gone is corruption, never a skip: the
        # archive promises raw data is the durable source of truth.
        missing = self.missing_segments(segments)
        if missing:
            raise StoreFormatError(
                f"{len(missing)} committed segment(s) are missing from the store "
                f"(e.g. {missing[0]!r}); raw archive data was deleted or modified "
                "externally"
            )
        return [(self.store / name).as_posix() for name in segments]

    def read_actuals(self, segments: Sequence[str]) -> pd.DataFrame:
        files = self._segment_files(segments)
        if not files:
            return empty_actuals()
        sql = (
            f"SELECT series_id, target, source, actual_value, actual_recorded_at, is_official "
            f"FROM {self._source_expr(files)} ORDER BY actual_recorded_at, series_id, target"
        )
        return self._query(sql, [])

    def read_officials(self, segments: Sequence[str]) -> pd.DataFrame:
        files = self._segment_files(segments)
        if not files:
            return _empty_officials()
        sql = (
            f"SELECT {', '.join(_OFFICIAL_COLUMNS)} "
            f"FROM {self._source_expr(files)} ORDER BY designated_at"
        )
        return self._query(sql, [])

    def list_segments(self) -> tuple[list[str], list[str]]:
        def relative(paths: list[str]) -> list[str]:
            return [Path(p).relative_to(self.store).as_posix() for p in paths]

        return (
            relative(self._files(self.actuals_dir)),
            relative(self._files(self.officials_dir)),
        )

    def read_summary(self) -> tuple[pd.DataFrame, str] | None:
        path = self.summary_dir / "summary.parquet"
        meta_path = self.summary_dir / "summary_meta.json"
        if not path.exists() or not meta_path.exists():
            return None
        # The summary is a disposable cache: any read or schema failure is
        # cache invalidation (rebuildable from raw), never a query error.
        try:
            token = str(json.loads(meta_path.read_text(encoding="utf-8"))["state_token"])
            frame = pd.read_parquet(path)
            frame["horizon"] = frame["horizon"].astype("int64")
            frame["n"] = frame["n"].astype("int64")
        except Exception:
            logger.warning(
                "stored summary is unreadable; treating it as absent (it will be rebuilt from raw)",
                exc_info=True,
            )
            return None
        return frame, token
