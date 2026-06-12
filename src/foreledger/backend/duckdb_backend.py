"""DuckDB-over-Parquet backend — the only v1 engine (amended ADR-002).

Engine specifics (DuckDB connection handling, ``read_parquet`` table
expressions, the physical Parquet layout) live here and never leak past the
seam. Raw segments are written append-only via temp-file + atomic rename;
forecast visibility is governed by the run manifest's active run_ids, so a
crashed ingest leaves the archive at its pre-run state.
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import logging
import os
import re
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ..errors import StoreFormatError
from ..jsonstore import atomic_write_json
from ..schema import SUMMARY_COLUMNS, empty_actuals, empty_forecasts
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
        # Generation publication: the data lands under a unique immutable
        # name first, then ONE atomic metadata write — naming the token, the
        # data file, AND the data file's content digest — publishes it. A
        # reader captures the metadata once and reads only that generation,
        # so it can never pair one generation's token with another's data, no
        # matter how the reads interleave with this replacement; and a
        # generation whose bytes no longer match the published digest (an
        # in-place edit, disk corruption) is discarded as an absent cache,
        # never served. (Writers are serialized by the archive's store lock;
        # the cleanup below can therefore never race another writer's
        # about-to-be-published generation.)
        name = f"summary-{uuid.uuid4().hex}.parquet"
        path = self.summary_dir / name
        try:
            self._atomic_write(frame, path)
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            atomic_write_json(
                self.summary_dir / "summary_meta.json",
                {"state_token": state_token, "data": name, "sha256": digest},
                indent=None,
            )
        except BaseException:
            # nothing was published: remove this attempt's data file AND its
            # temp file so repeated tolerated refresh failures (the boundary
            # this cache is designed to survive) cannot accumulate orphans
            for leftover in (path, path.with_suffix(".parquet.tmp")):
                with contextlib.suppress(OSError):
                    leftover.unlink()
            raise
        for stale in (
            *self.summary_dir.glob("summary*.parquet"),
            *self.summary_dir.glob("summary*.parquet.tmp"),  # crash leftovers
        ):
            if stale.name != name:
                # best-effort: a reader holding the old generation open keeps
                # its file alive (Windows) — the next replacement sweeps it; a
                # reader that loses this race treats the missing file as an
                # absent cache and computes from raw
                with contextlib.suppress(OSError):
                    stale.unlink()

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
        # defense in depth behind token validation: even a symlinked segment
        # must resolve inside the archive root
        root = self.store.resolve()
        files = []
        for name in segments:
            resolved = (self.store / name).resolve()
            if not resolved.is_relative_to(root):
                raise StoreFormatError(
                    f"segment {name!r} resolves outside the archive store; the "
                    "store is corrupt or was tampered with"
                )
            files.append(resolved.as_posix())
        return files

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

    #: A published summary generation is one flat, backend-minted file name.
    #: A metadata file naming anything else is treated as an absent cache,
    #: never resolved — a tampered pointer must not read files outside the
    #: cache. (Pre-generation layouts simply fail this contract and rebuild.)
    _SUMMARY_DATA_PATTERN = re.compile(r"^summary-[0-9a-f]{32}\.parquet$")

    #: Integer / float dtype contracts re-imposed on read; the object-typed
    #: identity columns need no coercion.
    _SUMMARY_INT_COLUMNS = ("horizon", "n", "n_forecasts")

    def read_summary(self) -> tuple[pd.DataFrame, str] | None:
        meta_path = self.summary_dir / "summary_meta.json"
        if not meta_path.exists():
            return None
        # The summary is a disposable cache: ANY failure here — unreadable
        # metadata, a generation lost to a concurrent replacement's cleanup,
        # bytes that no longer match the published content digest (in-place
        # edit, disk corruption), or a frame that violates the summary schema
        # contract — is cache invalidation (rebuildable from raw), never a
        # query error or a served number. The single metadata read yields a
        # coherent (token, generation, digest) triple, and the digest is
        # checked over the same bytes that are parsed. Boundary: this makes
        # single-file modification loud; coordinated edits to the generation
        # AND its pointer are caught by reconcile(), the same trust boundary
        # as the rest of the disposable cache.
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            token = str(meta["state_token"])
            name = str(meta["data"])
            digest = str(meta["sha256"])
            if not self._SUMMARY_DATA_PATTERN.match(name):
                raise ValueError(f"invalid summary data name {name!r}")
            raw = (self.summary_dir / name).read_bytes()
            if hashlib.sha256(raw).hexdigest() != digest:
                raise ValueError("summary generation does not match its published digest")
            frame = pd.read_parquet(io.BytesIO(raw))
            if frame.columns.duplicated().any():
                raise ValueError("summary frame has duplicate columns")
            missing = [column for column in SUMMARY_COLUMNS if column not in frame.columns]
            if missing:
                raise ValueError(f"summary frame is missing columns {missing}")
            frame = frame[SUMMARY_COLUMNS].copy()
            for column in self._SUMMARY_INT_COLUMNS:
                frame[column] = frame[column].astype("int64")
            frame["value"] = frame["value"].astype("float64")
        except Exception:
            logger.warning(
                "stored summary is unreadable; treating it as absent (it will be rebuilt from raw)",
                exc_info=True,
            )
            return None
        return frame, token
