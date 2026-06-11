"""Dialect-aware storage/query backend seam (amended ADR-002).

All storage and query operations are expressed in engine-neutral,
dialect-parameterized terms over the canonical schema. DuckDB-over-Parquet is
the v1 implementation; warehouse backends (Snowflake, v1.1) plug in additively.

Nothing outside this package may call DuckDB directly or use DuckDB-only SQL
idioms — a CI guard enforces this (warehouse-readiness guard).
"""

from .base import Backend, Dialect, ForecastFilter

__all__ = ["Backend", "Dialect", "ForecastFilter", "create_backend"]


def create_backend(name: str, store: object) -> Backend:
    """Resolve a backend by name. Only ``"duckdb"`` ships in v1; ``"snowflake"``
    is the committed v1.1 fast-follow."""
    from pathlib import Path

    from ..errors import ValidationError

    if name == "duckdb":
        from .duckdb_backend import DuckDBBackend

        return DuckDBBackend(Path(str(store)))
    raise ValidationError(
        f"unknown backend {name!r}: only 'duckdb' is available in v1 ('snowflake' arrives in v1.1)"
    )
