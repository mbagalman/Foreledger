"""Dialect-aware storage/query backend seam (amended ADR-002).

All storage and query operations are expressed here in engine-neutral,
dialect-parameterized terms over the canonical schema. DuckDB-over-Parquet is
the v1 implementation; warehouse backends (Snowflake, v1.1) plug in additively.

Nothing outside this package may call DuckDB directly or use DuckDB-only SQL
idioms — a CI guard enforces this (warehouse-readiness guard).
"""
