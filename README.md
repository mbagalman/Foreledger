# Foreledger — Production Forecast Archive

A Python library that ingests recurring forecast runs from multiple models and
versions (including parallel runs), stores them as a durable append-only
Parquet archive alongside a revisable actuals log, and answers:

- **Horizon-keyed accuracy** — on a `latest` or `official` actuals basis
- **Cross-model / cross-version comparison** — optionally vs. a champion
- **Bitemporal `as_of` queries** — what did we know, when?

All storage and query goes through a dialect-aware backend seam: DuckDB over
Parquet in v1, with warehouse-native backends (Snowflake) as a v1.1
fast-follow.

> **Status:** pre-alpha scaffolding. The public API lands in Phases 1–4 of the
> [implementation plan](docs/implementation-plan-forecast-archive.md).

## Development

Requires Python ≥ 3.11.

```bash
pip install -e ".[dev]"
pytest                 # test suite
ruff check .           # lint
ruff format .          # format
mypy src               # type check
python -m build        # build artifact
python examples/quickstart.py
```

## Documentation

- Agent / contributor ground rules: [AGENTS.md](AGENTS.md)
- Architecture and data model: [docs/tech-spec-forecast-archive-final.md](docs/tech-spec-forecast-archive-final.md)
- Decisions and rationale: ADR-001…ADR-007 in [docs/](docs/)
- Phased plan, risks, rollout: [docs/implementation-plan-forecast-archive.md](docs/implementation-plan-forecast-archive.md)
- Product requirements: [docs/prd-forecast-archive.md](docs/prd-forecast-archive.md)

> Note: the PyPI distribution name (`forecast-archive` is a placeholder) is an
> open question to resolve before the first release tag.
