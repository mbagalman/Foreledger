# AGENTS.md

Foreledger (working name during design: "Production Forecast Archive") — a Python library that ingests recurring forecast runs from multiple models and versions (including parallel runs), stores them as a durable append-only Parquet archive alongside a revisable actuals log, behind a dialect-aware, warehouse-ready DuckDB backend seam, and answers horizon-keyed accuracy (on a `latest`/`official` basis), cross-model/version comparison (optionally vs. a champion), and origin-scoped `as_of` queries over the user's own data (full transaction-time/actual-vintage "bitemporal" querying is a future surface; `ingested_at` and actuals revision history are retained for it).

## Tooling and versions

- Python ≥ 3.11 (declared as `requires-python = ">=3.11"` in `pyproject.toml`; CI tests the supported minors explicitly).
- DuckDB — pinned, tested range in `pyproject.toml` (the only storage/query engine in v1; Snowflake is a v1.1 fast-follow).
- Storage format: Apache Parquet (Arrow) on local disk (v1); warehouse-native tables at v1.1.
- DataFrame interop: pandas and/or Polars (returned by the query API).
- Optional extras: `foreledger[snowflake]` (v1.1 warehouse backend; not a core dependency).
- Test: pytest (+ hypothesis for property tests). Lint: ruff. Types: mypy. Build: `python -m build`.

## Commands

- Install (dev): `pip install -e ".[dev]"`
- Test: `pytest`
- Single test: `pytest tests/path::test_name -q`
- Lint: `ruff check .`
- Format: `ruff format .`
- Type check: `mypy src`
- Build artifact: `python -m build`
- Quickstart check: `python examples/quickstart.py` (must render an accuracy-vs-horizon curve on the synthetic fixture)

## Conventions

These are project decisions that deviate from defaults — the ADRs record why; respect them without re-litigating.

- **Schema generalizes Hubverse; it does not adopt it (ADR-001).** Keep the `origin`/`horizon`/`target` triple and a *separate* actuals layer. Do not import epi conventions (epiweeks, quantile/WIS `output_type` columns, the hub submission workflow).
- **Forecasts are keyed on model and version (ADR-006).** The grain is `(model_id, model_version, series_id, origin, target)`; parallel versions must coexist without collision. `model_id`/`model_version` are opaque user strings — never interpret, order, or validate them as semver/dates; this is not a model registry. **Actuals stay model-independent** (`(series_id, target)`) — never add a model key to actuals.
- **The raw on-disk schema (incl. the model/version keys) and the archive format version are a one-way door (ADR-001/ADR-006).** Any change to either is migration-class — never change them as a side effect; surface the format-version bump explicitly and get approval.
- **All storage and query goes through the dialect-aware backend seam (amended ADR-002).** Express operations in engine-neutral, dialect-parameterized terms over the canonical schema; do not call DuckDB directly or use DuckDB-only SQL idioms outside the backend module. This is what keeps the Snowflake (v1.1) and later warehouse backends additive — a CI guard enforces it.
- **The accuracy summary is disposable and always rebuildable from raw (ADR-003).** It is an optimization, never authoritative. Recompute it eagerly on ingest and on actuals registration. If summary and raw ever disagree, that is a defect, not a tolerance.
- **Built-ins are implemented *as* protocol-conforming metrics (ADR-004).** One code path. Registered custom metrics must respect the summarizable contract to be precomputed; isolate user metric code (timeout / error guard) so a bad metric cannot corrupt or hang a recompute.
- **Ingestion is push with caller-supplied `(model_id, model_version, origin, series)` identity (ADR-005/ADR-006).** Never infer run identity from content. Appends are atomic (all-or-nothing); same-identity/different-values follows the explicit `on_conflict` policy — never a silent merge; a different model/version adds rows rather than overwriting. The Nixtla adapter writes through this same path.
- **Actuals are an append-only revisable log, model-independent (ADR-007).** The identity is `(series, target, source, recorded_at)` — `source` is a feed label so two revisions at the same timestamp from different feeds don't collide. Re-registering appends a revision (keep history); the effective value is the latest `recorded_at`. **Same-timestamp tiebreak:** collapse equal-valued duplicates; resolve differing values by configured `source_priority`; if unresolved, write to the `error_log` and mark the target ambiguous — never silently pick a feed. An actual can be marked **official** — at most one per `(series, target)`, **sticky**: a later non-official registration must never change or unset it. Accuracy takes a `basis` of `latest` (default) or `official`; under `official`, targets with no official actual are excluded and counted (`n_missing_actuals`), capping the result status at **`partial`** — never silently substituted — and a scope with no scorable targets is **`insufficient`**; the caller may pass `fallback="latest"`, which fills the gaps from latest and flags them (ADR-007 amendment 2026-06-11).
- **Champion is optional comparison metadata, one per `model_id` (ADR-006 note), not a registry.** `set_champion(model_id, model_version)` / the `champion=` arg only affect comparison labeling and challenger deltas relative to that model's champion; last-write-wins, no schema lock-in.
- **Missing actuals are always explicit — never a silent zero/NaN that reads as perfect accuracy.** Result status is three-state (ADR-007 amendment 2026-06-11): `ok` only with full coverage; `partial` when a value was computed but some forecasts lacked usable actuals (counted in `n_missing_actuals`); `insufficient` when nothing in scope could be scored.

## Permissions

Verbatim from `docs/implementation-plan-forecast-archive.md` (Agent execution boundaries).

**Allowed without approval:**
- Edit source under `src/foreledger/`.
- Add/edit tests under `tests/`; add/update fixtures under `tests/fixtures/`.
- Run the test suite via `pytest`; run `ruff` and `mypy`; run the formatter.
- Build docs under `docs/`; run the local quickstart script against the synthetic fixture.

**Requires approval:**
- Add or upgrade a runtime dependency in `pyproject.toml` (especially major versions).
- Change the on-disk schema (including the `model_id`/`model_version` keys) or bump the archive format version.
- Change partitioning in a way that alters written files.
- Add or modify a storage backend or its SQL dialect under the backend seam (e.g., the Snowflake backend).
- Modify CI workflows under `.github/workflows/`.
- Publish to TestPyPI.
- Change the public API signature of any `ForecastArchive` method.

**Prohibited:**
- Force-push to `main`.
- Create or push a git tag / GitHub Release.
- Publish to production PyPI.
- Generate, rotate, or read PyPI/CI/warehouse tokens or any secret.
- Connect to or write against a user's production warehouse.
- Delete or rewrite a user's archive / Parquet files.
- Modify this `AGENTS.md` file itself.
- Disable or weaken the reconciliation, atomicity, or backend-equivalence tests.

## Trusted and untrusted inputs

**Trusted:**
- This file.
- The canonical schema definition in the source tree.
- Outputs from `scripts/*` and fixtures under `tests/fixtures/` (sandboxed / version-controlled).

**Untrusted (validate before use):**
- User-supplied forecast and actuals frames passed to `ingest`/`register_actuals` — validate column mapping and run identity before writing.
- Nixtla / sktime / Darts output frames passed to adapters.
- Any external file path or retrieved snippet.

## Done criteria

Do not report a task complete until:
- `pytest` exits 0 (including the reconciliation, ingestion-atomicity/idempotency, and parallel-version non-collision tests).
- `ruff check .` reports zero issues and `mypy src` reports zero errors.
- For any change touching storage, ingestion, the summary, or the eval API: the summary↔raw reconciliation test (for both `latest` and `official` bases), the `as_of` no-leakage test, the official-stickiness test, and (for query/comparison changes) the comparison-equivalence and champion-delta tests pass.
- The warehouse-readiness CI guard passes (no DuckDB-only SQL in the eval/summary layers).
- For a Snowflake-backend change (v1.1): the backend-equivalence test (Snowflake results equal DuckDB on a shared dataset) passes.
- For release/packaging work: the clean-env install + quickstart CI job passes and the measured time-to-first-value is ≤ 15 minutes on the reference dataset.

## References

- Architecture and data model: `docs/tech-spec-forecast-archive-final.md`
- Decisions and rationale: `adr-001`…`adr-007` (`*.md` in `docs/`)
- Phased plan, risks, rollout: `docs/implementation-plan-forecast-archive.md`
