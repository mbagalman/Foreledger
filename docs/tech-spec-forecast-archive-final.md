*This artifact was produced by applying `tech-spec.md`'s rules in Revision mode in Integrated mode. Review the prompt's full contract if you want to audit.*

# Tech Spec: Production Forecast Archive

**Stage:** Tech Spec (Final)
**Project:** Production Forecast Archive (working name)
**Date:** 2026-06-10
**Upstream artifacts:** tech-spec-forecast-archive-draft.md, adr-001-archive-schema-generalize-hubverse.md, adr-002-storage-engine-duckdb-backend-seam.md, adr-003-summary-eager-recompute.md, adr-004-metric-protocol.md, adr-005-ingestion-push-identity.md, adr-006-model-version-identity.md, adr-007-actuals-revisions-official.md

> **Amended 2026-06-10** to incorporate post-draft requirements: (1) **multiple models and versions, including parallel runs** — model/version added to the grain and identity per **ADR-006**, plus an optional **champion** designation on comparison; (2) **enterprise-warehouse storage** — the backend seam is now warehouse-ready with Snowflake as the committed fast-follow per the **amended ADR-002**; (3) **revisable actuals with an optional "official" value** — the actuals layer is an append-only revisable log with selectable accuracy basis (`latest`/`official`) per **ADR-007**.

## Assumptions and inferred inputs

| Input | Source | If inferred or user-confirmed: notes |
|---|---|---|
| Product behavior & capabilities | supplied (PRD §Functional requirements) | — |
| Form factor = standalone Python library | user-confirmed | User-selected in PRD scoping. |
| Reference scale = laptop-scale, local primary | user-confirmed | ~hundreds of series, daily/weekly runs, 1–3 yr retention; local DuckDB/Parquet primary in v1. |
| Multiple models & versions (incl. parallel) | supplied (ADR-006) | `model_id` + `model_version` added to the forecast grain and run identity; actuals stay model-independent; cross-model/version comparison (optionally vs. a champion) is a first-class query. |
| Revisable actuals + official designation | supplied (ADR-007) | Actuals are an append-only revisable log (`actual_recorded_at`) with an optional sticky `is_official` row; accuracy basis selectable `latest` (default) / `official`. Partial tri-temporality; full as-of-vintage querying deferred. |
| Schema strategy | supplied (ADR-001, refined by ADR-006/ADR-007) | Generalize the Hubverse conceptual model; lean own physical layout; cheap read-interop; grain widened by model/version; actuals widened to a revisable log. |
| Storage/query engine | supplied (ADR-002, amended) | DuckDB-over-Parquet in v1 behind a **dialect-aware, warehouse-ready** backend seam; Snowflake is the committed first fast-follow (v1.1). |
| Summary refresh strategy | supplied (ADR-003) | Eager recompute on ingest/actuals; summary disposable, rebuildable from raw. |
| Metric extensibility | supplied (ADR-004) | Built-ins + constrained registerable protocol; summarizable metrics precompute. |
| Ingestion contract | supplied (ADR-005) | Push with caller-supplied identity, atomic per-run; Nixtla pull adapter as sugar. |
| Non-functional targets | inferred | PRD targets (~15-min time-to-first-value; sub-second summary queries) are inferred starting points pinned against laptop-scale; no measured baseline yet. |

## Problem statement

The system is a greenfield Python library that ingests recurring forecast runs (model × version × origin × target × value), stores them as a durable append-only columnar archive alongside a revisable actuals log, and exposes horizon-keyed accuracy evaluation (on a `latest` or `official` actual basis), cross-model/version comparison (optionally vs. a champion), and bitemporal "as of" slicing as first-class operations over the user's own data. The spec covers the library's internal component boundaries, on-disk data model, and public API contract for a laptop-scale, local-first deployment, with a dialect-aware backend seam that makes an enterprise-warehouse backend (Snowflake first) an additive fast-follow; it does not cover producing forecasts, managing/registering models, or a hosted-service control plane. The engineering shape is a small, layered single package over a pluggable analytical engine (DuckDB in v1), with an eagerly-recomputed, disposable summary table as a performance optimization on top of the raw archive.

## System context diagram

```
                         ┌─────────────────────────────────────────┐
  user's forecasting     │        Production Forecast Archive       │
  pipeline (Nixtla /     │              (Python library)            │
  sktime / Darts /       │                                          │
  custom)                │   ┌──────────┐   ┌──────────────────┐    │
        │  run outputs   │   │ Ingestion│──▶│  Raw Archive     │    │
        ├───────────────▶│──▶│ (+Nixtla │   │  (Parquet via    │    │
        │                │   │  adapter)│   │   DuckDB seam)   │    │
        │                │   └──────────┘   └────────┬─────────┘    │
  actuals source         │   ┌──────────┐            │              │
  (warehouse / CSV /     │   │ Actuals  │──▶ join ───┤              │
  DataFrame)             │   │  intake  │            ▼              │
        ├───────────────▶│──▶└──────────┘   ┌──────────────────┐    │
        │                │   ┌──────────┐   │ Derived Accuracy │    │
        │                │   │ Eval &   │◀──│ Summary (eager,  │    │
  analyst / report  ◀────│◀──│ Query API│   │ rebuildable)     │    │
  (DataFrame, curve)     │   └────┬─────┘   └──────────────────┘    │
                         │        │ uses                            │
                         │   ┌────▼─────┐                           │
                         │   │ Metrics  │ (built-ins + protocol)    │
                         │   └──────────┘                           │
                         └─────────────────────────────────────────┘

  Trust boundary: entire library runs in-process in the user's environment.
  No network trust boundary in v1. Backend seam (ADR-002) isolates the DuckDB
  engine so a future warehouse backend is additive.
```

## Component breakdown

Decomposition is by responsibility (behavior plus its data), not by layer.

### Ingestion (incl. Nixtla adapter)
- **Responsibility:** Accept a run's forecasts (model × version × origin × target × value + series keys) and append them atomically and idempotently to the raw archive, mapping the user's columns onto the canonical fields. Per ADR-005/ADR-006, the caller supplies run identity (`model_id` + `model_version` + `origin` + series keys); the append is all-or-nothing. Forecasts from different models/versions for the same series/origin/target are distinct rows and never collide. A thin **Nixtla cross-validation adapter** maps `cutoff`→`origin` (and accepts `model_id`/`model_version`) and writes through the same push path.
- **Owned data:** the sole write path to the raw archive; run-identity bookkeeping for idempotency.
- **Dependencies (in):** user forecast frame (pandas/Polars) or Nixtla CV frame. **(out):** backend seam (write).
- **Non-functional posture:** atomic per-run append is the top correctness property (no torn runs); re-ingesting the same identity is a no-op, while a same-identity/different-values run triggers an explicit conflict/overwrite policy.
- **Rationale:** one writer keeps the append-only and atomicity invariants enforceable in a single place; the adapter is sugar over the one contract, not a second write path (ADR-005).

### Actuals intake
- **Responsibility:** Register actuals (model-independent) joinable to forecasts, as an **append-only revisable log** (ADR-007): each registration appends a row with a `source` label and an `actual_recorded_at`; reads default to the latest per `(series, target)` (deterministic tiebreak across sources). Supports marking an actual **official** (`is_official`, at most one per target, sticky) and never rewrites forecasts.
- **Owned data:** the actuals log (a separate layer per ADR-001/ADR-006), including revision history and the official designation.
- **Dependencies (in):** user actuals frame; `mark_official` calls. **(out):** backend seam (write), and triggers a summary recompute (ADR-003) for affected basis/cells.
- **Non-functional posture:** late/revised actuals are the norm; marking official is explicit and logged; a newer non-official registration does not disturb the official row. The log shape *is* the realized tri-temporality the brief flagged (ADR-007) — full as-of-vintage querying is deferred but the data is retained.
- **Rationale:** separate from forecast ingestion because of different grain (per target, revisable) and cadence (late/repeated arrival); the append-only log keeps revisions and the official value without mutating forecasts.

### Backend seam + DuckDB store (raw archive)
- **Responsibility:** Persist the append-only forecast history and actuals, and serve scans/filters/aggregations to the eval and summary layers — all expressed in **engine-neutral, dialect-parameterized** operations over the canonical schema (amended ADR-002). Owns partitioning (by run date and/or horizon; model/version are natural secondary partition/cluster keys) and compression. The only implementation in v1 is DuckDB-over-Parquet; the seam is built **warehouse-ready** so a Snowflake backend (committed v1.1 fast-follow) is an addition, not a rewrite.
- **Owned data:** raw archive (source of truth) + actuals; the archive **format version**. (Local: Parquet files. Warehouse v1.1: warehouse-native tables.)
- **Dependencies (in):** Ingestion, Actuals intake, Summary builder, Eval API. **(out):** the active engine (DuckDB+Parquet in v1; a warehouse client at v1.1).
- **Non-functional posture:** retains full history as source of truth; compressed footprint a fraction of an uncompressed row store via run-to-run redundancy. Engine *and dialect* specifics never leak past the seam; query construction avoids DuckDB-only idioms.
- **Rationale:** a single component owns physical layout, the engine boundary, and SQL-dialect generation, so consumers see logical rows and the adoption-critical warehouse backend is additive (amended ADR-002).

### Derived accuracy summary (eager, disposable)
- **Responsibility:** Maintain the materialized summary — one row per (model × version × series × horizon × metric × period × actual-basis) — recomputed eagerly on each ingest and each actuals registration/official-marking, and **always fully rebuildable from raw** (ADR-003). Reconciles exactly to a raw recomputation. The `latest` basis is always materialized; the `official` basis is materialized for targets that have an official actual. Note cardinality now scales with `models × versions × bases` (ADR-006/ADR-007), tightening the threshold at which eager recompute should give way to incremental.
- **Owned data:** the summary table (disposable cache).
- **Dependencies (in):** backend seam (raw + actuals), Metrics. **(out):** backend seam (write summary).
- **Non-functional posture:** summary is an optimization, never authoritative; if stale/absent, the eval layer recomputes from raw. Eager recompute keeps reconciliation trivial and auto-handles late-actual fan-out.
- **Rationale:** separating refresh strategy from the public API lets us later swap eager→incremental with no API or data-migration impact (ADR-003 Type 2).

### Evaluation & query API (public surface)
- **Responsibility:** `accuracy_at_horizon(h)`, `accuracy_curve(...)`, `as_of(origin)`, `compare_models(...)` / `compare_curve(...)`, `set_champion(...)`, `list_models()`, model/version/series/period scoping, **actual-basis selection (`latest`/`official`)**, metric selection, and `drill(...)` from a summary cell to raw rows. Routes to the summary when possible, falls back to raw computation otherwise — invisibly. Comparison is computed over a common scope so each per-model value equals the corresponding single-model result (FR-7.x); a champion is one-per-`model_id`, and challenger deltas are reported relative to their model's champion. Under `basis="official"`, targets without an official actual are reported as insufficient (never silently substituted) unless the caller opts into `fallback="latest"`, which fills those targets from the latest value and flags them.
- **Owned data:** none (read-only over store + summary).
- **Dependencies (in):** analyst/report caller. **(out):** backend seam, summary, Metrics.
- **Non-functional posture:** summary-backed queries target sub-second at laptop scale; results must equal a hand-written reference query (FR-3.1); comparison results must equal the per-model single calls (FR-7.1).
- **Rationale:** one read surface hides summary-vs-raw routing (the core ergonomics bet); correctness defined as raw-equivalence so the optimization can never silently diverge. Comparison is a thin grouping over the same machinery, not a separate code path.

### Metrics (built-ins + protocol)
- **Responsibility:** Provide MAE, RMSE, MAPE, MASE and a documented **metric protocol** users can register (ADR-004). Metrics satisfying the *summarizable* contract (aggregation over per-pair errors at the summary grain) are precomputed like built-ins; others compute over raw only, with the distinction explicit in the API.
- **Owned data:** none (pure functions / registered callables over aligned (forecast, actual) arrays).
- **Dependencies:** invoked by the summary builder and the eval API.
- **Non-functional posture:** registered user code in the refresh path is guarded (error isolation / timeout) so a bad metric cannot corrupt a recompute.
- **Rationale:** built-ins are implemented *as* protocol-conforming metrics (one code path); the protocol is the library's primary extension point (ADR-004).

## Data model

Canonical entities; physical layout follows ADR-001 (generalized Hubverse model), ADR-006 (model/version keys), and ADR-002 (Parquet via DuckDB in v1, warehouse-ready).

**Forecast (raw archive)** — append-only fact table. Grain: one row per `(model_id, model_version, series_id, origin, target)`.
- `model_id` (categorical, user-supplied), `model_version` (categorical, user-supplied opaque string), `series_id` (categorical), `origin` (transaction-time / run date), `target` (valid-time), `value` (numeric), `horizon` (derived `target − origin`, stored to enable horizon-partitioned scans).
- Ingestion metadata sufficient for run identity/idempotency (ADR-005/ADR-006).
- **Constraints:** append-only; uniqueness on `(model_id, model_version, series_id, origin, target)` under the caller-supplied identity model; forecasts differing only in model/version coexist (parallel versions); consecutive-`origin` redundancy is the compression lever. Partition by run date and/or horizon; model/version are secondary partition/cluster keys.

**Actual** — observations, a separate, **model-independent**, **append-only revisable log** (ADR-001/ADR-006/ADR-007). Grain: one row per `(series_id, target, source, actual_recorded_at)`.
- `series_id`, `target`, `source` (feed/label disambiguating revisions; defaults to a single value when not supplied), `actual_value`, `actual_recorded_at` (transaction-time / knowledge date for the actual), `is_official` (bool).
- **Constraints:** append-only (revisions are new rows, not overwrites); the **effective `latest`** actual per `(series_id, target)` is the row with the max `actual_recorded_at`. **Same-timestamp tiebreak:** (1) if the tied rows are equal-valued, collapse as duplicates; (2) if they differ, resolve by configured `source_priority` (ordered, highest first); (3) if unresolved (no priority, or a tied source not in the list), log an error to the error-log file and mark that target **ambiguous** for the latest basis — no silent pick. At most one `is_official=true` row per `(series_id, target)`, sticky (a later non-official registration does not change it). Shared across all models/versions. **Forward-compat:** retaining `actual_recorded_at` + `source` history *is* the realized tri-temporality; full "accuracy as of actual vintage D" querying is a later addition over already-retained data — no migration.

**Accuracy summary** — derived, disposable (ADR-003). Grain: one row per `(model_id, model_version, series_id, horizon, metric, period, actual_basis)`.
- `model_id`, `model_version`, `series_id`, `horizon`, `metric`, `period`, `actual_basis` (`latest` | `official`), `value`, `n` (sample count), provenance markers for drill-down (FR-5.2).
- **Constraints:** must reconcile exactly to the raw recomputation for the basis; rebuilt from raw on demand; recomputed eagerly on writes. `latest` rows always present; `official` rows only where an official actual exists. Cardinality scales with `models × versions × bases`.

Relationships: Forecast ⋈ Actual (on `(series_id, target)`, basis-selected actual) → per-model error series; summary = `metric(error series)` grouped by `(model_id, model_version, series_id, horizon, period, actual_basis)`. Model comparison = summary rows grouped/pivoted by `(model_id, model_version)` at fixed horizon(s) over a common scope, optionally champion-relative. `as_of(origin)` = transaction-time filter `Forecast.origin ≤ origin` (optionally scoped to a model/version).

**Archive format version** is written into the store header (ADR-001 mitigation); the library refuses to open a newer format than it understands and states the required upgrade. Because the summary is disposable, a format change rebuilds the summary rather than migrating it.

## Interfaces

The only v1 integration boundary is the **public Python API** (in-process; no network interface).

- **`archive = ForecastArchive(store=..., backend="duckdb", source_priority=None, error_log=None)`** — open/create against a local path (v1) or, at v1.1, a warehouse connection (e.g. `backend="snowflake"`). `source_priority` is an optional ordered list of source labels (highest first) used to resolve same-timestamp actual conflicts; `error_log` is the destination for unresolved-conflict errors (defaults to a log file under the archive). *Failure:* corrupt/incompatible/newer-format store raises a typed error; never silently re-initializes. Backend is pluggable via the dialect-aware seam; only `"duckdb"` ships in v1, `"snowflake"` is the committed fast-follow (amended ADR-002).
- **`archive.ingest(frame, mapping=..., model_id=..., model_version=..., series=..., origin=..., on_conflict="error"|"overwrite")`** — atomic append of one run. *Payload:* DataFrame/Parquet mappable to `model_id, model_version, series_id, origin, target, value`. *Idempotency:* re-ingesting the same `(model_id, model_version, origin, series)` is a no-op; same identity with different values follows `on_conflict`; a different model/version adds rows, never overwrites (ADR-005/ADR-006). *Failure:* a partial run is rejected atomically.
- **`archive.ingest_nixtla(cv_frame, model_id=..., model_version=..., ...)`** — adapter mapping a Nixtla cross-validation frame (`cutoff`→`origin`) through the same push path; model/version supplied by the caller.
- **`archive.register_actuals(frame, mapping=..., source=None, official=False, recorded_at=None)`** — append actuals by `(series_id, target, source)` (model-independent); each call is a revision (append, not overwrite); `source` defaults to a single label when omitted; `recorded_at` defaults to now; `official=True` marks the row official. Triggers eager summary recompute (ADR-007).
- **`archive.mark_official(series=..., target=..., source=..., recorded_at=...)`** — designate which registered actual is official for a target (sticky; at most one per `(series, target)`); explicit and logged.
- **`archive.list_models()`** — the `(model_id, model_version)` pairs present and their origin/target coverage (FR-6.2).
- **`archive.set_champion(model_id, model_version)`** — persist the champion *version* for a `model_id` (one champion per `model_id`; last-write-wins metadata) used as the comparison baseline for that model's versions (FR-7.3).
- **`archive.accuracy_at_horizon(h, metric="MAE", basis="latest", fallback=None, model_id=..., model_version=..., series=..., period=...)`** — equals the reference query on the same data for the chosen `basis`; missing actuals → explicit "insufficient actuals" result, never a silent zero; under `basis="official"`, targets without an official actual are reported insufficient unless `fallback="latest"` is set, in which case the latest value is used for those targets (and the result flags which were fallback-filled). Unscoped over model/version, it aggregates across all; scope to one for a single model's number.
- **`archive.accuracy_curve(metric=..., basis="latest", fallback=None, horizons=..., model_id=..., model_version=..., series=..., period=...)`** — one value per horizon; equals per-horizon `accuracy_at_horizon`. Returns a small typed curve object.
- **`archive.compare_models(h, models=[...], metric="MAE", basis="latest", fallback=None, champion=None, series=..., period=...)`** — the metric per listed `(model_id, model_version)` at horizon `h` over a common scope; each value equals the scoped `accuracy_at_horizon`; for any listed version whose `model_id` has a champion (persisted or via `champion=`), results include the challenger's delta vs. that model's champion (FR-7.1/7.3).
- **`archive.compare_curve(models=[...], metric=..., basis="latest", fallback=None, champion=None, horizons=..., series=..., period=...)`** — one accuracy-vs-horizon curve per listed model/version over a common scope; each equals the scoped `accuracy_curve` (FR-7.2/7.3).
- **`archive.as_of(origin, model_id=..., model_version=..., series=...)`** — forecasts known as of `origin`; guarantees no leakage from later runs; optionally scoped to a model/version (FR-4.1).
- **`archive.register_metric(name, fn, summarizable=True|False)`** — register a custom metric per the protocol (ADR-004); `summarizable=True` makes it eligible for precomputation.
- **`archive.drill(summary_cell)`** — raw `(model_id, model_version, origin, target, value, actual)` rows (for the cell's basis) behind a summary value; reconcile to it (FR-5.2).
- **Versioning:** on-disk format version gate as above. **Return shapes:** DataFrames + a typed curve object, native to the pandas/Polars/Nixtla ecosystem.

## Failure modes and reliability targets

- **Partial/interrupted ingestion (crash mid-append).** *Target — recovery point:* no torn run ever visible; a crashed ingest leaves the archive at its pre-run state, re-runnable idempotently (ADR-005). Highest priority — a torn run corrupts every downstream horizon computation.
- **Duplicate/replayed ingestion.** *Target:* idempotent on `(model_id, model_version, origin, series)`; state after N identical ingests equals state after one. A different model/version for the same origin/target adds rows rather than colliding (ADR-006).
- **Same-identity corrected run.** *Target:* explicit `on_conflict` resolution (error/overwrite); never a silent merge.
- **Stale or missing summary.** *Target — graceful degradation:* eval API recomputes from raw; correctness unaffected, only latency. Summary never authoritative over raw.
- **Summary/raw divergence (silent-correctness risk).** *Target:* a reconciliation check (in tests and available at runtime) guarantees equality on representative data; any divergence is surfaced, not absorbed. Eager recompute (ADR-003) makes divergence structurally unlikely.
- **Missing actuals for queried targets.** *Target:* explicit, typed "insufficient data" outcome with covered sample count — never an implicit zero/NaN reading as "perfectly accurate." Under `basis="official"`, targets lacking an official actual are reported insufficient and not silently substituted with `latest` — unless the caller explicitly opts into `fallback="latest"`, in which case fallback-filled targets are flagged in the result (ADR-007).
- **Official actual accidentally overwritten.** *Target:* the official row is sticky — a later non-official `register_actuals` for the same target must not change or unset it; changing official is only via an explicit, logged `mark_official` (ADR-007). Test-asserted.
- **Unresolvable same-timestamp actual conflict (differing values, no source priority).** *Target — fail loud:* the conflict is written to the error-log file and the affected target is marked ambiguous for the latest basis; the library never silently picks one feed's value. Duplicates (equal values) are collapsed, not logged. Test-asserted.
- **Misbehaving registered metric (slow/throwing).** *Target:* error isolation/timeout in the refresh path so one bad metric cannot corrupt or hang a recompute (ADR-004).
- **Corrupt/incompatible/newer-format store.** *Target:* typed error on open; no silent re-initialization discarding history.

No global availability SLA — an in-process library's availability is the host process's concern. Reliability here is about correctness invariants under failure, the right frame for a data asset.

## Security considerations

- **Authentication / authorization:** none introduced; runs in-process under the user's credentials, inheriting filesystem (or future warehouse) access controls. No new principal or auth surface.
- **Data at rest:** written only where the user points the archive; no encryption imposed (defers to FS/warehouse); must never copy data elsewhere or phone home.
- **Data in transit:** none in v1 (local). The v1.1 Snowflake backend inherits the warehouse client's transit security (TLS); the library must not weaken it, and must rely on the official client rather than rolling its own connection handling.
- **Data in logs:** no forecast/actual values, series identifiers, or model identifiers at default verbosity (a series or model name can itself be sensitive); value-level logging is debug-only/opt-in.
- **Secrets:** the library holds none; warehouse credentials (v1.1) pass through from the environment / the user-supplied connection object and are never persisted into the archive or logs.
- **Threat surface:** realistic v1 risk is accidental data exposure via logs or unintended writes, not external attackers (no listener). Trust boundary = the user's process.
- **Compliance:** none assumed; no retention/audit regime imposed.

## Observability

- **Logs:** structured ingest/refresh events (run identity, row counts, durations, actuals-overwrite notices) at info; value-level detail at debug only. Surfaced to callers as return metadata/warnings (in-process, so nothing to "alert" on externally). A dedicated **error-log file** (configurable via `error_log`) records unresolved same-timestamp actual conflicts — a data-integrity channel distinct from ordinary logs so these are not lost in noise.
- **Metrics:** ingest row-count/duration; summary refresh duration and rows touched; query latency and whether served from summary or raw (summary-hit rate is the SLI behind the sub-second target). Exposed as return metadata + optional counters; no backend mandated.
- **Traces:** out of scope for v1 (single-process, synchronous).
- **SLI/SLO link:** two SLIs — (1) summary↔raw reconciliation equality (correctness), (2) summary-served query latency (performance). Both verified in CI rather than monitored in production, which suits a library.

## Cross-cutting concerns

- **Configuration:** archive store + backend via the constructor; laptop-scale defaults (local Parquet under a project dir) so the quickstart needs near-zero config — serves the ~15-min time-to-first-value target. The Snowflake backend (v1.1) takes a connection/credentials object, never embedded secrets.
- **Schema/format versioning & migration:** format version in the archive header; raw-format changes are migration-class (ADR-001/ADR-006, Type 1) so the v1 raw schema bar is high — and because the model/version axis is now in the grain, it must be present from v1 (adding it later would be a migration); the summary is rebuilt, not migrated (ADR-003).
- **Partitioning:** raw partitioned by run date and/or horizon for cheap horizon-keyed scans, with model/version as secondary partition/cluster keys (within the dialect-aware seam, ADR-002).
- **Backend portability (warehouse-readiness):** query construction is engine-neutral and dialect-parameterized so the Snowflake backend (v1.1) is additive; no DuckDB-only SQL idioms leak into the eval/summary layers (amended ADR-002). The Snowflake client is an optional extra (`forecast-archive[snowflake]`).
- **Deployment/packaging:** PyPI package; minimal core deps (DuckDB + a DataFrame lib) to keep install friction low; warehouse clients are optional extras.
- **Extensibility:** the metric protocol (ADR-004) is the primary documented extension point; future ingestion adapters (sktime/Darts) follow the Nixtla adapter pattern over the one push path; future backends (BigQuery, etc.) follow the Snowflake pattern over the seam.
- **Tri-temporality (partial, realized in v1):** actuals are an append-only revisable log keyed on `actual_recorded_at` with an optional sticky `official` row (ADR-007); accuracy is selectable on a `latest`/`official` basis. The full "accuracy as of any actual vintage" query surface is deferred but the data to support it is retained — a later, migration-free addition. Actuals remain model-independent (ADR-006).

## Open architectural questions

> **Q-001 — Archive schema/format: adopt-or-generalize Hubverse vs. greenfield.** Resolved (see ADR-001-archive-schema-generalize-hubverse).
> **Q-002 — Storage & query engine architecture: DuckDB-direct vs. backend abstraction in v1.** Resolved (see ADR-002-storage-engine-duckdb-backend-seam).
> **Q-003 — Derived accuracy summary: refresh/materialization strategy.** Resolved (see ADR-003-summary-eager-recompute).
> **Q-004 — Metric set: fixed built-ins vs. user-defined metric protocol.** Resolved (see ADR-004-metric-protocol).
> **Q-005 — Ingestion contract: push vs. pull, and run-identity/idempotency.** Resolved (see ADR-005-ingestion-push-identity).

Post-draft refinements (raised by the user after the ADR loop, recorded as their own ADRs):

> **Multiple models & versions (incl. parallel runs).** Resolved (see ADR-006-model-version-identity) — `model_id` + `model_version` added to the grain and run identity; actuals model-independent; cross-model comparison first-class (optionally champion-relative).
> **Enterprise-warehouse storage.** Resolved (see amended ADR-002) — dialect-aware, warehouse-ready seam; DuckDB v1, Snowflake committed v1.1 fast-follow.
> **Revisable actuals + official designation.** Resolved (see ADR-007-actuals-revisions-official) — append-only actuals log with selectable `latest`/`official` basis; partial tri-temporality realized, full as-of-vintage querying deferred.

None outstanding — all open architectural questions are resolved.

---

**Mode:** Revision / Final. All five original open architectural questions are resolved by accepted ADRs, plus three post-draft refinements (ADR-006; amended ADR-002; ADR-007); none carried as overrides. **Next step:** `implementation-plan.md`, consuming this Final Tech Spec.
