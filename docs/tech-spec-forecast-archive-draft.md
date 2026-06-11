*This artifact was produced by applying `tech-spec.md`'s rules in Integrated mode (Draft mode). Review the prompt's full contract if you want to audit.*

# Tech Spec: Production Forecast Archive

**Stage:** Tech Spec (Draft)
**Project:** Production Forecast Archive (working name)
**Date:** 2026-06-10
**Upstream artifacts:** prd-forecast-archive.md

## Assumptions and inferred inputs

| Input | Source | If inferred or user-confirmed: notes |
|---|---|---|
| Product behavior & capabilities | supplied (PRD §Functional requirements) | — |
| Form factor = standalone Python library | user-confirmed | User selected "Standalone Python library" over service/spec in this session. Treated as a forced constraint, not an open question. |
| Reference scale = laptop-scale, local primary | user-confirmed | User selected "Small / laptop-scale": ~hundreds of series, daily/weekly runs, ~1–3 yr retention; local DuckDB/Parquet is the primary backend; warehouse is a later path. |
| Project shape = greenfield | user-confirmed | New build; no current architecture to retrofit. |
| Python-first, columnar storage | supplied (PRD assumptions; concept brief) | Aligns with Nixtla/sktime/Darts ecosystem; columnar handles run-to-run redundancy. |
| Non-functional targets | inferred | PRD targets (≈15-min time-to-first-value; sub-second summary queries) are inferred starting points; no measured baseline. Pinned against laptop-scale here. |
| Storage engine specifics | inferred (open) | Laptop-scale points strongly at DuckDB+Parquet, but the *engine-abstraction* decision is genuinely open — see Q-002. |

## Problem statement

The system is a greenfield Python library that ingests recurring forecast runs (forecast origin × target date × value), stores them as a durable append-only columnar archive, and exposes horizon-keyed accuracy evaluation and bitemporal "as of" slicing as first-class operations over the user's own data. The spec covers the library's internal component boundaries, on-disk data model, and public API contract for a laptop-scale, local-first deployment; it explicitly does not cover producing forecasts or a hosted-service control plane. The engineering shape is a small, layered single-package library over an embedded analytical engine, with a derived summary table as a performance optimization on top of the raw archive.

## System context diagram

```
                         ┌─────────────────────────────────────────┐
  user's forecasting     │        Production Forecast Archive       │
  pipeline (Nixtla /     │              (Python library)            │
  sktime / Darts /       │                                          │
  custom)                │   ┌──────────┐   ┌──────────────────┐    │
        │  run outputs   │   │ Ingestion│──▶│  Raw Archive     │    │
        ├───────────────▶│──▶│  API     │   │  (columnar store)│    │
        │                │   └──────────┘   └────────┬─────────┘    │
  actuals source         │   ┌──────────┐            │              │
  (warehouse / CSV /     │   │ Actuals  │──▶ joins ──┤              │
  DataFrame)             │   │  intake  │            ▼              │
        ├───────────────▶│──▶└──────────┘   ┌──────────────────┐    │
        │                │   ┌──────────┐   │ Derived Accuracy │    │
        │                │   │ Eval &   │◀──│ Summary          │    │
  analyst / report  ◀────│◀──│ Query API│   └──────────────────┘    │
  (DataFrame, curve)     │   └──────────┘                           │
                         └─────────────────────────────────────────┘

  Trust boundary: the entire library runs in-process in the user's environment.
  No network trust boundary in v1 (local files / user's own warehouse connection).
  External systems: (1) upstream forecasting libraries produce run outputs;
  (2) an actuals source provides settled targets; (3) the analyst/report consumes
  query results as DataFrames or curve objects.
```

## Component breakdown

Decomposition is by responsibility (a coherent slice of behavior plus its data), not by layer. Internal layering inside each component is left to implementation.

### Ingestion
- **Responsibility:** Accept a run's forecasts (origin × target × value, plus series keys) and append them to the raw archive idempotently. Map the user's column names onto the archive's canonical fields.
- **Owned data:** write-path to the raw archive; the run-identity bookkeeping used to make re-ingestion idempotent.
- **Dependencies (in):** user-supplied forecast frame (e.g., pandas/Polars DataFrame or Parquet path). **(out):** Storage engine (write).
- **Non-functional posture:** a single run's append must fit inside a pipeline step (FR-1.2 idempotency is the correctness-critical property). The push-vs-pull contract and run-identity rule are open — see Q-005.
- **Rationale:** ingestion is the only writer; isolating it keeps the append-only invariant enforceable in one place rather than smeared across the API.

### Actuals intake
- **Responsibility:** Register settled actuals (one per target for v1) and make them joinable to forecasts on target date + series.
- **Owned data:** the actuals relation.
- **Dependencies (in):** user-supplied actuals frame. **(out):** Storage engine (write).
- **Non-functional posture:** actuals arrive late and may be backfilled; intake must allow adding/overwriting an actual for a target without rewriting forecasts. The actuals representation must not foreclose vintaged actuals later (tri-temporality) — see Q-001 stakes.
- **Rationale:** kept separate from forecast ingestion because actuals have a different cardinality (one per target, not one per origin×target) and a different update cadence; coupling them would force forecast rewrites on actual backfill.

### Storage engine (raw archive)
- **Responsibility:** Persist the append-only forecast history and the actuals relation in a columnar format; serve scans/filters to the eval layer. Own partitioning and compression.
- **Owned data:** the raw archive (source of truth) and actuals, on disk.
- **Dependencies (in):** Ingestion, Actuals intake. **(out):** the embedded analytical engine / files.
- **Non-functional posture:** retains full history as source of truth (FR-2.1); compressed footprint should be a fraction of an uncompressed row store by exploiting run-to-run redundancy. Engine choice and whether a backend abstraction exists in v1 are open — see Q-002.
- **Rationale:** a single component owns physical layout so that partitioning/compression decisions are not leaked into the query API; consumers see logical rows, not files.

### Derived accuracy summary
- **Responsibility:** Maintain a materialized summary — one row per (series × horizon × metric × period) — so common accuracy questions return fast and identically each time, and keep it reconcilable to the raw archive.
- **Owned data:** the summary table.
- **Dependencies (in):** Storage engine (raw + actuals). **(out):** Storage engine (write summary).
- **Non-functional posture:** summary is an optimization, never a correctness dependency — if stale/absent, the eval layer recomputes from raw (PRD reliability requirement). Refresh/materialization strategy is open — see Q-003.
- **Rationale:** separating the summary from the eval API lets us change the refresh strategy (eager/incremental/on-demand) without touching the public query surface.

### Evaluation & query API
- **Responsibility:** The public surface. Implements `accuracy_at_horizon(h)`, the accuracy-vs-horizon curve, `as_of(origin)`, series/period scoping, metric selection, and drill-down from a summary cell to raw rows. Routes a query to the summary when it can, falls back to raw computation otherwise.
- **Owned data:** none (read-only over storage + summary).
- **Dependencies (in):** analyst/report caller. **(out):** Storage engine, Derived accuracy summary, Metrics.
- **Non-functional posture:** summary-backed queries target sub-second at laptop scale; results must equal a hand-written reference query (FR-3.1).
- **Rationale:** one read surface keeps the "summary vs. raw" routing invisible to the user, which is the core ergonomics bet; correctness is defined as raw-equivalence so the optimization can never silently diverge.

### Metrics
- **Responsibility:** Provide accuracy metrics (MAE, RMSE, MAPE, MASE at minimum) with standard definitions, consumed by both the eval API and the summary builder.
- **Owned data:** none (pure functions over (forecast, actual) aligned series).
- **Dependencies:** none inward beyond aligned arrays.
- **Non-functional posture:** whether the set is fixed or user-extensible is open — see Q-004.
- **Rationale:** isolating metrics as pure functions makes them independently testable against known cases (FR-5.1 acceptance) and is the natural seam for pluggability if Q-004 goes that way.

## Data model

Canonical entities. Storage shape per entity is stated where settled; where the physical schema/format is open it is deferred to Q-001/Q-002.

**Forecast (raw archive)** — the append-only fact table. Logical grain: one row per `(series_id, origin, target)`.
- `series_id` — identifier of the forecasted series (string/categorical).
- `origin` — forecast origin / run date / vintage (the transaction-time key).
- `target` — target date the prediction is about (the valid-time key).
- `value` — the predicted value (numeric).
- `horizon` — derived: `target − origin` (stored or computed; storing enables partition/index by horizon — a Q-002/Q-003 consideration).
- Optional: `run_id` / ingestion metadata for idempotency (shape depends on Q-005).
- **Constraints:** append-only; uniqueness on `(series_id, origin, target)` within the settled idempotency model (Q-005). Heavy run-to-run redundancy across consecutive `origin`s is the compression lever.

**Actual** — settled observations. Logical grain: one row per `(series_id, target)` for v1.
- `series_id`, `target`, `actual_value`.
- **Constraints:** one settled actual per `(series_id, target)` in v1. **Forward-compatibility requirement:** the representation must not foreclose adding an actual-vintage axis later (tri-temporality) — i.e., the v1 single-actual table should be a degenerate case of a future vintaged-actuals table, not a shape that requires migration. This constraint is a primary input to Q-001.

**Accuracy summary** — derived/materialized. Grain: one row per `(series_id, horizon, metric, period)`.
- `series_id`, `horizon`, `metric`, `period` (e.g., target-date bucket), `value` (the metric result), `n` (sample count), and a freshness/provenance marker linking back to the raw rows for drill-down (FR-5.2).
- **Constraints:** must reconcile exactly to the equivalent raw computation; refresh semantics per Q-003.

Relationships: Forecast ⋈ Actual on `(series_id, target)` produces the error series; the summary is `metric(error series)` grouped by `(series_id, horizon, period)`. `as_of(origin)` is a filter `Forecast.origin ≤ origin` (transaction-time slice) — no separate entity required.

## Interfaces

The only integration boundary in v1 is the **public Python API** (the library is in-process; there is no network interface). Contract terms the consumer cares about:

- **`archive = ForecastArchive(store=...)`** — open/create an archive at a local path (or a warehouse connection — gated by Q-002). *Failure semantics:* opening a corrupt/incompatible store raises a typed error; never silently re-initializes.
- **`archive.ingest(frame, mapping=..., series=...)`** — append a run. *Payload:* a DataFrame (pandas/Polars) or Parquet path with columns mappable to `series_id, origin, target, value`. *Idempotency:* re-ingesting the same run is a no-op or explicit conflict (Q-005), never silent duplication. *Failure:* partial run is rejected atomically (all-or-nothing per run) — see failure modes.
- **`archive.register_actuals(frame, mapping=...)`** — add/overwrite settled actuals. *Idempotency:* re-registering an actual overwrites by `(series_id, target)`; overwrite is logged.
- **`archive.accuracy_at_horizon(h, metric=..., series=..., period=...)`** — scalar/grouped accuracy at horizon `h`. *Contract:* result equals the hand-written reference query on the same data. *Failure:* missing actuals → explicit "insufficient actuals" result, not a silent zero.
- **`archive.accuracy_curve(metric=..., horizons=..., series=..., period=...)`** — one accuracy value per horizon over the range; equals per-horizon `accuracy_at_horizon`.
- **`archive.as_of(origin, series=...)`** — reconstruct forecasts known as of `origin`; guarantees no leakage from later runs (FR-4.1).
- **`archive.drill(summary_cell)`** — return the raw `(origin, target, value, actual)` rows behind a summary value; they reconcile to it (FR-5.2).
- **Versioning:** the on-disk archive carries a schema/format version; the library refuses to open a newer-format archive than it understands and states the required upgrade. (Schema-evolution strategy is a cross-cutting concern; tied to Q-001/Q-003.)
- **Return shapes:** queries return DataFrames (and a small typed curve object for `accuracy_curve`) to stay native to the Nixtla/pandas/Polars ecosystem.

## Failure modes and reliability targets

Named failure modes first; targets attach to specific modes.

- **Partial/interrupted ingestion (process crash mid-append).** A run must be atomic: either all of its rows are present or none are. *Target — recovery point:* no torn run is ever visible; a crashed ingest leaves the archive at its pre-run state, re-runnable idempotently (Q-005). This is the highest-priority reliability property because a torn run silently corrupts every horizon computation downstream.
- **Duplicate/replayed ingestion.** Re-ingesting an origin must not double-count. *Target:* idempotent — archive state after N identical ingests equals state after one.
- **Stale or missing derived summary.** *Target — graceful degradation:* eval API detects staleness/absence and recomputes from raw; correctness is unaffected, only latency. Summary is never authoritative over raw.
- **Summary/raw divergence (the silent-correctness risk).** A summary value that disagrees with raw recomputation is a defect, not a tolerance. *Target:* a reconciliation check (used in tests and available at runtime) guarantees equality on representative data; any divergence is surfaced, not absorbed.
- **Missing actuals for queried targets.** *Target:* explicit, typed "insufficient data" outcome with the covered sample count — never an implicit zero or NaN that reads as "perfectly accurate."
- **Corrupt/incompatible on-disk store.** *Target:* typed error on open; no silent re-initialization that would discard history.

No global availability SLA applies — this is an in-process library, so "availability" is the host process's concern, not the archive's. Reliability here is about *correctness invariants under failure*, which is the right frame for a data asset.

## Security considerations

- **Authentication / authorization:** none introduced by the library in v1. It runs in-process under the user's own credentials and inherits the access controls of the local filesystem or the user's warehouse connection. The library adds no new principal, no new auth surface.
- **Data handling — at rest:** data is written where the user points the archive; the library does not encrypt at rest (defers to filesystem/warehouse). It must not copy data to any location the user did not specify, and must not phone home.
- **Data handling — in transit:** none in v1 (local). If a warehouse backend is used (Q-002), transit security is the warehouse client's responsibility; the library must not weaken it.
- **Data handling — in logs:** logs must not emit forecast/actual values or series identifiers at default verbosity (a forecasted series name can itself be sensitive, e.g., a revenue line). Value-level logging is opt-in/debug only.
- **Secrets:** the library holds no secrets of its own; any warehouse credentials are passed through from the user's environment and never persisted into the archive or logs.
- **Threat surface:** the realistic v1 threat is accidental data exposure via logs or unintended file writes, not external attackers (no network listener). Trust boundary = the user's process; nothing crosses it by design.
- **Compliance:** none assumed. The library imposes no retention/audit regime; if a user operates under one, they control the store location and lifecycle.

## Observability

For an embedded library the three signals are scoped to what a host process and a debugging user need.

- **Logs:** structured ingest/refresh events (run identity, row counts, durations, overwrite-of-actuals notices) at info; value-level detail at debug only (per security). What's alerted on: not applicable in-process — surfaced as return metadata/warnings the caller can act on.
- **Metrics:** ingest row-count and duration; summary refresh duration and rows touched; query latency and whether served from summary or raw (the summary-hit rate is the SLI behind the "sub-second summary query" target). Exposed as return-value metadata and optional counters the host can scrape; no metrics backend is mandated.
- **Traces:** out of scope for v1 (single-process, synchronous). A single query maps to one call; span instrumentation adds no value yet.
- **SLI/SLO link:** the two reliability-relevant SLIs are (1) summary↔raw reconciliation equality (correctness) and (2) summary-served query latency (performance). Both are testable in CI rather than monitored in production, which suits a library.

## Cross-cutting concerns

- **Configuration:** archive location and backend selection via the `ForecastArchive(...)` constructor; sensible laptop-scale defaults (local Parquet under a project directory) so the quickstart needs near-zero config — directly serves the ~15-min time-to-first-value target.
- **Schema/format versioning & migration:** the archive carries a format version; forward/backward compatibility window and online-vs-offline migration strategy are **open — see Q-001 and Q-003** (the summary can be rebuilt from raw, which simplifies its migration, but the raw format's evolution rules are load-bearing).
- **Partitioning:** partition raw by run date and/or horizon to make horizon-keyed scans cheap — **tied to Q-002/Q-003**.
- **Deployment/packaging:** standard PyPI package; minimal dependencies (embedded engine + a DataFrame lib) to keep install friction low.
- **Feature flagging:** not needed in v1.
- **Tri-temporality forward-compatibility:** a design constraint across the data model — v1 ships single-actual, but the schema must allow a future actual-vintage axis without migrating existing archives (input to Q-001).

## Open architectural questions

> **Q-001 — Archive schema/format: adopt-or-generalize Hubverse vs. greenfield.** Context: the Hubverse model-output format already encodes `reference_date` (origin), `horizon`, `target_end_date` in Parquet with an Arrow schema and a separate target-data layer — i.e., most of our raw + actuals model already exists, proven, and documented; but it is shaped for collaborative epi-forecasting hubs (quantile/WIS output types, epiweek conventions, multi-team submission). Options sketched: (A) adopt the Hubverse model-output format directly and constrain to a point-forecast profile; (B) generalize it — same conceptual schema, our own leaner physical layout, drop epi conventions, keep interop where cheap; (C) greenfield schema designed only for the operational-single-team case, with Hubverse as inspiration not dependency. Stakes: drives the raw/actuals physical schema, the actuals-vintage forward-compatibility (tri-temporality), interop with an existing ecosystem, and how much "not invented here" risk we carry. This is the highest-leverage decision in the project. Next step: run `adr.md` with this question.

> **Q-002 — Storage & query engine architecture: DuckDB-direct vs. backend abstraction in v1.** Context: laptop-scale + local-primary points strongly at DuckDB over Parquet; but the PRD keeps a warehouse path open ("usable locally or against a warehouse through one query API"). The question is whether v1 builds a backend-abstraction seam now or commits to DuckDB-direct and abstracts later. Options sketched: (A) DuckDB-direct, no abstraction — fastest to value, refactor risk if/when warehouse arrives; (B) thin backend interface with a DuckDB implementation only in v1 — modest upfront cost, warehouse-ready seam; (C) full dual-backend (DuckDB + one warehouse) in v1 — contradicts the laptop-scale scope, heaviest. Stakes: time-to-first-value vs. future-proofing; whether `accuracy_*` query construction is engine-agnostic from day one. Next step: run `adr.md`.

> **Q-003 — Derived accuracy summary: refresh/materialization strategy.** Context: the summary must be fast and always reconcilable to raw, and it must refresh as new runs and late actuals land. Options sketched: (A) eager full recompute on each ingest — simplest, correct, may get slow as history grows; (B) incremental update touching only affected (series × horizon × period) cells — faster steady-state, more complex invalidation (late actuals retro-touch many cells); (C) on-demand compute with a cache, no standing materialized table — simplest storage, weaker latency guarantee. Stakes: the sub-second-query and time-to-first-value targets, invalidation correctness when actuals are backfilled, and on-disk footprint. Next step: run `adr.md`.

> **Q-004 — Metric set: fixed built-ins vs. user-defined metric protocol.** Context: MAE/RMSE/MAPE/MASE are required; the brief asks whether users can register their own. Options sketched: (A) fixed built-in set only — smallest API surface, fully testable, may not fit every team; (B) built-ins plus a documented metric-function protocol users can register — extensible, but pluggable metrics must still be summarizable/aggregatable by horizon (not all custom metrics compose cleanly with the summary grain); (C) built-ins plus arbitrary post-hoc metrics computed only over raw (not materialized). Stakes: public API shape, what the summary can precompute, and the support burden of arbitrary user code in the hot path. Next step: run `adr.md`.

> **Q-005 — Ingestion contract: push vs. pull, and run-identity/idempotency.** Context: FR-1.1/1.2 require append + idempotency; the brief lists push (pipeline writes each run) vs. pull (archive reads model outputs). Coupled to it is how a "run" is identified so re-ingestion is idempotent and atomic. Options sketched: (A) push-only with explicit `run_id`/origin as the idempotency key — simplest contract, requires the caller to supply stable identity; (B) push with inferred identity from `(series_id, origin)` content — zero caller ceremony, risk of false-dedupe if a run is legitimately corrected; (C) pull adapter that reads a forecasting library's output (e.g., Nixtla cross-validation frame) and derives identity — best ecosystem ergonomics, more surface to maintain. Stakes: the central idempotency/atomicity correctness property, caller ergonomics (time-to-first-value), and how tightly we couple to upstream forecasting libraries. Next step: run `adr.md`.

---

**Mode:** Draft. **Open architectural questions surfaced:** 5 (Q-001 … Q-005). **Next step:** run `adr.md` once per question (one decision per ADR — none of these should be bundled), then re-run `tech-spec.md` in Revision mode with the resolved ADRs to produce the Final.
