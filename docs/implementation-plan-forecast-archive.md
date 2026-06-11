*This artifact was produced by applying `implementation-plan.md`'s rules in Integrated mode. Review the prompt's full contract if you want to audit.*

# Implementation Plan: Production Forecast Archive

**Stage:** Implementation Plan
**Project:** Production Forecast Archive (working name)
**Date:** 2026-06-10
**Upstream artifacts:** tech-spec-forecast-archive-final.md, adr-001-archive-schema-generalize-hubverse.md, adr-002-storage-engine-duckdb-backend-seam.md (amended), adr-003-summary-eager-recompute.md, adr-004-metric-protocol.md, adr-005-ingestion-push-identity.md, adr-006-model-version-identity.md, adr-007-actuals-revisions-official.md

> **Amended 2026-06-10** for post-draft requirements: model/version axis in the schema (ADR-006); a warehouse-ready seam with Snowflake as a v1.1 fast-follow (amended ADR-002); an optional champion designation on comparison (ADR-006 note); and revisable actuals with an optional "official" value and selectable accuracy basis (ADR-007). New Phase 6 covers the Snowflake backend (does not gate the v1 release).

## Assumptions and inferred inputs

| Input | Source | If inferred or user-confirmed: notes |
|---|---|---|
| Tech Spec (Final) | supplied (`tech-spec-forecast-archive-final.md`) | — |
| Resolved ADRs (001–006) | supplied | ADR-006 (model/version axis) and amended ADR-002 (warehouse-ready seam, Snowflake v1.1) included. |
| Multiple models & versions | supplied (ADR-006) | Schema carries `model_id` + `model_version`; v1 ships cross-model/version comparison with an optional champion designation; not a model registry. |
| Revisable actuals + official value | supplied (ADR-007) | Actuals are an append-only revisable log; default `latest` basis, optional sticky `official` value; accuracy basis selectable. Partial tri-temporality; full as-of-vintage querying deferred. |
| Warehouse backend (Snowflake) | supplied (amended ADR-002) | v1 ships DuckDB behind a dialect-aware seam; Snowflake is a v1.1 fast-follow (Phase 6), not part of the v1 release gate. |
| Team capacity | inferred | No capacity stated. Plan assumes a solo or small (1–2 person) open-source maintainer working part-time; estimates are engineering-days, not calendar dates. Flag if this is actually a funded team — phase parallelism would change. |
| Definition of done | inferred | Derived from the PRD acceptance criteria + the user's goal to "make it publicly available": published, documented, quickstart-validated. |
| Reference dataset for perf/DX checks | inferred | A laptop-scale synthetic dataset (hundreds of series × daily runs × ~2 yr) must be built as a fixture; named in Phase 1. |

## Plan overview

- **Scope:** Build and publicly release v1 of a Python library that ingests recurring forecast runs from **multiple models and versions** (including parallel runs), stores them as a durable append-only Parquet archive — alongside a **revisable actuals log with an optional sticky "official" value** — behind a **dialect-aware, warehouse-ready** DuckDB backend seam, and exposes horizon-keyed accuracy evaluation (on a `latest`/`official` **basis**), **cross-model/version comparison** (optionally vs. a **champion**), and bitemporal `as_of` slicing — per the Final Tech Spec and ADRs 001–007. The **Snowflake** warehouse backend is a committed v1.1 fast-follow (Phase 6), built on the same seam.
- **Target completion shape:** Versioned open-source release. No production "cutover"; the rollout is a TestPyPI canary → PyPI publish, with the on-disk **format version** acting as the forward-compatibility safety latch. Soft target: a working, published v0.1 once Phases 1–5 gate through; v1.1 adds the Snowflake backend (Phase 6).
- **Exclusions:** No forecasting model; **no model registry / lifecycle management** (the archive keys on opaque model/version strings, compares them, and tracks an optional champion label, nothing more); no hosted service; no warehouse backend *in the v1 release* (Snowflake lands at v1.1, Phase 6; BigQuery/others later); **no full as-of-actual-vintage querying** (v1 ships revisable actuals + `latest`/`official` basis and retains revision history, but the arbitrary-vintage query surface is deferred); no GUI/dashboard; no epi-hub or enterprise-SCM feature parity. (Per Tech Spec/PRD non-goals.)

## Work breakdown

Ordering is constrained by the ADRs: the schema (ADR-001/ADR-006, incl. model/version) and the dialect-aware seam (amended ADR-002) are foundational and land first; ingestion identity/atomicity (ADR-005/ADR-006) builds on the schema; metrics + summary (ADR-004/003) precede the public eval API and comparison surface that consume them; packaging/release closes v1; the Snowflake backend (Phase 6) is a post-v1 fast-follow on the seam. Dependency graph: **P1 → P2 → P3 → P4 → P5**, with P3's metrics sub-stream parallelizable against P2 once P1 is done; **P6 depends on P4** (a stable seam + query layer) and runs after the v1 release.

### Phase 1 — Schema + storage foundation (warehouse-ready seam, DuckDB, Parquet)
- **Goal:** Persist and scan the raw forecast archive (keyed on model/version) and the separate, model-independent actuals layer through an engine-neutral, **dialect-aware** backend seam with a DuckDB-over-Parquet implementation, with an on-disk format version.
- **Deliverable:** `ForecastArchive(store, backend="duckdb")` that creates/opens a store; canonical schema (`model_id, model_version, series_id, origin, target, value, horizon`; actuals `series_id, target, actual_value`) written as partitioned Parquet; format-version header read/write; a **dialect layer** (query construction parameterized by dialect, DuckDB the only impl) so no DuckDB-only SQL leaks upward; a reusable laptop-scale synthetic fixture dataset **containing multiple models and ≥2 versions (incl. an overlapping/parallel pair)**; a raw-recomputation reconciliation harness (scaffold).
- **Approval-gate criteria:**
  - All unit tests pass on the phase branch in CI (round-trip persist→scan equality for forecasts and actuals, including model/version round-trip).
  - Opening a store whose format version is newer than the library raises the typed error (test-asserted); opening a corrupt store does not re-initialize (test-asserted).
  - A static check (lint rule or test) asserts the eval/summary layers issue only dialect-layer calls, no raw DuckDB-only SQL idioms (warehouse-readiness guard).
  - Linter and type-checker (`ruff` + `mypy`, or chosen equivalents) report zero errors.
- **Estimated effort:** 7–11 engineering-days, P50 = 9. The dialect-aware seam and the wider grain add ~2 days over a DuckDB-direct, single-model build; if partitioning needs iteration for scan performance, the higher end.
- **Dependencies:** ADR-001, ADR-006, amended ADR-002. None upstream in code.

### Phase 2 — Ingestion (push, caller identity incl. model/version, atomic, idempotent) + Nixtla adapter
- **Goal:** Append a run atomically and idempotently using caller-supplied `(model_id, model_version, origin, series)` identity, with an `on_conflict` policy and a Nixtla cross-validation adapter over the same write path.
- **Deliverable:** `archive.ingest(...)` (with `model_id`/`model_version`), `archive.ingest_nixtla(...)`, `archive.register_actuals(... source=, official=, recorded_at=)` (append-only revisions with a source/feed label), `archive.mark_official(...)`, `archive.list_models(...)`; run-identity bookkeeping; atomic all-or-nothing append; actuals revision log + sticky-official semantics.
- **Approval-gate criteria:**
  - Idempotency test passes: archive state after N identical `ingest` calls equals state after one.
  - **Parallel-version non-collision test passes:** ingesting two `model_version`s for the same `(series, origin, target)` yields two distinct rows; neither overwrites the other.
  - **Actuals revision test passes:** re-registering a revised actual appends (history retained); the effective `latest` is the newest `recorded_at`.
  - **Official-stickiness test passes:** after `mark_official` (or `official=True`), a later non-official registration for the same target leaves the official value unchanged; at most one official row per `(series, target)`.
  - **Source-disambiguation + tiebreak test passes:** two actuals for the same `(series, target, recorded_at)` but different `source` are retained as distinct rows; the `latest` tiebreak collapses equal-valued duplicates, resolves differing values by configured `source_priority`, and — when differing values have no resolving priority — writes the conflict to the error log and marks the target ambiguous (no silent pick).
  - Atomicity test passes: a fault injected mid-append leaves the archive at its exact pre-run state (no torn run visible).
  - `on_conflict="error"` and `"overwrite"` behaviors are each test-asserted; same-identity/different-values never silently merges.
  - `ingest_nixtla` maps `cutoff`→`origin` (with caller-supplied model/version) and produces an archive equal to the equivalent explicit `ingest` on a shared fixture (test-asserted).
  - `list_models()` returns the expected `(model_id, model_version)` set and coverage on the fixture.
  - CI green; linter/type-checker zero errors.
- **Estimated effort:** 7–11 engineering-days, P50 = 9. Atomic-append plus the actuals revision log and official-stickiness semantics are the cost drivers.
- **Dependencies:** Phase 1.

### Phase 3 — Metrics (built-ins + protocol) + derived summary (eager, rebuildable)
- **Goal:** Provide MAE/RMSE/MAPE/MASE and a registerable metric protocol, and an eagerly-recomputed summary that reconciles exactly to raw and rebuilds from raw on demand.
- **Deliverable:** metrics module (built-ins implemented *as* protocol-conforming metrics); `archive.register_metric(...)`; summary builder keyed on `(model_id, model_version, series_id, horizon, metric, period, actual_basis)` triggered on ingest/actuals/official-marking; runtime + test reconciliation check per basis; error isolation/timeout guard for registered metrics.
- **Approval-gate criteria:**
  - Each built-in metric matches its standard definition on a known hand-computed test case (test-asserted).
  - Summary-vs-raw reconciliation holds exactly on the multi-model/version synthetic fixture **for both `latest` and `official` bases**, including a **late-actual backfill** scenario that retro-touches multiple cells across models/versions (property/parametric test passes).
  - The `official` basis materializes only for targets with an official actual; targets without one are absent from `official` rows (not zero-filled) — test-asserted.
  - A deliberately slow/throwing registered metric is isolated and does not corrupt or hang a recompute (test-asserted).
  - CI green; linter/type-checker zero errors.
- **Estimated effort:** 6–10 engineering-days, P50 = 8. The summarizable-metric contract and late-actual reconciliation are the risk; metrics math itself is cheap.
- **Dependencies:** Phase 1 (metrics sub-stream may start in parallel with Phase 2; summary builder needs Phase 2's write path).

### Phase 4 — Evaluation, comparison & query API
- **Goal:** Ship the public read surface — `accuracy_at_horizon`, `accuracy_curve` (with `basis`), `as_of`, `compare_models`, `compare_curve` (with optional `champion`), `set_champion`, model/version/series/period scoping, `drill` — routing to the summary when possible and falling back to raw invisibly.
- **Deliverable:** the public query API (basis selection + cross-model/version comparison + champion deltas) returning DataFrames + a typed curve object.
- **Approval-gate criteria:**
  - `accuracy_at_horizon(h)` and `accuracy_curve(...)` results equal a hand-written reference query on the fixture (test-asserted), both summary-backed and raw-fallback, for both `basis="latest"` and `basis="official"`, including when scoped to a model/version.
  - **Basis test passes:** `latest` and `official` return different, individually-correct numbers on a fixture where an official actual differs from the latest; `official` reports insufficient for targets lacking an official actual; `basis="official", fallback="latest"` fills those targets from latest and flags them as fallback-filled.
  - **Comparison equivalence test passes:** `compare_models`/`compare_curve` return per-model values each equal to the corresponding scoped single-model call.
  - **Champion test passes:** with a champion set per `model_id` (via `set_champion` or the `champion=` arg), comparison identifies that model's champion and reports correct challenger deltas relative to it; at most one champion per `model_id`; with none, output is order-agnostic.
  - `as_of(origin)` no-leakage test passes: no forecast with `origin' > origin` appears; model/version scoping returns only that model/version.
  - `drill(cell)` returns rows (incl. model/version, for the cell's basis) that reconcile to the summary value (test-asserted).
  - Missing-actuals query returns the explicit "insufficient data" outcome, never a silent zero (test-asserted).
  - CI green; linter/type-checker zero errors.
- **Estimated effort:** 7–10 engineering-days, P50 = 8. Comparison and champion deltas are thin grouping over the same machinery; basis selection threads through every query; the equivalence/basis/champion tests are the gate.
- **Dependencies:** Phases 2 and 3.

### Phase 5 — Packaging, docs, quickstart, public release
- **Goal:** Publish v0.1 to PyPI with a quickstart that takes a new user from install to a first accuracy-vs-horizon curve in ~15 minutes.
- **Deliverable:** PyPI-ready package (pyproject, minimal core deps; warehouse clients as optional extras); README + quickstart + API docs covering ingestion, the horizon-accuracy queries, **multi-model/version archiving and comparison**, and a note on the **warehouse roadmap** (Snowflake v1.1); a runnable quickstart example on the synthetic fixture; TestPyPI canary then PyPI publish.
- **Approval-gate criteria:**
  - In a clean virtual environment in CI, `pip install` the built artifact and run the quickstart end-to-end to a rendered curve with zero manual steps (test-asserted in CI).
  - Time-to-first-value on the reference dataset, measured by a scripted timer in CI, is ≤ 15 minutes of wall-clock for the documented quickstart path.
  - Docs build with zero broken internal links; the README quickstart code block is executed in CI (doctest or equivalent) and passes; docs include at least one cross-model comparison example.
  - Package published to TestPyPI and installs cleanly from it before the PyPI publish step runs.
- **Estimated effort:** 4–7 engineering-days, P50 = 5. Docs/quickstart polish is the variable; packaging mechanics are well-trodden.
- **Dependencies:** Phase 4 (a working API to document and ship). **This phase closes the v1 release.**

### Phase 6 — Snowflake backend (v1.1 fast-follow — does not gate the v1 release)
- **Goal:** Implement a Snowflake backend against the existing dialect-aware seam so corporate users can keep the archive in their EDW, with no change to the public API.
- **Deliverable:** a `backend="snowflake"` implementation (connection/credentials passed in, never embedded); Snowflake dialect for the query layer; `forecast-archive[snowflake]` optional extra; warehouse-backend integration tests.
- **Approval-gate criteria:**
  - The full evaluation/comparison test suite passes against a Snowflake backend and returns results equal to the DuckDB backend on a shared test dataset (backend-equivalence test-asserted).
  - Ingestion atomicity and idempotency hold on Snowflake (test-asserted against a test database).
  - No credentials appear in logs or the archive (secrets-scan + log assertion pass).
  - CI green (Snowflake integration job, gated on a test-account secret); linter/type-checker zero errors.
- **Estimated effort:** 8–14 engineering-days, P50 = 10. Warehouse semantics (transactions, MERGE/atomicity, type mapping, CI against a live test account) are the cost drivers; the seam means the eval/summary layers are untouched.
- **Dependencies:** Phase 4 (stable seam + query layer) and the v1 release. Runs post-v1.

**Cumulative effort:** v1 (Phases 1–5) ~31–49 engineering-days (P50 ≈ 38), i.e. roughly 6–10 part-time weeks for a solo maintainer; compresses with a second contributor on the parallelizable metrics sub-stream. The actuals-revision/official work (Phase 2) and basis selection (Phases 3–4) account for the increase over the prior estimate. Phase 6 (Snowflake, v1.1) adds ~8–14 engineering-days post-release.

## Agent execution boundaries

Single project-level table. `agents-md-generator.md` consumes this verbatim. Surfaces assume a standard Python package layout: `src/forecast_archive/`, `tests/`, `docs/`, `.github/workflows/`, `pyproject.toml`.

| Action class | Examples for this project | Rationale |
|---|---|---|
| `allowed without approval` | Edit source under `src/forecast_archive/`; add/edit tests under `tests/`; add/update fixtures under `tests/fixtures/`; run the test suite via `pytest`; run `ruff`/`mypy`/formatter; build docs under `docs/`; run the local quickstart script against the synthetic fixture | These are the inner-loop development surfaces; they touch no user data, no secrets, and no published artifacts. Failures are caught by CI before any gate. |
| `requires approval` | Add or upgrade a runtime dependency in `pyproject.toml` (esp. major versions); **change the on-disk schema (incl. the model/version keys) or bump the archive format version**; modify partitioning that alters written files; **add or modify a storage backend or its SQL dialect under the backend seam** (e.g., the Snowflake backend); modify CI workflows under `.github/workflows/`; publish to TestPyPI; change the public API signature of any `ForecastArchive` method | The schema/format change is a Type-1 (irreversible) decision per ADR-001/ADR-006 — a wrong bump or a wrong model/version key strands user archives. Backend/dialect changes can break warehouse-equivalence; dependency, CI, API-signature, and TestPyPI changes have blast radius beyond the inner loop and need a human checkpoint. |
| `prohibited` | Force-push to `main`; create or push a git tag / GitHub Release; **publish to production PyPI**; generate, rotate, or read PyPI/CI/warehouse tokens or any secret; connect to or write against a user's production warehouse; delete or rewrite a user's archive/Parquet files; modify `AGENTS.md`/`CLAUDE.md` itself; disable or weaken the reconciliation, atomicity, or backend-equivalence tests | These are one-way, externally-visible, or trust-defining actions. Releasing, credential handling, and touching a real warehouse must stay human-owned; silently weakening the correctness tests would defeat the project's core guarantee. |

This table is what `agents-md-generator.md` reads. If anything here is wrong, the agent's runtime permissions will be wrong.

## Risk register

| Risk | Phase(s) affected | Likelihood | Impact | Mitigation | Owner |
|---|---|---|---|---|---|
| Schema/format wrong at v1 (Type-1 lock-in per ADR-001/ADR-006 — incl. the model/version keys) | P1, P5 | medium | high | Freeze the raw schema only after Phase 4 exercises it end-to-end across multiple models/versions; ship a format version from day one; keep the actuals layer tri-temporality-ready and model-independent; write a migration note before v0.1 tag | Maintainer |
| False-dedupe / silent data loss from identity model (ADR-005/ADR-006) | P2 | medium | high | `on_conflict` is explicit (no silent merge); idempotency, corrected-run, and parallel-version non-collision tests are gate criteria; validate caller-supplied identity (incl. model/version) and error loudly on inconsistency | Maintainer |
| Summary cardinality blow-up from model × version × basis (ADR-006/ADR-007) | P3 | low (at v1 scale) | medium | Eager recompute scoped to laptop-scale; `official` rows only where official actuals exist; document the threshold where incremental refresh is warranted; ADR-003 Type-2 swap is non-breaking | Maintainer |
| Official actual silently overwritten / basis confusion (ADR-007) | P2, P4 | medium | high | Official-stickiness and basis-correctness tests are gate criteria; `official` basis reports insufficient (never substitutes latest); docs make latest-vs-official explicit with the Finance example | Maintainer |
| Warehouse-readiness leaks (DuckDB-only SQL welds the eval layer) | P1, P6 | medium | medium | A CI guard asserts the eval/summary layers call only the dialect layer; the Snowflake backend (P6) is the real test of the seam; keep engine specifics behind it | Maintainer |
| Snowflake backend semantics (atomicity/MERGE, type mapping, CI account) | P6 | medium | medium | Backend-equivalence + atomicity tests against a test account gate P6; isolate Snowflake-specific code to the backend module; treat as v1.1, not v1 | Maintainer |
| Summary↔raw divergence (silent-correctness failure) | P3, P4 | low | high | Reconciliation equality is a gate in P3 and P4; summary is disposable and never authoritative; raw-fallback path tested to match | Maintainer |
| Eager recompute too slow as history grows | P3 | low (at v1 scale) | medium | Scoped to laptop-scale; document the threshold where incremental refresh becomes warranted; ADR-003 is Type-2 so the swap is non-breaking | Maintainer |
| Low adoption — "roll your own felt good enough" (the moat is ergonomics, per viability memo) | P5 | medium | high | Time-to-first-value is a hard gate, not a nice-to-have; ship the Nixtla adapter so the dominant ecosystem onboards in minutes; lead docs with the one-liner horizon curve | Maintainer |
| Confusion vs. Hubverse ("is this Hubverse?") | P5 | medium | low | Docs state the relationship explicitly (generalizes the conceptual model, not the epi workflow); position for the operational-single-team user | Maintainer |
| DuckDB hard-dependency risk (API churn) | P1 | low | medium | Pin a tested DuckDB range; keep engine specifics behind the backend seam (ADR-002) so a swap is localized | Maintainer |

## Test strategy

- **Unit tests:** `pytest` under `tests/`; cover metric definitions, schema round-trip, format-version gating, query results vs. reference. Target meaningful coverage of the public API and the correctness invariants (not a blanket %).
- **Property/parametric tests:** the summary↔raw reconciliation and late-actual backfill scenarios (e.g., via `hypothesis` or parametrized fixtures) — these encode the core correctness guarantee.
- **Integration tests:** end-to-end ingest (multiple models/versions) → register actuals → query and compare on the synthetic fixture, run in CI; includes the raw-fallback path with the summary deleted. **Phase 6 adds a Snowflake integration job** (gated on a test-account secret) asserting backend-equivalence to DuckDB.
- **Performance checks:** a scripted micro-benchmark asserting sub-second summary queries and the ≤15-min time-to-first-value on the reference dataset, run in CI (thresholds gate Phase 5).
- **Security tests:** dependency and secrets scanning in CI (e.g., `pip-audit` + a secrets scanner); assert logs contain no forecast/actual values, series, or model identifiers at default verbosity, and (Phase 6) no warehouse credentials.
- **Tooling and CI:** GitHub Actions; every suite runs on PRs; a failing suite blocks the phase gate. Clean-env install + quickstart execution is its own CI job for Phase 5.

## Rollout plan

- **Versioning / releases:** semantic versioning; the on-disk **format version** is the forward-compat latch (library refuses newer-than-known archives). v0.x signals pre-stable API.
- **Canary:** publish to **TestPyPI** and install from a clean env before the production PyPI publish step.
- **Observability checkpoints:** CI dashboards for the reconciliation, atomicity, and time-to-first-value jobs; a regression is any of those jobs going red, which blocks release.
- **Cutover / ramp:** not applicable (a library, not a service); "ramp" is the TestPyPI→PyPI sequence.
- **Rollback procedure:** if a published release is found broken, **yank** the affected PyPI version and advise pinning the prior version; because archives carry a format version, a yanked release cannot silently corrupt existing archives. Success criterion for the rollback: the prior version installs cleanly from PyPI and the quickstart passes against it.
- **Communication:** changelog entry per release; a "known issues" note on yank. (Solo-maintainer context — no multi-team coordination assumed.)

## Definition of done

**v1 (Phases 1–5):**
- Phases 1–5 have passed their deterministic approval gates in CI.
- The summary↔raw reconciliation, ingestion atomicity/idempotency, and parallel-version non-collision invariants are enforced by passing CI tests.
- The schema carries `model_id` + `model_version`; multi-model/version archiving and cross-model comparison (with optional champion) work and are covered by passing tests.
- Actuals are revisable (history retained), an actual can be marked official (sticky), and accuracy is correct on both `latest` and `official` bases — all covered by passing tests.
- The library is published to PyPI and installs cleanly from a clean environment.
- The quickstart takes a new user from `pip install` to a rendered accuracy-vs-horizon curve in ≤ 15 minutes, verified by the CI timer.
- Public API docs and a README quickstart are live (including a comparison example); the quickstart code is executed in CI and passes.
- The on-disk format version is in place and a migration/compatibility note is written.
- The warehouse-readiness CI guard passes (no DuckDB-only SQL in the eval/summary layers).
- No open execution questions remain unresolved (or each is explicitly accepted as a tracked v0.x issue).
- (If proceeding to the optional final stage) `AGENTS.md`/`CLAUDE.md` is generated from this plan's execution-boundaries table.

**v1.1 (Phase 6 — Snowflake, post-release):**
- The Snowflake backend passes the backend-equivalence and atomicity/idempotency suites against a test account, returning results equal to DuckDB on a shared dataset.
- `forecast-archive[snowflake]` installs cleanly; no credentials appear in logs or the archive.

## Open execution questions

- **Public package name** (resolves the brief's naming open question and the Hubverse-collision risk): "Forecast Archive" is a placeholder; the PyPI distribution name must be chosen before the Phase 5 tag. Recommend a name signalling *operational horizon-accuracy*, not *dataset archive*.
- **License choice** (e.g., MIT vs. Apache-2.0) — needed before public release; not yet specified.
- **Minimum supported Python version and DuckDB version range** — pin before Phase 1 freezes the dependency set.
- **Reference-dataset definition** — exact series count / cadence / horizon range **plus number of models and versions** for the perf and DX gates must be fixed in Phase 1 so the ≤15-min and sub-second thresholds are reproducible.
Resolved this round (folded into Phases 1–4): champion is **one per `model_id`** (`set_champion(model_id, model_version)`); the official basis offers an opt-in **`fallback="latest"`**; actuals carry a **source/feed label** in the identity `(series, target, source, recorded_at)`; and the **same-timestamp tiebreak** is duplicate-collapse → `source_priority` → log-error-and-mark-ambiguous (configured via `source_priority`/`error_log` on the constructor).

- **Snowflake test account for CI** — Phase 6 needs a test Snowflake account/secret in CI; provisioning and cost are a prerequisite for the Phase 6 gate.
