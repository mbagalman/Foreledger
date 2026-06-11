*This artifact was produced by applying `prd.md`'s rules in Integrated mode. Review the prompt's full contract if you want to audit.*

# PRD: Production Forecast Archive

**Stage:** PRD
**Project:** Production Forecast Archive (working name)
**Date:** 2026-06-10
**Upstream artifacts:** none (Medium chain — BRD skipped by default; business context sourced from the uploaded concept brief and the go/no-go viability memo, marked user-confirmed below)

## Assumptions and inferred inputs

| Input | Source | If inferred or user-confirmed: notes |
|---|---|---|
| Business problem & outcome | user-confirmed | From `forecast-archive-concept-brief.md`: teams running recurring forecasts cannot answer the horizon-dependent accuracy question without bespoke analysis each time; the outcome is making "accuracy at lead time *L*" a first-class, fast query. |
| Target user segment | user-confirmed | Sharpened in `viability-memo-forecast-archive.md`: general analytics/DS teams running recurring operational forecasts, **outside** epi-forecasting hubs (served by Hubverse) and enterprise SCM suites (served by SAP IBP et al.). |
| Personas | user-confirmed | Derived from the brief's "Users" section; named explicitly below. No personas were invented beyond what the brief states. |
| Success criteria (business) | inferred | BRD skipped — no measured baseline exists. Adoption/DX targets below are inferred from the viability memo's finding that the moat is ergonomics, not math. Not validated against real users yet. |
| Non-functional / performance targets | inferred | No measured baseline; thresholds below are starting targets for the Tech Spec to pin against real data volumes (see Open question on scale). |
| Constraints (Python-first, columnar storage) | user-confirmed | Stated in the brief's "Constraints & technical considerations." Treated as product-shaping constraints, not implementation decisions — the *choice* of engine/format is deferred to Tech Spec/ADR. |
| Form factor (library vs service vs spec) | inferred (deferred) | Brief lists this as an open question; filed in Open product questions as a Tech Spec input, not resolved here. |
| Multiple models & versions (incl. parallel runs) | user-confirmed | User: most teams run several forecasting models, each evolving through versions over time, sometimes in parallel. The archive must key on model and version and support cross-model/version comparison. Drives the schema (ADR-001/ADR-006). |
| Warehouse / EDW storage (e.g. Snowflake) | user-confirmed | User: corporate users will want the archive in their EDW; important for adoption. v1 ships local DuckDB with a warehouse-ready seam; Snowflake is the committed first fast-follow (ADR-002). |
| Actuals change over time + optional "official" value | user-confirmed | User: actuals get revised (delayed feeds); default last-write-wins, plus an optional sticky "official" actual (e.g. an estimate reported to Finance that stays the booked number even after real data lands). Drives the actuals model (ADR-007); accuracy is selectable per basis (latest/official). |
| Champion designation | user-confirmed | User: include an optional "champion" tag so champion-vs-challenger comparison reads naturally. Lightweight metadata on the comparison API (ADR-006 note), not a registry. |

## Product overview

Teams that re-run a forecasting model on a schedule (daily or weekly) produce many forecasts for the same target date — one per run — so "how accurate is the forecast?" has no single honest answer. Accuracy is a curve over **horizon** (how far ahead of the target date the forecast was made), not a scalar. Today, general analytics and data-science teams retain this in an append-only log and re-derive the answer with bespoke SQL every time, if they retain it at all. The business outcome this product serves: let a team answer the horizon-dependent accuracy question as a first-class, fast, repeatable operation — and reconstruct what the forecast looked like "as of" any past run — without rolling their own infrastructure, adopting epidemiology-hub conventions, or buying an enterprise planning suite.

The product is a lightweight forecast archive that sits between training-time evaluation tools (which generate this structure but discard it) and ML monitoring platforms (which store predictions but key them on the wrong axis). It ingests each run's forecasts, stores them efficiently, and exposes horizon-keyed evaluation and bitemporal slicing as first-class operations. The contribution is integration and ergonomics, not new math.

Real teams run **more than one model**, and each model evolves through **versions** over time — often with two versions running in parallel during a migration (champion vs. challenger). The archive therefore keys every forecast on its model and version, keeps actuals model-independent (the truth doesn't depend on who predicted it), and makes **comparing models or versions at a given horizon** — optionally against a designated **champion** — a first-class query. Storage is local by default (zero setup), but the archive is built to live in a team's **enterprise data warehouse** — Snowflake first — so it can be a production asset, not just a laptop tool.

Actuals themselves are **not settled once**: delayed and revised data feeds change the truth for a target over time. The archive records actuals as a revisable history (default: the latest value wins) and lets a team mark an **official** actual that stays put — for example, an estimate reported to Finance that remains the booked number even after the real figure lands later. Accuracy can then be reported against either basis: *what we booked* (official) or *what actually happened* (latest).

Scope boundary in plain language: this product **archives and evaluates** forecasts produced elsewhere. It does not produce forecasts, and it is not a general temporal-database engine. The target user is a practitioner on a team running recurring operational forecasts outside the two domains where good tooling already exists (epi-forecasting hubs; enterprise demand planning).

## Target users and personas

### Operational forecasting practitioner (primary)
- **Role / title:** Data scientist, ML engineer, or analytics engineer who owns one or more recurring forecasting pipelines.
- **What they do with the product:** Wire the archive into an existing pipeline so each run's forecasts — across several models and versions — are captured; query accuracy at a given horizon and the accuracy-vs-horizon curve to report and defend model performance; compare a challenger version against the incumbent at the horizons that matter; reconstruct the forecast "as of" a past run when investigating an incident or a stakeholder challenge; drill from a summary number down to the raw rows behind it.
- **Context of use:** Recurring (daily/weekly) cadence; runs multiple models and rolls versions over time; comfortable in Python and SQL; works locally and/or against a team data warehouse (often Snowflake); high expertise; wants minimal ceremony and to keep using their existing forecast log as the source of truth.

### Forecast consumer / decision-maker (secondary)
- **Role / title:** Business stakeholder, planner, or executive who acts on forecast numbers.
- **What they do with the product:** Consumes accuracy figures (usually surfaced by the practitioner or a downstream report) and needs horizon-appropriate numbers — "accuracy for the lead time my decision actually runs at" — rather than a single misleading scalar.
- **Context of use:** Infrequent, read-only, non-technical; never touches the API directly; interacts through reports, dashboards, or numbers the practitioner exposes.

### Analytics lead / manager (secondary)
- **Role / title:** Team lead accountable for the team's forecasting quality.
- **What they do with the product:** Needs a defensible, consistent answer to "how accurate are we?" across models, versions, and periods, computed the same way every time; needs to see whether a new model version actually beats the one it replaced before signing off on the cutover, without each analyst re-inventing the calculation.
- **Context of use:** Periodic review cadence; semi-technical; cares about consistency, auditability, and that the summary numbers reconcile to the raw archive.

## User stories

- As an **operational forecasting practitioner**, I want each run's forecasts captured automatically so that I don't lose the horizon information I'll need months later.
- As an **operational forecasting practitioner**, I want `accuracy_at_horizon(h)` and an accuracy-vs-horizon curve as one-line calls so that I can report and defend model performance without writing the join every time.
- As an **operational forecasting practitioner**, I want to reconstruct the forecast "as of" a past run so that I can investigate what we predicted at the time a decision was made.
- As an **operational forecasting practitioner**, I want to point the archive at my existing append-only log without reformatting it so that adoption costs me minutes, not a migration.
- As an **operational forecasting practitioner**, I want to archive forecasts from several models and from successive (and parallel) versions without them colliding, so that each forecast stays attributable to the model and version that made it.
- As an **operational forecasting practitioner**, I want to compare two models — or a challenger version against the incumbent — by accuracy at each horizon, so that a cutover decision is evidence-based.
- As an **analytics lead**, I want accuracy computed the same way every time and reconcilable to the raw rows so that the team's numbers are consistent and auditable.
- As an **analytics lead** at a corporate shop, I want the archive to live in our enterprise data warehouse (e.g. Snowflake), so that it's governed and queryable alongside the rest of our production data.
- As a **forecast consumer**, I want accuracy reported at the horizon my decision runs at so that I'm not misled by a blended single number.

## Functional requirements

Organized by user-facing capability. Each requirement names a user-visible behavior and has an acceptance criterion.

### Ingestion — capturing forecast runs

- **FR-1.1** A practitioner can append a run's forecasts (keyed on model, version, forecast origin, and target date, with the predicted value) to the archive.
  - **Acceptance criterion:** After appending a run, every `(model_id, model_version, origin, target, value)` row from that run is retrievable from the archive, and prior runs — including forecasts from other models/versions — are unchanged (append-only).
  - **Dependencies:** A forecast dataset with model, version, origin, target, and value identifiable (column mapping, named not specified).
- **FR-1.2** A practitioner can re-run ingestion for a run already present without creating duplicate or conflicting rows, and can ingest forecasts from a different model or version for the same origin/target without collision.
  - **Acceptance criterion:** Ingesting the same `(model_id, model_version, origin, series)` run twice yields the same archive state as ingesting it once (idempotent), or surfaces an explicit conflict — never silent duplication; ingesting a *different* model or version for the same origin/target adds rows rather than overwriting.
  - **Dependencies:** A stable run identity (model, version, origin, series keys).
- **FR-1.3** A practitioner can register actuals (model-independent) so accuracy can be computed, and can re-register revised actuals for a target over time — optionally tagged with a **source/feed label** — without losing the prior values.
  - **Acceptance criterion:** Given actuals for a set of targets, the archive can join each forecast to the actual for its target; re-registering a revised actual retains the prior value(s) and, by default, the latest registered value is the one used; two revisions for the same target/timestamp from different sources are retained as distinct rows, not a collision.
  - **Dependencies:** An actuals dataset keyed on target date (optional source label).
- **FR-1.4** A practitioner can mark a registered actual as **official** for a target; the official value is sticky — later non-official registrations do not change it.
  - **Acceptance criterion:** After marking an actual official for a target, registering a newer (non-official) actual for that target leaves the official value unchanged; at most one official actual exists per `(series, target)`; re-marking official is explicit and logged.
  - **Dependencies:** FR-1.3.
- **FR-1.5** When two actuals tie at the same most-recent timestamp for a target, the archive resolves which value is "latest" predictably, and never silently guesses.
  - **Acceptance criterion:** Tied entries with equal values are treated as duplicates (collapsed); tied entries with differing values are resolved by a configured source priority; if differing values cannot be resolved (no priority configured), the conflict is written to an error log and the target is reported as ambiguous rather than silently picking one.
  - **Dependencies:** Source-labeled actuals (FR-1.3); an optional source-priority configuration.

### Storage — durable, efficient source of truth

- **FR-2.1** The archive retains the full append-only history as the source of truth, in a columnar format, without the practitioner managing storage layout by hand.
  - **Acceptance criterion:** The complete forecast history is recoverable from storage, and storage size benefits materially from run-to-run redundancy (compressed footprint is a fraction of an equivalent uncompressed row store on representative data).
  - **Dependencies:** A storage location (local path or warehouse, named not specified).
- **FR-2.2** A practitioner can use the archive locally (default, zero setup) and, on the same query API, against a team enterprise data warehouse, without changing how they query.
  - **Acceptance criterion:** The same query API returns equivalent results whether the backing store is local (DuckDB/Parquet, v1) or a warehouse. v1 ships the local backend with a warehouse-ready seam; the Snowflake backend is the committed first fast-follow (ADR-002) and must return equivalent results to the local backend on a shared test dataset.
  - **Dependencies:** Backend selection (resolved in ADR-002).

### Horizon-keyed evaluation — the headline capability

- **FR-3.1** A practitioner can compute accuracy at a specified horizon `h` in a single call.
  - **Acceptance criterion:** `accuracy_at_horizon(h)` returns the chosen metric over all `(origin, target)` pairs where `target − origin = h`, matching a hand-written reference query on the same data.
  - **Dependencies:** Actuals present (FR-1.3); a selected metric (FR-5.1).
- **FR-3.2** A practitioner can retrieve the accuracy-vs-horizon curve across the available horizon range in a single call.
  - **Acceptance criterion:** The call returns one accuracy value per horizon over the requested range; values equal the per-horizon results of FR-3.1.
  - **Dependencies:** As FR-3.1.
- **FR-3.3** A practitioner can scope any accuracy query by model, version, series, and time period (target-date window).
  - **Acceptance criterion:** Filtering by model/version/series and/or period returns accuracy computed only over the matching rows; an unfiltered call covers all rows.
  - **Dependencies:** Model, version, and series identifiers present in the data.
- **FR-3.4** A practitioner can choose the **actual basis** for any accuracy query — `latest` (default) or `official` — and can opt into a `latest` fallback under the official basis.
  - **Acceptance criterion:** With `basis="latest"`, accuracy uses the most recently registered actual per target; with `basis="official"`, it uses the official actual and reports targets lacking an official value as insufficient (never silently substituting latest); with `basis="official", fallback="latest"`, those targets are filled from the latest value and flagged as fallback-filled in the result.
  - **Dependencies:** Actuals with revision/official support (FR-1.3/FR-1.4).

### Multiple models & versions

- **FR-6.1** A practitioner can archive forecasts from multiple models and from multiple versions of a model — including versions running in parallel — without them colliding.
  - **Acceptance criterion:** Forecasts that share `(series, origin, target)` but differ in `model_id` or `model_version` are stored and retrievable as distinct rows; none overwrites another.
  - **Dependencies:** Model and version supplied at ingestion (FR-1.1).
- **FR-6.2** A practitioner can list the models and versions present in the archive and the coverage (origin/target range) of each.
  - **Acceptance criterion:** A call returns each `(model_id, model_version)` present with its origin/target span; matches what was ingested.
  - **Dependencies:** FR-6.1.

### Model comparison

- **FR-7.1** A practitioner can compare two or more models — or two versions of the same model — by accuracy at a given horizon.
  - **Acceptance criterion:** A comparison call returns the chosen metric per model/version at horizon `h` over a common scope (shared series/period), and each value equals the corresponding single-model `accuracy_at_horizon` result.
  - **Dependencies:** Actuals present (FR-1.3); the model/version axis (FR-6.1).
- **FR-7.2** A practitioner can compare models/versions across the whole horizon range (overlaid accuracy-vs-horizon curves).
  - **Acceptance criterion:** The call returns one curve per model/version over the requested horizons on a common scope; each curve equals the corresponding single-model `accuracy_curve`.
  - **Dependencies:** As FR-7.1.
- **FR-7.3** A practitioner can optionally designate a **champion version per `model_id`** (each logical model's production incumbent); comparisons mark the champion and express each challenger version's accuracy relative to its model's champion.
  - **Acceptance criterion:** When a champion is set for a `model_id` (persisted) or passed to a comparison, the result identifies that model's champion and reports each other version's metric and its delta vs. the champion at each horizon; at most one champion per `model_id`; with no champion, comparison is order-agnostic.
  - **Dependencies:** FR-7.1; the model/version axis (FR-6.1).

### Bitemporal slicing — reconstructing the past

- **FR-4.1** A practitioner can reconstruct the set of forecasts "as of" a given origin, optionally scoped to a model/version.
  - **Acceptance criterion:** `as_of(origin)` returns exactly the forecasts that had been recorded as of that origin — no forecasts from later runs leak in; when scoped to a model/version it returns only that model/version's forecasts.
  - **Dependencies:** Origin and model/version recorded per forecast (FR-1.1).

### Derived summary & drill-down — fast, consistent answers

- **FR-5.0** Common accuracy questions are served from a derived, materialized summary (one row per model × version × series × horizon × metric × period × actual-basis) so they return quickly and identically each time.
  - **Acceptance criterion:** Summary-backed answers match the equivalent computation over the raw archive for the selected basis, and refresh after new ingestion or actuals revision (including newly added models/versions and newly marked official actuals) to reflect the change.
  - **Dependencies:** Raw archive (FR-2.1); actuals (FR-1.3/FR-1.4).
- **FR-5.1** A practitioner can choose the accuracy metric from a provided set (MAE, RMSE, MAPE, MASE at minimum).
  - **Acceptance criterion:** Each provided metric returns values matching its standard definition on a known test case.
  - **Dependencies:** Actuals present.
- **FR-5.2** A practitioner can drill from any summary number down to the raw archive rows that produced it.
  - **Acceptance criterion:** From a summary cell, the user can retrieve the exact `(origin, target, value, actual)` rows aggregated into it, and they reconcile to the summary value.
  - **Dependencies:** Summary (FR-5.0) and raw archive (FR-2.1) share keys.

## Non-functional requirements

### Performance
- **Time-to-first-value (headline DX metric):** a new user goes from install to a first accuracy-vs-horizon curve on their own data in under ~15 minutes, following the quickstart, with no schema migration. (Inferred target — the viability memo identifies ergonomics as the only moat; this metric is a gate, not a nicety.)
- **Summary query latency:** common summary-backed queries (FR-3.1/3.2 via FR-5.0) return interactively (target: sub-second on a local store at the v1 reference scale; reference scale to be pinned in Tech Spec — see Open questions).
- **Ingestion throughput:** a single run's append completes fast enough to sit inside an existing pipeline step without becoming a bottleneck (target to be quantified against the reference scale).

### Reliability
- Append-only durability: once a run is ingested and the operation reports success, its rows survive process restarts and are never silently mutated by later runs.
- Idempotent ingestion (FR-1.2): re-ingesting a run does not corrupt the archive.
- Degradation behavior: if the derived summary is stale or absent, accuracy queries still return correct answers computed from the raw archive (summary is an optimization, not a correctness dependency).

### Security
- None required as a product-level capability for v1. The archive is a library operating on data the user already holds; it introduces no new authentication surface and inherits the access controls of whatever store (local filesystem or warehouse) the user points it at. (Revisit if a hosted-service form factor is chosen — see Open questions.)

### Accessibility
- None required in the WCAG sense — v1 has no end-user GUI. The accessibility-equivalent obligation for a developer tool is API and documentation ergonomics, which is captured under Performance (time-to-first-value) and the DX emphasis above.

### Compliance
- None required for v1. No regulated-data assumptions are baked in. Data retention is whatever the user's store provides; the archive does not impose or claim a retention/audit regime. (Inherit constraints later if a target user brings a regulatory regime.)

## Out-of-scope

- **Producing forecasts.** The archive evaluates forecasts made elsewhere; it ships no forecasting model.
- **A full SCD Type-2 / valid-from–valid-to temporal engine.** The run/origin date is the transaction-time key for an append-only log; heavy temporal-table machinery is out of scope for v1.
- **Full tri-temporality (accuracy as of any actual vintage).** v1 *does* support revisable actuals and an optional sticky "official" value, and retains the revision history (ADR-007) — so accuracy can be reported on a `latest` or `official` basis. But the full "what did we believe the actual was as of date D?" query surface (accuracy as of an arbitrary actual knowledge date) is deferred; the retained history means it's a later, migration-free addition.
- **Epi-forecasting-hub feature parity.** No quantile/WIS-centric submission-and-validation workflow, no multi-team challenge hosting, no epiweek conventions. That space is served by Hubverse; competing there is explicitly not the goal.
- **Enterprise demand-planning / SCM parity.** No S&OP workflow, no forecast-value-added governance suite. That space is served by enterprise planning tools.
- **A model registry / model lifecycle manager.** The archive keys forecasts on `model_id` and `model_version` (opaque user-supplied strings) and compares their accuracy, but it does not register, store, version, deploy, or govern models themselves. `model_version` semantics (semver, dates, hashes) are the user's; the archive does not interpret or order them. Model registries (MLflow, etc.) own that space.
- **All warehouse backends in v1.** v1 ships the local DuckDB backend only, behind a warehouse-ready seam. Snowflake is the committed first fast-follow (v1.1); BigQuery and others follow the same pattern but are not v1.
- **General BI dashboarding.** v1 exposes a query API and summary table; building a full interactive dashboard product is out of scope (a thin reference visualization is acceptable but not the product).
- **Anomaly detection / drift monitoring as such.** Adjacent to ML-monitoring tools; not a v1 goal beyond what falls out of horizon-keyed accuracy.

## Open product questions

Resolved since the first draft (kept for traceability): form factor → standalone library; schema → generalize Hubverse (ADR-001); ingestion → push with caller identity (ADR-005); metrics → built-ins + protocol (ADR-004); scale → laptop-scale primary; warehouse → DuckDB v1 + warehouse-ready seam, Snowflake fast-follow (ADR-002); multiple models/versions → keyed in the grain with cross-model comparison (ADR-006).

Resolved in this round: **champion designation** → optional persisted champion, **one per `model_id`**, with challenger deltas vs. that model's champion (FR-7.3, ADR-006 note); **conflicting / changing actuals** → revisable append-only actuals with a **source/feed label**, default last-write-wins (`latest`), plus an optional sticky `official` value and selectable accuracy basis with an opt-in **`fallback="latest"`** (FR-1.3/1.4/3.4, ADR-007).

Still open:

- **Warehouse-backend coverage beyond Snowflake.** Snowflake is the committed fast-follow; the order and timing of BigQuery / Databricks / Postgres backends is a roadmap question, not a v1 question.
- **Naming.** "Forecast Archive" collides with dataset-archive usage (Zoltar, Monash) and undersells the operational accuracy-over-horizon angle. Product/marketing decision, deferred.

## Acceptance criteria summary

- **Ingestion:** a run's forecasts and actuals can be appended (idempotently) and fully retrieved; the archive is append-only and never silently mutated; forecasts from different models/versions never collide.
- **Actuals over time:** actuals are revisable (default latest-wins) and the prior values are retained; an actual can be marked **official** and stays sticky; accuracy is reportable on a `latest` or `official` basis.
- **Multiple models & versions:** forecasts from several models and successive/parallel versions coexist without collision; the archive can list models/versions and their coverage.
- **Model comparison:** two or more models/versions can be compared by accuracy at a horizon and across the horizon range, optionally against a designated **champion** (with challenger deltas), each comparison value equal to the corresponding single-model result.
- **Storage:** full history is durable in a columnar store, usable locally (DuckDB v1) and — via the warehouse-ready seam — in an enterprise warehouse (Snowflake fast-follow) through one query API, with a materially compressed footprint.
- **Horizon-keyed evaluation:** `accuracy_at_horizon(h)` and the accuracy-vs-horizon curve are one-line calls whose results match a hand-written reference query, scoped by model, version, series, and period.
- **Bitemporal slicing:** `as_of(origin)` reconstructs exactly the forecasts known at that origin (optionally per model/version), with no leakage from later runs.
- **Derived summary & drill-down:** summary-backed answers are fast, reconcile exactly to the raw archive, and refresh after ingestion; the user can pick a standard metric and drill from any summary number to its source rows.
- **Developer experience:** install-to-first-curve on the user's own data in ~15 minutes with no schema migration.
