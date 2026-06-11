*This artifact was produced by applying `adr.md`'s rules in Integrated mode. Review the prompt's full contract if you want to audit.*

# ADR-002: Warehouse-ready backend seam — DuckDB in v1, Snowflake as first fast-follow

**Stage:** ADR-002
**Project:** Production Forecast Archive (working name)
**Date:** 2026-06-10
**Upstream artifacts:** tech-spec-forecast-archive-draft.md

> **Amended 2026-06-10** (same day, pre-release): originally "DuckDB-over-Parquet behind a thin seam, no second backend in v1." Revised after the user flagged that corporate users will want to store the archive in their EDW (e.g., Snowflake) and that this matters for adoption. The seam is now required to be **dialect-aware / warehouse-ready**, and a warehouse backend is an explicit committed fast-follow rather than an open-ended "later."

**Originating question-id:** Q-002

## Assumptions and inferred inputs

| Input | Source | If inferred or user-confirmed: notes |
|---|---|---|
| Architectural context | supplied (Tech Spec Draft §Open architectural questions, Q-002) | — |
| Warehouse/EDW matters for adoption | user-confirmed | User: "any corporate users are going to want to store this in their EDW… will users be able to store this, for example, in their Snowflake database? That's another feature that will be important for adoption." |
| Chosen scope (DuckDB v1, warehouse-ready seam, Snowflake fast-follow) | user-confirmed | User selected this over dual-backend-in-v1 and warehouse-only. |
| Reversibility classification | inferred | Type 2 — the seam means adding the warehouse backend is an internal addition with no user-data migration. |

## Status

accepted

## Context

The reference scale is laptop-scale with a local-first backend, which points at DuckDB over Parquet: embedded, zero-server, strong columnar scans, native Parquet/Arrow. But the PRD always kept a warehouse path open, and the user has now made it explicit — corporate adoption depends on being able to keep the archive **in the enterprise data warehouse (Snowflake first)** rather than as local files. So the live question is not *which engine for v1* (DuckDB, clearly) but *how warehouse-ready the seam must be* and *when the warehouse backend lands*.

The forces in tension: **time-to-first-value and v1 shippability** (every backend and every test matrix is cost) versus **corporate adoption** (without an EDW path, a large class of users cannot put this into production) versus **scope discipline** (building two full backends in v1 would slow the first release and contradict the laptop-scale starting point). The seam was the lever that lets us avoid choosing badly: if query construction is engine-neutral *and dialect-aware* from day one, the warehouse backend is an addition, not a rewrite.

This decision depends on ADR-001 (the schema the engine stores) and ADR-006 (the model/version keys widen the grain the engine scans), and it shapes how the Evaluation & query API and the summary builder are written.

## Decision

We will ship a **single DuckDB-over-Parquet backend in v1, behind a backend seam that is explicitly dialect-aware / warehouse-ready** — storage and query operations are expressed as engine-neutral operations over the canonical schema, with SQL generation parameterized by dialect rather than hard-coded to DuckDB. A **Snowflake backend is the committed first fast-follow (target v1.1)**, implemented against the same seam with no change to the public API; BigQuery and others follow the same pattern. The local DuckDB quickstart remains the default so the zero-setup time-to-first-value bet is preserved.

## Consequences

**Reversibility:** Type 2 (reversible/additive — adding the Snowflake backend is an internal addition; user archives and the public API are unaffected because the seam isolates engine and dialect specifics).

### Positive
- Preserves the zero-setup laptop quickstart (DuckDB) that the ergonomics/time-to-first-value bet depends on, while giving corporate users a credible, committed EDW path.
- A dialect-aware seam makes Snowflake (then BigQuery, etc.) additive: the eval API, summary builder, and tests above the seam do not change when a backend is added.
- Keeps v1 shippable — one backend to harden and test now — without painting us into a local-only corner.

### Negative
- "Dialect-aware from day one" is more upfront design than a DuckDB-only seam: query construction must avoid DuckDB-only SQL idioms and route through a dialect layer, even though only one dialect ships in v1 — a real, if bounded, tax.
- A seam designed against a single dialect can still leak DuckDB assumptions; the Snowflake backend may surface interface adjustments (the dialect layer reduces, not eliminates, this).
- Warehouse backends introduce concerns DuckDB does not (connection/auth pass-through, transit security, cost of large scans) that the spec must now anticipate even before v1.1.

### Neutral
- Establishes an engine-neutral, dialect-parameterized query-construction style that all query code must follow — a mild standing discipline on contributors.
- DuckDB is a hard v1 dependency; the Snowflake client becomes an optional extra (`pip install forecast-archive[snowflake]`) at v1.1.

## Options considered

### Option A — DuckDB v1, warehouse-ready seam, Snowflake fast-follow (chosen)
- **Description:** One DuckDB backend in v1; seam is dialect-aware; Snowflake committed for v1.1 against the same seam.
- **Why accepted:** Best balance — ships a fast, zero-setup v1 while making the adoption-critical EDW path a near, low-risk addition rather than a rewrite.

### Option B — Dual backend (DuckDB + Snowflake) in v1
- **Description:** Both backends are v1 hard requirements.
- **Why rejected:** Strongest launch story but materially heavier v1 (two backends, two test/CI matrices, warehouse credentials in CI), slowing the first release and contradicting the laptop-scale starting scope. The seam lets us get most of the benefit later at much lower v1 cost.

### Option C — Warehouse-only, drop local DuckDB
- **Description:** Target the EDW exclusively.
- **Why rejected:** Abandons the zero-setup local quickstart that the entire ergonomics/time-to-first-value differentiation rests on; raises the barrier to first use precisely for the underserved-middle user the product targets.

### Option D — DuckDB-direct, no seam (the original pre-amendment leaning's weaker cousin)
- **Description:** Weld queries to DuckDB with no abstraction.
- **Why rejected:** Would make the now-committed warehouse path a rewrite of the hot path — exactly the foreseeable cost the seam exists to avoid.

## Related ADRs
- **Depends on:** ADR-001 (schema persisted/scanned), ADR-006 (model/version keys widen the scanned grain).
- **Influences:** ADR-003 (summary materialization runs through this seam).
