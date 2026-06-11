*This artifact was produced by applying `adr.md`'s rules in Integrated mode. Review the prompt's full contract if you want to audit.*

# ADR-004: Built-in metric set plus a constrained, registerable metric protocol

**Stage:** ADR-004
**Project:** Production Forecast Archive (working name)
**Date:** 2026-06-10
**Upstream artifacts:** tech-spec-forecast-archive-draft.md
**Originating question-id:** Q-004

## Assumptions and inferred inputs

| Input | Source | If inferred or user-confirmed: notes |
|---|---|---|
| Architectural context | supplied (Tech Spec Draft §Open architectural questions, Q-004) | — |
| Chosen option & rationale | inferred (tech-lead recommendation) | Decision made by Claude in the tech-lead role; flagged for ratification. |
| Reversibility classification | inferred | Type 2 — the protocol is additive API surface; it can be widened or narrowed later without migrating data. |

## Status

accepted

## Context

The archive must ship MAE, RMSE, MAPE, and MASE; the brief asks whether users may register their own metrics. The complication is the derived summary (ADR-003): for a metric to be *materializable* per (series × horizon × period), it must be expressible as an aggregation over the per-pair errors within a cell. Not all conceivable custom metrics compose that way, and running arbitrary user code in the ingestion/refresh hot path is a support and correctness liability.

The forces: **ergonomics/extensibility** (the moat is developer experience; teams have house metrics and will resent a closed set) versus **summary soundness and support burden** (pluggable metrics must not break precomputation or smuggle slow/incorrect user code into the hot path).

Depends on ADR-001 (the (forecast, actual) alignment the metric consumes) and ADR-003 (what the summary can precompute).

## Decision

We will ship the **built-in set (MAE, RMSE, MAPE, MASE) and expose a documented metric protocol** users can register. Registered metrics that satisfy the protocol's *summarizable* contract — expressible as an aggregation over per-pair errors at the (series × horizon × period) grain — are precomputed into the summary like built-ins. Metrics that do not fit are still allowed but compute **over raw only** (not materialized), and the API makes that distinction explicit.

## Consequences

**Reversibility:** Type 2 (reversible — the protocol is additive; we can tighten or relax the summarizable contract, or add built-ins, without touching stored data).

### Positive
- Teams can express house metrics without forking the library — directly feeds the ergonomics moat.
- The summarizable contract keeps the summary sound: pluggable metrics that precompute do so on the same footing as built-ins, with the same reconciliation guarantee.
- The raw-only fallback means no legitimate metric is outright refused; the cost (no materialization) is transparent.

### Negative
- Two metric tiers (summarizable vs. raw-only) is conceptual surface users must understand; poor documentation here would undercut the ergonomics goal it serves.
- Accepting user code in the refresh path requires guarding against slow/throwing metrics (timeouts/error isolation), a real implementation cost.

### Neutral
- Defines the metric protocol as the library's primary extension point, shaping how other extensibility (if any) is later designed.
- Built-ins are implemented *as* protocol-conforming metrics, so there is one code path, not two.

## Options considered

### Option A — Fixed built-in set only
- **Description:** Ship MAE/RMSE/MAPE/MASE; no user metrics.
- **Why rejected:** Smallest, most testable surface, but the target users have house metrics; a closed set pushes them back to rolling their own — the exact outcome the product exists to prevent.

### Option B — Built-ins plus constrained registerable protocol (chosen)
- **Description:** Register metrics; summarizable ones precompute, others compute over raw.
- **Why accepted:** Captures extensibility without compromising summary soundness, at the cost of a two-tier concept that documentation must carry.

### Option C — Built-ins plus arbitrary post-hoc metrics over raw only
- **Description:** Allow any custom metric, but never materialize custom ones.
- **Why rejected:** Simpler than B but leaves common custom metrics permanently slow (never precomputed) even when they would compose cleanly — a worse experience for a foreseeable, supportable case.

## Related ADRs
- **Depends on:** ADR-001 (forecast↔actual alignment), ADR-003 (summary materialization contract the protocol must respect).
