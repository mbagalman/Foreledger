*This artifact was produced by applying `adr.md`'s rules in Integrated mode. Review the prompt's full contract if you want to audit.*

# ADR-003: Derived accuracy summary is eagerly recomputed and always rebuildable from raw

**Stage:** ADR-003
**Project:** Production Forecast Archive (working name)
**Date:** 2026-06-10
**Upstream artifacts:** tech-spec-forecast-archive-draft.md
**Originating question-id:** Q-003

## Assumptions and inferred inputs

| Input | Source | If inferred or user-confirmed: notes |
|---|---|---|
| Architectural context | supplied (Tech Spec Draft §Open architectural questions, Q-003) | — |
| Chosen option & rationale | inferred (tech-lead recommendation) | Decision made by Claude in the tech-lead role; justified by the laptop-scale constraint. Flagged for ratification. |
| Reversibility classification | inferred | Type 2 — the summary is derived and disposable; the strategy can change without migrating raw data. |

## Status

accepted

## Context

The derived summary (one row per series × horizon × metric × period) is what makes the headline accuracy questions fast and identical every time, but it must stay reconcilable to the raw archive and refresh as new runs and *late actuals* arrive. Late actuals are the hard part: registering an actual for a past target retroactively changes the error for every forecast that targeted it, touching many summary cells across horizons.

The forces: **simplicity and correctness** (the summary must never silently diverge from raw — the spec treats divergence as a defect, not a tolerance) versus **steady-state performance and footprint** (a smarter incremental strategy is faster and smaller but carries complex invalidation logic). The user's laptop-scale decision (~hundreds of series, daily/weekly runs, 1–3 years retention) is decisive here: at that volume a full recompute is cheap, so the complexity of incremental invalidation buys little while adding real correctness risk.

Depends on ADR-001 (summary grain) and ADR-002 (recompute runs through the backend seam).

## Decision

We will **eagerly recompute the affected summary on each ingest and on each actuals registration**, and treat the summary as a **disposable cache that is always fully rebuildable from raw**. At laptop scale "affected" may be the whole summary; the implementation is free to recompute everything when that is simplest. Correctness is defined as exact equality to a raw recomputation, checked in tests and available at runtime.

## Consequences

**Reversibility:** Type 2 (reversible — because the summary is derived and rebuildable, we can switch to an incremental strategy later with no impact on stored raw data or the public API).

### Positive
- Trivially correct reconciliation: a freshly recomputed summary equals raw by construction, satisfying the spec's no-silent-divergence invariant.
- Late-actual handling is automatic — a recompute picks up retroactive error changes with no bespoke invalidation logic to get wrong.
- The summary being disposable simplifies migrations (ADR-001): on a format change, rebuild rather than migrate the summary.

### Negative
- Recompute cost grows with history; at much larger scales than v1 targets, eager full recompute would become a bottleneck — we are explicitly deferring that problem.
- Ingestion latency includes the summary refresh, making appends heavier than a raw-only write (acceptable at laptop scale; revisit if it threatens the ingestion-throughput target).

### Neutral
- Establishes "summary is an optimization, never authoritative" as a load-bearing invariant the eval layer relies on for graceful degradation.
- Leaves incremental refresh as a clearly-scoped future optimization with a known trigger (when recompute time exceeds the ingestion budget at a user's scale).

## Options considered

### Option A — Eager recompute, disposable summary (chosen)
- **Description:** Recompute affected summary on ingest/actuals; always rebuildable from raw.
- **Why accepted:** Correctness and simplicity dominate at laptop scale; eliminates the riskiest logic (incremental invalidation under late actuals) for a cost the chosen scale absorbs.

### Option B — Incremental cell-level update
- **Description:** Update only the (series × horizon × period) cells affected by each change.
- **Why rejected:** Faster steady-state and smaller writes, but late-actual fan-out makes invalidation complex and error-prone; the correctness risk is unjustified at v1 scale.

### Option C — On-demand compute with a query cache, no standing table
- **Description:** No materialized summary; compute on demand and cache results.
- **Why rejected:** Simplest storage but weakest latency guarantee (cold queries pay full cost) and no consistent, inspectable summary artifact for the analytics-lead persona who wants a stable, auditable table.

> **Note (2026-06-10):** the summary grain is **widened by ADR-006** to include `(model_id, model_version, …)` and by **ADR-007** to include an `actual_basis` dimension (`latest`, and `official` where set). This raises cardinality by `models × versions × bases`, which tightens the threshold at which eager recompute should give way to incremental; at v1 laptop scale the eager strategy still holds.

## Related ADRs
- **Depends on:** ADR-001 (summary grain/keys), ADR-002 (recompute executes through the backend seam), ADR-006 (model/version widen the summary grain), ADR-007 (actual_basis widens the summary grain).
