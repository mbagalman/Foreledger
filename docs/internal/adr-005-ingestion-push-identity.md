*This artifact was produced by applying `adr.md`'s rules in Integrated mode. Review the prompt's full contract if you want to audit.*

# ADR-005: Push ingestion with caller-supplied run identity; Nixtla pull adapter as sugar

**Stage:** ADR-005
**Project:** Production Forecast Archive (working name)
**Date:** 2026-06-10
**Upstream artifacts:** tech-spec-forecast-archive-draft.md
**Originating question-id:** Q-005

## Assumptions and inferred inputs

| Input | Source | If inferred or user-confirmed: notes |
|---|---|---|
| Architectural context | supplied (Tech Spec Draft §Open architectural questions, Q-005) | — |
| Chosen option & rationale | inferred (tech-lead recommendation) | Decision made by Claude in the tech-lead role; flagged for ratification. |
| Reversibility classification | inferred | Type 1 for the identity-key semantics — once runs are stored under a given identity rule, changing it affects dedupe of existing archives. |

## Status

accepted

## Context

FR-1.1/1.2 require appending a run and idempotent re-ingestion; the atomic, idempotent append is the single most important correctness property in the system, because a torn or double-counted run silently corrupts every horizon computation downstream. The open question pairs the *interface model* (push vs. pull) with *how a run is identified* so re-ingestion is safe.

The forces: **caller ergonomics / time-to-first-value** (less ceremony is better) versus **idempotency correctness** (the dedupe key must not collapse two legitimately different runs, nor fail to dedupe a genuine replay). A third force is **ecosystem fit** — the dominant operational source is forecasting-library output (Nixtla cross-validation frames keyed on `cutoff`), so a pull convenience that reads those directly lowers adoption friction.

Depends on ADR-001 (the schema over which identity is defined).

## Decision

We will make **push the primary ingestion contract, with the caller supplying a stable run identity** — the forecast `origin` plus series keys — as the idempotency key, and each run appended **atomically (all-or-nothing)**. We will additionally ship a **thin pull adapter for Nixtla-style cross-validation output** that maps `cutoff`→`origin` and writes through the same push path — convenience sugar over one contract, not a second contract.

## Consequences

**Reversibility:** Type 1 for the identity semantics (irreversible — changing what constitutes a run's identity after archives exist alters how existing data deduplicates, a breaking, migration-class change); the push *interface* itself is Type 2.

### Positive
- Caller-supplied `(origin, series)` identity gives a precise, predictable idempotency guarantee: re-ingesting a run is a clean no-op; a genuinely corrected run is an explicit, intentional act.
- Atomic per-run append makes the top reliability property (no torn runs) enforceable in one place (the Ingestion component).
- The Nixtla adapter meets the dominant user where they already are, supporting the ~15-minute time-to-first-value target, without splitting the write path.

### Negative
- Push requires the caller to supply stable identity; a user who passes inconsistent origins can defeat idempotency. We mitigate with validation and clear errors, not by guessing identity for them.
- Being Type 1 on identity semantics, the v1 rule for "what is a run" must be right; revisiting it later is a breaking change with migration cost.
- Re-ingesting a *corrected* run (same identity, different values) needs an explicit conflict/overwrite policy, which adds a small amount of API surface.

### Neutral
- Couples us loosely to the Nixtla output shape via one adapter; other libraries (sktime/Darts) become future adapters over the same path, not new contracts.
- Establishes "the caller owns run identity" as a documented expectation users must internalize.

## Options considered

### Option A — Push with caller-supplied explicit identity (chosen)
- **Description:** Caller passes `(origin, series)`; atomic append; idempotent on that key.
- **Why accepted:** Strongest, most predictable idempotency/atomicity guarantee with minimal ceremony; the Nixtla adapter recovers most of the convenience of a pull model without a second write path.

### Option B — Push with inferred identity from content
- **Description:** Derive run identity from `(series, origin)` content hashing, zero caller ceremony.
- **Why rejected:** Lowest ceremony but risks false-dedupe when a run is legitimately *corrected* (same origin, new values would be silently dropped or mis-merged) — an unacceptable silent-correctness failure.

### Option C — Pull adapter as the primary contract
- **Description:** The archive reads a forecasting library's output as the main ingestion path.
- **Why rejected:** Best ecosystem ergonomics but makes the core write path depend on an external tool's output shape; better as a thin convenience (folded into the chosen option) than as the foundation.

> **Note (2026-06-10):** the run identity decided here is **widened by ADR-006** to `(model_id, model_version, origin, series)` so parallel versions of a model do not collide. Push-with-caller-identity and atomic append are unchanged; the caller now also supplies `model_id` and `model_version`.

## Related ADRs
- **Depends on:** ADR-001 (identity key is defined over the canonical schema).
- **Refined by:** ADR-006 (adds model/version to the run identity).
