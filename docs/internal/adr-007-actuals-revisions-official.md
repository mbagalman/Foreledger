*This artifact was produced by applying `adr.md`'s rules in Integrated mode. Review the prompt's full contract if you want to audit.*

# ADR-007: Append-only actuals with revisions and an optional "official" designation

**Stage:** ADR-007
**Project:** Production Forecast Archive (working name)
**Date:** 2026-06-10
**Upstream artifacts:** tech-spec-forecast-archive-final.md
**Originating question-id:** user-supplied (raised after the Final Tech Spec; refines the actuals layer from ADR-001/ADR-006 and partially pulls in the tri-temporality that v1 had deferred)

> **Amended 2026-06-11 — result-status contract for missing actuals.**
> Implementation surfaced an ambiguity in "targets … are reported
> insufficient": read as a *whole-result* status, any scope containing recent
> targets (whose actuals simply haven't arrived) would be insufficient,
> making the status useless as a signal. The ratified contract is
> three-state, applied identically under both bases and on both query routes:
> `"ok"` — every forecast in scope was scored; `"partial"` — a value was
> computed over the covered pairs and the uncovered targets are counted in
> `n_missing_actuals` (this realizes the per-target "reported insufficient"
> requirement: gaps are explicit and never silently substituted);
> `"insufficient"` — nothing in scope could be scored. Under the strict
> official basis, targets lacking an official actual count as missing (so the
> status is at best `partial`) unless the caller opts into
> `fallback="latest"`, whose fills are flagged per the original decision.
> `AccuracyResult.ok` is true for `ok` and `partial`. The fail-loud posture is
> unchanged: missing coverage can never read as complete or as perfect
> accuracy.

## Assumptions and inferred inputs

| Input | Source | If inferred or user-confirmed: notes |
|---|---|---|
| Requirement: actuals change over time; need an "official" value that stays put | user-confirmed | User: "actuals can change over time (delayed data feeds and such)… default to last-write-wins, but include a flag for marking an 'official' actual." Finance example: an estimate is reported and remains the official booked number even after real data arrives later. |
| Chosen representation & rationale | user-confirmed | Append-only actuals + `is_official` flag + selectable accuracy basis is the tech-lead recommendation under the stated need; the user asked for the capability, not this specific shape — flagged for ratification. |
| Reversibility classification | inferred | Type 1 — the actuals representation is part of the on-disk schema; reshaping it after archives exist is a migration. |

## Context

ADR-001/ADR-006 modeled actuals as one settled value per `(series_id, target)`, model-independent, with overwrite-on-reregister, and explicitly deferred tri-temporality (vintaged actuals) to keep v1 lean — while requiring the schema "not foreclose" it. The user has now surfaced the case that forces the issue: **actuals are not settled once.** Delayed and revised data feeds change the actual for a target over time, and — critically — an organization often **reports an estimate** as the number of record and then keeps that estimate **official** even after the "real" figure lands. The user's Finance example: a good estimate is given at report time with partial data; when real data arrives later, the books keep the estimate.

This produces two legitimate accuracy questions for the same forecast: *how good was the forecast against what we actually booked (official)* and *how good was it against what truly happened (latest)*. The current single-value-overwrite model can answer at most one and silently loses the other. The forces in tension: **simplicity** (a single value is easy) versus **fidelity to how organizations actually use actuals** (revisions happen; the booked number is sticky and distinct from the final number); and **scope** (full tri-temporality was deferred) versus **getting the Type-1 actuals schema right now** so we don't migrate later.

This decision refines the actuals layer in ADR-001/ADR-006; forecasts, model/version identity, and the model-independence of actuals are unchanged.

## Decision

We will store actuals as an **append-only log** keyed on `(series_id, target, source, actual_recorded_at)`, with an `is_official` marker (at most one official row per `(series_id, target)`). The **`source`** field is a feed/label that disambiguates revisions — two values landing at the same `actual_recorded_at` from different feeds are distinct rows, not a collision (`source` defaults to a single value when the user doesn't supply one). Reads default to **last-write-wins** — the most recent `actual_recorded_at` per `(series, target)`. When two rows tie at the max `actual_recorded_at` from different sources, the tiebreak is: (1) **duplicate check** — if the tied values are equal, collapse them (no conflict); (2) **source priority** — if they differ, resolve by a configured `source_priority` (ordered, highest first); (3) **unresolved** — if they differ and priority cannot resolve it (no priority configured, or a tied source is absent from the list), write an **error to the configured error-log file** and report that target as **ambiguous** for the latest basis (not a silent guess). An actual can be **marked official** (via a flag on registration or an explicit call); the official value is **sticky** — later non-official registrations do not change it. Accuracy and comparison queries take an **`actual_basis`** parameter: `"latest"` (default) or `"official"`; under `"official"`, an opt-in **`fallback="latest"`** uses the latest value for targets that have no official actual (default is strict — those targets are reported insufficient). Retaining the full revision history (rather than overwriting) means the schema also does not foreclose future "accuracy as of any actual vintage" queries — partial tri-temporality now, full tri-temporality as a later, migration-free addition.

## Consequences

**Reversibility:** Type 1 (irreversible — the append-only actuals schema and the `is_official`/`actual_recorded_at` fields are on-disk; changing them after archives exist is a migration. Mitigated by the archive format version from ADR-001.)

### Positive
- Directly serves the reported use case: the "official" booked estimate and the later "real" actual coexist, and accuracy can be reported against either basis.
- Default `latest` behavior preserves the simple last-write-wins semantics for users who don't care about revisions — no added ceremony for the common case.
- Retaining revision history delivers the tri-temporality the brief flagged as the natural extension, without committing to its full query surface in v1; "as of actual vintage" becomes a later add over already-retained data.
- Keeps actuals model-independent (one truth layer shared across models/versions) — consistent with ADR-006.

### Negative
- The actuals layer is now more than a value lookup: an append-only log with a stickiness rule for `is_official` and a basis selector on every accuracy query — more surface to implement and test, and more concepts for users to understand.
- The summary must materialize per `actual_basis` (latest, and official where it exists), increasing summary cardinality and recompute work (compounds the model × version widening from ADR-006).
- Being Type 1, this widens the must-get-right-before-v1 schema surface again; an actuals-schema mistake is a migration.
- Semantics need careful definition: under `actual_basis="official"`, targets with no official actual are reported as insufficient (not silently substituted with latest) so "official" stays honest — a rule users must learn.

### Neutral
- `actual_recorded_at` is the transaction-time axis for actuals, mirroring `origin` for forecasts — the archive becomes genuinely bitemporal on both layers, which is conceptually cleaner even though only latest/official are exposed in v1.
- Establishes "basis selection" as a first-class query concept, which the summary, comparison, and drill-down all thread through.
- Introduces an optional `source_priority` configuration and a conflict/error log. Same-timestamp conflicts between differing feeds are surfaced loudly (logged, target flagged ambiguous) rather than silently resolved — a deliberate fail-loud posture for a data-integrity edge that would otherwise corrupt the latest basis.

## Options considered

### Option A — Append-only actuals + `source` + `is_official` + basis selection (chosen)
- **Description:** Append revisions keyed on `(series, target, source, actual_recorded_at)`; optional sticky official row; queries pick `latest` (default) or `official`, with an opt-in `fallback="latest"` under the official basis.
- **Why accepted:** Covers the booked-estimate-vs-real-data case exactly, the `source` label keeps multi-feed revisions unambiguous, keeps the default simple, and future-proofs the Type-1 actuals schema toward full tri-temporality — at the cost of more actuals/summary surface.

### Option B — Single value, overwrite, plus a separate pinned "official" field
- **Description:** Keep one mutable latest value and add one optional official value; no revision history.
- **Why rejected:** Handles the user's two-number case but throws away revision history, foreclosing later "as of actual vintage" without a migration — and since actuals are Type-1, that's exactly the foreclosure ADR-001 told us to avoid. The append-only form costs little more and keeps the door open.

### Option C — Full tri-temporality now (as-of-actual-vintage queries in v1)
- **Description:** Ship the complete vintaged-actuals query surface (accuracy as of any actual knowledge date) in v1.
- **Why rejected:** More than the user asked for and a meaningful v1 cost; the chosen option stores the data to support it but defers the query surface, which is the right scope trade.

## Related ADRs
- **Depends on:** ADR-001 (actuals layer it refines), ADR-006 (actuals stay model-independent).
- **Influences:** ADR-003 (summary gains an `actual_basis` dimension; cardinality up), ADR-004 (metrics compute over the selected basis).
