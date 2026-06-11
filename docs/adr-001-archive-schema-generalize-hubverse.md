*This artifact was produced by applying `adr.md`'s rules in Integrated mode. Review the prompt's full contract if you want to audit.*

# ADR-001: Generalize the Hubverse model-output schema rather than adopt-as-is or greenfield

**Stage:** ADR-001
**Project:** Production Forecast Archive (working name)
**Date:** 2026-06-10
**Upstream artifacts:** tech-spec-forecast-archive-draft.md
**Originating question-id:** Q-001

## Assumptions and inferred inputs

| Input | Source | If inferred or user-confirmed: notes |
|---|---|---|
| Architectural context | supplied (Tech Spec Draft §Open architectural questions, Q-001) | — |
| Chosen option & rationale | inferred (tech-lead recommendation) | Decision made by Claude acting in the tech-lead role of `adr.md` under the constraints the user set (library, laptop-scale, ergonomics-as-moat). Flagged for user ratification. |
| Reversibility classification | inferred | Classified Type 1 via cost-of-reversal: changing the raw on-disk schema after users hold archives forces a data migration. |

## Status

accepted

## Context

The archive's raw and actuals schema is the foundation every other component sits on, and the viability scan surfaced that the Hubverse model-output format already encodes almost exactly our conceptual model: `reference_date` (forecast origin), `horizon`, `target_end_date`, persisted as Parquet with a published Arrow schema, plus a *separate* target-data (actuals) layer. It is proven, documented, and ecosystem-backed. The tension: Hubverse is shaped for collaborative infectious-disease forecasting hubs — it carries quantile/WIS-centric `output_type`/`output_type_id` columns, epiweek date conventions, and a multi-team submission-and-validation workflow that our single-team operational user does not want and should not pay for.

Three forces are in tension. First, **not-invented-here risk**: inventing a fresh schema when a good conceptual one exists wastes effort and forfeits interop. Second, **fit**: adopting Hubverse wholesale imports epi baggage that contradicts the PRD's explicit out-of-scope ("no epiweek conventions, no quantile/WIS submission workflow"). Third, **forward-compatibility**: v1 ships a single settled actual per target, but the data model must not foreclose vintaged actuals (tri-temporality) — and Hubverse's separate target-data layer is exactly the pattern that keeps that door open.

This decision bounds ADR-002 (engine), ADR-003 (summary), and ADR-005 (ingestion identity), all of which key off the schema chosen here.

## Decision

We will design our own lean physical schema that **generalizes the Hubverse conceptual model** — the origin/horizon/target triple and a separate actuals layer — while dropping epi-specific conventions (epiweeks, quantile/WIS `output_type` machinery, the hub submission workflow). We will keep interop cheap where it is cheap: the ability to *read* point-forecast data out of Hubverse-format files, without committing to Hubverse as a dependency or to round-trip fidelity.

## Consequences

**Reversibility:** Type 1 (irreversible — once users hold archives in our physical schema, changing it forces a data migration; mitigated, not eliminated, by an on-disk format version and the fact that the derived summary is always rebuildable from raw).

### Positive
- Reuses a proven conceptual model, cutting design risk and signalling to a knowledgeable audience that we understand the prior art.
- Sheds epi baggage that the target user (general operational forecasting) does not want, keeping the schema and API small — directly serving the ergonomics moat.
- The separate actuals layer preserves the tri-temporality upgrade path: v1's single-actual table is a degenerate case of a future vintaged-actuals table, addable without migrating forecasts.
- Optional read-interop with Hubverse files gives a credible on-ramp for anyone with data already in that format.

### Negative
- We own a schema spec and its evolution rules — a maintenance burden adopting Hubverse wholesale would have outsourced.
- "Generalize, don't adopt" risks subtly diverging from Hubverse in ways that confuse users who know it; we must document the relationship explicitly.
- Being Type 1, a schema mistake discovered after release is expensive (migration tooling) — raising the bar on getting the v1 raw schema right.

### Neutral
- Introduces a small format-versioning concern in the archive header (carried as a cross-cutting concern in the spec).
- Shifts complexity from "learning the hub workflow" to "owning a minimal schema," which is the trade we want for this user.

## Options considered

### Option A — Adopt Hubverse model-output format as-is
- **Description:** Use the Hubverse format directly, constraining ourselves to a point-forecast profile of it.
- **Why rejected:** Imports epiweek conventions and quantile/WIS `output_type` columns the operational user neither needs nor understands; couples our roadmap to an epi-governance project; contradicts the PRD's out-of-scope boundaries.

### Option B — Generalize the Hubverse conceptual model (chosen)
- **Description:** Keep the origin/horizon/target triple and separate actuals layer; own a lean physical layout; drop epi conventions; keep cheap read-interop.
- **Why accepted:** Best balance of low NIH risk, tight fit to the target user, and a clean tri-temporality upgrade path, at the cost of owning a small schema spec.

### Option C — Greenfield schema, Hubverse as inspiration only
- **Description:** Design purely for the operational single-team case with no interop intent.
- **Why rejected:** Forfeits interop and the credibility of building on recognized prior art for marginal additional simplicity over Option B; the brief explicitly values connecting to the established pattern.

> **Note (2026-06-10):** the forecast grain decided here is **widened by ADR-006** to add `model_id` and `model_version` (multiple models/versions, including parallel runs), and the **actuals layer is refined by ADR-007** into an append-only, revisable log with an optional "official" designation (partial tri-temporality). The generalize-Hubverse decision is unchanged; the key sets grow. See ADR-006 and ADR-007.

## Related ADRs
- **Influences:** ADR-002 (storage engine — physical layout/partitioning assume this schema), ADR-003 (summary grain keys off these fields), ADR-005 (ingestion idempotency key is defined over this schema), ADR-006 (adds model/version to this grain), ADR-007 (refines the actuals layer into a revisable log with an official flag).
