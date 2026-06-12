*This artifact was produced by applying `adr.md`'s rules in Integrated mode. Review the prompt's full contract if you want to audit.*

# ADR-006: Carry model and version as first-class keys in the forecast grain and run identity

**Stage:** ADR-006
**Project:** Production Forecast Archive (working name)
**Date:** 2026-06-10
**Upstream artifacts:** tech-spec-forecast-archive-draft.md
**Originating question-id:** user-supplied (raised after the Tech Spec Draft; refines the schema decided in ADR-001 and the identity decided in ADR-005)

## Assumptions and inferred inputs

| Input | Source | If inferred or user-confirmed: notes |
|---|---|---|
| Requirement: multiple models, multiple versions over time, sometimes running in parallel | user-confirmed | Raised by the user: "most users will have multiple forecasting models, and those models will have different versions over time (including sometimes running in parallel)." |
| Chosen representation & rationale | user-confirmed | User selected "Schema + cross-model comparison" scope; representation below is the tech-lead recommendation under that scope. |
| Reversibility classification | inferred | Type 1 — the model/version axis is part of the raw grain; adding or reshaping it after archives exist forces a migration. This is why it must be settled before v1. |

## Status

accepted

## Context

ADR-001 generalized the Hubverse schema with a forecast grain of `(series_id, origin, target)` and a separate actuals layer. That grain quietly assumes a single forecasting model. Real users run **multiple models** (e.g., a baseline and a champion), each evolving through **versions** over time, and during a migration two versions of the same model often **run in parallel** for overlapping origins. With no model dimension in the grain, parallel-version forecasts for the same `(series, origin, target)` collide, and the archive cannot answer "which model/version is more accurate at horizon *h*" — a question the user explicitly wants.

The forces in tension. First, **correctness of the grain**: forecasts from different models/versions for the same series/origin/target are distinct facts and must not collapse. Second, **irreversibility**: the raw grain is the Type-1 surface from ADR-001 — getting the key wrong now is a migration later, so the model axis cannot be deferred even though the *comparison features* could be. Third, **actuals are model-independent**: the settled truth for a target does not depend on which model predicted it, so the actuals layer must *not* gain a model key (doing so would duplicate truth and let models disagree about reality).

This decision refines ADR-001 (grain), ADR-005 (run identity), and ADR-003 (summary grain). It does not overturn their core choices — generalize-Hubverse, push-with-caller-identity, and eager-rebuildable-summary all still hold; the keys simply widen.

## Decision

We will add **`model_id` and `model_version`** (user-supplied opaque strings) to the **forecast grain and to the ingestion run identity**. The forecast grain becomes `(model_id, model_version, series_id, origin, target)`; run identity becomes `(model_id, model_version, origin, series)`. The **actuals layer stays model-independent**, keyed on `(series_id, target)`. The derived summary grain widens to `(model_id, model_version, series_id, horizon, metric, period)`, and the query API gains model/version scoping plus first-class **cross-model / cross-version comparison at a horizon** (champion-vs-challenger by lead time).

## Consequences

**Reversibility:** Type 1 (irreversible — the keys are part of the raw grain; reshaping them after archives exist is a migration. Settling it pre-v1 is the entire point of this ADR. Mitigated by the archive format version from ADR-001.)

### Positive
- Parallel versions of a model no longer collide; every forecast is attributable to a specific model and version.
- Enables the comparison the user wants: accuracy-vs-horizon *per model/version*, and direct champion-vs-challenger comparison at a given lead time — a genuine differentiator over a roll-your-own log.
- Keeping actuals model-independent preserves a single source of truth and avoids models disagreeing about what actually happened.
- Aligns with Hubverse's own `model_id` convention, keeping the read-interop path (ADR-001) coherent.

### Negative
- Wider keys increase summary cardinality by `models × versions`, raising summary size and recompute cost; at laptop scale this is absorbed, but it tightens the threshold at which eager recompute (ADR-003) needs revisiting.
- More required fields at ingestion (`model_id`, `model_version`) — a small ceremony increase that the ingestion ergonomics (defaults, the Nixtla adapter) must keep cheap.
- Being Type 1, this widens the surface that must be right before v1; a mistake here is a migration.

### Neutral
- `model_version` is an opaque user string, not a managed/registry concept — the archive does not interpret or order versions; semantics (semver, dates, hashes) are the user's. This keeps the library out of model-registry territory (explicitly a non-goal) while supporting the comparison use case.
- Establishes that "compare models/versions by horizon" is a first-class query, shaping the eval API surface.

## Options considered

### Option A — `model_id` + `model_version` in grain and identity; actuals model-independent (chosen)
- **Description:** Two opaque string keys added to forecasts and run identity; actuals unchanged; summary and query API widen; comparison-by-horizon shipped.
- **Why accepted:** Correctly models parallel versions, enables the wanted comparison, keeps actuals as single truth, and matches Hubverse's convention — at the cost of higher cardinality, which laptop scale absorbs.

### Option B — Single combined `model` key (e.g., "demand_v3")
- **Description:** One opaque string encoding both model and version.
- **Why rejected:** Pushes the model/version split into string parsing; makes "all versions of model X" and champion-vs-challenger queries fragile and convention-dependent. Two explicit fields cost almost nothing and keep comparison queries clean.

### Option C — Model dimension as metadata only, not in the grain
- **Description:** Store model/version as non-key attributes alongside forecasts.
- **Why rejected:** Does not prevent collisions for parallel versions at the same `(series, origin, target)`, and makes per-model uniqueness/idempotency undefined. The whole point is that these are *identifying* facts, so they belong in the key.

> **Note (2026-06-10):** two small follow-ons. (1) **Champion tag** — comparison gains an optional persisted "champion" designation **scoped one-per-`model_id`** (each logical model has at most one champion version — its production incumbent). `set_champion(model_id, model_version)` records it; comparisons mark the champion and report each challenger version's delta vs. its model's champion. Lightweight, last-write-wins metadata (Type 2), *not* a model registry; implemented in the comparison API rather than warranting its own ADR. (2) Actuals remain **model-independent**, but are refined by **ADR-007** into a revisable append-only log with an optional "official" value — the model-independence decided here is unchanged.

## Related ADRs
- **Depends on:** ADR-001 (the schema this refines), ADR-005 (the run identity this widens).
- **Influences:** ADR-003 (summary grain widens by model/version), ADR-007 (refines the model-independent actuals layer).
