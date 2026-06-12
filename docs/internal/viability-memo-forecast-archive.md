# Viability Memo: Production Forecast Archive

**Document type:** Go/No-Go gate (pre-BRD). Not a suite artifact — this is the kill-or-proceed check the user asked for before any documentation work begins.
**Project:** Production Forecast Archive (working name)
**Date:** 2026-06-10
**Author context:** Competitive/prior-art scan against the attached concept brief.
**Verdict:** **GO — qualified.** Proceed to the documentation chain, but with sharpened positioning and one architecture decision (adopt-vs-greenfield) elevated to a day-one ADR.

---

## The question this memo answers

The user's instruction was explicit: *if we can't build something better than what people already do, identify that early and abort.* So the bar is not "is the idea coherent?" (it is) but "is there an underserved user for whom no good option exists today?" This memo tests the brief's central claim — that *"a durable, bitemporal-aware production forecast archive with a clean horizon-accuracy API is not a single well-known product"* — against what actually ships in June 2026.

## What I found: the seam is real, but narrower and more crowded than the brief claims

The brief names three adjacent communities (forecasting libraries, ML monitoring, econ real-time data). All three checks held up. But the scan surfaced **two strong pieces of prior art the brief did not mention**, and they materially change the positioning — though not the verdict.

### 1. Forecasting libraries generate the structure but don't manage it — confirmed

Nixtla (`statsforecast`/`mlforecast`/`neuralforecast`) cross-validation output is keyed on `cutoff` (the forecast origin) plus target date, actual, and prediction — exactly the origin × target × horizon shape. sktime, Darts (`historical_forecasts`/`backtest`), and R's fable/tsibble produce the equivalent. But this is a *training-time, in-memory artifact for model selection*. None of them persists it as a durable, queryable production asset with an as-of/horizon API. The brief is correct here.

### 2. ML monitoring is keyed on the wrong axis — confirmed

NannyML, Evidently, and Arize store predictions vs. actuals over time and do drift/performance estimation (NannyML's CBPE/DLE are notably about *estimating* performance when labels are delayed — a real adjacency for forecasting). But they are organized around prediction timestamp and model version, not origin × target × horizon. "Accuracy at lead time *L*" is not a native query. Confirmed.

### 3. The Hubverse — the closest existing system, and the brief missed it

This is the most important finding. **The Hubverse** (`hubData`, `hubEvals`, `hubEnsemble`, from the Reich Lab / CDC epidemic-forecasting community) is a mature, open-source forecast archive that already does most of what the brief proposes:

- Stores forecasts keyed on `reference_date` (the origin), `horizon`, and `target_end_date` — the exact bitemporal triple.
- Persists to **Parquet/Arrow**, queryable directly via DuckDB/Polars — the exact storage stack the brief proposes.
- Carries a separate **target (observed) data** layer to join actuals against.
- Ships **`hubEvals`**, which scores forecasts **by horizon** (MAE, WIS, interval coverage) — i.e., the accuracy-vs-horizon curve is already a first-class output.

The gap: Hubverse is purpose-built for *collaborative, multi-team infectious-disease forecasting challenges* ("hubs"). It is quantile/WIS-centric, assumes a submission-and-validation workflow across many modeling teams, and is structured around epi conventions (epiweeks, a `hub` directory contract). It is **not** a turnkey library a single analytics team drops onto its own operational forecast log. But it is close enough that "why not just adopt or generalize the Hubverse format?" is a legitimate question we must answer before writing a line of code.

### 4. Zoltar — a research forecast repository, same community, same caveat

Zoltar (also Reich Lab) is a hosted forecast repository with push/pull APIs (`zoltr`/`zoltpy`) for hosting forecasting challenges. Again: research/challenge-hosting, not an operational single-team library.

### 5. Supply-chain demand planning already productized this — the brief's blind spot

In demand planning, **accuracy-by-lag is a first-class, well-understood, productized concept.** SAP IBP and peers compute "lag-based forecast error" natively (lag-1, lag-N snapshots), and the discipline has established vocabulary — *lag accuracy*, *forecast value added (FVA)*, *snapshot tables*. So the brief's claim that "the structure is rarely recognized for what it is" is **false in this domain** — it's recognized, named, and shipped. The catch: it's locked inside expensive enterprise planning suites, not available as a lightweight open library, and it's framed in supply-chain terms, not general ML/DS terms.

## Synthesis: who is actually underserved?

| User segment | Best current option | Underserved? |
|---|---|---|
| Epidemiological / collaborative-challenge forecasters | Hubverse, Zoltar — mature, free | **No.** Don't target them. |
| Large supply-chain / demand-planning orgs | SAP IBP, o9, Blue Yonder — lag accuracy & FVA built in | **No.** They've bought the capability. |
| **General analytics/DS teams running recurring operational forecasts** (finance, ops, energy, marketplaces, capacity) outside the two worlds above | **Roll their own append-only log + bespoke SQL every time.** No turnkey, general, open, Python-first option. | **Yes.** This is the target. |

The honest differentiation is **not** "this pattern doesn't exist" — it demonstrably does, in two domains. It is: *the pattern is trapped in domain-specific tooling (epi hubs) and expensive enterprise suites (demand planning). There is no lightweight, general, open, Python-first library a single DS team can `pip install`, point at its existing forecast log, and immediately get horizon-keyed accuracy, as-of reconstruction, and columnar storage — without adopting epiweek conventions or buying a planning suite.*

That gap is real but **modest**, and the contribution is "integration and ergonomics, not new math" (the brief's own words). That means **the moat is adoption and developer experience, not technology** — the design is easy to replicate, so the risk is not "can't build it" but "builds it, nobody adopts it because rolling their own felt good enough."

## Why this clears the abort bar (the GO case)

For the target segment, the status quo is genuinely worse than what we'd ship: every team re-derives the same `target_date − run_date` join, no one materializes the accuracy summary, and the horizon question is answered ad hoc or not at all. A small, well-documented library that makes `accuracy_at_horizon(h)` and `as_of(origin)` one-liners over their existing log is a clear improvement for them. We *can* build something better than what they do today. **GO.**

## Why it's qualified, not unconditional (the risks)

1. **Thin moat / easy to replicate.** Ergonomics is the only differentiator. If DX isn't excellent, "roll your own" wins. → The PRD must treat time-to-first-value (minutes from `pip install` to a horizon curve) as a headline success metric, not an afterthought.
2. **Adopt-vs-build is unsettled and load-bearing.** Generalizing the Hubverse model-output format (proven, documented, Arrow-schema'd) vs. inventing a fresh schema is the single highest-leverage decision. Picking wrong wastes the build or saddles general users with epi baggage. → Elevate to **ADR-001** before the tech spec hardens.
3. **Naming/positioning collision.** "Forecast Archive" overlaps Zoltar's and Monash's "archive" usage (both are *dataset* archives, a different thing). The name must signal *operational accuracy-over-horizon*, not *dataset repository*.
4. **Segment discipline.** If we let the design drift toward epi-hub or enterprise-SCM feature parity, we lose to the incumbents who own those segments. Stay in the underserved middle.

## Recommendation

Proceed to the documentation chain with three constraints baked in from the start:

1. **Target user is fixed:** general analytics/DS teams running recurring operational forecasts, outside epi-hubs and enterprise SCM. The BRD/PRD scope to *them*.
2. **DX is a first-class success metric**, not a nicety — time-to-first-value and "works on my existing log without reformatting" are gate criteria.
3. **First architecture decision is an ADR:** adopt/generalize the Hubverse model-output format vs. greenfield schema. Don't let the tech spec assume greenfield by default.

If, during the PRD, we cannot articulate a target user who would choose this over (a) rolling their own and (b) Hubverse, *that* is the real abort trigger — but the evidence says such a user exists.

---

## Sources

- Nixtla cross-validation (cutoff/origin keying): https://nixtlaverse.nixtla.io/neuralforecast/docs/tutorials/cross_validation.html ; https://www.nixtla.io/docs/forecasting/evaluation/cross_validation
- NannyML performance estimation (delayed labels): https://nannyml.readthedocs.io/en/stable/how_it_works/performance_estimation.html ; https://www.nannyml.com/blog/monitoring-energy-forecasts
- ML monitoring tool comparison (Evidently/NannyML/Arize): https://medium.com/@tanish.kandivlikar1412/comprehensive-comparison-of-ml-model-monitoring-tools-evidently-ai-alibi-detect-nannyml-a016d7dd8219
- Hubverse model-output format (reference_date/horizon/target_end_date, Parquet): https://docs.hubverse.io/en/latest/user-guide/model-output.html ; https://hubverse-org.github.io/hubData/articles/connect_hub.html ; https://hubverse.io/en/latest/user-guide/target-data.html
- hubEvals (scoring by horizon — MAE/WIS/coverage): https://hubverse-org.github.io/hubEvals/ ; https://github.com/hubverse-org/hubEvals
- Zoltar forecast repository: https://www.zoltardata.com/about ; https://docs.zoltardata.com/ ; https://reichlab.io/zoltr/
- Supply-chain lag-based accuracy / FVA: https://www.linkedin.com/pulse/lag-based-forecast-calculations-ahmed-khaled ; https://community.sap.com/t5/supply-chain-management-q-a/how-quot-lag-based-forecast-error-calculations-in-demand-planning-quot/qaq-p/12169801 ; https://nicolas-vandeput.medium.com/cumulative-and-lag-1-forecasts-are-the-most-important-d842ff282197
- DuckDB + Parquet for forecast storage / point-in-time: https://duckdb.org/2021/06/25/querying-parquet
- Zoltar/Monash "archive" naming collision: https://arxiv.org/pdf/2006.03922 ; https://arxiv.org/pdf/2105.06643
