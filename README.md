# Foreledger

**A durable ledger for your forecasts — so you can finally answer "how accurate are we, really?"**

## The problem

Every team that forecasts — demand, revenue, staffing, inventory — produces
predictions on a schedule, acts on them, and then quietly throws them away.
Next week brings a new forecast, and the old one is overwritten in a
spreadsheet, buried in a model artifact, or simply gone.

That makes the questions leadership actually asks surprisingly hard to answer
honestly:

- *"How far out can we trust the forecast?"* Nobody stored what was predicted
  14 days ahead vs. 2 days ahead, so nobody knows where accuracy falls off.
- *"Is the new model actually better?"* The data scientist's backtest says
  yes, but there's no like-for-like record of what each model predicted in
  production.
- *"Why did we order so much stock in March?"* The forecast that drove the
  decision no longer exists — only the embarrassing outcome does.

And there's a subtler trap: **the truth itself changes.** Actuals get
restated — a late data feed, a returns adjustment, a finance correction. If
you score yesterday's forecast against today's revised numbers without
keeping track, your accuracy reports quietly stop meaning anything.

## What Foreledger does

Foreledger treats forecasts the way accounting treats money: **every entry is
recorded, nothing is erased, and the books always reconcile.**

You push each forecast run into the ledger as it happens (one line of code in
your existing pipeline), register actuals as they arrive — including
revisions and an optional "official" designation for the numbers finance
signed off on — and Foreledger answers, instantly and reproducibly:

1. **Accuracy by horizon** — a curve showing exactly how error grows the
   further out you predict, for any model, series, or time window.
2. **Model vs. model** — side-by-side comparisons across models and versions
   over a common scope, with deltas against your designated champion. Every
   row reports its own sample count and coverage, and each value provably
   equals the corresponding single-model query — no hidden re-weighting.
3. **Origin-scoped snapshots** — query the current record filtered to runs
   dated on or before any past day, with no later-dated runs leaking in. One
   honest caveat: an explicit overwrite revises this view (supersessions are
   recorded), so it reflects the archive's current state of those runs, not a
   transaction-time replay.

When the data can't fully support an answer, Foreledger says so out loud: a
scope with gaps is flagged *partial* with the missing count attached, and one
where nothing can be scored is an explicit *insufficient* — never a number
that quietly reads as perfect accuracy.

## Quick start

```bash
pip install -e .            # PyPI release coming with v0.1
python examples/quickstart.py   # full demo on synthetic data, zero config
```

```python
from foreledger import ForecastArchive

archive = ForecastArchive("./forecast_ledger")

# after every forecast run:
archive.ingest(predictions_df, model_id="prophet", model_version="2.1")

# as actuals arrive (revisions welcome):
archive.register_actuals(actuals_df, source="warehouse")

# the payoff:
curve = archive.accuracy_curve(metric="MAE", model_id="prophet", model_version="2.1")
print(curve.to_frame())
```

**[Read the full quick-start guide →](docs/quickstart.md)**

## Key capabilities

- **Append-only forecast archive** keyed on
  `(model, version, series, run date, target date)` — parallel model versions
  coexist; ingestion is atomic and idempotent, so crashed or replayed
  pipelines can't corrupt or duplicate history.
- **Revisable actuals log** — every restatement is kept; accuracy can be
  scored against the *latest* numbers or only the *official* ones, and
  same-timestamp conflicts between feeds are resolved by your priority rules
  or flagged loudly, never picked silently.
- **Built-in metrics** (MAE, RMSE, MAPE, MASE) plus a protocol for
  registering your own — custom metrics run behind an error/timeout guard,
  so one that crashes is skipped and one that hangs is skipped and
  quarantined for the session, rather than corrupting a recompute.
  (Containment, not a security sandbox: registered code runs in-process.)
- **Champion/challenger workflow** — designate a champion per model and every
  comparison reports challenger deltas automatically.
- **Drill-down provenance** — any headline accuracy number can be exploded
  into the exact forecast/actual pairs behind it, and they reconcile exactly.
- **Crash-safe and tamper-evident** — data becomes visible only when its
  manifest commits, so a crash mid-write leaves the ledger at its prior
  state; every committed file is fingerprinted (size/mtime/sha256), checked
  cheaply on every query, and fully hash-audited by `reconcile()`. Files
  deleted or modified outside the library fail loudly with a typed error —
  never silently absent rows.
- **Local-first, warehouse-ready** — v1 stores Parquet on your disk via
  DuckDB (your data never leaves your machine); the storage layer is built
  behind a dialect-aware seam so a Snowflake backend (v1.1) drops in without
  changing your code.

## How it works (the technical bit)

Foreledger is a small, layered Python library (≥ 3.11):

- **Raw archive** — append-only Parquet, the permanent source of truth, with
  an on-disk format version gate. Visibility is committed via a run manifest,
  which is what makes ingestion all-or-nothing.
- **Actuals log** — model-independent, append-only revisions keyed
  `(series, target, source, recorded_at)`, with sticky official designations.
- **Integrity registry** — a fingerprint journal over every committed file,
  doubling as the commit log: entries are staged before a write becomes
  visible and confirmed after, so the store can always tell a failed write's
  leftovers from externally tampered data.
- **Accuracy summary** — a precomputed, *disposable* cache rebuilt eagerly on
  every write. Queries route to it invisibly and fall back to raw
  computation; both paths share one code path and a `reconcile()` check
  asserts they agree exactly.
- **Backend seam** — all storage and query runs through an engine-neutral,
  dialect-parameterized layer; a CI guard (AST scan) ensures no DuckDB-only
  code leaks out of the backend module.

Architecture and the decisions behind it are documented in
[docs/internal/](docs/internal/): the
[tech spec](docs/internal/tech-spec-forecast-archive-final.md), ADRs 001–007,
and the [implementation plan](docs/internal/implementation-plan-forecast-archive.md).
Contributor ground rules live in [AGENTS.md](AGENTS.md).

## Development

```bash
pip install -e ".[dev]"
pytest                 # incl. atomicity, reconciliation, replay, no-leakage tests
ruff check . && ruff format --check .
mypy src               # strict
python examples/quickstart.py
```

## Status & roadmap

Working MVP (Phases 1–4 of the [plan](docs/internal/implementation-plan-forecast-archive.md)):
the full ingestion → actuals → evaluation surface described above, gated by CI.
Next: packaging and PyPI release (Phase 5), then the Snowflake warehouse
backend as v1.1 (Phase 6).

## License

[MIT](LICENSE).
