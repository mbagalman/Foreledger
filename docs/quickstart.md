# Foreledger quick-start guide

This walkthrough takes you from an empty directory to accuracy curves, model
comparisons, and as-of replays in about ten minutes. The snippets form one
continuous session: paste them into a Python REPL or script top to bottom and
every one runs as written.

For the elevator pitch and architecture overview, see the [README](../README.md).

## 1. Install

Foreledger needs Python ≥ 3.11.

```bash
git clone https://github.com/mbagalman/Foreledger.git
cd Foreledger
pip install -e .
```

Prefer to see it run before reading on? `python examples/quickstart.py`
builds a synthetic two-model archive and renders the accuracy curves.

## 2. Create (or open) an archive

```python
import pandas as pd

from foreledger import ForecastArchive

archive = ForecastArchive("./forecast_ledger")
```

That's the whole setup. The directory is created if it doesn't exist and
reopened if it does. Everything lives in plain Parquet/JSON files under that
path — your data never leaves your machine.

Two safety properties worth knowing from the start:

- Foreledger **never re-initializes** an existing non-archive directory; it
  raises `StoreFormatError` instead of touching your files.
- The archive carries a **format version** (currently 2; version-1 stores
  migrate automatically on open). A store written by a newer Foreledger
  raises a clear error rather than being misread — which also means an older
  library refuses a newer store instead of silently misinterpreting it.

## 3. Ingest forecast runs

Suppose your pipeline produced this frame on June 1st — two SKUs, predictions
for the next two days:

```python
predictions = pd.DataFrame(
    {
        "sku": ["A-100", "A-100", "B-200", "B-200"],
        "date": pd.to_datetime(["2026-06-02", "2026-06-03"] * 2),
        "yhat": [105.2, 107.9, 51.0, 49.5],
    }
)

archive.ingest(
    predictions,
    mapping={"series_id": "sku", "target": "date", "value": "yhat"},
    model_id="prophet",
    model_version="2.1",
    origin="2026-06-01",  # the run date; or map an origin column instead
)
```

Identity is **caller-supplied, never guessed**: `model_id` and
`model_version` are opaque strings of your choosing, and each
`(model, version, run date, series)` is one run. That buys you:

- **Idempotency** — re-running the same ingest (a retried Airflow task, a
  replayed job, even a partial replay of just one series) is a no-op. State
  after N identical calls equals one.
- **No collisions** — ingesting `model_version="2.2"` for the same dates
  *adds* rows; versions live side by side for fair comparison.
- **Explicit conflicts** — the same run identity with *different* values
  raises `IngestConflictError` unless you pass `on_conflict="overwrite"`,
  which supersedes the old run on the record. Nothing is ever silently
  merged.
- **Atomicity** — a crash mid-ingest leaves the archive exactly as it was.

A challenger model's runs go through the same call:

```python
challenger = predictions.assign(yhat=[103.8, 108.4, 50.1, 49.0])
archive.ingest(
    challenger,
    mapping={"series_id": "sku", "target": "date", "value": "yhat"},
    model_id="prophet",
    model_version="2.2",
    origin="2026-06-01",
)
```

Using [Nixtla](https://github.com/Nixtla)? Cross-validation frames go through
the same path with the column mapping handled for you —
`archive.ingest_nixtla(cv_df, model_id="AutoETS", model_version="1.0")` maps
`unique_id`/`ds`/`cutoff` automatically.

## 4. Register actuals (and revisions)

Actuals are a separate, model-independent log — every model is scored against
the same truth:

```python
actuals = pd.DataFrame(
    {
        "sku": ["A-100", "A-100", "B-200", "B-200"],
        "date": pd.to_datetime(["2026-06-02", "2026-06-03"] * 2),
        "value": [101.0, 109.5, 50.2, 48.8],
    }
)

archive.register_actuals(
    actuals,
    mapping={"series_id": "sku", "target": "date"},
    source="warehouse",
    recorded_at="2026-06-04",
)
```

When the numbers get restated, register again with a newer `recorded_at` —
the log is append-only, the newest registration becomes the effective
"latest" value, and full history is retained:

```python
corrected = actuals.assign(value=[101.4, 109.5, 50.0, 48.8])
archive.register_actuals(
    corrected,
    mapping={"series_id": "sku", "target": "date"},
    source="warehouse",
    recorded_at="2026-06-10",
)
```

Things Foreledger handles that ad-hoc scoring scripts get wrong:

- **One identity, one truth.** An actual's identity is
  `(series, target, source, recorded_at)`. Replaying an identical
  registration is a no-op; re-registering the same identity with a
  *different* value is rejected — revisions need a new `recorded_at` (or
  another `source`), so the history stays unambiguous.
- **Conflicting feeds at the same instant.** If two sources register
  different values with the same timestamp, Foreledger resolves by your
  configured priority — `ForecastArchive(..., source_priority=["finance",
  "ops"])` — or, if it can't, writes the conflict to an error log and
  excludes the target from accuracy instead of guessing.
- **Official numbers.** Mark the value finance signed off on, and it stays
  marked — later registrations can't displace it:

  ```python
  signed_off = pd.DataFrame(
      {
          "sku": ["A-100"],
          "date": pd.to_datetime(["2026-06-02"]),
          "value": [101.4],
      }
  )
  archive.register_actuals(
      signed_off,
      mapping={"series_id": "sku", "target": "date"},
      source="finance",
      recorded_at="2026-06-15",
      official=True,
  )
  # changing an official designation later requires the explicit path:
  archive.mark_official(series="A-100", target="2026-06-02", source="warehouse")
  ```

- **Missing actuals are visible, never flattering.** Forecast rows without a
  usable actual are excluded from the metric, counted in the result's
  `n_missing_actuals`, and downgrade the result to `status="partial"` —
  identically on the precomputed and raw query paths. A scope where
  *nothing* can be scored comes back as an explicit `status="insufficient"`,
  never a zero that reads as perfect accuracy. `status="ok"` means full
  coverage, nothing less.

## 5. Ask the questions

### How does accuracy decay with horizon?

```python
result = archive.accuracy_at_horizon(
    1, metric="MAE", model_id="prophet", model_version="2.1"
)
print(result.value, result.n, result.n_missing_actuals, result.status)

curve = archive.accuracy_curve(metric="MAE", model_id="prophet", model_version="2.1")
print(curve.to_frame())  # one row per horizon
```

(`curve.plot()` renders it if matplotlib is installed.) Scope any query with
`series=`, a `period=(start, end)` window on run dates, or leave the model
unset to pool across models. Metrics: `MAE`, `RMSE`, `MAPE`, `MASE`, or your
own (step 7).

### Score against latest or official truth?

```python
strict = archive.accuracy_at_horizon(
    1, basis="official", model_id="prophet", model_version="2.1"
)
print(strict.status, strict.n, strict.n_missing_actuals)

filled = archive.accuracy_at_horizon(
    1,
    basis="official",
    fallback="latest",
    model_id="prophet",
    model_version="2.1",
)
print(filled.value, filled.n_fallback)
```

Under `basis="official"`, targets without an official actual count toward
`n_missing_actuals` and cap the status at `partial` — they are never
silently substituted, and a scope with no official actuals at all is
`insufficient`. The explicit `fallback="latest"` opt-in fills the gaps from
the latest value and reports how many were filled (`n_fallback`).

### Which model wins?

```python
archive.set_champion("prophet", "2.1")  # one champion per model_id

table = archive.compare_models(
    1,
    [("prophet", "2.1"), ("prophet", "2.2")],
    metric="MAE",
)
print(table)
```

Every row is computed over the same scope (same series/period filters) and
provably equals the corresponding single-model query; each row carries its
own `n`, so differing data coverage between models is visible rather than
hidden. Versions whose model has a champion get a `delta_vs_champion`
(negative = better on error metrics). `compare_curve(...)` gives the same
head-to-head across all horizons.

### What did we know on June 1st?

```python
snapshot = archive.as_of("2026-06-01")
print(len(snapshot))
```

Returns every forecast whose run date is on or before that day — and nothing
from later runs. This is the audit answer to "what drove the decision?" and
the honest input to any backtest.

### Don't trust a headline number? Drill into it.

```python
rows = archive.drill(
    {
        "model_id": "prophet",
        "model_version": "2.1",
        "horizon": 1,
        "metric": "MAE",
        "basis": "latest",
    }
)
print(rows)
```

You get the exact forecast/actual pairs behind the cell; recomputing the
metric over them reproduces the summary value to the last bit.

## 6. Inventory and integrity

```python
print(archive.list_models())  # every (model, version) with coverage
archive.reconcile()  # assert the precomputed summary == recomputation from raw
```

`reconcile()` should never fail — the summary is rebuilt eagerly on every
write, validated against the raw state before it is ever served, and fully
disposable (delete it and queries fall back to raw, invisibly). If it ever
does fail, that's a bug worth reporting, not a tolerance to widen.

## 7. Custom metrics

Any callable over aligned `(forecast, actual)` NumPy arrays:

```python
import numpy as np


def pinball_p90(forecast: np.ndarray, actual: np.ndarray) -> float:
    diff = actual - forecast
    return float(np.mean(np.maximum(0.9 * diff, (0.9 - 1) * diff)))


archive.register_metric("pinball90", pinball_p90, summarizable=True)
print(archive.accuracy_at_horizon(1, metric="pinball90",
                                  model_id="prophet", model_version="2.1"))
```

`summarizable=True` precomputes it alongside the built-ins. Registered
metrics run behind an error/timeout guard: one that raises yields explicit
"insufficient" cells, and one that hangs past the timeout is skipped and
quarantined for the session. This is failure containment, not a security
sandbox — registered code runs in-process with your privileges, so only
register code you trust.

## What's on disk

```
forecast_ledger/
├── archive_meta.json     # format version — the compatibility gate
├── forecasts/*.parquet   # the raw ledger, one file per ingest call
├── actuals/*.parquet     # the revisable actuals log
├── officials/*.parquet   # official designations (append-only)
├── summary/              # the disposable accuracy cache (+ validity token)
├── runs.json             # run manifest (identity, visibility, idempotency)
├── actuals_manifest.json # actuals/officials visibility (transactional commit)
├── segment_integrity.json# size/mtime/sha256 per committed segment (tamper check)
├── champions.json        # champion per model_id
└── error_log.txt         # unresolved actuals conflicts (created on demand)
```

Plain files, open formats: anything that reads Parquet — DuckDB, pandas,
Polars, Spark — can read your ledger directly.
