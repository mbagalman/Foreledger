# Foreledger quick-start guide

This walkthrough takes you from an empty directory to accuracy curves,
model comparisons, and as-of replays in about ten minutes. Every snippet is
copy-pasteable; together they form one continuous session.

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
from foreledger import ForecastArchive

archive = ForecastArchive("./forecast_ledger")
```

That's the whole setup. The directory is created if it doesn't exist and
reopened if it does. Everything lives in plain Parquet/JSON files under that
path — your data never leaves your machine.

Two safety properties worth knowing from the start:

- Foreledger **never re-initializes** an existing non-archive directory; it
  raises `StoreFormatError` instead of touching your files.
- The archive carries a **format version**. A store written by a newer
  Foreledger raises a clear error rather than being misread.

## 3. Ingest forecast runs

Suppose your pipeline produces a frame like this after every run:

```python
import pandas as pd

predictions = pd.DataFrame({
    "sku":        ["A-100", "A-100", "B-200", "B-200"],
    "date":       pd.to_datetime(["2026-06-02", "2026-06-03"] * 2),
    "yhat":       [105.2, 107.9, 51.0, 49.5],
})
```

Push it into the ledger, telling Foreledger which columns are which and who
produced it:

```python
archive.ingest(
    predictions,
    mapping={"series_id": "sku", "target": "date", "value": "yhat"},
    model_id="prophet",
    model_version="2.1",
    origin="2026-06-01",        # the run date; or map an origin column instead
)
```

Identity is **caller-supplied, never guessed**: `model_id` and
`model_version` are opaque strings of your choosing, and the combination
`(model, version, run date, series)` defines the run. That buys you:

- **Idempotency** — re-running the same ingest (a retried Airflow task, a
  replayed job) is a no-op. State after N identical calls equals one.
- **No collisions** — ingesting `model_version="2.2"` for the same dates
  *adds* rows; versions live side by side for fair comparison.
- **Explicit conflicts** — same identity with *different* values raises
  `IngestConflictError` unless you pass `on_conflict="overwrite"`, which
  supersedes the old run on the record. Nothing is ever silently merged.
- **Atomicity** — a crash mid-ingest leaves the archive exactly as it was.

Using [Nixtla](https://github.com/Nixtla)? Cross-validation frames go through
the same path with the column mapping handled for you:

```python
archive.ingest_nixtla(cv_df, model_id="AutoETS", model_version="1.0")
```

## 4. Register actuals (and revisions)

Actuals are a separate, model-independent log — every model is scored against
the same truth:

```python
actuals = pd.DataFrame({
    "sku":   ["A-100", "A-100", "B-200", "B-200"],
    "date":  pd.to_datetime(["2026-06-02", "2026-06-03"] * 2),
    "value": [101.0, 109.5, 50.2, 48.8],
})

archive.register_actuals(
    actuals,
    mapping={"series_id": "sku", "target": "date"},
    source="warehouse",
)
```

When the numbers get restated, just register again — the log is append-only
and the newest registration becomes the effective "latest" value, with full
history retained:

```python
archive.register_actuals(corrected, mapping={...}, source="warehouse")
```

Three things Foreledger handles that ad-hoc scoring scripts get wrong:

- **Conflicting feeds at the same instant.** If two sources register
  different values with the same timestamp, Foreledger resolves by your
  configured priority — `ForecastArchive(..., source_priority=["finance", "ops"])`
  — or, if it can't, writes the conflict to an error log and excludes the
  target from accuracy (reported as insufficient) instead of guessing.
- **Official numbers.** Mark the value finance signed off on, and it stays
  marked — later registrations can't displace it:

  ```python
  archive.register_actuals(signed_off, mapping={...}, source="finance", official=True)
  # changing it later requires the explicit path:
  archive.mark_official(series="A-100", target="2026-06-02", source="warehouse")
  ```

- **Missing actuals.** Targets with no actual are *reported* as insufficient,
  never silently scored as zero error.

## 5. Ask the questions

### How does accuracy decay with horizon?

```python
result = archive.accuracy_at_horizon(7, metric="MAE",
                                     model_id="prophet", model_version="2.1")
print(result.value, result.n, result.status)

curve = archive.accuracy_curve(metric="MAE",
                               model_id="prophet", model_version="2.1")
print(curve.to_frame())     # one row per horizon
curve.plot()                # if matplotlib is installed
```

Scope any query with `series=`, `period=(start, end)` (a window on run
dates), or leave model unset to pool across models. Metrics: `MAE`, `RMSE`,
`MAPE`, `MASE`, or your own (step 7).

### Score against latest or official truth?

```python
archive.accuracy_at_horizon(7, basis="official", ...)                      # strict
archive.accuracy_at_horizon(7, basis="official", fallback="latest", ...)   # filled + flagged
```

Under `basis="official"`, targets without an official actual count as
insufficient — unless you explicitly opt into `fallback="latest"`, in which
case the result tells you how many values were filled (`result.n_fallback`).

### Which model wins?

```python
archive.set_champion("prophet", "2.1")     # one champion per model_id

table = archive.compare_models(
    7,
    [("prophet", "2.1"), ("prophet", "2.2"), ("AutoETS", "1.0")],
    metric="MAE",
)
print(table)   # value, n, status, champion_version, delta_vs_champion per row
```

Every row is computed over the same scope and provably equals the
corresponding single-model query — comparisons are fair by construction.
`compare_curve([...])` gives the same head-to-head across all horizons.

### What did we know on June 1st?

```python
snapshot = archive.as_of("2026-06-01")
```

Returns every forecast whose run date is on or before that day — and nothing
from later runs. This is the audit answer to "what drove the decision?" and
the honest input to any backtest.

### Don't trust a headline number? Drill into it.

```python
rows = archive.drill({
    "model_id": "prophet", "model_version": "2.1",
    "horizon": 7, "metric": "MAE", "basis": "latest",
})
```

You get the exact forecast/actual pairs behind the cell; recomputing the
metric over them reproduces the summary value to the last bit.

## 6. Inventory and integrity

```python
archive.list_models()    # every (model, version) with row counts and coverage
archive.reconcile()      # assert the precomputed summary == recomputation from raw
```

`reconcile()` should never fail — the summary is rebuilt eagerly on every
write and is fully disposable (delete it and queries fall back to raw,
invisibly). If it ever does fail, that's a bug worth reporting, not a
tolerance to widen.

## 7. Custom metrics

Any callable over aligned `(forecast, actual)` NumPy arrays:

```python
import numpy as np

def pinball_p90(forecast: np.ndarray, actual: np.ndarray) -> float:
    diff = actual - forecast
    return float(np.mean(np.maximum(0.9 * diff, (0.9 - 1) * diff)))

archive.register_metric("pinball90", pinball_p90, summarizable=True)
archive.accuracy_curve(metric="pinball90", model_id="prophet", model_version="2.1")
```

`summarizable=True` precomputes it alongside the built-ins. Registered
metrics run behind an error/timeout guard: one that raises or hangs yields
"insufficient" cells, never a corrupted archive.

## What's on disk

```
forecast_ledger/
├── archive_meta.json     # format version — the compatibility gate
├── forecasts/*.parquet   # the raw ledger, one file per ingested run
├── actuals/*.parquet     # the revisable actuals log
├── officials/*.parquet   # official designations (append-only)
├── summary/              # the disposable accuracy cache
├── runs.json             # run manifest (identity, visibility, idempotency)
├── champions.json        # champion per model_id
└── error_log.txt         # unresolved actuals conflicts (created on demand)
```

Plain files, open formats: anything that reads Parquet — DuckDB, pandas,
Polars, Spark — can read your ledger directly.
