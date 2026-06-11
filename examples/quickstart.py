"""Quickstart: ingest two models' forecast runs, register actuals, and render
an accuracy-vs-horizon curve — all on a synthetic fixture, zero config.

Run:  python examples/quickstart.py
"""

from __future__ import annotations

import math
import tempfile
from pathlib import Path

import pandas as pd

from forecast_archive import ForecastArchive

SERIES = [f"store_{i}" for i in range(1, 5)]
ORIGINS = pd.date_range("2026-01-01", periods=30, freq="D")
HORIZONS = list(range(1, 15))


def demand(series: str, day: pd.Timestamp) -> float:
    """The 'true' demand each series follows (weekly seasonality + trend)."""
    base = 100.0 + 25.0 * SERIES.index(series)
    season = 12.0 * math.sin(2 * math.pi * day.dayofweek / 7)
    trend = 0.3 * (day - ORIGINS[0]).days
    return base + season + trend


def forecast_rows(model: str) -> pd.DataFrame:
    """Two synthetic forecasters whose error grows differently with horizon."""
    rows = []
    for origin in ORIGINS:
        for series in SERIES:
            for h in HORIZONS:
                target = origin + pd.Timedelta(days=h)
                truth = demand(series, target)
                if model == "seasonal_naive":
                    error = 2.0 + 0.9 * h + 3.0 * math.sin(h + SERIES.index(series))
                else:  # "drift"
                    error = 1.0 + 1.6 * h
                rows.append(
                    {
                        "series_id": series,
                        "origin": origin,
                        "target": target,
                        "value": truth + error,
                    }
                )
    return pd.DataFrame(rows)


def actual_rows() -> pd.DataFrame:
    targets = sorted({o + pd.Timedelta(days=h) for o in ORIGINS for h in HORIZONS})
    return pd.DataFrame(
        [{"series_id": s, "target": t, "value": demand(s, t)} for s in SERIES for t in targets]
    )


def ascii_chart(label: str, frame: pd.DataFrame, width: int = 48) -> None:
    print(f"\n{label}")
    top = float(frame["value"].max())
    for _, row in frame.iterrows():
        bar = "#" * max(1, round(width * float(row["value"]) / top))
        print(f"  h={int(row['horizon']):>2}  {float(row['value']):8.2f}  |{bar}")


def main() -> None:
    store = Path(tempfile.mkdtemp(prefix="forecast_archive_quickstart_"))
    print(f"archive store: {store}")
    archive = ForecastArchive(store)

    # 1. push each model's runs through the one ingestion path
    for model in ("seasonal_naive", "drift"):
        result = archive.ingest(forecast_rows(model), model_id=model, model_version="v1")
        print(f"ingested {model:>14}: {result.n_runs_written} runs, {result.n_rows} rows")

    # 2. register actuals (model-independent, revisable log)
    archive.register_actuals(actual_rows(), source="demo-feed")

    # 3. accuracy vs. horizon, per model — the core question
    print("\nMAE by horizon (basis=latest):")
    curves = {}
    for model in ("seasonal_naive", "drift"):
        curves[model] = archive.accuracy_curve(metric="MAE", model_id=model, model_version="v1")
        ascii_chart(f"{model} (MAE vs. horizon)", curves[model].to_frame())

    # 4. head-to-head comparison at a fixed horizon, champion-relative
    archive.set_champion("seasonal_naive", "v1")
    comparison = archive.compare_models(
        7, [("seasonal_naive", "v1"), ("drift", "v1")], metric="MAE"
    )
    print("\ncomparison at h=7:")
    print(comparison[["model_id", "model_version", "value", "n", "status"]].to_string(index=False))

    # 5. optional: a real plot if matplotlib is available
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(7, 4))
        for model, curve in curves.items():
            frame = curve.to_frame()
            ax.plot(frame["horizon"], frame["value"], marker="o", label=model)
        ax.set_xlabel("horizon (days)")
        ax.set_ylabel("MAE")
        ax.set_title("Accuracy vs. horizon (synthetic fixture)")
        ax.legend()
        out = store / "accuracy_curve.png"
        fig.savefig(out, dpi=120, bbox_inches="tight")
        print(f"\ncurve image written to {out}")
    except ImportError:
        print("\n(matplotlib not installed; skipped the PNG render)")

    archive.reconcile()
    print("\nsummary reconciles exactly to raw — quickstart complete.")


if __name__ == "__main__":
    main()
