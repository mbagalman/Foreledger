"""Quickstart: ingest forecasts, register actuals, plot accuracy vs. horizon.

Phase 5 done-criterion: this script must render an accuracy-vs-horizon curve
on the synthetic fixture (see AGENTS.md "Commands"). It will be filled in as
the ingestion (Phase 2) and evaluation (Phase 4) APIs land.
"""

import forecast_archive


def main() -> None:
    print(f"forecast-archive {forecast_archive.__version__}")
    print("Quickstart pending: ingestion and evaluation APIs land in Phases 2-4.")


if __name__ == "__main__":
    main()
