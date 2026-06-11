"""Smoke test: the package imports and reports a version."""

import forecast_archive


def test_package_imports() -> None:
    assert forecast_archive.__version__
