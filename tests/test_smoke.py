"""Smoke test: the package imports and reports a version."""

import foreledger


def test_package_imports() -> None:
    assert foreledger.__version__
