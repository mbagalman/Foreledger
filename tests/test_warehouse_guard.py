"""Warehouse-readiness CI guard (amended ADR-002).

Asserts that no module outside the backend package imports the engine or
embeds engine-only idioms: the eval/summary/ingestion layers must speak only
through the dialect-aware seam, keeping a warehouse backend additive.
"""

from __future__ import annotations

import ast
from pathlib import Path

import foreledger

ENGINE_MODULES = {"duckdb", "pyarrow"}
ENGINE_IDIOMS = ("read_parquet", "parquet_scan", "pragma ", ".df(")


def _docstring_nodes(tree: ast.Module) -> set[int]:
    """ids of Constant nodes that are docstrings (allowed to mention engines)."""
    ids: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = node.body
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                ids.add(id(body[0].value))
    return ids


def package_modules_outside_backend() -> list[Path]:
    package_root = Path(foreledger.__file__).parent
    return [
        path
        for path in sorted(package_root.rglob("*.py"))
        if "backend" not in path.relative_to(package_root).parts
    ]


def test_no_engine_imports_outside_backend() -> None:
    offenders: list[str] = []
    for path in package_modules_outside_backend():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name.split(".")[0] for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [(node.module or "").split(".")[0]]
            else:
                continue
            for name in names:
                if name in ENGINE_MODULES:
                    offenders.append(f"{path.name}: imports {name}")
    assert not offenders, f"engine leaked past the backend seam: {offenders}"


def test_no_engine_idioms_outside_backend() -> None:
    offenders: list[str] = []
    for path in package_modules_outside_backend():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        allowed = _docstring_nodes(tree)
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and id(node) not in allowed
            ):
                lowered = node.value.lower()
                for idiom in ENGINE_IDIOMS:
                    if idiom in lowered:
                        offenders.append(f"{path.name}:{node.lineno}: {idiom!r}")
            if isinstance(node, ast.Attribute) and node.attr in ("df",):
                offenders.append(f"{path.name}:{node.lineno}: .df() result conversion")
    assert not offenders, f"engine-only SQL idiom outside the backend seam: {offenders}"
