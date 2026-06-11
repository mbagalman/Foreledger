"""Tiny shared utilities for the archive's JSON metadata files.

Every metadata file (manifests, integrity journal, champions, conflict
markers, format metadata) follows the same two rules: writes go to a temp
file and land via atomic rename, and cross-handle freshness is detected with
a cheap (mtime_ns, size) key. This module is the single place those rules
live.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def atomic_write_json(path: Path, payload: Any, indent: int | None = 1) -> None:
    """Write ``payload`` as JSON via temp-file + atomic rename.

    ``path`` must end in ``.json``; the temp file is ``<path>.tmp`` (i.e.
    ``*.json.tmp``), which the store-initialization plumbing knows to ignore.
    """
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=indent), encoding="utf-8")
    os.replace(tmp, path)


def file_key(path: Path) -> tuple[int, int] | None:
    """A cheap change marker (mtime_ns, size) for a metadata file, or None
    when the file does not exist."""
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    return (stat.st_mtime_ns, stat.st_size)
