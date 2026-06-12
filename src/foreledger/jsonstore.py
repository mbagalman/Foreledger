"""Tiny shared utilities for the archive's JSON metadata files.

Every metadata file (manifests, integrity journal, champions, conflict
markers, format metadata) follows the same two rules: writes go to a temp
file and land via atomic rename, and cross-handle freshness is detected with
a cheap (mtime_ns, size) key. This module is the single place those rules
live.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any


def json_digest(payload: Any, indent: int | None = 1) -> str:
    """sha256 over exactly the bytes :func:`atomic_write_json` would write
    for ``payload`` — so a digest can be computed *before* the file lands
    and later compared against :func:`file_digest` of the file itself."""
    return hashlib.sha256(json.dumps(payload, indent=indent).encode("utf-8")).hexdigest()


def file_digest(path: Path) -> str:
    """sha256 of a file's current bytes."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def atomic_write_json(path: Path, payload: Any, indent: int | None = 1) -> None:
    """Write ``payload`` as JSON via temp-file + atomic rename.

    ``path`` must end in ``.json``; the temp file lands beside it as
    ``<name>.json.tmp``. A reader can therefore never observe a partial
    file — it sees the old content or the new, nothing in between.
    """
    tmp = path.with_suffix(".json.tmp")
    # newline pinned: the file's bytes must equal json.dumps(...).encode()
    # exactly (no platform CRLF translation), so json_digest(payload) of a
    # candidate manifest always matches file_digest() of the saved file
    tmp.write_text(json.dumps(payload, indent=indent), encoding="utf-8", newline="\n")
    # On Windows, replacing a file a lock-free reader momentarily holds open
    # raises PermissionError; reads are milliseconds, so a brief bounded
    # retry absorbs the race instead of failing a writer's commit spuriously.
    for attempt in range(5):
        try:
            os.replace(tmp, path)
            return
        except PermissionError:  # pragma: no cover - timing-dependent, Windows
            if attempt == 4:
                raise
            time.sleep(0.01 * (attempt + 1))


def file_key(path: Path) -> tuple[int, int] | None:
    """A cheap change marker (mtime_ns, size) for a metadata file, or None
    when the file does not exist."""
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    return (stat.st_mtime_ns, stat.st_size)
