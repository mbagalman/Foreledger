"""Committed-segment integrity registry.

Every committed segment (forecast, actuals, officials) gets a fingerprint —
size, mtime_ns, and a sha256 content hash — recorded at commit time, before
the segment becomes visible. Reads verify size+mtime cheaply on every query;
``reconcile()`` verifies the full content hash. Raw data modified or replaced
outside the library therefore fails loudly with a typed error instead of
letting the disposable summary stay authoritative over changed raw.

Deleting ``segment_integrity.json`` and reopening re-fingerprints the current
content as authoritative — the explicit recovery path after intentional
external surgery.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_REQUIRED_KEYS = {"size", "mtime_ns", "sha256"}


@dataclass
class SegmentIntegrity:
    """Mapping of committed segment token -> recorded fingerprint."""

    path: Path
    entries: dict[str, dict[str, Any]] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path) -> SegmentIntegrity:
        from .errors import StoreFormatError

        if not path.exists():
            # absent registry = adopt-current-content at open (upgrade path
            # from stores written before integrity tracking existed)
            return cls(path=path)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            entries = payload["segments"]
            if not isinstance(entries, dict) or not all(
                isinstance(record, dict) and set(record) >= _REQUIRED_KEYS
                for record in entries.values()
            ):
                raise TypeError("malformed segments mapping")
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            raise StoreFormatError(
                f"segment integrity registry at {path} is unreadable or corrupt"
            ) from exc
        return cls(path=path, entries=entries)

    def save(self) -> None:
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps({"segments": self.entries}, indent=1), encoding="utf-8")
        os.replace(tmp, self.path)
