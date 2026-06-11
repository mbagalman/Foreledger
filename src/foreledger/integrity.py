"""Committed-segment integrity registry.

Every referenced segment (forecast — active or superseded history — actuals,
officials) gets a fingerprint — size, mtime_ns, and a sha256 content hash —
recorded at commit time, before the segment becomes visible. Reads verify
size+mtime cheaply on every query; ``reconcile()`` verifies the full content
hash; the recorded hashes are bound into the summary's state token. Raw data
modified or replaced outside the library therefore fails loudly with a typed
error instead of letting the disposable summary stay authoritative over
changed raw.

Boundary: the per-query probe is stat-based, so an adversary who restores a
file's exact size and mtime can evade it until the next ``reconcile()``
hash audit. External modification of committed segments is unsupported; the
registry exists to make it loud, not to make it safe.

The registry is mandatory at format 3 — its absence is corruption.
Fingerprints adopt only during the format migration; recovering after
intentional external surgery means updating the registry entries by hand
(and deleting the summary so it rebuilds), deliberately not a casual
operation.

The registry doubles as the durable commit journal: entries are written
``committed: false`` (staged) before a visibility commit and flipped to
``committed: true`` right after it. That distinction is what lets open-time
maintenance prune the orphans of failed writes while treating a *committed*
fingerprint that the mutable manifests no longer reference as corruption —
editing ``runs.json``/``actuals_manifest.json`` can never silently erase
committed history. (Entries from older format-3 stores lack the flag and are
normalized on first open.)
"""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .jsonstore import atomic_write_json

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
            raise StoreFormatError(
                f"segment integrity registry is missing from {path.parent}; the "
                "archive is corrupt or was modified externally"
            )
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
        atomic_write_json(self.path, {"segments": self.entries})


def record_segments(
    path: Path,
    fingerprint: Callable[[str], dict[str, Any]],
    tokens: Sequence[str],
    *,
    committed: bool = False,
    create_if_missing: bool = False,
) -> None:
    """Journal fingerprints for freshly written segments — called inside the
    store lock, after the segment write and before the visibility commit
    (staged), then flipped via :func:`mark_committed` once the commit
    succeeds. ``create_if_missing`` is reserved for migrations; at format 3 a
    missing registry is corruption, never something a write path may
    recreate."""
    if not tokens:
        return
    if create_if_missing and not path.exists():
        registry = SegmentIntegrity(path=path)
    else:
        registry = SegmentIntegrity.load(path)
    for token in tokens:
        registry.entries[token] = {**fingerprint(token), "committed": committed}
    registry.save()


def mark_committed(path: Path, tokens: Sequence[str]) -> None:
    """Flip staged journal entries to committed, right after the visibility
    commit succeeded (still under the store lock)."""
    if not tokens:
        return
    registry = SegmentIntegrity.load(path)
    for token in tokens:
        if token in registry.entries:
            registry.entries[token]["committed"] = True
    registry.save()


def reconcile_journal(path: Path, referenced: set[str]) -> None:
    """Open-time journal reconciliation (caller holds the store lock).

    Three-way rule per registry entry:
    - referenced by a manifest → it was committed; ensure the flag says so
      (repairs the crash window between a manifest save and the flip);
    - unreferenced and staged → the orphan of a failed write: prune;
    - unreferenced and *committed* → the mutable manifests stopped
      referencing committed history: corruption, never a prune.

    Entries without the flag (older format-3 stores) are normalized:
    referenced ones become committed, unreferenced ones prune as the old
    semantics did.
    """
    from .errors import StoreFormatError

    registry = SegmentIntegrity.load(path)
    changed = False
    for token, record in list(registry.entries.items()):
        flag = record.get("committed")
        if token in referenced:
            if flag is not True:
                record["committed"] = True
                changed = True
        elif flag is True:
            raise StoreFormatError(
                f"visibility metadata no longer references committed segment "
                f"{token!r}; runs.json or actuals_manifest.json was modified externally"
            )
        else:
            del registry.entries[token]
            changed = True
    if changed:
        registry.save()
