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

The journal also binds the *content* of the visibility manifests, not just
their segment references: each commit records the manifest file's expected
sha256 as ``pending`` before the manifest save (:func:`stage_commit`) and
promotes it to ``current`` right after (:func:`confirm_commit`). A manifest
whose bytes match neither digest was edited outside a commit — selective
record removal, a flipped ``superseded`` flag, a swapped run_id — and is
corruption, while the two crash windows (before the save; between save and
confirm) remain deterministically healable by :func:`reconcile_journal`.
(Older format-3 stores lack the digests and adopt them on first open.)
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
    """Mapping of committed segment token -> recorded fingerprint, plus the
    expected content digest per visibility manifest (``current``/``pending``)."""

    path: Path
    entries: dict[str, dict[str, Any]] = field(default_factory=dict)
    manifests: dict[str, dict[str, Any]] = field(default_factory=dict)

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
            # absent on pre-binding format-3 stores; adopted on first open
            manifests = payload.get("manifests", {})
            if not isinstance(manifests, dict) or not all(
                isinstance(slot, dict) for slot in manifests.values()
            ):
                raise TypeError("malformed manifests mapping")
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            raise StoreFormatError(
                f"segment integrity registry at {path} is unreadable or corrupt"
            ) from exc
        return cls(path=path, entries=entries, manifests=manifests)

    def save(self) -> None:
        atomic_write_json(self.path, {"segments": self.entries, "manifests": self.manifests})

    def manifest_slot(self, name: str) -> dict[str, Any]:
        return self.manifests.setdefault(name, {"current": None, "pending": None})


def stage_commit(
    path: Path,
    fingerprint: Callable[[str], dict[str, Any]],
    tokens: Sequence[str],
    manifest_name: str,
    manifest_digest: str,
    *,
    create_if_missing: bool = False,
) -> None:
    """Phase one of a visibility commit, in a single journal write: record
    staged (``committed: false``) fingerprints for the freshly written
    segments, plus the content digest the manifest file will have once the
    commit lands (``pending``). Called inside the store lock, after the
    segment write and before the manifest save. ``create_if_missing`` is
    reserved for migrations; at format 3 a missing registry is corruption,
    never something a write path may recreate."""
    if create_if_missing and not path.exists():
        registry = SegmentIntegrity(path=path)
    else:
        registry = SegmentIntegrity.load(path)
    for token in tokens:
        registry.entries[token] = {**fingerprint(token), "committed": False}
    registry.manifest_slot(manifest_name)["pending"] = manifest_digest
    registry.save()


def confirm_commit(
    path: Path,
    tokens: Sequence[str],
    manifest_name: str,
    manifest_digest: str,
) -> None:
    """Phase two, right after the manifest save succeeded (still under the
    store lock): flip the staged entries to committed and promote the
    manifest digest from pending to current, in one journal write.

    A token staged moments ago that is now absent from the registry is real
    corruption and raises — silently skipping it would leave committed data
    without integrity evidence.
    """
    from .errors import StoreFormatError

    registry = SegmentIntegrity.load(path)
    for token in tokens:
        record = registry.entries.get(token)
        if record is None:
            raise StoreFormatError(
                f"segment {token!r} was staged in the integrity journal but its "
                "entry is gone; the registry was modified during the commit"
            )
        record["committed"] = True
    slot = registry.manifest_slot(manifest_name)
    slot["current"] = manifest_digest
    slot["pending"] = None
    registry.save()


def reconcile_journal(path: Path, referenced: set[str], manifest_digests: dict[str, str]) -> None:
    """Journal reconciliation at open and before every locked write (caller
    holds the store lock).

    Manifest digest rule per visibility manifest (``manifest_digests`` maps
    manifest name -> sha256 of the file's bytes right now):
    - matches ``current`` → no commit in flight; clear a leftover ``pending``
      (a commit that failed before its manifest save);
    - matches ``pending`` → the crash window between the manifest save and
      the confirm: promote pending to current (the commit completes);
    - no recorded digests (pre-binding format-3 store) → adopt;
    - matches neither → the manifest was edited outside a commit (records
      removed, ``superseded`` flipped, identities rewritten): corruption.

    Three-way rule per segment entry:
    - referenced by a manifest → it was committed; ensure the flag says so
      (the same save-to-confirm crash window);
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
    for name, digest_now in manifest_digests.items():
        slot = registry.manifests.get(name)
        if slot is None or (slot.get("current") is None and slot.get("pending") is None):
            registry.manifests[name] = {"current": digest_now, "pending": None}
            changed = True
        elif digest_now == slot.get("current"):
            if slot.get("pending") is not None:
                slot["pending"] = None
                changed = True
        elif digest_now == slot.get("pending"):
            slot["current"] = digest_now
            slot["pending"] = None
            changed = True
        else:
            raise StoreFormatError(
                f"{name} does not match its recorded content digest; visibility "
                "metadata was modified outside a commit"
            )
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
