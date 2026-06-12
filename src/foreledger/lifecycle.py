"""Store lifecycle: the format gate, initialization, and migrations.

Everything here runs at open time, under the store lock, before the archive
serves a single row: refuse directories that are not ours, gate the format
version, lay down the mandatory metadata for new stores, and migrate older
formats (1 → 2 adopts actuals visibility; → 3 adopts integrity fingerprints
and makes all metadata mandatory). The version marker is always written last,
so an interrupted initialization or migration simply reruns.
"""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import pandas as pd

from .actuals import ActualsManifest
from .backend import Backend, ForecastFilter
from .errors import StoreFormatError
from .ingestion import (
    RunManifest,
    RunRecord,
    content_hash,
    validate_forecast_segment_token,
)
from .integrity import SegmentIntegrity
from .jsonstore import atomic_write_json, file_digest
from .schema import FORMAT_VERSION

logger = logging.getLogger("foreledger.lifecycle")

META_FILE = "archive_meta.json"

#: Files a crashed/concurrent initializer may legitimately leave mid-init.
#: Anything else — including arbitrary ``*.tmp`` files — is user content.
_INIT_PLUMBING = frozenset(
    {
        "archive_meta.json.tmp",
        "actuals_manifest.json",
        "actuals_manifest.json.tmp",
        "runs.json",
        "runs.json.tmp",
        "segment_integrity.json",
        "segment_integrity.json.tmp",
    }
)


def foreign_entries(store: Path) -> bool:
    """True when the store directory holds anything that is not ours.

    Initialization plumbing is trusted only alongside the lock file: a
    genuine concurrent initializer always creates the lock first, so a lone
    ``archive_meta.json.tmp`` is user content, not ours.
    """
    names = {entry.name for entry in store.iterdir()}
    allowed = {".foreledger.lock"}
    if ".foreledger.lock" in names:
        allowed.update(_INIT_PLUMBING)
    return bool(names - allowed)


def refuse_non_archive_dir(store: Path) -> None:
    """Refuse to treat an existing non-archive directory as a store — the
    library never silently initializes over a user's contents."""
    if not store.exists():
        return
    if not store.is_dir():
        raise StoreFormatError(f"store path {store} is not a directory")
    if foreign_entries(store) and not (store / META_FILE).exists():
        raise StoreFormatError(
            f"{store} is not empty and has no archive metadata; refusing "
            "to initialize over existing contents"
        )


def stored_format_version(store: Path) -> int | None:
    """The declared format version, or None for a store with no metadata yet.

    Strictly an integer: corrupt metadata or a non-integer version (bool,
    float, string) raises :class:`StoreFormatError` — the gate never guesses.
    """
    meta_path = store / META_FILE
    if not meta_path.exists():
        return None
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        raw = meta["format_version"]
    except (KeyError, json.JSONDecodeError) as exc:
        raise StoreFormatError(f"archive metadata at {meta_path} is unreadable or corrupt") from exc
    # strict: booleans are ints in Python, "1" is not a version
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise StoreFormatError(
            f"archive metadata at {meta_path} declares a non-integer format "
            f"version {raw!r}; the store is corrupt"
        )
    return int(raw)


def write_format_version(store: Path, version: int) -> None:
    """Stamp the format version into the store metadata (atomically),
    preserving other fields; first write also stamps ``created_at``."""
    meta_path = store / META_FILE
    payload: dict[str, Any] = {"format_version": version}
    if meta_path.exists():
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
        payload["format_version"] = version
    else:
        payload["created_at"] = pd.Timestamp.now().isoformat()
    atomic_write_json(meta_path, payload)


def check_or_init_store(
    store: Path,
    manifest_path: Path,
    actuals_manifest_path: Path,
    integrity_path: Path,
) -> int:
    """Gate the format version; initialize a new store at the current one.

    Returns the stored format version (``FORMAT_VERSION`` for a freshly
    initialized store); versions below it are migrated later, once the
    backend exists. Caller holds the store lock.
    """
    refuse_non_archive_dir(store)
    stored = stored_format_version(store)
    if stored is not None:
        if stored == FORMAT_VERSION:
            return stored
        if stored > FORMAT_VERSION:
            raise StoreFormatError(
                f"archive format version {stored} is newer than this library "
                f"supports ({FORMAT_VERSION}); upgrade foreledger to open this store"
            )
        if stored in (1, 2):
            return stored
        # only versions 1 and 2 have defined migrations; anything else older
        # is an unknown layout we must not reinterpret or rewrite
        raise StoreFormatError(
            f"archive format version {stored} is not supported; no migration is defined for it"
        )
    store.mkdir(parents=True, exist_ok=True)
    # Mandatory metadata before the version marker: a crash in between leaves
    # no metadata file, so the next constructor simply finishes the
    # initialization. A healthy format-3 store therefore always has all
    # three — an absence later is corruption, never emptiness.
    if not actuals_manifest_path.exists():
        ActualsManifest(path=actuals_manifest_path).save()
    if not manifest_path.exists():
        RunManifest(path=manifest_path).save()
    if not integrity_path.exists():
        # bind the just-created manifests' content into the journal from the
        # very first moment of the store's life
        SegmentIntegrity(
            path=integrity_path,
            manifests={
                "runs.json": {"current": file_digest(manifest_path), "pending": None},
                "actuals_manifest.json": {
                    "current": file_digest(actuals_manifest_path),
                    "pending": None,
                },
            },
        ).save()
    write_format_version(store, FORMAT_VERSION)
    return FORMAT_VERSION


def migrate_v1_actuals_visibility(backend: Backend, actuals_manifest_path: Path) -> None:
    """Format-1 → 2 step: adopt manifest-committed actuals visibility.

    Format 1 had directory-scan visibility: adopt every actuals segment (they
    were all visible), and adopt an officials segment only if its
    designations dereference registered actuals — a dangling officials file
    is the leftover of a failed pre-manifest call and must stay inert. Caller
    holds the store lock; the version bump happens at the end of the full
    migration chain.
    """
    if actuals_manifest_path.exists():
        return
    actuals_segments, officials_segments = backend.list_segments()
    actuals = backend.read_actuals(actuals_segments)
    identity = ["series_id", "target", "source", "actual_recorded_at"]
    known = actuals[identity].drop_duplicates()
    adopted_officials = []
    for segment in officials_segments:
        rows = backend.read_officials([segment])
        live = rows.merge(known, on=identity, how="left", indicator=True)
        if not rows.empty and (live["_merge"] == "both").all():
            adopted_officials.append(segment)
    ActualsManifest(
        path=actuals_manifest_path,
        actuals=actuals_segments,
        officials=adopted_officials,
    ).save()
    logger.info(
        "migrated format-1 actuals store: %d actuals segment(s), %d of %d "
        "officials segment(s) adopted",
        len(actuals_segments),
        len(adopted_officials),
        len(officials_segments),
    )


def migrate_to_v3(
    store: Path,
    backend: Backend,
    integrity_path: Path,
    referenced_tokens: Sequence[str],
    manifest_digests: dict[str, str],
) -> None:
    """Final migration step: adopt integrity fingerprints and bump.

    Every referenced segment — including superseded history, which the
    append-only promise still protects — gets a fingerprint of its current
    content, and the visibility manifests' content digests are recorded so
    later selective edits to them are detectable. Adoption is migration-only:
    once at format 3, a missing registry or fingerprint is corruption, never
    an implicit authorization to trust whatever bytes are present. The
    version is bumped last so an interrupted migration simply reruns. Caller
    holds the store lock.
    """
    registry = (
        SegmentIntegrity.load(integrity_path)
        if integrity_path.exists()
        else SegmentIntegrity(path=integrity_path)
    )
    tokens = list(referenced_tokens)
    adopted = 0
    for token in tokens:
        if token not in registry.entries:
            registry.entries[token] = {**backend.fingerprint_segment(token), "committed": True}
            adopted += 1
        else:
            registry.entries[token]["committed"] = True
    for stale in set(registry.entries) - set(tokens):
        del registry.entries[stale]
    registry.manifests = {
        name: {"current": digest, "pending": None} for name, digest in manifest_digests.items()
    }
    registry.save()
    write_format_version(store, FORMAT_VERSION)
    logger.info(
        "archive migrated to format version %d (%d fingerprint(s) adopted)",
        FORMAT_VERSION,
        adopted,
    )


def migrate_legacy_run_manifest(
    backend: Backend,
    manifest_path: Path,
    entries: list[dict[str, Any]],
    stage_integrity: Callable[[list[str], dict[str, Any]], None],
    confirm_integrity: Callable[[list[str], dict[str, Any]], None],
) -> RunManifest:
    """Deterministically migrate per-series-set run records (pre-57930cc)
    to per-series records.

    Each active legacy run's rows are re-tagged with per-series run_ids in a
    new segment; the legacy segment files stay on disk (never deleted) but
    are no longer referenced. Crash-safe via the same staged-then-confirmed
    journal protocol as normal ingestion: the replacement segment is staged
    before the manifest save and confirmed after, so an interruption at any
    point leaves either the untouched legacy manifest plus a prunable staged
    orphan, or a completed commit — a rerun is always clean. Caller holds
    the store lock.
    """
    legacy = [e for e in entries if "series_key" in e]
    modern = [e for e in entries if "series_key" not in e]
    required = ("run_id", "model_id", "model_version", "origin", "ingested_at", "segment")
    for entry in legacy:
        missing = [key for key in required if key not in entry]
        if missing:
            raise StoreFormatError(
                f"legacy run manifest record is missing fields {missing}; the "
                "manifest may be corrupt or written by an incompatible version"
            )
        # same canonical-token rule as modern records, BEFORE any backend
        # access: a tampered legacy manifest must not be able to make the
        # migration read Parquet files outside the archive
        validate_forecast_segment_token(str(entry["segment"]), manifest_path)
    records = RunManifest.from_entries(manifest_path, modern).runs

    migrated: list[RunRecord] = []
    tagged_frames: list[pd.DataFrame] = []
    for entry in legacy:
        if entry.get("superseded"):
            continue  # invisible before the migration, invisible after
        rows = backend.read_forecasts(
            ForecastFilter(
                active_run_ids=[str(entry["run_id"])],
                segments=[str(entry["segment"])],
            )
        )
        for series_id, group in rows.groupby("series_id", sort=True):
            run_id = uuid.uuid4().hex
            frame = group.copy()
            frame["run_id"] = run_id
            tagged_frames.append(frame)
            migrated.append(
                RunRecord(
                    run_id=run_id,
                    model_id=str(entry["model_id"]),
                    model_version=str(entry["model_version"]),
                    origin=str(entry["origin"]),
                    series_id=str(series_id),
                    content_hash=content_hash(group),
                    segment="",
                    ingested_at=str(entry["ingested_at"]),
                )
            )
    staged: list[str] = []
    if tagged_frames:
        segment = backend.write_forecast_segment(pd.concat(tagged_frames, ignore_index=True))
        for record in migrated:
            record.segment = segment
        staged.append(segment)

    manifest = RunManifest(path=manifest_path, runs=[*records, *migrated])
    stage_integrity(staged, manifest.payload())
    manifest.save()
    confirm_integrity(staged, manifest.payload())
    logger.info(
        "migrated %d legacy run record(s) to %d per-series record(s)",
        len(legacy),
        len(migrated),
    )
    return manifest
