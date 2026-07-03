"""Cross-process store lock for read-modify-replace metadata updates.

The run manifest, champions file, and conflict bookkeeping are all updated by
reading, modifying, and atomically replacing a file. Without serialization,
two archive handles (same process or different processes) could each commit
from a stale snapshot and silently erase the other's entries. Every such
update happens while holding this lock, and re-reads the file inside it.

The lock is an OS-level byte lock on a dedicated file, so it works across
processes and between two handles in one process. It is NOT reentrant — the
acquisition sites in ``ForecastArchive`` (enumerated on :meth:`_lock`) are
mutually exclusive by construction, which is why post-commit summary refreshes
run only after the write releases the lock. Because a fresh :class:`StoreLock`
is minted per acquisition, a same-thread nested acquire would otherwise block
on its own byte lock until the full timeout; a thread-local held-path guard
turns that latent deadlock into an immediate, self-describing error instead.
"""

from __future__ import annotations

import os
import sys
import threading
import time
from pathlib import Path
from types import TracebackType

from .errors import StoreLockTimeout

if sys.platform == "win32":
    import msvcrt
else:
    import fcntl

_POLL_INTERVAL = 0.05

#: Lock-file paths currently held by the calling thread. A nested acquire of a
#: path already in this set is a non-reentrancy bug, not something to wait on.
_held = threading.local()


def _held_paths() -> set[str]:
    paths: set[str] | None = getattr(_held, "paths", None)
    if paths is None:
        paths = set()
        _held.paths = paths
    return paths


class StoreLock:
    """Exclusive, blocking-with-timeout lock on ``<store>/.foreledger.lock``."""

    def __init__(self, path: Path, timeout: float = 30.0) -> None:
        self._path = path
        self._timeout = timeout
        self._fd: int | None = None
        self._key = os.path.normcase(os.path.abspath(path))

    def __enter__(self) -> StoreLock:
        held = _held_paths()
        if self._key in held:
            raise RuntimeError(
                f"the store lock at {self._path} is already held by this thread; "
                "StoreLock is not reentrant — a locked section must not re-acquire "
                "it (this is a caller bug, not lock contention)"
            )
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self._path, os.O_RDWR | os.O_CREAT)
        deadline = time.monotonic() + self._timeout
        while True:
            try:
                if sys.platform == "win32":
                    os.lseek(fd, 0, os.SEEK_SET)
                    msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                else:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                self._fd = fd
                held.add(self._key)
                return self
            except OSError:
                if time.monotonic() >= deadline:
                    os.close(fd)
                    raise StoreLockTimeout(
                        f"could not acquire the store lock at {self._path} within "
                        f"{self._timeout:.0f}s; another writer may be stuck"
                    ) from None
                time.sleep(_POLL_INTERVAL)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._fd is None:
            return
        try:
            if sys.platform == "win32":
                os.lseek(self._fd, 0, os.SEEK_SET)
                msvcrt.locking(self._fd, msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)
            self._fd = None
            _held_paths().discard(self._key)
