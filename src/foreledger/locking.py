"""Cross-process store lock for read-modify-replace metadata updates.

The run manifest, champions file, and conflict bookkeeping are all updated by
reading, modifying, and atomically replacing a file. Without serialization,
two archive handles (same process or different processes) could each commit
from a stale snapshot and silently erase the other's entries. Every such
update happens while holding this lock, and re-reads the file inside it.

The lock is an OS-level byte lock on a dedicated file, so it works across
processes and between two handles in one process. It is not reentrant:
acquisition sites in ``ForecastArchive`` (constructor init/migrations, the
public write commits, summary publication, the snapshot capture fallback)
are mutually exclusive by construction — a helper that needs the lock is
never called while it is held, which is why post-commit summary refreshes
run only after the write releases it.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from types import TracebackType

from .errors import StoreLockTimeout

if sys.platform == "win32":
    import msvcrt
else:
    import fcntl

_POLL_INTERVAL = 0.05


class StoreLock:
    """Exclusive, blocking-with-timeout lock on ``<store>/.foreledger.lock``."""

    def __init__(self, path: Path, timeout: float = 30.0) -> None:
        self._path = path
        self._timeout = timeout
        self._fd: int | None = None

    def __enter__(self) -> StoreLock:
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
