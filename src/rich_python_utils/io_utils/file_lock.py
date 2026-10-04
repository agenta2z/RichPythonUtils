"""An exclusive advisory lock on a file, held through an open handle.

POSIX uses ``fcntl.flock``, Windows ``msvcrt.locking``. A ``flock`` lock belongs to the
open file description, so two ``FileLock`` objects on one path exclude each other even
inside one process (two threads, two owners), and the operating system drops the lock
when the holding process dies.
"""

import sys
import time
from typing import IO, Optional

if sys.platform == "win32":
    import msvcrt
else:
    import fcntl


class FileLock:
    """An exclusive advisory lock on ``path`` (created if missing).

    ``acquire(timeout=0)`` tries once, ``acquire(timeout=t)`` waits up to ``t``
    seconds, and ``acquire()`` waits until the lock is free. ``release`` unlocks and
    closes the handle; it is a no-op when the lock is not held.
    """

    def __init__(self, path: str) -> None:
        self.path = path
        self._handle: Optional[IO[str]] = None

    @property
    def held(self) -> bool:
        return self._handle is not None

    def acquire(
        self, timeout: Optional[float] = None, poll_interval: float = 0.01
    ) -> bool:
        """Take the lock; return whether it is held."""
        if self._handle is not None:
            return True
        handle = open(self.path, "a")
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            try:
                _lock(handle)
            except OSError:
                if deadline is not None and time.monotonic() >= deadline:
                    handle.close()
                    return False
                time.sleep(poll_interval)
                continue
            self._handle = handle
            return True

    def release(self) -> None:
        handle, self._handle = self._handle, None
        if handle is None:
            return
        try:
            _unlock(handle)
        except OSError:
            pass
        finally:
            handle.close()

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(self, *exc_info) -> None:
        self.release()


def _lock(handle: IO[str]) -> None:
    if sys.platform == "win32":
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock(handle: IO[str]) -> None:
    if sys.platform == "win32":
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


__all__ = ["FileLock"]
