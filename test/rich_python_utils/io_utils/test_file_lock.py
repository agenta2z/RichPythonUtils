"""``FileLock``: an exclusive advisory lock that excludes other handles, other
threads and other processes, and dies with its process."""

import os
import subprocess
import sys
import threading
import time

from rich_python_utils.io_utils.file_lock import FileLock

HOLD_AND_REPORT = """
import sys, time
sys.path[:0] = {path!r}
from rich_python_utils.io_utils.file_lock import FileLock
lock = FileLock({lock!r})
assert lock.acquire(timeout=0)
print("held", flush=True)
time.sleep(30)
"""


def test_a_second_handle_cannot_take_a_held_lock(tmp_path):
    path = str(tmp_path / "x.lock")
    first, second = FileLock(path), FileLock(path)
    assert first.acquire(timeout=0) and first.held
    assert not second.acquire(timeout=0) and not second.held
    first.release()
    assert second.acquire(timeout=0)
    second.release()


def test_acquire_is_idempotent_and_release_is_safe(tmp_path):
    lock = FileLock(str(tmp_path / "x.lock"))
    lock.release()
    assert lock.acquire(timeout=0) and lock.acquire(timeout=0)
    lock.release()
    lock.release()
    assert not lock.held


def test_a_timed_acquire_waits_for_the_holder(tmp_path):
    path = str(tmp_path / "x.lock")
    holder = FileLock(path)
    holder.acquire()
    threading.Timer(0.2, holder.release).start()
    start = time.monotonic()
    waiter = FileLock(path)
    assert waiter.acquire(timeout=5)
    assert time.monotonic() - start >= 0.15
    waiter.release()


def test_a_timed_acquire_gives_up(tmp_path):
    path = str(tmp_path / "x.lock")
    with FileLock(path):
        start = time.monotonic()
        assert not FileLock(path).acquire(timeout=0.1)
        assert time.monotonic() - start >= 0.1


def test_threads_of_one_process_exclude_each_other(tmp_path):
    path = str(tmp_path / "x.lock")
    locks = [FileLock(path) for _ in range(4)]
    results = []
    barrier = threading.Barrier(4)

    def contend(lock):
        barrier.wait()
        results.append(lock.acquire(timeout=0))

    threads = [threading.Thread(target=contend, args=(lock,)) for lock in locks]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(results) == [False, False, False, True]
    for lock in locks:
        lock.release()


def test_another_process_holding_it_excludes_us_until_it_dies(tmp_path):
    path = str(tmp_path / "x.lock")
    script = HOLD_AND_REPORT.format(path=sys.path, lock=path)
    holder = subprocess.Popen(
        [sys.executable, "-c", script], stdout=subprocess.PIPE, text=True
    )
    try:
        assert holder.stdout.readline().strip() == "held"
        assert not FileLock(path).acquire(timeout=0)
    finally:
        holder.kill()
        holder.wait()
    assert FileLock(path).acquire(timeout=1)


def test_the_lock_file_is_created(tmp_path):
    path = tmp_path / "sub.lock"
    with FileLock(str(path)):
        assert os.path.exists(path)
