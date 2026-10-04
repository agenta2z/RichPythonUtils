"""Tests for ``iterate_async_in_thread``: iterate an async iterator from sync code,
with its event loop and task owned by a worker thread that is cancelled and joined
when the sync consumer stops."""

import asyncio
import contextvars
import gc
import logging
import threading

import pytest
from rich_python_utils.common_utils.async_utils import (
    _DONE,
    _ThreadedAsyncIteration,
    iterate_async_in_thread,
)

_VAR = contextvars.ContextVar("_iterate_async_in_thread_var", default="unset")
_THREAD_PREFIX = "iterate_async_in_thread["


def _bridge_threads():
    return [t for t in threading.enumerate() if t.name.startswith(_THREAD_PREFIX)]


class _Recorder:
    """An async generator factory that records how its generator ended."""

    def __init__(self, count=3):
        self.count = count
        self.calls = 0
        self.cancelled = False
        self.finalized = False
        self.first_item = threading.Event()

    async def agen(self):
        self.calls += 1
        try:
            for index in range(self.count):
                await asyncio.sleep(0)
                yield index
                self.first_item.set()
            while True:
                await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        finally:
            self.finalized = True


async def _finite(count):
    for index in range(count):
        await asyncio.sleep(0)
        yield index


async def _yield_then_raise():
    yield 1
    await asyncio.sleep(0)
    raise ValueError("boom")


class TestIteration:
    def test_yields_every_item_in_order_then_joins(self):
        assert list(iterate_async_in_thread(lambda: _finite(5))) == [0, 1, 2, 3, 4]
        assert _bridge_threads() == []

    def test_exception_reaches_the_consumer_after_earlier_items(self):
        stream = iterate_async_in_thread(_yield_then_raise)
        assert next(stream) == 1
        with pytest.raises(ValueError, match="boom"):
            next(stream)
        assert _bridge_threads() == []

    def test_thread_is_named_after_the_owner(self):
        seen = []

        async def agen():
            seen.append(threading.current_thread().name)
            yield 1

        assert list(iterate_async_in_thread(agen, owner="Leaf")) == [1]
        assert seen == ["iterate_async_in_thread[Leaf]"]


class TestEarlyStop:
    def test_close_after_first_item_cancels_and_joins(self):
        recorder = _Recorder()
        stream = iterate_async_in_thread(recorder.agen)
        assert next(stream) == 0
        stream.close()
        assert recorder.cancelled and recorder.finalized
        assert _bridge_threads() == []

    def test_close_before_first_item_starts_nothing(self):
        recorder = _Recorder()
        stream = iterate_async_in_thread(recorder.agen)
        stream.close()
        assert recorder.calls == 0
        assert _bridge_threads() == []

    def test_garbage_collection_cancels_and_joins(self):
        recorder = _Recorder()
        stream = iterate_async_in_thread(recorder.agen)
        assert next(stream) == 0
        del stream
        gc.collect()
        assert recorder.cancelled and recorder.finalized
        assert _bridge_threads() == []

    def test_stop_before_the_pump_starts_never_calls_the_factory(self):
        recorder = _Recorder()
        iteration = _ThreadedAsyncIteration(recorder.agen)
        idle = threading.Thread(target=lambda: None)
        idle.start()
        iteration.stop(idle, join_timeout=1.0, owner="Leaf")
        iteration.run()
        assert recorder.calls == 0
        assert iteration.items.get_nowait() == (_DONE, None)

    def test_error_raised_while_cancelling_is_logged_at_warning(self, caplog):
        async def fails_on_cancel():
            try:
                yield 1
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                raise RuntimeError("teardown failed") from None

        stream = iterate_async_in_thread(fails_on_cancel, owner="FlakyLeaf")
        assert next(stream) == 1
        with caplog.at_level(logging.WARNING):
            stream.close()
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert "FlakyLeaf" in warnings[0].getMessage()
        assert isinstance(warnings[0].exc_info[1], RuntimeError)
        assert _bridge_threads() == []

    def test_iterator_ignoring_cancellation_is_logged_at_error(self, caplog):
        release = threading.Event()

        async def stubborn():
            yield 1
            while not release.is_set():
                try:
                    await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    pass

        stream = iterate_async_in_thread(
            stubborn, owner="StubbornLeaf", join_timeout=0.2
        )
        assert next(stream) == 1
        with caplog.at_level(logging.ERROR):
            stream.close()
        try:
            errors = [r for r in caplog.records if r.levelno == logging.ERROR]
            assert len(errors) == 1
            assert "StubbornLeaf" in errors[0].getMessage()
            assert "ignored cancellation" in errors[0].getMessage()
        finally:
            release.set()
            for thread in _bridge_threads():
                thread.join(5.0)
        assert _bridge_threads() == []


class TestContext:
    def test_sees_the_context_of_the_first_next_and_writes_stay_isolated(self):
        seen = []

        async def agen():
            seen.append(_VAR.get())
            _VAR.set("agen")
            yield _VAR.get()

        stream = iterate_async_in_thread(agen)
        token = _VAR.set("first-next")
        try:
            assert next(stream) == "agen"
            assert _VAR.get() == "first-next"
        finally:
            _VAR.reset(token)
        assert list(stream) == []
        assert seen == ["first-next"]

    @pytest.mark.asyncio
    async def test_works_from_sync_code_inside_a_running_loop(self):
        caller_loop = asyncio.get_running_loop()
        loops = []

        async def agen():
            loops.append(asyncio.get_running_loop())
            yield 1
            yield 2

        assert list(iterate_async_in_thread(agen)) == [1, 2]
        assert loops and loops[0] is not caller_loop
        assert _bridge_threads() == []
