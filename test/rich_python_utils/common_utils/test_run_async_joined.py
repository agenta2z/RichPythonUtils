"""Tests for ``run_async_joined``: run a coroutine from sync code, with or without
a running event loop, keeping the caller's ContextVars visible to it."""

import asyncio
import contextvars
import threading

import pytest
from rich_python_utils.common_utils.async_utils import run_async_joined

_VAR = contextvars.ContextVar("_run_async_joined_var", default="unset")


async def _read_var():
    await asyncio.sleep(0)
    return _VAR.get()


async def _write_var():
    _VAR.set("coroutine")
    await asyncio.sleep(0)
    return _VAR.get()


async def _raise():
    await asyncio.sleep(0)
    raise ValueError("boom")


async def _where():
    return threading.get_ident(), asyncio.get_running_loop()


def _read_under(value):
    token = _VAR.set(value)
    try:
        return run_async_joined(_read_var())
    finally:
        _VAR.reset(token)


def _write_under(value):
    token = _VAR.set(value)
    try:
        return run_async_joined(_write_var()), _VAR.get()
    finally:
        _VAR.reset(token)


class TestNoRunningLoop:
    def test_returns_result(self):
        assert run_async_joined(asyncio.sleep(0, result=7)) == 7

    def test_caller_context_var_visible(self):
        assert _read_under("caller") == "caller"

    def test_coroutine_writes_stay_in_its_context(self):
        assert _write_under("caller") == ("coroutine", "caller")

    def test_exception_propagates(self):
        with pytest.raises(ValueError, match="boom") as info:
            run_async_joined(_raise())
        assert info.value.__context__ is None


class TestInsideRunningLoop:
    @pytest.mark.asyncio
    async def test_returns_result(self):
        assert run_async_joined(asyncio.sleep(0, result=7)) == 7

    @pytest.mark.asyncio
    async def test_caller_context_var_visible(self):
        assert _read_under("caller") == "caller"

    @pytest.mark.asyncio
    async def test_coroutine_writes_stay_in_its_context(self):
        assert _write_under("caller") == ("coroutine", "caller")

    @pytest.mark.asyncio
    async def test_exception_propagates(self):
        with pytest.raises(ValueError, match="boom") as info:
            run_async_joined(_raise())
        assert info.value.__context__ is None

    @pytest.mark.asyncio
    async def test_runs_on_its_own_thread_and_loop(self):
        ident, loop = run_async_joined(_where())
        assert ident != threading.get_ident()
        assert loop is not asyncio.get_running_loop()
