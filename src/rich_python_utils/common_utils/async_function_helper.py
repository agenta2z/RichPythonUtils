"""Async function helper utilities.

Provides async-compatible versions of function helpers, including retry logic
and async/sync bridge utilities for seamless use of async code from sync contexts.
"""

import asyncio
import random
from typing import Any, Callable, Dict, Sequence, Tuple


async def async_execute_with_retry(
    func: Callable,
    args: Sequence = None,
    kwargs: Dict = None,
    max_retry: int = 3,
    min_retry_wait: float = 0.0,
    max_retry_wait: float = 1.0,
    retry_on_exceptions: Tuple[type, ...] = (Exception,),
    default_return_or_raise: Any = None,
    on_retry_callback: Callable = None,
) -> Any:
    """Async execute a function with retry logic.

    Args:
        func: Async function to execute.
        args: Positional arguments.
        kwargs: Keyword arguments.
        max_retry: Maximum number of retry attempts.
        min_retry_wait: Minimum wait time between retries.
        max_retry_wait: Maximum wait time between retries.
        retry_on_exceptions: Exceptions that trigger retry.
        default_return_or_raise: Value to return or exception to raise on failure.
        on_retry_callback: Callback function on each retry (can be sync or async).

    Returns:
        Result of the function call.

    Raises:
        Exception: If all retries fail and default_return_or_raise is an Exception.
    """
    args = args or []
    kwargs = kwargs or {}

    last_exception = None
    for attempt in range(max(1, max_retry)):
        try:
            result = func(*args, **kwargs)
            if asyncio.iscoroutine(result):
                return await result
            return result
        except retry_on_exceptions as e:
            last_exception = e

            if on_retry_callback:
                callback_result = on_retry_callback(attempt, e)
                if asyncio.iscoroutine(callback_result):
                    await callback_result

            if attempt < max_retry - 1:
                wait_time = random.uniform(min_retry_wait, max_retry_wait)
                if wait_time > 0:
                    await asyncio.sleep(wait_time)

    # All retries failed
    if default_return_or_raise is None:
        if last_exception:
            raise last_exception
        raise RuntimeError(
            f"Function {func.__name__} failed after {max_retry} attempts"
        )
    elif isinstance(default_return_or_raise, Exception):
        raise default_return_or_raise
    else:
        return default_return_or_raise


def _run_async(coro) -> Any:
    """Run an async coroutine from a sync context.

    This is a sync-to-async bridge that handles the common case of needing
    to run async code from synchronous Python code.

    WARNING — Cross-loop hazards:
        If an async SDK client (like ClaudeSDKClient or DevmateSDKClient) was
        connected in one event loop and then used from a sync bridge that creates
        a different event loop, the client's internal state (background tasks,
        anyio TaskGroups, subprocess pipes) will be invalid. This manifests as
        cryptic errors like "Task was destroyed but it is pending!" or
        "Event loop is closed" on the second call.

        Callers should detect and guard against this situation. See the
        ClaudeCodeInferencer._infer() implementation for an example.

    Args:
        coro: Async coroutine to run.

    Returns:
        Result of the coroutine.

    Raises:
        RuntimeError: If called from within a running event loop (use await directly).
    """
    try:
        asyncio.get_running_loop()
        raise RuntimeError(
            "Cannot use _run_async() from within a running event loop. "
            "Use 'await coro' directly instead."
        )
    except RuntimeError as e:
        if "no running event loop" in str(e).lower() or "no current event loop" in str(e).lower():
            return asyncio.run(coro)
        raise
