"""Async utility functions.

Provides universal async callable detection and execution utilities, async retry logic,
and sync-to-async bridge utilities. This module consolidates all async helpers that were
previously in async_function_helper.py, with extensions for output_validator and
pre_condition support in async_execute_with_retry.

Key design decision: call_maybe_async() uses the "call first, check result" pattern
via inspect.isawaitable() instead of asyncio.iscoroutinefunction(). This works universally
with functools.partial, descriptors, __call__-based objects, and decorated/wrapped callables
— unlike iscoroutinefunction() which fails on many callable wrappers.
"""

import asyncio
import inspect
import logging
import random
from typing import Any, Callable, Dict, Sequence, Tuple


async def maybe_await(result: Any) -> Any:
    """If result is awaitable (coroutine, Future, Task), await it; otherwise return as-is.

    Uses inspect.isawaitable() which covers coroutines, asyncio.Future, asyncio.Task,
    and any object with __await__.

    Args:
        result: Any value or awaitable to potentially await.

    Returns:
        The awaited result if awaitable, or the original value otherwise.
    """
    if inspect.isawaitable(result):
        return await result
    return result


async def call_maybe_async(func: Callable, *args: Any, **kwargs: Any) -> Any:
    """Call func(*args, **kwargs), then await the result if it's awaitable.

    This pattern works universally with:
    - Regular sync functions
    - Regular async functions
    - functools.partial wrapping async functions
    - Descriptors and __call__-based callables
    - Decorated/wrapped callables

    Unlike asyncio.iscoroutinefunction(), which pre-inspects the callable
    and fails on partials/descriptors, this calls first and checks the result.

    Args:
        func: Any callable (sync or async).
        *args: Positional arguments to pass to func.
        **kwargs: Keyword arguments to pass to func.

    Returns:
        The result of calling func, awaited if the result was awaitable.
    """
    result = func(*args, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


async def async_execute_with_retry(
    func: Callable,
    args: Sequence = None,
    kwargs: Dict = None,
    max_retry: int = 3,
    min_retry_wait: float = 0.0,
    max_retry_wait: float = 1.0,
    retry_on_exceptions: Tuple[type, ...] = (Exception,),
    output_validator: Callable[..., bool] = None,
    pre_condition: Callable[..., bool] = None,
    default_return_or_raise: Any = None,
    on_retry_callback: Callable = None,
) -> Any:
    """Async execute a function with retry logic, pre-condition guard, and output validation.

    Mirrors the sync execute_with_retry behavior with async support. Both pre_condition
    and output_validator are invoked via call_maybe_async, so they can be sync or async.

    Args:
        func: Function to execute (sync or async — handled via call-first pattern).
        args: Positional arguments passed to func and pre_condition.
        kwargs: Keyword arguments passed to func and pre_condition.
        max_retry: Maximum number of retry attempts.
        min_retry_wait: Minimum wait time between retries in seconds.
        max_retry_wait: Maximum wait time between retries in seconds.
        retry_on_exceptions: Tuple of exception types that trigger retry.
        output_validator: Post-check callable invoked via call_maybe_async(output_validator, result).
            Should return True if the output is valid. If False, triggers a retry.
        pre_condition: Guard callable invoked via call_maybe_async(pre_condition, *args, **kwargs)
            before each attempt. If it returns False, execution stops and returns
            default_return_or_raise.
        default_return_or_raise: Value to return or exception to raise on failure
            or when pre_condition is False.
        on_retry_callback: Callback function on each retry (can be sync or async).
            Called with (attempt_number, exception).

    Returns:
        Result of the function call.

    Raises:
        Exception: If all retries fail and default_return_or_raise is an Exception or None.
    """
    args = args or []
    kwargs = kwargs or {}

    last_exception = None
    for attempt in range(max(1, max_retry)):
        # Pre-condition guard: check before each attempt
        if pre_condition is not None:
            condition_result = await call_maybe_async(pre_condition, *args, **kwargs)
            if not condition_result:
                # Pre-condition failed — stop and return default
                if default_return_or_raise is None:
                    return None
                elif isinstance(default_return_or_raise, Exception):
                    raise default_return_or_raise
                else:
                    return default_return_or_raise

        try:
            result = func(*args, **kwargs)
            result = await maybe_await(result)

            # Output validation: check after successful execution
            if output_validator is not None:
                is_valid = await call_maybe_async(output_validator, result)
                if not is_valid:
                    last_exception = ValueError("Output validation failed")
                    if on_retry_callback:
                        await maybe_await(on_retry_callback(attempt, last_exception))
                    if attempt < max_retry - 1:
                        wait_time = random.uniform(min_retry_wait, max_retry_wait)
                        if wait_time > 0:
                            await asyncio.sleep(wait_time)
                    continue

            return result
        except retry_on_exceptions as e:
            last_exception = e

            logging.getLogger(__name__).info(
                "Retry attempt %d/%d after %s: %s",
                attempt + 1,
                max_retry,
                type(e).__name__,
                str(e)[:200],
            )

            if on_retry_callback:
                await maybe_await(on_retry_callback(attempt, e))

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
