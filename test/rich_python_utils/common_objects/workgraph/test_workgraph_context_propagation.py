"""``WorkGraph(use_async=True).run()`` keeps the caller's ContextVars visible to
its nodes, including when the sync call is made inside a running event loop."""

import asyncio
import contextvars

import pytest
from rich_python_utils.common_objects.workflow.workgraph import WorkGraph, WorkGraphNode

_VAR = contextvars.ContextVar("_workgraph_ctx_var", default="unset")


def _read_sync(_x):
    return _VAR.get()


async def _read_async(_x):
    await asyncio.sleep(0)
    return _VAR.get()


def _run_graph_under(value, fn):
    graph = WorkGraph(
        start_nodes=[WorkGraphNode(name="read", value=fn)], use_async=True
    )
    token = _VAR.set(value)
    try:
        return graph.run(0)
    finally:
        _VAR.reset(token)


@pytest.mark.parametrize("fn", [_read_sync, _read_async])
def test_context_var_visible_without_running_loop(fn):
    assert _run_graph_under("caller", fn) == "caller"


@pytest.mark.parametrize("fn", [_read_sync, _read_async])
@pytest.mark.asyncio
async def test_context_var_visible_inside_running_loop(fn):
    assert _run_graph_under("caller", fn) == "caller"
