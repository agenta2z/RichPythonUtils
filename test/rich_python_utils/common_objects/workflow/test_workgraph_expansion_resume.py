"""Resume of a WorkGraph whose start node expanded before the run crashed.

The start node's own result and its expansion record are both saved on the
first run; on resume the re-attached subgraph must execute (completed subgraph
nodes load their results) while the start node itself is not recomputed.
"""

import asyncio
import os
import shutil
import tempfile

import pytest
from rich_python_utils.common_objects.workflow.common.expansion import (
    GraphExpansionResult,
    SubgraphSpec,
)
from rich_python_utils.common_objects.workflow.common.result_pass_down_mode import (
    ResultPassDownMode,
)
from rich_python_utils.common_objects.workflow.workgraph import WorkGraph, WorkGraphNode

EXPANSION_ID = "shards"


class _TestNode(WorkGraphNode):
    def __init__(self, save_dir, **kwargs):
        super().__init__(**kwargs)
        self._save_dir = save_dir

    def _get_result_path(self, name, *args, **kwargs) -> str:
        os.makedirs(self._save_dir, exist_ok=True)
        return os.path.join(self._save_dir, f"{name}.pkl")


def _make_node(name, fn, save_dir):
    return _TestNode(
        save_dir=save_dir,
        name=name,
        value=fn,
        result_pass_down_mode=ResultPassDownMode.ResultAsFirstArg,
        enable_result_save=True,
        resume_with_saved_results=True,
    )


class _Scenario:
    """Builds the same graph for every run; ``crash`` makes the last subgraph node fail."""

    def __init__(self, save_dir):
        self.save_dir = save_dir
        self.calls = []
        self.crash = True

    def _record(self, name, value):
        self.calls.append(name)
        return value

    def _last(self, x):
        self.calls.append("sub_b")
        if self.crash:
            raise RuntimeError("sub_b crashed")
        return x + 20

    def build_subgraph(self, expansion_id):
        sub_a = _make_node(
            "sub_a", lambda x: self._record("sub_a", x + 10), self.save_dir
        )
        sub_b = _make_node("sub_b", self._last, self.save_dir)
        sub_a.add_next(sub_b)
        return SubgraphSpec(nodes=[sub_a, sub_b], entry_nodes=[sub_a])

    def _expand(self, x):
        self.calls.append("expander")
        return GraphExpansionResult(
            result=x + 1,
            subgraph=self.build_subgraph(EXPANSION_ID),
            expansion_id=EXPANSION_ID,
        )

    def graph(self):
        self.expander = _make_node("expander", self._expand, self.save_dir)
        return WorkGraph(
            start_nodes=[self.expander],
            max_expansion_depth=1,
            max_total_nodes=50,
            resume_with_saved_results=True,
            subgraph_registry={EXPANSION_ID: self.build_subgraph},
        )


@pytest.fixture
def scenario():
    d = tempfile.mkdtemp(prefix="wg_exp_resume_test_")
    yield _Scenario(d)
    shutil.rmtree(d, ignore_errors=True)


def _assert_crashed_first_run(scenario):
    assert scenario.calls == ["expander", "sub_a", "sub_b"]
    probe = scenario.expander
    for name in ("expander", "__graph_expansion__expander", "sub_a"):
        assert probe._exists_result(name, probe._get_result_path(name))
    assert not probe._exists_result("sub_b", probe._get_result_path("sub_b"))
    scenario.calls.clear()
    scenario.crash = False


class TestStartNodeExpansionResume:
    def test_resume_runs_the_reattached_subgraph(self, scenario):
        with pytest.raises(RuntimeError, match="sub_b crashed"):
            scenario.graph().run(5)
        _assert_crashed_first_run(scenario)

        result = scenario.graph().run(5)

        assert scenario.calls == ["sub_b"]
        assert result == 36
        assert [n.name for n in scenario.expander.next] == ["sub_a"]

    def test_async_resume_runs_the_reattached_subgraph(self, scenario):
        with pytest.raises(RuntimeError, match="sub_b crashed"):
            asyncio.run(scenario.graph().arun(5))
        _assert_crashed_first_run(scenario)

        result = asyncio.run(scenario.graph().arun(5))

        assert scenario.calls == ["sub_b"]
        assert result == 36
        assert [n.name for n in scenario.expander.next] == ["sub_a"]

    def test_reconstruction_reports_the_reattached_node(self, scenario):
        with pytest.raises(RuntimeError, match="sub_b crashed"):
            scenario.graph().run(5)

        graph = scenario.graph()

        assert graph._reconstruct_graph_expansions(5) == {id(scenario.expander)}

    def test_reconstruction_reports_nothing_without_records(self, scenario):
        assert scenario.graph()._reconstruct_graph_expansions(5) == set()
