"""``resume_with_saved_results=True`` scans back from the last step.

``True`` is an ``int`` in Python, so the backward resume scan must not read it
as the step index ``1``.
"""

import asyncio
import os

import pytest
from attr import attrib, attrs
from rich_python_utils.common_objects.workflow.common.result_pass_down_mode import (
    ResultPassDownMode,
)
from rich_python_utils.common_objects.workflow.common.step_result_save_options import (
    StepResultSaveOptions,
)
from rich_python_utils.common_objects.workflow.workflow import Workflow


@attrs(slots=False)
class SaveableWorkflow(Workflow):
    _save_dir = attrib(default=None)

    def _get_result_path(self, result_id, *args, **kwargs):
        return os.path.join(self._save_dir, f"step_{result_id}.pkl")


def _steps(n, executed, unsaved):
    def make(i):
        def step(x):
            executed.append(i)
            return x + 10**i

        if i in unsaved:
            step.enable_result_save = StepResultSaveOptions.NoSave
        return step

    return [make(i) for i in range(n)]


def _workflow(save_dir, n, executed, resume, unsaved=()):
    return SaveableWorkflow(
        steps=_steps(n, executed, unsaved),
        result_pass_down_mode=ResultPassDownMode.ResultAsFirstArg,
        enable_result_save=StepResultSaveOptions.Always,
        resume_with_saved_results=resume,
        save_dir=str(save_dir),
    )


def _run(wf, use_async):
    return asyncio.run(wf.arun(0)) if use_async else wf.run(0)


@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("n", [1, 4])
def test_true_resumes_a_completed_run_without_rerunning(tmp_path, n, use_async):
    first = _run(_workflow(tmp_path, n, [], resume=False), use_async)
    executed = []
    resumed = _run(_workflow(tmp_path, n, executed, resume=True), use_async)
    assert resumed == first
    assert executed == []


@pytest.mark.parametrize("use_async", [False, True])
def test_true_finds_the_latest_saved_step(tmp_path, use_async):
    first = _run(_workflow(tmp_path, 4, [], resume=False, unsaved=(3,)), use_async)
    executed = []
    resumed = _run(_workflow(tmp_path, 4, executed, resume=True), use_async)
    assert resumed == first
    assert executed == [3]


@pytest.mark.parametrize("use_async", [False, True])
def test_int_still_scans_back_from_that_index(tmp_path, use_async):
    first = _run(_workflow(tmp_path, 4, [], resume=False), use_async)
    executed = []
    resumed = _run(_workflow(tmp_path, 4, executed, resume=1), use_async)
    assert resumed == first
    assert executed == [2, 3]
