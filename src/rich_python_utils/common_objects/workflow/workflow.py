import os
import shutil
from abc import ABC
from enum import Enum
from typing import Any, Dict, Optional, Sequence, Callable

from attr import attrs, attrib

from rich_python_utils.common_objects.workflow.common.exceptions import WorkflowAborted
from rich_python_utils.common_objects.workflow.common.step_result_save_options import (
    StepResultSaveOptions
)
from rich_python_utils.common_objects.workflow.common.result_pass_down_mode import ResultPassDownMode
from rich_python_utils.common_objects.workflow.common.worknode_base import WorkNodeBase
from rich_python_utils.common_utils.async_utils import call_maybe_async


@attrs(slots=False)
class Workflow(WorkNodeBase, ABC):
    """
    Orchestrates a sequence of computational steps with support for resuming, saving, and post-processing.

    This workflow supports:
        1. Executing a predefined sequence of steps (callabless).
        2. Optional hooks for processing results after each step.
        3. Saving and loading intermediate step results to facilitate resuming workflows.
        4. Configurable behavior for saving step results based on predefined options.

    Attributes:
        _steps (Sequence[Callable]):
            The steps to be executed, defined as callables.
        enable_optional_post_process (bool):
            If True, enables the `_optional_post_process` hook after each step, provided the step allows it.
        enable_result_save (Union[rich_python_utils.general_utils.common_objects.workflow.worknode_base.StepResultSaveOptions, bool, str]):
            Determines when to save step results. Supported values:
                - `StepResultSaveOptions.NoSave`
                - `StepResultSaveOptions.Always`
                - `StepResultSaveOptions.OnError`
                - True (equivalent to StepResultSaveOptions.Always)
                - False (equivalent to StepResultSaveOptions.NoSave)
        resume_with_saved_results (Union[bool, int]):
            Configures workflow resumption:
                - `False`: Runs the workflow from the beginning.
                - `True`: Resumes from the last successfully saved step.
                - `int`: Resumes from the specified step index.
        result_pass_down_mode (Union[str, rich_python_utils.common_objects.workflow.common.result_pass_down_mode.ResultPassDownMode, Any]):
            The mode for passing results to downstream nodes. Defaults to `NoPassDown`.
            - If a ResultPassDownMode enum: controls positional pass-down behavior.
            - If a string: injects the result into kwargs under this key (overwriting existing key).
        logger (Optional[Union[Callable, logging.Logger]]):
            A logger for workflow-related messages.

    Methods:
        run(*args, **kwargs):
            Executes all steps in the workflow in sequence. Resumes from a saved step if configured to do so.

    Example:
        >>> import os
        >>> import shutil
        >>> from enum import Enum
        >>> from abc import ABC
        >>> from typing import Callable, Iterable, Union, Any
        >>> from attr import attrs, attrib

        >>> class StepResultSaveOptions(str, Enum):
        ...     NoSave = 'no_save'
        ...     Always = 'always'
        ...     OnError = 'on_error'

        >>> @attrs(slots=False)
        ... class MyWorkflow(Workflow):
        ...     def _get_result_path(self, result_id, *args, **kwargs) -> str:
        ...         # Save step results in a fixed directory for testing
        ...         return os.path.join('workflow_test_steps', f'step_{result_id}.pkl')
        ...
        ...     def _post_process(self, result, *args, **kwargs):
        ...         # Example post-processing: print the step result
        ...         print(f"Post-step processing result: {result}")
        ...         return result
        ...
        ...     def _optional_post_process(self, result, *args, **kwargs):
        ...         # Example optional post-processing: log the step result
        ...         print(f"Optional post-step processing result: {result}")
        ...         return result
        ...
        ...     def __attrs_post_init__(self):
        ...         super().__attrs_post_init__()
        ...         # Ensure the test directory exists
        ...         os.makedirs('workflow_test_steps', exist_ok=True)
        ...
        ...     def __del__(self):
        ...         # Clean up the test directory upon deletion
        ...         shutil.rmtree('workflow_test_steps', ignore_errors=True)

        # Define steps
        >>> def step0(x):
        ...     return x + 1
        ...
        >>> def step1(x):
        ...     return x * 2
        ...

        # Instantiate the workflow with steps and configurations
        >>> w = MyWorkflow(
        ...     steps=[step0, step1],
        ...     result_pass_down_mode=ResultPassDownMode.ResultAsFirstArg,
        ...     enable_result_save=StepResultSaveOptions.Always,
        ...     resume_with_saved_results=False,
        ...     debug_mode=True
        ... )
        >>> result = w.run(5)
        Post-step processing result: 6
        Optional post-step processing result: 6
        Post-step processing result: 12
        Optional post-step processing result: 12
        >>> result
        12

        # Simulate resuming from a saved step
        # Create a new workflow instance with the same steps and configurations
        >>> w_resume = MyWorkflow(
        ...     steps=[step0, step1],
        ...     result_pass_down_mode=ResultPassDownMode.ResultAsFirstArg,
        ...     enable_result_save=StepResultSaveOptions.Always,
        ...     logger=print,
        ...     resume_with_saved_results=True  # Resume from the last saved step
        ... )
        >>> resumed_result = w_resume.run(5)
        {'level': 20, 'name': 'MyWorkflow', 'type': 'WorkflowMessage', 'item': ['step 1 result exists', True], 'time': ...
        >>> resumed_result
        12

        >>> del w_resume
    """
    _steps = attrib(type=Sequence[Callable], default=None)
    _state = attrib(default=None, init=False)
    max_loop_iterations = attrib(type=int, default=10)

    # ------------------------------------------------------------------
    # State & step hooks (override in subclasses)
    # ------------------------------------------------------------------

    def _init_state(self) -> dict:
        """Initialize flow state. Override for custom initial state."""
        return {}

    def _get_step_name(self, step, index) -> Optional[str]:
        """Get step name via per-step attribute pattern."""
        return getattr(step, 'name', None)

    def _update_state(self, state, result, step, step_name, step_index) -> dict:
        """Update state after a step completes.

        Delegates to a per-step ``update_state`` attribute if present.
        """
        updater = getattr(step, 'update_state', None)
        if updater is not None:
            updated = updater(state, result)
            if updated is not None:
                return updated
        return state

    def _on_step_complete(self, result, step_name, step_index, state,
                          *args, **kwargs):
        """Hook called after a step completes (only on non-loop-back iterations).

        Default is a no-op.  Subclasses override for per-step dispatch.
        """
        return None

    def _default_error_handler(self, error, step_result_so_far, state,
                               step_name, step_index):
        """Default error handler — re-raises, preserving current behaviour."""
        raise error

    def _handle_abort(self, abort_exc, step_result, state):
        """Handle a :class:`WorkflowAborted` exception.

        Default: return *step_result*.  Subclasses override to build a
        richer partial result from *abort_exc* and *state*.
        """
        return step_result

    def _resolve_step_index(self, target, steps) -> int:
        """Resolve a *loop_back_to* target (name or int) to a step index."""
        if isinstance(target, int):
            return target
        for idx, step in enumerate(steps):
            if getattr(step, 'name', None) == target:
                return idx
        raise ValueError(f"Loop target step '{target}' not found")

    def _get_step_identifier(self, step: Callable, index: int) -> Dict[str, Any]:
        """Get serializable identifier for a step callable.
        
        Args:
            step: The callable step function
            index: The index of the step in the sequence
            
        Returns:
            Dict containing step identifier information
        """
        identifier = {
            'index': index,
            'name': None,
            'module': None,
            'ref': None,
        }
        
        if hasattr(step, '__name__'):
            identifier['name'] = step.__name__
        if hasattr(step, '__module__'):
            identifier['module'] = step.__module__
        if identifier['name'] and identifier['module']:
            identifier['ref'] = f"{identifier['module']}.{identifier['name']}"
        
        return identifier

    def to_serializable_obj(
        self, 
        mode: str = 'auto',
        _output_format: Optional[str] = None
    ) -> Dict[str, Any]:
        """Serialize Workflow to dict.
        
        Serializes step configuration and stores step identifiers that can
        be resolved during deserialization.
        
        Args:
            mode: Serialization mode ('auto', 'dict', 'pickle')
            _output_format: Target output format for conflict detection
            
        Returns:
            Dict containing workflow configuration and step identifiers.
        """
        # Get result_pass_down_mode as string for serialization
        result_pass_down_mode_str = self.result_pass_down_mode
        if isinstance(self.result_pass_down_mode, ResultPassDownMode):
            result_pass_down_mode_str = self.result_pass_down_mode.value
        elif callable(self.result_pass_down_mode):
            # For callable mode, store reference if possible
            if hasattr(self.result_pass_down_mode, '__name__'):
                result_pass_down_mode_str = f"callable:{self.result_pass_down_mode.__module__}.{self.result_pass_down_mode.__name__}"
            else:
                result_pass_down_mode_str = None  # Non-serializable callable
        
        # Serialize step identifiers
        step_identifiers = []
        if self._steps:
            for i, step in enumerate(self._steps):
                step_identifiers.append(self._get_step_identifier(step, i))
        
        return {
            '_type': type(self).__name__,
            '_module': type(self).__module__,
            'version': '1.0',
            'name': self.name,
            'steps': step_identifiers,
            'config': {
                'enable_result_save': (
                    self.enable_result_save.value 
                    if isinstance(self.enable_result_save, StepResultSaveOptions) 
                    else self.enable_result_save
                ),
                'resume_with_saved_results': self.resume_with_saved_results,
                'result_pass_down_mode': result_pass_down_mode_str,
                'enable_optional_post_process': self.enable_optional_post_process,
            }
        }

    def _run(self, *args, **kwargs):
        # If no steps are defined, simply return as there's nothing to run.
        if not self._steps:
            return

        # Determine the starting step index for execution. By default, start from -1,
        # which means no steps have been completed and we start from the first step (index 0).
        # If resuming is enabled, we try to find the latest completed step result to resume from.
        start_step_i = -1
        step_result = None
        prev_step_result = None

        # Check if we should resume from a previously saved step result.
        # resume_with_saved_results can be:
        #   - False: Do not resume, run from the beginning.
        #   - True: Resume from the last saved step result.
        #   - int: Resume from the specific step index given.
        if self.resume_with_saved_results is not False:
            # We iterate backward from the given start index (or the last step)
            # looking for an existing saved result. The first found result sets
            # our start_step_i to that step index.
            saved_step_results_back_search_start_index = (
                self.resume_with_saved_results
                if isinstance(self.resume_with_saved_results, int)
                else len(self._steps) - 1
            )

            for i in range(saved_step_results_back_search_start_index, -1, -1):
                result_id = self._get_step_name(self._steps[i], i) or i
                step_result_path = self._get_result_path(result_id, *args, **kwargs)
                exists_step_result_or_preloaded_step_result = self._exists_result(
                    result_id=result_id, result_path=step_result_path
                )
                if (
                        exists_step_result_or_preloaded_step_result is not None and
                        exists_step_result_or_preloaded_step_result is not False
                ):
                    # If a result file (or preloaded result) is found, log this and set start_step_i to i.
                    self.log_info((f'step {i} result exists', True))
                    start_step_i = i
                    break
                else:
                    # No result found for this step, log the absence.
                    self.log_info((f'step {i} result exists', False))

        # If start_step_i is not -1, it means we found a previous step's result to resume from.
        # Load that result into 'step_result' so that we can continue from the next step.
        if start_step_i != -1:
            step_result = self._load_result(
                result_id=start_step_i,
                result_path_or_preloaded_result=(
                    step_result_path if
                    isinstance(exists_step_result_or_preloaded_step_result, bool)
                    else exists_step_result_or_preloaded_step_result
                )
            )

        # --- State initialization ---
        # Detect if any step uses state features.  When no step declares
        # update_state or receives_state the state stays None and all
        # state code-paths are no-ops (full backward compatibility).
        _uses_state = any(
            getattr(s, 'update_state', None) is not None
            or getattr(s, 'receives_state', False)
            for s in self._steps
        )
        state = self._init_state() if _uses_state else None
        self._state = state

        # Per-run loop tracking (reset each run, not an attrs attribute).
        self._loop_counts = {}

        # Now proceed to execute the steps from start_step_i+1 onward.
        # Uses a while loop (instead of for) so loop_back_to can rewind i.
        i = start_step_i + 1

        try:  # OUTER try: catches WorkflowAborted → _handle_abort
            while i < len(self._steps):
                this_step = self._steps[i]
                step_name = self._get_step_name(this_step, i)

                try:  # INNER try: per-step error handling
                    # Handle input arguments to the step:
                    if i > 0:
                        prev_step_result = step_result
                        nargs, nkwargs = self._get_args_for_downstream(
                            prev_step_result, args, kwargs
                        )
                        step_result = this_step(*nargs, **nkwargs)
                    else:
                        # For the very first step (i=0), there's no previous result.
                        step_result = this_step(*args, **kwargs)

                except Exception as err:
                    # Per-step error handler.  Steps WITH an error_handler
                    # attribute delegate to it; steps WITHOUT use the
                    # default path (OnError save + re-raise).
                    error_handler = getattr(this_step, 'error_handler', None)
                    if error_handler is not None:
                        # Error handler outcomes:
                        #   1. RETURN a value → becomes step_result, execution continues
                        #   2. RAISE (e.g. WorkflowAborted) → propagates to outer try
                        step_result = error_handler(
                            err, step_result, state, step_name, i
                        )
                        # If we reach here the handler returned — continue
                        # to post-process below.
                    else:
                        # Default path: preserves existing OnError save + re-raise.
                        result_save_on_error_enabled = (
                            i > 0
                            and (not isinstance(self.enable_result_save, bool))
                            and self.enable_result_save == StepResultSaveOptions.OnError
                        )

                        self.log_error({
                            'step_failed': i,
                            'result_save_on_error_enabled': result_save_on_error_enabled,
                        })

                        if i > 0 and result_save_on_error_enabled:
                            self._save_result(
                                prev_step_result,
                                output_path=self._get_result_path(
                                    step_name or i, *args, **kwargs
                                ),
                            )
                        raise err

                # After the step executes successfully, run the mandatory _post_process hook.
                _step_result = self._post_process(step_result, *args, **kwargs)
                if _step_result is not None:
                    step_result = _step_result

                # If optional post-processing is enabled both on the workflow and on this step,
                # run the _optional_post_process hook next.
                if getattr(this_step, 'enable_optional_post_process',
                           self.enable_optional_post_process):
                    _step_result = self._optional_post_process(
                        step_result, *args, **kwargs
                    )
                    if _step_result is not None:
                        step_result = _step_result

                # Update flow state (no-op when state is None).
                if state is not None:
                    state = self._update_state(
                        state, step_result, this_step, step_name, i
                    )
                    self._state = state

                # Save result based on the configured saving options.
                enable_result_save = getattr(
                    this_step, 'enable_result_save', self.enable_result_save
                )
                if (enable_result_save is True
                        or enable_result_save == StepResultSaveOptions.Always):
                    self._save_result(
                        step_result,
                        output_path=self._get_result_path(
                            step_name or i, *args, **kwargs
                        ),
                    )

                # Loop check — only evaluated when the step has loop_back_to.
                loop_back_to = getattr(this_step, 'loop_back_to', None)
                if loop_back_to is not None:
                    loop_condition = getattr(this_step, 'loop_condition', None)
                    should_loop = (
                        loop_condition(state, step_result)
                        if loop_condition else False
                    )
                    if should_loop:
                        max_iters = getattr(
                            this_step, 'max_loop_iterations',
                            self.max_loop_iterations,
                        )
                        count = self._loop_counts.get(i, 0)
                        if count < max_iters:
                            self._loop_counts[i] = count + 1
                            i = self._resolve_step_index(
                                loop_back_to, self._steps
                            )
                            continue  # jump back, skip _on_step_complete
                        else:
                            # Loop exhausted — invoke handler if present.
                            on_exhausted = getattr(
                                this_step, 'on_loop_exhausted', None
                            )
                            if on_exhausted:
                                on_exhausted(state, step_result)

                # Step-complete hook (only fires when NOT looping back).
                self._on_step_complete(
                    step_result, step_name, i, state, *args, **kwargs
                )

                i += 1

        except WorkflowAborted as exc:
            # Any WorkflowAborted raised by error_handler,
            # on_loop_exhausted, or _on_step_complete is caught here.
            return self._handle_abort(exc, step_result, state)

        # After completing all steps, return the final result of the last step.
        return step_result

    async def _arun(self, *args, **kwargs):
        # If no steps are defined, simply return as there's nothing to run.
        if not self._steps:
            return

        # Determine the starting step index for execution. By default, start from -1,
        # which means no steps have been completed and we start from the first step (index 0).
        # If resuming is enabled, we try to find the latest completed step result to resume from.
        start_step_i = -1
        step_result = None
        prev_step_result = None

        # Check if we should resume from a previously saved step result.
        # resume_with_saved_results can be:
        #   - False: Do not resume, run from the beginning.
        #   - True: Resume from the last saved step result.
        #   - int: Resume from the specific step index given.
        if self.resume_with_saved_results is not False:
            # We iterate backward from the given start index (or the last step)
            # looking for an existing saved result. The first found result sets
            # our start_step_i to that step index.
            saved_step_results_back_search_start_index = (
                self.resume_with_saved_results
                if isinstance(self.resume_with_saved_results, int)
                else len(self._steps) - 1
            )

            for i in range(saved_step_results_back_search_start_index, -1, -1):
                result_id = self._get_step_name(self._steps[i], i) or i
                step_result_path = self._get_result_path(result_id, *args, **kwargs)
                exists_step_result_or_preloaded_step_result = self._exists_result(
                    result_id=result_id, result_path=step_result_path
                )
                if (
                        exists_step_result_or_preloaded_step_result is not None and
                        exists_step_result_or_preloaded_step_result is not False
                ):
                    # If a result file (or preloaded result) is found, log this and set start_step_i to i.
                    self.log_info((f'step {i} result exists', True))
                    start_step_i = i
                    break
                else:
                    # No result found for this step, log the absence.
                    self.log_info((f'step {i} result exists', False))

        # If start_step_i is not -1, it means we found a previous step's result to resume from.
        # Load that result into 'step_result' so that we can continue from the next step.
        if start_step_i != -1:
            step_result = self._load_result(
                result_id=start_step_i,
                result_path_or_preloaded_result=(
                    step_result_path if
                    isinstance(exists_step_result_or_preloaded_step_result, bool)
                    else exists_step_result_or_preloaded_step_result
                )
            )

        # --- State initialization ---
        # Detect if any step uses state features.  When no step declares
        # update_state or receives_state the state stays None and all
        # state code-paths are no-ops (full backward compatibility).
        _uses_state = any(
            getattr(s, 'update_state', None) is not None
            or getattr(s, 'receives_state', False)
            for s in self._steps
        )
        state = self._init_state() if _uses_state else None
        self._state = state

        # Per-run loop tracking (reset each run, not an attrs attribute).
        self._loop_counts = {}

        # Now proceed to execute the steps from start_step_i+1 onward.
        # Uses a while loop (instead of for) so loop_back_to can rewind i.
        i = start_step_i + 1

        try:  # OUTER try: catches WorkflowAborted → _handle_abort
            while i < len(self._steps):
                this_step = self._steps[i]
                step_name = self._get_step_name(this_step, i)

                try:  # INNER try: per-step error handling
                    # Handle input arguments to the step:
                    if i > 0:
                        prev_step_result = step_result
                        nargs, nkwargs = self._get_args_for_downstream(
                            prev_step_result, args, kwargs
                        )
                        step_result = await call_maybe_async(this_step, *nargs, **nkwargs)
                    else:
                        # For the very first step (i=0), there's no previous result.
                        step_result = await call_maybe_async(this_step, *args, **kwargs)

                except Exception as err:
                    # Per-step error handler.  Steps WITH an error_handler
                    # attribute delegate to it; steps WITHOUT use the
                    # default path (OnError save + re-raise).
                    error_handler = getattr(this_step, 'error_handler', None)
                    if error_handler is not None:
                        # Error handler outcomes:
                        #   1. RETURN a value → becomes step_result, execution continues
                        #   2. RAISE (e.g. WorkflowAborted) → propagates to outer try
                        step_result = await call_maybe_async(
                            error_handler, err, step_result, state, step_name, i
                        )
                        # If we reach here the handler returned — continue
                        # to post-process below.
                    else:
                        # Default path: preserves existing OnError save + re-raise.
                        result_save_on_error_enabled = (
                            i > 0
                            and (not isinstance(self.enable_result_save, bool))
                            and self.enable_result_save == StepResultSaveOptions.OnError
                        )

                        self.log_error({
                            'step_failed': i,
                            'result_save_on_error_enabled': result_save_on_error_enabled,
                        })

                        if i > 0 and result_save_on_error_enabled:
                            self._save_result(
                                prev_step_result,
                                output_path=self._get_result_path(
                                    step_name or i, *args, **kwargs
                                ),
                            )
                        raise err

                # After the step executes successfully, run the mandatory _post_process hook.
                _step_result = await call_maybe_async(self._post_process, step_result, *args, **kwargs)
                if _step_result is not None:
                    step_result = _step_result

                # If optional post-processing is enabled both on the workflow and on this step,
                # run the _optional_post_process hook next.
                if getattr(this_step, 'enable_optional_post_process',
                           self.enable_optional_post_process):
                    _step_result = await call_maybe_async(
                        self._optional_post_process, step_result, *args, **kwargs
                    )
                    if _step_result is not None:
                        step_result = _step_result

                # Update flow state (no-op when state is None).
                if state is not None:
                    state = await call_maybe_async(
                        self._update_state, state, step_result, this_step, step_name, i
                    )
                    self._state = state

                # Save result based on the configured saving options.
                enable_result_save = getattr(
                    this_step, 'enable_result_save', self.enable_result_save
                )
                if (enable_result_save is True
                        or enable_result_save == StepResultSaveOptions.Always):
                    self._save_result(
                        step_result,
                        output_path=self._get_result_path(
                            step_name or i, *args, **kwargs
                        ),
                    )

                # Loop check — only evaluated when the step has loop_back_to.
                loop_back_to = getattr(this_step, 'loop_back_to', None)
                if loop_back_to is not None:
                    loop_condition = getattr(this_step, 'loop_condition', None)
                    should_loop = (
                        await call_maybe_async(loop_condition, state, step_result)
                        if loop_condition else False
                    )
                    if should_loop:
                        max_iters = getattr(
                            this_step, 'max_loop_iterations',
                            self.max_loop_iterations,
                        )
                        count = self._loop_counts.get(i, 0)
                        if count < max_iters:
                            self._loop_counts[i] = count + 1
                            i = self._resolve_step_index(
                                loop_back_to, self._steps
                            )
                            continue  # jump back, skip _on_step_complete
                        else:
                            # Loop exhausted — invoke handler if present.
                            on_exhausted = getattr(
                                this_step, 'on_loop_exhausted', None
                            )
                            if on_exhausted:
                                await call_maybe_async(on_exhausted, state, step_result)

                # Step-complete hook (only fires when NOT looping back).
                await call_maybe_async(
                    self._on_step_complete, step_result, step_name, i, state, *args, **kwargs
                )

                i += 1

        except WorkflowAborted as exc:
            # Any WorkflowAborted raised by error_handler,
            # on_loop_exhausted, or _on_step_complete is caught here.
            return await call_maybe_async(self._handle_abort, exc, step_result, state)

        # After completing all steps, return the final result of the last step.
        return step_result

