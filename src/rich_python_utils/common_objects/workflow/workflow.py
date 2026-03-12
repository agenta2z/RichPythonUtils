import glob
import os
import pickle
import shutil
from abc import ABC
from enum import Enum
from typing import Any, Dict, Optional, Sequence, Callable, Union

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

    # ------------------------------------------------------------------
    # Loop + resume checkpoint helpers
    # ------------------------------------------------------------------

    def _has_loop_steps(self) -> bool:
        """Detect if any step uses loop_back_to."""
        return any(
            getattr(s, 'loop_back_to', None) is not None
            for s in (self._steps or ())
        )

    @staticmethod
    def _make_seq_result_id(step_name_or_index, exec_seq) -> str:
        """Return a unique result ID with a sequence number suffix."""
        return f"{step_name_or_index}___seq{exec_seq}"

    def _save_checkpoint(self, checkpoint_dict, *args, **kwargs):
        """Save a workflow checkpoint, delegating to _save_result for subclass compat."""
        checkpoint_path = self._get_result_path("__wf_checkpoint__", *args, **kwargs)
        self._save_result(checkpoint_dict, output_path=checkpoint_path)

    def _try_load_checkpoint(self, *args, **kwargs) -> Optional[dict]:
        """Try loading a checkpoint. Returns None on any failure (falls back to backward scan)."""
        try:
            ckpt_id = "__wf_checkpoint__"
            ckpt_path = self._get_result_path(ckpt_id, *args, **kwargs)
            exists = self._exists_result(result_id=ckpt_id, result_path=ckpt_path)
            if not exists:
                return None
            ckpt = self._load_result(
                result_id=ckpt_id,
                result_path_or_preloaded_result=(
                    ckpt_path if isinstance(exists, bool) else exists
                ),
            )
            if not isinstance(ckpt, dict) or "next_step_index" not in ckpt:
                return None
            return ckpt
        except Exception:
            return None

    def _save_loop_checkpoint(self, step_index, next_step_index,
                              last_saved_result_id, state, *args, **kwargs):
        """Save a loop checkpoint after the loop decision is resolved.

        Shared by both _run() and _arun() to avoid duplication.
        """
        # Validate state picklability on first checkpoint only.
        # _save_checkpoint → _save_result will pickle the whole dict anyway;
        # this pre-check gives a clear error message.
        if not getattr(self, '_state_picklability_verified', False):
            try:
                pickle.dumps(state)
            except Exception as e:
                raise TypeError(
                    f"Workflow state is not picklable and cannot be checkpointed "
                    f"for loop resume. Either make state picklable or set "
                    f"enable_result_save=False. Original error: {e}"
                ) from e
            self._state_picklability_verified = True

        self._save_checkpoint({
            "version": 1,
            "exec_seq": self._exec_seq,
            "step_index": step_index,
            "result_id": last_saved_result_id,
            "next_step_index": next_step_index,
            "loop_counts": dict(self._loop_counts),
            "state": state,
        }, *args, **kwargs)

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

        # Detect if any step uses loop_back_to — needed by both resume and save blocks.
        _has_loops = self._has_loop_steps()

        # Determine the starting step index for execution. By default, start from -1,
        # which means no steps have been completed and we start from the first step (index 0).
        # If resuming is enabled, we try to find the latest completed step result to resume from.
        start_step_i = -1
        step_result = None
        prev_step_result = None
        _checkpoint = None
        _checkpoint_state = None
        _checkpoint_next_i = None

        # Check if we should resume from a previously saved step result.
        # resume_with_saved_results can be:
        #   - False: Do not resume, run from the beginning.
        #   - True: Resume from the last saved step result.
        #   - int: Resume from the specific step index given.
        if self.resume_with_saved_results is not False:
            # Try checkpoint-based resume for auto-resume with loops
            if _has_loops and self.resume_with_saved_results is True:
                _checkpoint = self._try_load_checkpoint(*args, **kwargs)

            if _checkpoint is not None:
                # --- Checkpoint-based resume ---
                try:
                    _ckpt_result_id = _checkpoint["result_id"]
                    step_result = self._load_result(
                        result_id=_ckpt_result_id,
                        result_path_or_preloaded_result=self._get_result_path(
                            _ckpt_result_id, *args, **kwargs
                        ),
                    )
                except Exception:
                    self.log_warning(
                        "Checkpoint result file not found, falling back to backward scan"
                    )
                    _checkpoint = None

            if _checkpoint is not None:
                start_step_i = _checkpoint["step_index"]
                self._loop_counts = _checkpoint.get("loop_counts", {})
                self._exec_seq = _checkpoint.get("exec_seq", 0)
                _checkpoint_state = _checkpoint.get("state")
                _checkpoint_next_i = _checkpoint["next_step_index"]
            else:
                # --- Existing backward scan (unchanged) ---
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

                    # Glob fallback for ___seqN files when loops are active
                    if _has_loops and not (
                        exists_step_result_or_preloaded_step_result is not None
                        and exists_step_result_or_preloaded_step_result is not False
                    ):
                        base_path = step_result_path
                        base_dir = os.path.dirname(base_path)
                        base_name_parts = os.path.basename(base_path).rsplit('.', 1)
                        pattern = os.path.join(base_dir, f"{base_name_parts[0]}___seq*")
                        if len(base_name_parts) > 1:
                            pattern += f".{base_name_parts[1]}"
                        matches = sorted(glob.glob(pattern))
                        if matches:
                            step_result_path = matches[-1]
                            exists_step_result_or_preloaded_step_result = True

                    if (
                            exists_step_result_or_preloaded_step_result is not None and
                            exists_step_result_or_preloaded_step_result is not False
                    ):
                        self.log_info((f'step {i} result exists', True))
                        start_step_i = i
                        break
                    else:
                        self.log_info((f'step {i} result exists', False))

                # Load the found result
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
        _uses_state = any(
            getattr(s, 'update_state', None) is not None
            or getattr(s, 'receives_state', False)
            for s in self._steps
        )
        if _checkpoint_state is not None:
            state = _checkpoint_state
        else:
            state = self._init_state() if _uses_state else None
        self._state = state

        # Initialize loop-resume tracking variables.
        # These must be set BEFORE the while loop to avoid NameError.
        if _checkpoint is None:
            self._exec_seq = 0
            self._loop_counts = {}
        self._state_picklability_verified = False
        _last_saved_result_id = None

        # Determine starting step index.
        if _checkpoint_next_i is not None:
            i = _checkpoint_next_i
        else:
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
                    error_handler = getattr(this_step, 'error_handler', None)
                    if error_handler is not None:
                        step_result = error_handler(
                            err, step_result, state, step_name, i
                        )
                    else:
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
                    if _has_loops:
                        self._exec_seq += 1
                        _current_result_id = self._make_seq_result_id(
                            step_name or i, self._exec_seq
                        )
                    else:
                        _current_result_id = step_name or i
                    self._save_result(
                        step_result,
                        output_path=self._get_result_path(
                            _current_result_id, *args, **kwargs
                        ),
                    )
                    _last_saved_result_id = _current_result_id

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
                            target_i = self._resolve_step_index(
                                loop_back_to, self._steps
                            )
                            # Checkpoint: looping back
                            if _has_loops and _last_saved_result_id is not None:
                                self._save_loop_checkpoint(
                                    i, target_i, _last_saved_result_id,
                                    state, *args, **kwargs
                                )
                            i = target_i
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

                # Checkpoint: advancing to next step
                if _has_loops and _last_saved_result_id is not None:
                    self._save_loop_checkpoint(
                        i, i + 1, _last_saved_result_id,
                        state, *args, **kwargs
                    )

                i += 1

        except WorkflowAborted as exc:
            return self._handle_abort(exc, step_result, state)

        # After completing all steps, return the final result of the last step.
        return step_result

    async def _arun(self, *args, **kwargs):
        # If no steps are defined, simply return as there's nothing to run.
        if not self._steps:
            return

        # Detect if any step uses loop_back_to — needed by both resume and save blocks.
        _has_loops = self._has_loop_steps()

        start_step_i = -1
        step_result = None
        prev_step_result = None
        _checkpoint = None
        _checkpoint_state = None
        _checkpoint_next_i = None

        if self.resume_with_saved_results is not False:
            # Try checkpoint-based resume for auto-resume with loops
            if _has_loops and self.resume_with_saved_results is True:
                _checkpoint = self._try_load_checkpoint(*args, **kwargs)

            if _checkpoint is not None:
                try:
                    _ckpt_result_id = _checkpoint["result_id"]
                    step_result = self._load_result(
                        result_id=_ckpt_result_id,
                        result_path_or_preloaded_result=self._get_result_path(
                            _ckpt_result_id, *args, **kwargs
                        ),
                    )
                except Exception:
                    self.log_warning(
                        "Checkpoint result file not found, falling back to backward scan"
                    )
                    _checkpoint = None

            if _checkpoint is not None:
                start_step_i = _checkpoint["step_index"]
                self._loop_counts = _checkpoint.get("loop_counts", {})
                self._exec_seq = _checkpoint.get("exec_seq", 0)
                _checkpoint_state = _checkpoint.get("state")
                _checkpoint_next_i = _checkpoint["next_step_index"]
            else:
                # --- Existing backward scan (unchanged) ---
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

                    # Glob fallback for ___seqN files when loops are active
                    if _has_loops and not (
                        exists_step_result_or_preloaded_step_result is not None
                        and exists_step_result_or_preloaded_step_result is not False
                    ):
                        base_path = step_result_path
                        base_dir = os.path.dirname(base_path)
                        base_name_parts = os.path.basename(base_path).rsplit('.', 1)
                        pattern = os.path.join(base_dir, f"{base_name_parts[0]}___seq*")
                        if len(base_name_parts) > 1:
                            pattern += f".{base_name_parts[1]}"
                        matches = sorted(glob.glob(pattern))
                        if matches:
                            step_result_path = matches[-1]
                            exists_step_result_or_preloaded_step_result = True

                    if (
                            exists_step_result_or_preloaded_step_result is not None and
                            exists_step_result_or_preloaded_step_result is not False
                    ):
                        self.log_info((f'step {i} result exists', True))
                        start_step_i = i
                        break
                    else:
                        self.log_info((f'step {i} result exists', False))

                # Load the found result
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
        _uses_state = any(
            getattr(s, 'update_state', None) is not None
            or getattr(s, 'receives_state', False)
            for s in self._steps
        )
        if _checkpoint_state is not None:
            state = _checkpoint_state
        else:
            state = self._init_state() if _uses_state else None
        self._state = state

        # Initialize loop-resume tracking variables.
        if _checkpoint is None:
            self._exec_seq = 0
            self._loop_counts = {}
        self._state_picklability_verified = False
        _last_saved_result_id = None

        # Determine starting step index.
        if _checkpoint_next_i is not None:
            i = _checkpoint_next_i
        else:
            i = start_step_i + 1

        try:  # OUTER try: catches WorkflowAborted → _handle_abort
            while i < len(self._steps):
                this_step = self._steps[i]
                step_name = self._get_step_name(this_step, i)

                try:  # INNER try: per-step error handling
                    if i > 0:
                        prev_step_result = step_result
                        nargs, nkwargs = self._get_args_for_downstream(
                            prev_step_result, args, kwargs
                        )
                        step_result = await call_maybe_async(this_step, *nargs, **nkwargs)
                    else:
                        step_result = await call_maybe_async(this_step, *args, **kwargs)

                except Exception as err:
                    error_handler = getattr(this_step, 'error_handler', None)
                    if error_handler is not None:
                        step_result = await call_maybe_async(
                            error_handler, err, step_result, state, step_name, i
                        )
                    else:
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

                # Post-process hooks
                _step_result = await call_maybe_async(self._post_process, step_result, *args, **kwargs)
                if _step_result is not None:
                    step_result = _step_result

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
                    if _has_loops:
                        self._exec_seq += 1
                        _current_result_id = self._make_seq_result_id(
                            step_name or i, self._exec_seq
                        )
                    else:
                        _current_result_id = step_name or i
                    self._save_result(
                        step_result,
                        output_path=self._get_result_path(
                            _current_result_id, *args, **kwargs
                        ),
                    )
                    _last_saved_result_id = _current_result_id

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
                            target_i = self._resolve_step_index(
                                loop_back_to, self._steps
                            )
                            # Checkpoint: looping back
                            if _has_loops and _last_saved_result_id is not None:
                                self._save_loop_checkpoint(
                                    i, target_i, _last_saved_result_id,
                                    state, *args, **kwargs
                                )
                            i = target_i
                            continue  # jump back, skip _on_step_complete
                        else:
                            on_exhausted = getattr(
                                this_step, 'on_loop_exhausted', None
                            )
                            if on_exhausted:
                                await call_maybe_async(on_exhausted, state, step_result)

                # Step-complete hook (only fires when NOT looping back).
                await call_maybe_async(
                    self._on_step_complete, step_result, step_name, i, state, *args, **kwargs
                )

                # Checkpoint: advancing to next step
                if _has_loops and _last_saved_result_id is not None:
                    self._save_loop_checkpoint(
                        i, i + 1, _last_saved_result_id,
                        state, *args, **kwargs
                    )

                i += 1

        except WorkflowAborted as exc:
            return await call_maybe_async(self._handle_abort, exc, step_result, state)

        return step_result

