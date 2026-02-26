from abc import ABC
from os import path
from typing import Union, Any

from attr import attrs, attrib

from rich_python_utils.common_objects.workflow.common.step_result_save_options import \
    StepResultSaveOptions
from rich_python_utils.io_utils.pickle_io import pickle_save, pickle_load


@attrs(slots=False)
class Resumable(ABC):
    """
    Base class for workflows that support saving and resuming.

    Attributes:
        enable_result_save (Union[StepResultSaveOptions, bool, str]): Enables/disables result saving.
            Supported options:
                - StepResultSaveOptions.NoSave
                - StepResultSaveOptions.Always
                - StepResultSaveOptions.OnError
                - True (equivalent to StepResultSaveOptions.Always)
                - False (equivalent to StepResultSaveOptions.NoSave)
        resume_with_saved_results (bool): If True, resumes the workflow using saved results.
    """
    enable_result_save = attrib(
        type=Union[StepResultSaveOptions, bool, str],
        default=False
    )
    resume_with_saved_results = attrib(type=bool, default=False)

    def _save_result(self, result, output_path: str):
        """
        Saves the result to a path.

        Args:
            result (Any): The result object to save.
            output_path (str): The path where the result will be saved.
        """
        pickle_save(result, output_path, verbose=getattr(self, 'verbose', False))

    def _load_result(self, result_id: Any, result_path_or_preloaded_result: Union[str, Any]):
        """
        Loads a previously saved step result from a path, or can also pass in a preloaded result object.

        Args:
            result_id (Any): The identifier for the step.
            result_path_or_preloaded_result (Union[str, Any]): Path or preloaded result object.

        Returns:
            Any: The loaded result.
        """
        if isinstance(result_path_or_preloaded_result, str):
            return pickle_load(result_path_or_preloaded_result)
        else:
            return result_path_or_preloaded_result

    def _exists_result(self, result_id: Any, result_path: str) -> Union[bool, Any]:
        """
        Checks whether a step result exists at the given path.

        Args:
            result_id (Any): The index of the step to check.
            result_path (str): The file path to check for the step result.

        Returns:
            Union[bool, Any]: Returns True if the result exists, False otherwise.
                Subclasses can override this to implement custom existence checks or return preloaded results.
        """
        return path.exists(result_path)

    def _get_result_path(self, result_id: Any, *args, **kwargs) -> str:
        """
        Abstract method to define the path for saving/loading results.

        Args:
            result_id (Any): The identifier for the result.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            str: The path for saving/loading the result.
        """
        raise NotImplementedError
