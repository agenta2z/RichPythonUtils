from enum import Enum


class StepResultSaveOptions(str, Enum):
    """
    Enumeration to specify when to save step results.

    Attributes:
        NoSave (str): Do not save the result of the step.
        Always (str): Always save the result after the step completes successfully.
        OnError (str): Save the result only if the step raises an error.
    """
    NoSave = 'no_save'
    Always = 'always'
    OnError = 'on_error'
