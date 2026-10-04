"""Tests for how sync execute_with_retry surfaces output_validator rejections."""

import pytest
from rich_python_utils.common_utils.function_helper import (
    execute_with_retry,
    OutputValidationExhaustedError,
)


class _Counter:
    def __init__(self, result="out"):
        self.calls = 0
        self.result = result

    def __call__(self):
        self.calls += 1
        return self.result


class TestValidationExhaustionIsTyped:
    """With no default and no fallback, an exhausted validation loop raises
    OutputValidationExhaustedError itself, as async_execute_with_retry does."""

    def test_string_verdict_raises_typed_error_with_handler(self):
        func = _Counter()
        with pytest.raises(OutputValidationExhaustedError) as exc_info:
            execute_with_retry(
                func=func, max_retry=2, output_validator=lambda r: "RESTART"
            )
        assert type(exc_info.value) is OutputValidationExhaustedError
        assert exc_info.value.args == ("Output validation failed", "RESTART")
        assert func.calls == 3

    def test_false_verdict_raises_typed_error_without_handler(self):
        with pytest.raises(OutputValidationExhaustedError) as exc_info:
            execute_with_retry(
                func=_Counter(), max_retry=2, output_validator=lambda r: False
            )
        assert exc_info.value.args == ("Output validation failed",)

    def test_other_errors_keep_generic_wrapper(self):
        def always_fail():
            raise RuntimeError("boom")

        with pytest.raises(Exception, match="All retries failed") as exc_info:
            execute_with_retry(func=always_fail, max_retry=2)
        assert not isinstance(exc_info.value, OutputValidationExhaustedError)
        assert isinstance(exc_info.value.__cause__, RuntimeError)

    def test_enclosing_non_retryable_gate_stops_at_first_exhaustion(self):
        inner = _Counter()

        def inner_call():
            return execute_with_retry(
                func=inner, max_retry=2, output_validator=lambda r: False
            )

        with pytest.raises(OutputValidationExhaustedError):
            execute_with_retry(
                func=inner_call,
                max_retry=3,
                non_retryable_exceptions=(OutputValidationExhaustedError,),
            )
        assert inner.calls == 3


class TestSingleAttemptIsValidated:
    """max_retry <= 1 makes a single attempt whose output is still checked by
    output_validator, as async_execute_with_retry does."""

    @pytest.mark.parametrize("max_retry", [0, 1])
    def test_accepted_output_is_returned(self, max_retry):
        func = _Counter()
        seen = []

        def validator(result):
            seen.append(result)
            return True

        assert (
            execute_with_retry(
                func=func, max_retry=max_retry, output_validator=validator
            )
            == "out"
        )
        assert seen == ["out"]
        assert func.calls == 1

    @pytest.mark.parametrize("max_retry", [0, 1])
    def test_rejection_without_default_raises_typed_error(self, max_retry):
        func = _Counter()
        with pytest.raises(OutputValidationExhaustedError) as exc_info:
            execute_with_retry(
                func=func, max_retry=max_retry, output_validator=lambda r: "RESTART"
            )
        assert exc_info.value.args == ("Output validation failed", "RESTART")
        assert func.calls == 1

    def test_rejection_returns_non_exception_default(self):
        assert (
            execute_with_retry(
                func=_Counter(),
                output_validator=lambda r: False,
                default_return_or_raise="fallback",
            )
            == "fallback"
        )

    def test_rejection_raises_exception_default_chained(self):
        default = RuntimeError("configured")
        with pytest.raises(RuntimeError) as exc_info:
            execute_with_retry(
                func=_Counter(),
                output_validator=lambda r: False,
                default_return_or_raise=default,
            )
        assert exc_info.value is default
        assert isinstance(exc_info.value.__cause__, OutputValidationExhaustedError)

    def test_false_pre_condition_skips_call_and_validation(self):
        func = _Counter()
        seen = []
        assert (
            execute_with_retry(
                func=func,
                output_validator=seen.append,
                pre_condition=lambda: False,
                default_return_or_raise="skipped",
            )
            == "skipped"
        )
        assert func.calls == 0
        assert seen == []


class TestFromVerdict:
    def test_string_verdict_is_kept(self):
        assert OutputValidationExhaustedError.from_verdict("update").args == (
            "Output validation failed",
            "update",
        )

    @pytest.mark.parametrize("verdict", [False, 0])
    def test_non_string_verdict_is_dropped(self, verdict):
        assert OutputValidationExhaustedError.from_verdict(verdict).args == (
            "Output validation failed",
        )
