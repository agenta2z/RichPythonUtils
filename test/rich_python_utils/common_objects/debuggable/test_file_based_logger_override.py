"""FileBasedLogger contract + the per-write file_path override seam in Debuggable.log.

The override is the generic mechanism a subclass uses to route a file-based logger's
write to a dynamically-resolved path (e.g. a run-context workspace) without mutating
the logger. JsonLogger satisfies the contract natively (its call accepts file_path).
"""

import os

from rich_python_utils.common_objects.debuggable import Debuggable, FileBasedLogger
from rich_python_utils.io_utils.json_io import JsonLogger


def test_jsonlogger_satisfies_file_based_logger():
    jl = JsonLogger(file_path="/tmp/whatever/session.jsonl", append=True)
    assert isinstance(jl, FileBasedLogger)
    # the marker is the file_path property
    assert jl.file_path == "/tmp/whatever/session.jsonl"


def test_non_file_loggers_are_not_file_based():
    assert not isinstance(print, FileBasedLogger)

    class _Plain:
        def __call__(self, *a, **k):
            return None

    assert not isinstance(_Plain(), FileBasedLogger)


def test_jsonlogger_call_time_file_path_wins(tmp_path):
    """The native contract: a call-time file_path overrides the baked one for that write."""
    baked = tmp_path / "baked.jsonl"
    override = tmp_path / "sub" / "override.jsonl"
    jl = JsonLogger(file_path=str(baked), append=True)

    jl({"item": "hello"}, file_path=str(override))

    assert override.exists(), "call-time file_path is honored"
    assert not baked.exists(), "baked path not written when overridden"


class _Redirecting(Debuggable):
    """Debuggable whose hook redirects file-based loggers to ``_override`` when set."""

    _override = None

    def _log_path_override(self, logger_name, logger):
        return self._override


def test_log_seam_routes_file_based_logger_to_override(tmp_path):
    baked = tmp_path / "baked.jsonl"
    override = tmp_path / "deep" / "routed.jsonl"
    d = _Redirecting(
        logger={"_ws": JsonLogger(file_path=str(baked), append=True)},
        debug_mode=True,
        always_add_logging_based_logger=False,
    )

    d._override = str(override)
    d.log("hello", log_type="test")

    assert override.exists(), "log() routed the write to the hook's override path"
    assert not baked.exists(), "baked path untouched when overridden"


def test_log_seam_no_override_uses_baked_path(tmp_path):
    baked = tmp_path / "baked.jsonl"
    d = _Redirecting(
        logger={"_ws": JsonLogger(file_path=str(baked), append=True)},
        debug_mode=True,
        always_add_logging_based_logger=False,
    )
    # _override stays None -> default behavior, baked path used.
    d.log("hello", log_type="test")
    assert baked.exists()


def test_default_debuggable_hook_is_noop():
    d = Debuggable(debug_mode=True, always_add_logging_based_logger=False)
    assert d._log_path_override("any", JsonLogger(file_path="/tmp/x.jsonl")) is None
