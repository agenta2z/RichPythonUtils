# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

"""Tests for the ``{{ __super__ }}`` super-ref primitive in the file-based
variable resolver.

``{{ __super__ }}`` resolves, inside a variable file, to the same key one
cascade level down (the value this file shadows), turning override into
compose-on-override: append / prepend / wrap, chainable across cascade levels
and extension roots. These tests pin the composition semantics, graceful
degradation (missing base, top-level use), fail-loud behaviour on decorated
forms, and that ordinary self-references still trip cycle detection. They also
pin the ``extra_roots`` cascade precedence -- an extension root's shallow
(global-level) file outranks a base file at a deeper (space/type) level.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from rich_python_utils.common_objects.variable_manager.config import (
    VariableManagerConfig,
    VariableSyntax,
)
from rich_python_utils.common_objects.variable_manager.exceptions import (
    CircularReferenceError,
    SuperRefError,
)
from rich_python_utils.common_objects.variable_manager.file_based import (
    FileBasedVariableManager,
)

_FILE_BASED_LOGGER = "rich_python_utils.common_objects.variable_manager.file_based"


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def _make_manager(base: Path) -> FileBasedVariableManager:
    config = VariableManagerConfig(
        file_extensions=[".jinja2"],
        variables_folder_name="_variables",
        variable_syntax=VariableSyntax.JINJA2,
    )
    return FileBasedVariableManager(base_path=str(base), config=config)


class SuperRefCompositionTest(unittest.TestCase):
    """Author-positioned ``{{ __super__ }}`` composes against the shadowed value."""

    def test_super_appends_base_before_delta(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            _write(base / "_variables" / "greeting" / "default.jinja2", "BASE")
            _write(
                base
                / "myspace"
                / "main"
                / "_variables"
                / "greeting"
                / "default.jinja2",
                "{{ __super__ }}\nDERIVED",
            )
            result = _make_manager(base).resolve_from_content(
                content="{{ greeting }}",
                variable_root_space="myspace",
                variable_type="main",
            )
            self.assertEqual(result["greeting"], "BASE\nDERIVED")

    def test_super_prepends_delta_before_base(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            _write(base / "_variables" / "greeting" / "default.jinja2", "BASE")
            _write(
                base
                / "myspace"
                / "main"
                / "_variables"
                / "greeting"
                / "default.jinja2",
                "PREFACE\n{{ __super__ }}",
            )
            result = _make_manager(base).resolve_from_content(
                content="{{ greeting }}",
                variable_root_space="myspace",
                variable_type="main",
            )
            self.assertEqual(result["greeting"], "PREFACE\nBASE")

    def test_super_wraps_base_between_delta(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            _write(base / "_variables" / "greeting" / "default.jinja2", "BASE")
            _write(
                base
                / "myspace"
                / "main"
                / "_variables"
                / "greeting"
                / "default.jinja2",
                "PRE\n{{ __super__ }}\nPOST",
            )
            result = _make_manager(base).resolve_from_content(
                content="{{ greeting }}",
                variable_root_space="myspace",
                variable_type="main",
            )
            self.assertEqual(result["greeting"], "PRE\nBASE\nPOST")

    def test_super_chains_across_three_cascade_levels(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            _write(base / "_variables" / "greeting" / "default.jinja2", "A")
            _write(
                base / "myspace" / "_variables" / "greeting" / "default.jinja2",
                "{{ __super__ }}\nB",
            )
            _write(
                base
                / "myspace"
                / "main"
                / "_variables"
                / "greeting"
                / "default.jinja2",
                "{{ __super__ }}\nC",
            )
            result = _make_manager(base).resolve_from_content(
                content="{{ greeting }}",
                variable_root_space="myspace",
                variable_type="main",
            )
            self.assertEqual(result["greeting"], "A\nB\nC")

    def test_super_composes_across_extra_root_and_base(self) -> None:
        with (
            tempfile.TemporaryDirectory() as tmp_base,
            tempfile.TemporaryDirectory() as tmp_ext,
        ):
            base = Path(tmp_base)
            ext = Path(tmp_ext)
            _write(base / "_variables" / "greeting" / "default.jinja2", "BASE")
            _write(
                ext / "_variables" / "greeting" / "default.jinja2",
                "{{ __super__ }}\nEXT",
            )
            result = _make_manager(base).resolve_from_content(
                content="{{ greeting }}",
                extra_roots=[ext],
            )
            self.assertEqual(result["greeting"], "BASE\nEXT")


class SuperRefDegradationTest(unittest.TestCase):
    """Exhausted / top-level supers degrade to ``""`` rather than erroring."""

    def test_super_with_no_base_resolves_to_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            _write(
                base
                / "myspace"
                / "main"
                / "_variables"
                / "greeting"
                / "default.jinja2",
                "X{{ __super__ }}",
            )
            result = _make_manager(base).resolve_from_content(
                content="{{ greeting }}",
                variable_root_space="myspace",
                variable_type="main",
            )
            self.assertEqual(result["greeting"], "X")

    def test_super_chain_terminates_when_bottom_layer_also_supers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            _write(
                base / "_variables" / "greeting" / "default.jinja2",
                "{{ __super__ }}B",
            )
            _write(
                base
                / "myspace"
                / "main"
                / "_variables"
                / "greeting"
                / "default.jinja2",
                "{{ __super__ }}T",
            )
            result = _make_manager(base).resolve_from_content(
                content="{{ greeting }}",
                variable_root_space="myspace",
                variable_type="main",
            )
            self.assertEqual(result["greeting"], "BT")

    def test_top_level_super_resolves_to_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = _make_manager(Path(tmp))._resolve_content(
                "{{ __super__ }}", "", "", "", []
            )
            self.assertEqual(resolved, "")

    def test_top_level_super_logs_warning(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mgr = _make_manager(Path(tmp))
            with self.assertLogs(_FILE_BASED_LOGGER, level="WARNING"):
                mgr._resolve_content("{{ __super__ }}", "", "", "", [])


class SuperRefFailLoudTest(unittest.TestCase):
    """Decorated ``{{ __super__ }}`` forms are an authoring error -> raise."""

    def test_caret_scoped_super_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mgr = _make_manager(Path(tmp))
            with self.assertRaises(SuperRefError):
                mgr._resolve_content(
                    "^{{ __super__ }}", "myspace", "main", "", ["greeting"]
                )

    def test_dot_scoped_super_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mgr = _make_manager(Path(tmp))
            with self.assertRaises(SuperRefError):
                mgr._resolve_content(
                    ".{{ __super__ }}", "myspace", "main", "", ["greeting"]
                )

    def test_optional_super_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mgr = _make_manager(Path(tmp))
            with self.assertRaises(SuperRefError):
                mgr._resolve_content(
                    "{{ __super__ }}?", "myspace", "main", "", ["greeting"]
                )


class SuperRefAuthoringWarningTest(unittest.TestCase):
    """Two bare supers in one file duplicate rather than compose -> warn."""

    def test_duplicate_bare_super_logs_warning(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            greeting_file = base / "_variables" / "greeting" / "default.jinja2"
            _write(greeting_file, "BASE")
            mgr = _make_manager(base)
            with self.assertLogs(_FILE_BASED_LOGGER, level="WARNING"):
                mgr._resolve_content(
                    "{{ __super__ }}\n{{ __super__ }}",
                    "",
                    "",
                    "",
                    ["greeting"],
                    current_file_path=greeting_file,
                )


class SuperRefCycleGuardTest(unittest.TestCase):
    """A normal same-key self-reference still trips cycle detection."""

    def test_ordinary_self_reference_raises_circular(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            _write(
                base / "_variables" / "greeting" / "default.jinja2",
                "{{ greeting }}",
            )
            mgr = _make_manager(base)
            with self.assertRaises(CircularReferenceError):
                mgr.resolve_from_content(content="{{ greeting }}")


class ExtraRootCascadeLevelTest(unittest.TestCase):
    """An ``extra_roots`` entry's shallow file outranks a base file at a deeper
    cascade level, so a per-inferencer override wins for that key even when the
    base ships the value only at a more-specific space/type level (plan section 7).
    """

    def test_base_resolves_space_specific_level_without_extra_root(self) -> None:
        # Precondition: the base cascade itself resolves a value shipped only at
        # the deep space/type level (isolates the cascade from template lookup).
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            _write(
                base
                / "myspace"
                / "main"
                / "_variables"
                / "greeting"
                / "default.jinja2",
                "BASE_SPECIFIC",
            )
            result = _make_manager(base).resolve_from_content(
                content="{{ greeting }}",
                variable_root_space="myspace",
                variable_type="main",
            )
            self.assertEqual(result["greeting"], "BASE_SPECIFIC")

    def test_extra_root_global_level_beats_base_space_specific_level(self) -> None:
        with (
            tempfile.TemporaryDirectory() as tmp_base,
            tempfile.TemporaryDirectory() as tmp_ext,
        ):
            base = Path(tmp_base)
            ext = Path(tmp_ext)
            _write(
                base
                / "myspace"
                / "main"
                / "_variables"
                / "greeting"
                / "default.jinja2",
                "BASE_SPECIFIC",
            )
            _write(
                ext / "_variables" / "greeting" / "default.jinja2",
                "EXT_GLOBAL",
            )
            result = _make_manager(base).resolve_from_content(
                content="{{ greeting }}",
                variable_root_space="myspace",
                variable_type="main",
                extra_roots=[ext],
            )
            self.assertEqual(result["greeting"], "EXT_GLOBAL")
