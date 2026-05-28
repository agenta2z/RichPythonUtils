"""Tests for master_version support in auto-discovery (resolve_from_content).

Verifies that when master_version is set, variables are resolved from
the <var_name>/<master_version>/ subdirectory before the flat path —
matching the behavior of load_variables(master_version=...).
"""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from rich_python_utils.common_objects.variable_manager.file_based import (
    FileBasedVariableManager,
)
from rich_python_utils.common_objects.variable_manager.config import (
    VariableManagerConfig,
)


def _make_manager(tmp_path: Path) -> FileBasedVariableManager:
    config = VariableManagerConfig(
        file_extensions=[".jinja2"],
        variables_folder_name="_variables",
    )
    return FileBasedVariableManager(base_path=str(tmp_path), config=config)


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


class TestMasterVersionAutoDiscovery(unittest.TestCase):

    def test_resolves_from_master_version_subdirectory(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write(
                root / "_variables" / "task_preamble" / "aggregation" / "default.jinja2",
                "You are aggregating upstream artifacts.",
            )
            _write(
                root / "_variables" / "task_preamble" / "default.jinja2",
                "Generic planning preamble.",
            )

            mgr = _make_manager(root)
            result = mgr.resolve_from_content(
                content="{{ task_preamble }}",
                variable_root_space="",
                variable_type="",
                version="",
                master_version="aggregation",
            )
            self.assertEqual(
                result.get("task_preamble"),
                "You are aggregating upstream artifacts.",
            )

    def test_falls_back_to_flat_when_master_version_subdir_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write(
                root / "_variables" / "task_preamble" / "default.jinja2",
                "Generic planning preamble.",
            )

            mgr = _make_manager(root)
            result = mgr.resolve_from_content(
                content="{{ task_preamble }}",
                variable_root_space="",
                variable_type="",
                version="",
                master_version="aggregation",
            )
            self.assertEqual(
                result.get("task_preamble"),
                "Generic planning preamble.",
            )

    def test_no_master_version_uses_flat_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write(
                root / "_variables" / "task_preamble" / "aggregation" / "default.jinja2",
                "Aggregation-specific.",
            )
            _write(
                root / "_variables" / "task_preamble" / "default.jinja2",
                "Generic.",
            )

            mgr = _make_manager(root)
            result = mgr.resolve_from_content(
                content="{{ task_preamble }}",
                variable_root_space="",
                variable_type="",
                version="",
                master_version=None,
            )
            self.assertEqual(result.get("task_preamble"), "Generic.")

    def test_master_version_with_version_specificity(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write(
                root / "_variables" / "task_instructions" / "aggregation" / "create_role.jinja2",
                "Role-specific aggregation instructions.",
            )
            _write(
                root / "_variables" / "task_instructions" / "aggregation" / "default.jinja2",
                "Default aggregation instructions.",
            )

            mgr = _make_manager(root)
            result = mgr.resolve_from_content(
                content="{{ task_instructions }}",
                variable_root_space="",
                variable_type="",
                version="create_role",
                master_version="aggregation",
            )
            self.assertEqual(
                result.get("task_instructions"),
                "Role-specific aggregation instructions.",
            )

    def test_master_version_default_when_version_not_found(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write(
                root / "_variables" / "task_instructions" / "aggregation" / "default.jinja2",
                "Default aggregation instructions.",
            )

            mgr = _make_manager(root)
            result = mgr.resolve_from_content(
                content="{{ task_instructions }}",
                variable_root_space="",
                variable_type="",
                version="nonexistent_version",
                master_version="aggregation",
            )
            self.assertEqual(
                result.get("task_instructions"),
                "Default aggregation instructions.",
            )

    def test_multiple_variables_with_master_version(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write(
                root / "_variables" / "task_preamble" / "aggregation" / "default.jinja2",
                "Aggregation preamble.",
            )
            _write(
                root / "_variables" / "task_instructions" / "aggregation" / "default.jinja2",
                "Aggregation instructions.",
            )
            _write(
                root / "_variables" / "other_var" / "default.jinja2",
                "Other variable (no aggregation subdir).",
            )

            mgr = _make_manager(root)
            result = mgr.resolve_from_content(
                content="{{ task_preamble }} {{ task_instructions }} {{ other_var }}",
                variable_root_space="",
                variable_type="",
                version="",
                master_version="aggregation",
            )
            self.assertEqual(result["task_preamble"], "Aggregation preamble.")
            self.assertEqual(result["task_instructions"], "Aggregation instructions.")
            self.assertEqual(result["other_var"], "Other variable (no aggregation subdir).")


class TestMasterVersionRecursiveComposition(unittest.TestCase):
    """Verify master_version propagates through variable-to-variable composition.

    This is the critical test: a composition file like
    context/user_request_with_task_preamble.jinja2 references {{ task_preamble }}.
    The recursive resolver must use master_version to route task_preamble to
    the aggregation/ subdirectory, not the flat default.
    """

    def test_nested_variable_uses_master_version(self):
        """Composition file references {{ task_preamble }}, which should
        resolve via master_version='aggregation' subdirectory."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write(
                root / "_variables" / "task_preamble" / "aggregation" / "default.jinja2",
                "AGGREGATION preamble content.",
            )
            _write(
                root / "_variables" / "task_preamble" / "default.jinja2",
                "GENERIC preamble content.",
            )
            _write(
                root / "_variables" / "context" / "user_request_with_task_preamble.jinja2",
                "{{ task_preamble }}\n\nUser request: {{ input }}",
            )

            mgr = _make_manager(root)
            result = mgr.resolve_from_content(
                content="{{ context.user_request_with_task_preamble }}",
                variable_root_space="",
                variable_type="",
                version="",
                master_version="aggregation",
            )
            # resolve_from_content returns flat keys (dot-notation)
            composed = result.get("context.user_request_with_task_preamble", "")
            self.assertIn("AGGREGATION preamble content", composed,
                          f"Expected aggregation preamble in composed result. Got: {result}")
            self.assertNotIn("GENERIC preamble content", composed)

    def test_nested_variable_without_master_version_uses_flat(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write(
                root / "_variables" / "task_preamble" / "aggregation" / "default.jinja2",
                "AGGREGATION preamble.",
            )
            _write(
                root / "_variables" / "task_preamble" / "default.jinja2",
                "GENERIC preamble.",
            )
            _write(
                root / "_variables" / "context" / "user_request_with_task_preamble.jinja2",
                "{{ task_preamble }}\n\nUser request: {{ input }}",
            )

            mgr = _make_manager(root)
            result = mgr.resolve_from_content(
                content="{{ context.user_request_with_task_preamble }}",
                variable_root_space="",
                variable_type="",
                version="",
                master_version=None,
            )
            composed = result.get("context.user_request_with_task_preamble", "")
            self.assertIn("GENERIC preamble", composed,
                          f"Expected generic preamble. Got: {result}")
            self.assertNotIn("AGGREGATION preamble", composed)


if __name__ == "__main__":
    unittest.main()
