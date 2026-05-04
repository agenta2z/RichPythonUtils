"""Tests for TemplateManager.load_variable two-pass version search.

Covers Refactor 12 changes to load_variable:
- Per-folder version convention (``<var_name>/<version>.<ext>``)
- ``.config.yaml`` alias map (version -> filename)
- Pass 2 default fallback when version is missing
- Returns None (not empty string) when nothing matches
"""

from pathlib import Path

import pytest

from rich_python_utils.string_utils.formatting.template_manager import (
    TemplateManager,
)


@pytest.fixture
def templates_dir(tmp_path):
    """Build a minimal template tree used by load_variable.

    Layout::
        <root>/<root_space>/<tmpl_type>/_variables/<var_name>/<file>
    """
    root_space = tmp_path / "plan"
    main_dir = root_space / "main"
    vars_dir = main_dir / "_variables"
    main_dir.mkdir(parents=True)
    vars_dir.mkdir()
    # Required for TemplateManager: at least one file at the active template type level
    (main_dir / "default.j2").write_text("dummy", encoding="utf-8")
    return tmp_path


def _make_manager(templates_dir, root_space="plan"):
    return TemplateManager(
        templates=str(templates_dir),
        active_template_type="main",
        active_template_root_space=root_space,
    )


class TestPerFolderVersionConvention:
    def test_load_variable_per_folder_convention(self, templates_dir):
        # plan/main/_variables/task_preamble/aggregation.jinja2
        var_folder = (
            templates_dir / "plan" / "main" / "_variables" / "task_preamble"
        )
        var_folder.mkdir(parents=True)
        (var_folder / "aggregation.jinja2").write_text(
            "aggregation preamble", encoding="utf-8"
        )

        tm = _make_manager(templates_dir)
        result = tm.load_variable("task_preamble", "aggregation", "plan")
        assert result == "aggregation preamble"

    def test_load_variable_default_fallback(self, templates_dir):
        # Folder has only default.jinja2 -- caller asks for "aggregation"
        var_folder = (
            templates_dir / "plan" / "main" / "_variables" / "task_instructions"
        )
        var_folder.mkdir(parents=True)
        (var_folder / "default.jinja2").write_text(
            "default instructions", encoding="utf-8"
        )

        tm = _make_manager(templates_dir)
        result = tm.load_variable("task_instructions", "aggregation", "plan")
        assert result == "default instructions"

    def test_load_variable_config_yaml_alias(self, templates_dir):
        var_folder = (
            templates_dir / "plan" / "main" / "_variables" / "task_preamble"
        )
        var_folder.mkdir(parents=True)
        (var_folder / "agg_v2.jinja2").write_text(
            "aliased content", encoding="utf-8"
        )
        (var_folder / ".config.yaml").write_text(
            "aggregation: agg_v2\n", encoding="utf-8"
        )

        tm = _make_manager(templates_dir)
        result = tm.load_variable("task_preamble", "aggregation", "plan")
        assert result == "aliased content"

    def test_load_variable_default_alias(self, templates_dir):
        var_folder = (
            templates_dir / "plan" / "main" / "_variables" / "task_preamble"
        )
        var_folder.mkdir(parents=True)
        (var_folder / "generic.jinja2").write_text(
            "generic default", encoding="utf-8"
        )
        (var_folder / ".config.yaml").write_text(
            "default: generic\n", encoding="utf-8"
        )

        tm = _make_manager(templates_dir)
        # Caller asks for missing version -> should fall back to default alias
        result = tm.load_variable("task_preamble", "aggregation", "plan")
        assert result == "generic default"


class TestNotFound:
    def test_returns_none_when_nothing_found(self, templates_dir):
        var_folder = (
            templates_dir / "plan" / "main" / "_variables" / "task_preamble"
        )
        var_folder.mkdir(parents=True)
        # Empty folder

        tm = _make_manager(templates_dir)
        result = tm.load_variable("task_preamble", "aggregation", "plan")
        assert result is None, (
            "load_variable must return None (not empty string) when nothing matches"
        )

    def test_returns_none_when_folder_missing(self, templates_dir):
        # No _variables/task_preamble folder at all
        tm = _make_manager(templates_dir)
        result = tm.load_variable("task_preamble", "aggregation", "plan")
        assert result is None

    def test_returns_none_when_version_empty(self, templates_dir):
        # Even with default.jinja2 present, version="" should return None
        var_folder = (
            templates_dir / "plan" / "main" / "_variables" / "task_preamble"
        )
        var_folder.mkdir(parents=True)
        (var_folder / "default.jinja2").write_text(
            "default content", encoding="utf-8"
        )

        tm = _make_manager(templates_dir)
        result = tm.load_variable("task_preamble", "", "plan")
        assert result is None


class TestRootSpaceCascade:
    def test_root_space_argument_overrides_active(self, templates_dir):
        # Build version file under "plan" root space
        plan_folder = (
            templates_dir / "plan" / "main" / "_variables" / "task_preamble"
        )
        plan_folder.mkdir(parents=True)
        (plan_folder / "aggregation.jinja2").write_text(
            "plan content", encoding="utf-8"
        )
        # Build a different version under a different root space
        impl_dir = templates_dir / "implementation" / "main"
        impl_dir.mkdir(parents=True)
        (impl_dir / "default.j2").write_text("dummy", encoding="utf-8")
        impl_folder = impl_dir / "_variables" / "task_preamble"
        impl_folder.mkdir(parents=True)
        (impl_folder / "aggregation.jinja2").write_text(
            "impl content", encoding="utf-8"
        )

        # Manager active = "plan" but caller passes root_space="implementation"
        tm = _make_manager(templates_dir, root_space="plan")
        result = tm.load_variable("task_preamble", "aggregation", "implementation")
        assert result == "impl content"


class TestEmptyFileSemantics:
    def test_empty_file_returns_empty_string_not_none(self, templates_dir):
        # Empty file is found but content is empty -- return "" not None
        var_folder = (
            templates_dir / "plan" / "main" / "_variables" / "task_preamble"
        )
        var_folder.mkdir(parents=True)
        (var_folder / "aggregation.jinja2").write_text("", encoding="utf-8")

        tm = _make_manager(templates_dir)
        result = tm.load_variable("task_preamble", "aggregation", "plan")
        assert result == "", (
            "Empty file should return '' to distinguish from None (not-found)"
        )
