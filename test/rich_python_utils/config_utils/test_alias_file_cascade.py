"""Tests for alias-file cascade resolution in _resolve_import_.

When a non-Hydra key's value is a registered alias string, _resolve_import_
searches for a matching YAML file before falling back to plain string
shorthand expansion. Search order:
  1. ./field_name/AliasName.yaml  (field-hierarchy path)
  2. ./AliasName.yaml             (local to config dir)
  3. Leave as string              (shorthand expansion in _walk creates {_target_: Alias})
"""

import pytest
from pathlib import Path

from rich_python_utils.config_utils import load_config, instantiate
from rich_python_utils.config_utils._instantiate import _resolve_import_
from rich_python_utils.config_utils._registry import register_alias, _registry


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

from test.rich_python_utils.config_utils._cascade_fixtures import (
    MockBase, MockParent,
)

_FIXTURE_MOD = "test.rich_python_utils.config_utils._cascade_fixtures"


@pytest.fixture(autouse=True)
def _register_test_aliases():
    register_alias("TestBase", f"{_FIXTURE_MOD}.MockBase", "test")
    register_alias("TestParent", f"{_FIXTURE_MOD}.MockParent", "test")
    yield
    _registry.pop("TestBase", None)
    _registry.pop("TestParent", None)


@pytest.fixture
def cascade_dir(tmp_path):
    """Create a config directory with alias-named YAML files."""
    # Field-hierarchy path: base_inferencer/TestBase.yaml
    (tmp_path / "base_inferencer").mkdir()
    (tmp_path / "base_inferencer" / "TestBase.yaml").write_text(
        "_target_: TestBase\nmodel_name: from_field_dir\npermission_mode: bypass\n"
    )
    # Main config that uses the alias
    (tmp_path / "main.yaml").write_text(
        "_target_: TestParent\nbase_inferencer: TestBase\nmax_iterations: 30\n"
    )
    return tmp_path


# ---------------------------------------------------------------------------
# _resolve_import_ cascade tests
# ---------------------------------------------------------------------------


class TestAliasCascadeResolve:
    """Test _resolve_import_ alias-file cascade at the raw dict level."""

    def test_field_hierarchy_path_found(self, cascade_dir):
        """Alias resolved via ./field_name/AliasName.yaml."""
        node = {"base_inferencer": "TestBase", "max_iterations": 30}
        result = _resolve_import_(node, cascade_dir)
        assert isinstance(result["base_inferencer"], dict)
        assert result["base_inferencer"]["_target_"] == "TestBase"
        assert result["base_inferencer"]["model_name"] == "from_field_dir"

    def test_local_path_fallback(self, tmp_path):
        """If ./field_name/Alias.yaml doesn't exist, try ./Alias.yaml."""
        (tmp_path / "TestBase.yaml").write_text(
            "_target_: TestBase\nmodel_name: from_local\n"
        )
        node = {"child": "TestBase"}
        result = _resolve_import_(node, tmp_path)
        assert isinstance(result["child"], dict)
        assert result["child"]["model_name"] == "from_local"

    def test_no_file_leaves_string(self, tmp_path):
        """If no YAML file found, string stays for shorthand expansion."""
        node = {"child": "TestBase"}
        result = _resolve_import_(node, tmp_path)
        assert result["child"] == "TestBase"

    def test_non_alias_string_ignored(self, tmp_path):
        """Strings that aren't registered aliases are never searched."""
        (tmp_path / "NotAnAlias.yaml").write_text("_target_: Foo\n")
        node = {"child": "NotAnAlias"}
        result = _resolve_import_(node, tmp_path)
        assert result["child"] == "NotAnAlias"

    def test_hydra_keys_skipped(self, cascade_dir):
        """Keys starting with _ are not cascade-resolved."""
        node = {"_target_": "TestBase"}
        result = _resolve_import_(node, cascade_dir)
        assert result["_target_"] == "TestBase"

    def test_field_hierarchy_takes_precedence(self, tmp_path):
        """./field_name/Alias.yaml wins over ./Alias.yaml."""
        (tmp_path / "base_inferencer").mkdir()
        (tmp_path / "base_inferencer" / "TestBase.yaml").write_text(
            "_target_: TestBase\nmodel_name: from_field_dir\n"
        )
        (tmp_path / "TestBase.yaml").write_text(
            "_target_: TestBase\nmodel_name: from_local\n"
        )
        node = {"base_inferencer": "TestBase"}
        result = _resolve_import_(node, tmp_path)
        assert result["base_inferencer"]["model_name"] == "from_field_dir"

    def test_nested_cascade(self, tmp_path):
        """Cascade works at nested levels — field-hierarchy search applies recursively."""
        inner_dir = tmp_path / "inner"
        inner_dir.mkdir()
        (inner_dir / "TestBase.yaml").write_text(
            "_target_: TestBase\nmodel_name: nested_local\n"
        )
        node = {"outer": {"inner": "TestBase"}}
        result = _resolve_import_(node, tmp_path)
        # The recursive call processes "outer" dict with current_yaml_dir=tmp_path.
        # It finds tmp_path/inner/TestBase.yaml at the field-hierarchy path.
        assert isinstance(result["outer"]["inner"], dict)
        assert result["outer"]["inner"]["model_name"] == "nested_local"

    def test_cascade_in_loaded_yaml(self, tmp_path):
        """Cascade resolved via _import_ in a loaded file."""
        (tmp_path / "child").mkdir()
        (tmp_path / "child" / "TestBase.yaml").write_text(
            "_target_: TestBase\nmodel_name: imported_cascade\n"
        )
        (tmp_path / "parent.yaml").write_text(
            "wrapper:\n  _import_: child_config.yaml\n"
        )
        (tmp_path / "child_config.yaml").write_text(
            "child: TestBase\n"
        )
        cfg = load_config(str(tmp_path / "parent.yaml"))
        from omegaconf import OmegaConf
        d = OmegaConf.to_container(cfg, resolve=True)
        assert d["wrapper"]["child"]["model_name"] == "imported_cascade"


# ---------------------------------------------------------------------------
# End-to-end: load_config + instantiate with cascade
# ---------------------------------------------------------------------------


class TestAliasCascadeEndToEnd:
    """Full pipeline: YAML → load_config (cascade) → instantiate → objects."""

    def test_full_pipeline(self, cascade_dir):
        """load_config resolves cascade, instantiate creates objects."""
        cfg = load_config(str(cascade_dir / "main.yaml"))
        obj = instantiate(cfg)
        assert isinstance(obj, MockParent)
        assert isinstance(obj.base_inferencer, MockBase)
        assert obj.base_inferencer.model_name == "from_field_dir"
        assert obj.base_inferencer.permission_mode == "bypass"
        assert obj.max_iterations == 30

    def test_no_file_falls_back_to_class_defaults(self, tmp_path):
        """When no YAML file exists, shorthand creates class with defaults."""
        (tmp_path / "simple.yaml").write_text(
            "_target_: TestParent\nbase_inferencer: TestBase\n"
        )
        cfg = load_config(str(tmp_path / "simple.yaml"))
        obj = instantiate(cfg)
        assert isinstance(obj.base_inferencer, MockBase)
        assert obj.base_inferencer.model_name == "default"

    def test_override_after_cascade(self, cascade_dir):
        """Runtime overrides apply on top of cascade-resolved config."""
        cfg = load_config(str(cascade_dir / "main.yaml"))
        from omegaconf import OmegaConf
        d = OmegaConf.to_container(cfg, resolve=True)
        d["base_inferencer"]["model_name"] = "overridden"
        obj = instantiate(OmegaConf.create(d))
        assert obj.base_inferencer.model_name == "overridden"
        assert obj.base_inferencer.permission_mode == "bypass"


# ---------------------------------------------------------------------------
# _import_ .yaml extension fallback
# ---------------------------------------------------------------------------


class TestImportYamlExtensionFallback:
    """_import_: name tries name.yaml when name doesn't exist."""

    def test_import_without_extension(self, tmp_path):
        """_import_: child_config finds child_config.yaml."""
        (tmp_path / "child_config.yaml").write_text(
            "_target_: TestBase\nmodel_name: no_ext\n"
        )
        (tmp_path / "main.yaml").write_text(
            "child:\n  _import_: child_config\n"
        )
        cfg = load_config(str(tmp_path / "main.yaml"))
        from omegaconf import OmegaConf
        d = OmegaConf.to_container(cfg, resolve=True)
        assert d["child"]["_target_"] == "TestBase"
        assert d["child"]["model_name"] == "no_ext"

    def test_import_with_extension_still_works(self, tmp_path):
        """_import_: child_config.yaml still works (no regression)."""
        (tmp_path / "child_config.yaml").write_text(
            "_target_: TestBase\nmodel_name: with_ext\n"
        )
        (tmp_path / "main.yaml").write_text(
            "child:\n  _import_: child_config.yaml\n"
        )
        cfg = load_config(str(tmp_path / "main.yaml"))
        from omegaconf import OmegaConf
        d = OmegaConf.to_container(cfg, resolve=True)
        assert d["child"]["model_name"] == "with_ext"

    def test_import_missing_still_raises(self, tmp_path):
        """_import_: nonexistent raises FileNotFoundError."""
        (tmp_path / "main.yaml").write_text(
            "child:\n  _import_: nonexistent\n"
        )
        with pytest.raises(FileNotFoundError):
            load_config(str(tmp_path / "main.yaml"))


# ---------------------------------------------------------------------------
# Field-hierarchy directory narrowing
# ---------------------------------------------------------------------------


class TestDirectoryNarrowing:
    """_resolve_import_ narrows current_yaml_dir when subdirectories match keys."""

    def test_import_resolves_via_narrowed_dir(self, tmp_path):
        """_import_ inside nested keys searches narrowed directory."""
        (tmp_path / "outer").mkdir()
        (tmp_path / "outer" / "inner").mkdir()
        (tmp_path / "outer" / "inner" / "component.yaml").write_text(
            "_target_: TestBase\nmodel_name: narrowed\n"
        )
        (tmp_path / "main.yaml").write_text(
            "outer:\n  inner:\n    _import_: component\n"
        )
        cfg = load_config(str(tmp_path / "main.yaml"))
        from omegaconf import OmegaConf
        d = OmegaConf.to_container(cfg, resolve=True)
        assert d["outer"]["inner"]["_target_"] == "TestBase"
        assert d["outer"]["inner"]["model_name"] == "narrowed"

    def test_narrowing_with_sibling_override(self, tmp_path):
        """_import_ in narrowed dir with sibling override keys."""
        (tmp_path / "base").mkdir()
        (tmp_path / "base" / "planner").mkdir()
        (tmp_path / "base" / "planner" / "plan.yaml").write_text(
            "_target_: TestBase\nmodel_name: from_plan\npermission_mode: strict\n"
        )
        (tmp_path / "main.yaml").write_text(
            "base:\n  planner:\n    _import_: plan\n    permission_mode: relaxed\n"
        )
        cfg = load_config(str(tmp_path / "main.yaml"))
        from omegaconf import OmegaConf
        d = OmegaConf.to_container(cfg, resolve=True)
        assert d["base"]["planner"]["model_name"] == "from_plan"
        assert d["base"]["planner"]["permission_mode"] == "relaxed"

    def test_no_narrowing_when_dir_absent(self, tmp_path):
        """Without matching subdirectory, current_yaml_dir stays unchanged."""
        (tmp_path / "component.yaml").write_text(
            "_target_: TestBase\nmodel_name: from_root\n"
        )
        (tmp_path / "main.yaml").write_text(
            "outer:\n  inner:\n    _import_: component\n"
        )
        # No outer/ or outer/inner/ dirs — _import_ resolves from root
        cfg = load_config(str(tmp_path / "main.yaml"))
        from omegaconf import OmegaConf
        d = OmegaConf.to_container(cfg, resolve=True)
        assert d["outer"]["inner"]["model_name"] == "from_root"
