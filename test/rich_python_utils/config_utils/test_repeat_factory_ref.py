"""Tests for _repeat_, _factory_, and $ref config directives."""

import pytest
from omegaconf import OmegaConf

from rich_python_utils.config_utils import load_config
from rich_python_utils.config_utils._instantiate import (
    _resolve_repeat_,
    _resolve_sibling_refs,
    _resolve_factory_directives,
)


# ---------------------------------------------------------------------------
# _repeat_: N — basic expansion
# ---------------------------------------------------------------------------


class TestRepeatBasic:
    def test_repeat_expands_list(self, tmp_path):
        (tmp_path / "main.yaml").write_text("items:\n  - _repeat_: 3\n    value: hello\n")
        cfg = load_config(str(tmp_path / "main.yaml"))
        d = OmegaConf.to_container(cfg, resolve=True)
        assert len(d["items"]) == 3
        assert all(item == {"value": "hello"} for item in d["items"])

    def test_repeat_deep_copies(self, tmp_path):
        (tmp_path / "main.yaml").write_text("items:\n  - _repeat_: 2\n    nested:\n      x: 1\n")
        cfg = load_config(str(tmp_path / "main.yaml"))
        d = OmegaConf.to_container(cfg, resolve=True)
        assert d["items"][0] is not d["items"][1]
        assert d["items"][0] == d["items"][1]

    def test_repeat_strips_key(self, tmp_path):
        (tmp_path / "main.yaml").write_text("items:\n  - _repeat_: 2\n    v: 1\n")
        cfg = load_config(str(tmp_path / "main.yaml"))
        d = OmegaConf.to_container(cfg, resolve=True)
        assert "_repeat_" not in d["items"][0]

    def test_repeat_mixed_with_normal(self, tmp_path):
        (tmp_path / "main.yaml").write_text(
            "items:\n  - name: static\n  - _repeat_: 2\n    name: repeated\n"
        )
        cfg = load_config(str(tmp_path / "main.yaml"))
        d = OmegaConf.to_container(cfg, resolve=True)
        assert len(d["items"]) == 3
        assert d["items"][0]["name"] == "static"
        assert d["items"][1]["name"] == "repeated"

    def test_repeat_one_is_identity(self, tmp_path):
        (tmp_path / "main.yaml").write_text("items:\n  - _repeat_: 1\n    v: x\n")
        cfg = load_config(str(tmp_path / "main.yaml"))
        d = OmegaConf.to_container(cfg, resolve=True)
        assert len(d["items"]) == 1
        assert d["items"][0] == {"v": "x"}


# ---------------------------------------------------------------------------
# _repeat_ distribution (list + dict)
# ---------------------------------------------------------------------------


class TestRepeatDistribution:
    def test_list_distribution(self):
        node = [{"_repeat_": 3, "inf": {"_target_": ["A", "B", "C"]}}]
        result = _resolve_repeat_(node)
        assert len(result) == 3
        assert [r["inf"]["_target_"] for r in result] == ["A", "B", "C"]

    def test_dict_distribution(self):
        node = [{"_repeat_": 3, "inf": {"_target_": {"X": 2, "Y": 1}}}]
        result = _resolve_repeat_(node)
        targets = [r["inf"]["_target_"] for r in result]
        assert targets.count("X") == 2
        assert targets.count("Y") == 1

    def test_list_shorter_than_repeat_pads_with_first(self):
        # graceful: a distribution list shorter than _repeat_ pads with item[0]
        node = [{"_repeat_": 3, "val": [1, 2]}]
        result = _resolve_repeat_(node)
        assert [r["val"] for r in result] == [1, 2, 1]

    def test_list_longer_than_repeat_truncates(self):
        # graceful: a distribution list longer than _repeat_ takes the first n
        node = [{"_repeat_": 2, "val": [1, 2, 3]}]
        result = _resolve_repeat_(node)
        assert [r["val"] for r in result] == [1, 2]

    def test_single_element_list_broadcasts(self):
        node = [{"_repeat_": 3, "val": ["X"]}]
        result = _resolve_repeat_(node)
        assert [r["val"] for r in result] == ["X", "X", "X"]

    def test_dict_count_mismatch_not_distributed(self):
        """Dict with counts not summing to N is treated as regular dict (not distributed)."""
        node = [{"_repeat_": 3, "val": {"a": 1, "b": 1}}]
        result = _resolve_repeat_(node)
        assert len(result) == 3
        assert result[0]["val"] == {"a": 1, "b": 1}  # kept as-is

    def test_scalar_not_distributed(self):
        node = [{"_repeat_": 2, "v": "same"}]
        result = _resolve_repeat_(node)
        assert result[0]["v"] == "same"
        assert result[1]["v"] == "same"

    def test_zip_effect(self):
        node = [{"_repeat_": 2, "a": ["X", "Y"], "b": ["X", "Y"]}]
        result = _resolve_repeat_(node)
        assert result[0]["a"] == "X" and result[0]["b"] == "X"
        assert result[1]["a"] == "Y" and result[1]["b"] == "Y"


# ---------------------------------------------------------------------------
# $ref sibling references
# ---------------------------------------------------------------------------


class TestSiblingRef:
    def test_same_level_sibling(self):
        node = {"x": "hello", "y": "$x"}
        result = _resolve_sibling_refs(node)
        assert result["y"] == "hello"

    def test_parent_level_lookup(self):
        node = {
            "initial_inferencer": {"_target_": "Foo"},
            "followup_inferencer": {"_target_": "$initial_inferencer"},
        }
        result = _resolve_sibling_refs(node)
        assert result["followup_inferencer"]["_target_"] == "Foo"

    def test_does_not_touch_omegaconf_interpolation(self):
        node = {"x": "${some.path}"}
        result = _resolve_sibling_refs(node)
        assert result["x"] == "${some.path}"

    def test_unresolvable_left_as_is(self):
        node = {"x": "$nonexistent"}
        result = _resolve_sibling_refs(node)
        assert result["x"] == "$nonexistent"

    def test_deep_copy_prevents_sharing(self):
        node = {
            "a": {"nested": [1, 2]},
            "b": "$a",
        }
        result = _resolve_sibling_refs(node)
        assert result["b"] == {"nested": [1, 2]}
        result["b"]["nested"].append(3)
        assert result["a"]["nested"] == [1, 2]  # not shared


# ---------------------------------------------------------------------------
# $ref in list items (parent-dict sibling lookup)
# ---------------------------------------------------------------------------


class TestSiblingRefInList:
    """$ref strings inside list items resolve against the parent dict's siblings."""

    def test_list_item_resolves_parent_sibling(self):
        node = {
            "main_inferencer": "RovoDevCLI",
            "flow_inferencers": ["$main_inferencer", "$main_inferencer"],
        }
        result = _resolve_sibling_refs(node)
        assert result["flow_inferencers"] == ["RovoDevCLI", "RovoDevCLI"]

    def test_list_item_nested_in_params(self):
        node = {
            "_params": {
                "default_inferencer": "ClaudeCodeCLI",
                "main_inferencer": "RovoDevCLI",
                "flow_inferencers": ["$main_inferencer", "$main_inferencer"],
            },
        }
        result = _resolve_sibling_refs(node)
        assert result["_params"]["flow_inferencers"] == ["RovoDevCLI", "RovoDevCLI"]

    def test_list_item_mixed_ref_and_literal(self):
        node = {
            "x": "hello",
            "items": ["$x", "literal", "$x"],
        }
        result = _resolve_sibling_refs(node)
        assert result["items"] == ["hello", "literal", "hello"]

    def test_list_item_unresolvable_left_as_is(self):
        node = {"items": ["$nonexistent", "plain"]}
        result = _resolve_sibling_refs(node)
        assert result["items"] == ["$nonexistent", "plain"]

    def test_list_item_omegaconf_interpolation_not_touched(self):
        node = {
            "x": "resolved",
            "items": ["${_params.x}", "$x"],
        }
        result = _resolve_sibling_refs(node)
        assert result["items"][0] == "${_params.x}"  # OmegaConf syntax untouched
        assert result["items"][1] == "resolved"       # $ref resolved

    def test_list_item_deep_copy_prevents_sharing(self):
        node = {
            "template": {"key": "value"},
            "copies": ["$template", "$template"],
        }
        result = _resolve_sibling_refs(node)
        assert result["copies"][0] == {"key": "value"}
        assert result["copies"][1] == {"key": "value"}
        result["copies"][0]["key"] = "mutated"
        assert result["copies"][1]["key"] == "value"  # independent copy
        assert result["template"]["key"] == "value"   # original untouched

    def test_list_item_walks_up_scope_chain(self):
        """$ref in a list nested 2+ levels deep still finds ancestors."""
        node = {
            "root_val": "found_it",
            "level1": {
                "level2": {
                    "items": ["$root_val"],
                },
            },
        }
        result = _resolve_sibling_refs(node)
        assert result["level1"]["level2"]["items"] == ["found_it"]

    def test_list_item_with_dict_items_recurse(self):
        """Non-$ref dict items inside lists still recurse normally."""
        node = {
            "x": "hello",
            "items": [
                {"val": "$x"},
                "$x",
            ],
        }
        result = _resolve_sibling_refs(node)
        assert result["items"][0]["val"] == "hello"
        assert result["items"][1] == "hello"

    def test_list_item_ref_resolves_to_dict_field_mirror(self):
        """When referent is a dict, list items get the full dict (no field mirroring)."""
        node = {
            "base": {"_target_": "Foo", "extra": 1},
            "items": ["$base"],
        }
        result = _resolve_sibling_refs(node)
        assert result["items"][0] == {"_target_": "Foo", "extra": 1}

    def test_end_to_end_with_load_config(self, tmp_path):
        """$ref in list items works through the full load_config pipeline."""
        (tmp_path / "main.yaml").write_text(
            "_params:\n"
            "  default_inferencer: ClaudeCodeCLI\n"
            "  main_inferencer: Override\n"
            "  flow_inferencers:\n"
            "    - $main_inferencer\n"
            "    - $main_inferencer\n"
        )
        cfg = load_config(str(tmp_path / "main.yaml"))
        d = OmegaConf.to_container(cfg, resolve=True)
        assert d["_params"]["flow_inferencers"] == ["Override", "Override"]

    def test_ref_resolved_before_omegaconf_copies(self, tmp_path):
        """$ref in _params list resolves for direct access, not via ${} + _repeat_.

        Pipeline order: OmegaConf.resolve → _repeat_ → $ref.
        OmegaConf copies $ref strings BEFORE they're resolved, so distributed
        copies land in a new scope where the ref name isn't found.
        For _repeat_ distribution, use OmegaConf interpolation instead.
        """
        (tmp_path / "main.yaml").write_text(
            "_params:\n"
            "  inf_a: Alpha\n"
            "  inf_b: Beta\n"
            "  via_ref:\n"
            "    - $inf_a\n"
            "    - $inf_b\n"
            "  via_omegaconf:\n"
            "    - ${_params.inf_a}\n"
            "    - ${_params.inf_b}\n"
            "flows:\n"
            "  - _repeat_: 2\n"
            "    worker:\n"
            "      _target_: ${_params.via_omegaconf}\n"
        )
        cfg = load_config(str(tmp_path / "main.yaml"))
        d = OmegaConf.to_container(cfg, resolve=True)
        # $ref resolves in _params (direct access)
        assert d["_params"]["via_ref"] == ["Alpha", "Beta"]
        # OmegaConf interpolation works through _repeat_ distribution
        assert d["flows"][0]["worker"]["_target_"] == "Alpha"
        assert d["flows"][1]["worker"]["_target_"] == "Beta"


# ---------------------------------------------------------------------------
# _factory_: directive
# ---------------------------------------------------------------------------


class TestFactoryDirective:
    def test_converts_to_target(self):
        node = {"worker": {"_factory_": "Foo", "x": 1}}
        result = _resolve_factory_directives(node)
        assert result["worker"]["_target_"] == "Foo"
        assert "_factory_" not in result["worker"]
        assert result["worker"]["x"] == 1

    def test_preserves_boolean_factory_marker(self):
        node = {"child": {"_factory_": True, "_target_": "Bar"}}
        result = _resolve_factory_directives(node)
        assert result["child"]["_factory_"] is True
        assert result["child"]["_target_"] == "Bar"

    def test_nested_factory(self):
        node = {
            "parent": {
                "worker_factory": {
                    "_factory_": "Multi",
                    "config": {"_target_": "Inner"},
                }
            }
        }
        result = _resolve_factory_directives(node)
        wf = result["parent"]["worker_factory"]
        assert wf["_target_"] == "Multi"
        assert wf["config"]["_target_"] == "Inner"

    def test_factory_in_load_config(self, tmp_path):
        (tmp_path / "main.yaml").write_text(
            "worker_factory:\n  _factory_: MyClass\n  value: 42\n"
        )
        cfg = load_config(str(tmp_path / "main.yaml"))
        d = OmegaConf.to_container(cfg, resolve=True)
        assert d["worker_factory"]["_target_"] == "MyClass"
        assert d["worker_factory"]["value"] == 42
        assert "_factory_" not in d["worker_factory"]


# ---------------------------------------------------------------------------
# _FalsyChainableUndefined
# ---------------------------------------------------------------------------


class TestFalsyChainableUndefined:
    def test_undefined_is_falsy_in_if(self):
        from rich_python_utils.string_utils.formatting.jinja2_format import (
            _FalsyChainableUndefined,
        )
        from jinja2 import Environment

        env = Environment(undefined=_FalsyChainableUndefined)
        t = env.from_string("{% if flag %}YES{% endif %}")
        assert t.render() == ""

    def test_defined_true_renders(self):
        from rich_python_utils.string_utils.formatting.jinja2_format import (
            _FalsyChainableUndefined,
        )
        from jinja2 import Environment

        env = Environment(undefined=_FalsyChainableUndefined)
        t = env.from_string("{% if flag %}YES{% endif %}")
        assert t.render(flag=True) == "YES"

    def test_chained_access_no_crash(self):
        from rich_python_utils.string_utils.formatting.jinja2_format import (
            _FalsyChainableUndefined,
        )
        from jinja2 import Environment

        env = Environment(undefined=_FalsyChainableUndefined)
        t = env.from_string("{{ a.b.c }}")
        assert t.render() == ""

    def test_chained_with_value(self):
        from rich_python_utils.string_utils.formatting.jinja2_format import (
            _FalsyChainableUndefined,
        )
        from jinja2 import Environment

        env = Environment(undefined=_FalsyChainableUndefined)
        t = env.from_string("{{ a.b }}")
        assert t.render(a={"b": "found"}) == "found"
