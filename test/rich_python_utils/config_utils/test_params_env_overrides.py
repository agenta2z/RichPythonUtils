"""Tests for the ``_params`` environment-override loader feature.

Every key under ``_params`` is overridable by ``<PREFIX>__<KEY>`` (upper-case).
PREFIX comes from an explicit ``env_prefix`` (in ``_params`` or top-level) or,
if absent, the parent-of-``configs`` folder name. Values are coerced to each
key's default type; lists accept comma-separated or JSON; CLI ``overrides=``
still win (merged after env).
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from omegaconf import OmegaConf

from rich_python_utils.config_utils._instantiate import (
    _coerce_env_value,
    _parse_env_list,
    _resolve_env_prefix,
    load_config,
)


# --------------------------------------------------------------------------- #
# Coercion units
# --------------------------------------------------------------------------- #

class TestCoercion:
    @pytest.mark.parametrize("raw,default,expected", [
        ("5", 0, 5),                  # int default → int
        ("3.5", 0.0, 3.5),            # float default → float
        ("false", True, False),       # bool default → bool
        ("on", False, True),
        ("hello world", "x", "hello world"),  # str default → verbatim
        ("3", "${ref}", 3),           # interpolation default → generic (int)
        ("DevmateCLI", "${ref}", "DevmateCLI"),  # generic → string
    ])
    def test_scalar_coercion(self, raw, default, expected):
        assert _coerce_env_value(raw, default) == expected
        assert type(_coerce_env_value(raw, default)) is type(expected)

    def test_bool_default_checked_before_int(self):
        # bool is an int subclass; a bool default must NOT int()-parse.
        assert _coerce_env_value("true", False) is True

    def test_invalid_bool_raises(self):
        with pytest.raises(ValueError):
            _coerce_env_value("maybe", True)

    @pytest.mark.parametrize("raw,expected", [
        ("a,b,c", ["a", "b", "c"]),
        ("a, b , c", ["a", "b", "c"]),
        ('["x","y"]', ["x", "y"]),
        ("[x, y]", ["x", "y"]),
        ("1,2,3", [1, 2, 3]),         # elements scalar-coerced
        ("solo", ["solo"]),
    ])
    def test_list_parsing(self, raw, expected):
        assert _parse_env_list(raw) == expected

    def test_list_default_routes_to_list(self):
        assert _coerce_env_value("a,b", ["default"]) == ["a", "b"]


# --------------------------------------------------------------------------- #
# Prefix resolution
# --------------------------------------------------------------------------- #

class TestPrefix:
    def test_explicit_in_params_wins_and_is_popped(self):
        c = {"_params": {"env_prefix": "myapp", "x": 1}}
        assert _resolve_env_prefix(c, "/whatever/x.yaml") == "MYAPP"
        assert "env_prefix" not in c["_params"]

    def test_top_level_explicit(self):
        c = {"env_prefix": "Top", "_params": {"x": 1}}
        assert _resolve_env_prefix(c, "/x/x.yaml") == "TOP"
        assert "env_prefix" not in c

    def test_empty_prefix_disables(self):
        c = {"_params": {"env_prefix": "", "x": 1}}
        assert _resolve_env_prefix(c, "/a/configs/x.yaml") is None

    def test_derived_from_configs_folder(self):
        assert _resolve_env_prefix({"_params": {}}, "/root/task/configs/x.yaml") == "TASK"

    def test_no_prefix_when_not_in_configs(self):
        assert _resolve_env_prefix({"_params": {}}, "/root/elsewhere/x.yaml") is None

    def test_external_override_wins_over_config_and_folder(self):
        # externally-passed prefix beats both the in-config env_prefix AND folder
        c = {"_params": {"env_prefix": "TASK"}}
        assert _resolve_env_prefix(c, "/root/task/configs/x.yaml", "RESEARCH_PROPOSE") == "RESEARCH_PROPOSE"
        # ...and the in-config marker is still stripped
        assert "env_prefix" not in c["_params"]

    def test_external_override_uppercased(self):
        assert _resolve_env_prefix({"_params": {}}, "/x/x.yaml", "research_propose") == "RESEARCH_PROPOSE"


# --------------------------------------------------------------------------- #
# End-to-end via load_config on a temp YAML
# --------------------------------------------------------------------------- #

@pytest.fixture
def cfg_file(tmp_path):
    d = tmp_path / "task" / "configs"
    d.mkdir(parents=True)
    f = d / "demo.yaml"
    f.write_text(textwrap.dedent("""
        _params:
          env_prefix: TASK
          main_inferencer: ClaudeCodeCLI
          flow_inferencers:
            - ${_params.main_inferencer}
            - ${_params.main_inferencer}
          num_flows: ${len:${_params.flow_inferencers}}
          plan_max_breakdown: 3
          enable_deep_mode: true
        out:
          model: ${_params.main_inferencer}
          n: ${_params.num_flows}
          breakdown: ${_params.plan_max_breakdown}
          deep: ${_params.enable_deep_mode}
          flows: ${_params.flow_inferencers}
    """))
    return f


@pytest.fixture
def clean_env(monkeypatch):
    for v in ("TASK__MAIN_INFERENCER", "TASK__FLOW_INFERENCERS", "TASK__NUM_FLOWS",
              "TASK__PLAN_MAX_BREAKDOWN", "TASK__ENABLE_DEEP_MODE"):
        monkeypatch.delenv(v, raising=False)
    return monkeypatch


def _out(f, overrides=None):
    return OmegaConf.to_container(load_config(str(f), overrides=overrides), resolve=True)["out"]


def test_defaults(cfg_file, clean_env):
    out = _out(cfg_file)
    assert out == {"model": "ClaudeCodeCLI", "n": 2, "breakdown": 3, "deep": True,
                   "flows": ["ClaudeCodeCLI", "ClaudeCodeCLI"]}

def test_env_prefix_stripped(cfg_file, clean_env):
    c = OmegaConf.to_container(load_config(str(cfg_file)), resolve=True)
    assert "env_prefix" not in c["_params"]

def test_scalar_override(cfg_file, clean_env):
    clean_env.setenv("TASK__MAIN_INFERENCER", "DevmateCLI")
    clean_env.setenv("TASK__PLAN_MAX_BREAKDOWN", "9")
    out = _out(cfg_file)
    assert out["model"] == "DevmateCLI" and out["breakdown"] == 9
    assert out["flows"] == ["DevmateCLI", "DevmateCLI"]  # propagates via ${main_inferencer}

def test_bool_override(cfg_file, clean_env):
    clean_env.setenv("TASK__ENABLE_DEEP_MODE", "false")
    assert _out(cfg_file)["deep"] is False

def test_list_override_drives_derived_count(cfg_file, clean_env):
    """Single env var sets the whole list; num_flows (derived) follows."""
    clean_env.setenv("TASK__FLOW_INFERENCERS", "A,B,C")
    out = _out(cfg_file)
    assert out["flows"] == ["A", "B", "C"] and out["n"] == 3

def test_cli_override_beats_env(cfg_file, clean_env):
    clean_env.setenv("TASK__PLAN_MAX_BREAKDOWN", "9")
    out = _out(cfg_file, overrides={"_params.plan_max_breakdown": 99})
    assert out["breakdown"] == 99

def test_external_env_prefix_gives_own_namespace(cfg_file, clean_env):
    """A caller-passed env_prefix (e.g. a derived tool) gives a SHARED config its
    own env namespace; it wins over the config's own env_prefix: TASK."""
    clean_env.setenv("TASK__MAIN_INFERENCER", "should_not_win")
    clean_env.setenv("RP__MAIN_INFERENCER", "DevmateCLI")
    clean_env.setenv("RP__FLOW_INFERENCERS", "A,B,C,D")
    out = OmegaConf.to_container(
        load_config(str(cfg_file), env_prefix="RP"), resolve=True
    )["out"]
    assert out["model"] == "DevmateCLI"          # RP__ won, not TASK__
    assert out["flows"] == ["A", "B", "C", "D"] and out["n"] == 4

def test_external_env_prefix_does_not_leak_to_other_loads(cfg_file, clean_env):
    """Without the override, the config falls back to its own TASK prefix."""
    clean_env.setenv("RP__MAIN_INFERENCER", "DevmateCLI")
    out = _out(cfg_file)  # no env_prefix override → uses TASK
    assert out["model"] == "ClaudeCodeCLI"  # RP__ ignored here

def test_config_defaults_are_below_env_and_cli(cfg_file, clean_env):
    """config_defaults are TOOL DEFAULTS: above YAML, below env, below CLI.

    Mirrors a derived tool (e.g. research_propose) shipping
    ``_params.plan_max_breakdown=5`` as a default a user can still env-override.
    Precedence: CLI overrides > env (<PREFIX>__<KEY>) > config_defaults > YAML.
    """
    cd = {"_params.plan_max_breakdown": 5}  # YAML default is 3

    # 1) tool default beats YAML default
    out = OmegaConf.to_container(
        load_config(str(cfg_file), config_defaults=cd), resolve=True)["out"]
    assert out["breakdown"] == 5

    # 2) env beats tool default
    clean_env.setenv("TASK__PLAN_MAX_BREAKDOWN", "9")
    out = OmegaConf.to_container(
        load_config(str(cfg_file), config_defaults=cd), resolve=True)["out"]
    assert out["breakdown"] == 9

    # 3) CLI override beats env (and tool default)
    out = OmegaConf.to_container(
        load_config(str(cfg_file), overrides={"_params.plan_max_breakdown": 99},
                    config_defaults=cd), resolve=True)["out"]
    assert out["breakdown"] == 99


def test_config_defaults_none_is_noop(cfg_file, clean_env):
    """The default (config_defaults=None) leaves loading byte-identical."""
    a = OmegaConf.to_container(load_config(str(cfg_file)), resolve=True)
    b = OmegaConf.to_container(load_config(str(cfg_file), config_defaults=None), resolve=True)
    assert a == b


def test_config_defaults_create_nested_non_params_path(cfg_file, clean_env):
    """A dotted config_default descends, creating missing intermediate dicts,
    and the value resolves (interpolation strings allowed)."""
    cd = {"out.injected": "${_params.main_inferencer}", "_params.plan_max_breakdown": 7}
    c = OmegaConf.to_container(load_config(str(cfg_file), config_defaults=cd), resolve=True)
    assert c["out"]["injected"] == "ClaudeCodeCLI"   # interpolation resolved
    assert c["out"]["breakdown"] == 7                 # _params default applied


def test_folder_derived_prefix_without_env_prefix_key(tmp_path, monkeypatch):
    d = tmp_path / "myproj" / "configs"
    d.mkdir(parents=True)
    f = d / "c.yaml"
    f.write_text("_params:\n  k: 1\nout:\n  k: ${_params.k}\n")
    monkeypatch.setenv("MYPROJ__K", "42")
    out = OmegaConf.to_container(load_config(str(f)), resolve=True)["out"]
    assert out["k"] == 42
