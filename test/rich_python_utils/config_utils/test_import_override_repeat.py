"""Integration tests: _params contract + _repeat_ distribution + overrides.

Tests the pattern where _params defines the override contract: the parent
YAML parameterizes flow_inferencers/num_flows in _params, and callers
override via load_config(overrides=...) — no child YAML or _import_ needed.
"""

import os
import textwrap

import pytest
from omegaconf import OmegaConf

from rich_python_utils.config_utils import load_config
from rich_python_utils.config_utils._instantiate import (
    _resolve_import_,
    _resolve_repeat_,
    _resolve_sibling_refs,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def parameterized_topology(tmp_path):
    """Parent topology with _params as the override contract.

    Models the task tool's breakdown-multiflow-plan.yaml: flow_inferencers
    defaults to repeated default_inferencer, num_flows controls _repeat_.
    """
    (tmp_path / "topology.yaml").write_text(textwrap.dedent("""\
        _params:
          default_inferencer: DefaultInf
          num_flows: 2
          flow_inferencers:
            - ${_params.default_inferencer}
            - ${_params.default_inferencer}
          plan_max_breakdown: 3
          flow_max_steps: 3

        _target_: Dual
        base_inferencer:
          _target_: BTA
          breakdown_inferencer:
            _target_: ${_params.default_inferencer}
          worker_factory:
            _factory_: MultiFlowDual
            flow_configs:
              - _repeat_: ${_params.num_flows}
                max_steps: ${_params.flow_max_steps}
                initial_inferencer:
                  _target_: ${_params.flow_inferencers}
                followup_inferencer:
                  _target_: ${_params.flow_inferencers}
          aggregator_inferencer:
            _target_: ${_params.default_inferencer}
        review_inferencer:
          _target_: ${_params.default_inferencer}
    """))
    return tmp_path


@pytest.fixture
def env_topology(tmp_path):
    """Topology using ${oc.env:...} in _params (no hardcoded defaults)."""
    (tmp_path / "topology.yaml").write_text(textwrap.dedent("""\
        _params:
          default_inferencer: ParentDefault
          num_flows: 2
          flow_inferencers:
            - ${_params.default_inferencer}
            - ${_params.default_inferencer}
        items:
          - _repeat_: ${_params.num_flows}
            worker:
              _target_: ${_params.flow_inferencers}
    """))
    return tmp_path


# ---------------------------------------------------------------------------
# 1. Default behavior (no overrides) — all flows use default_inferencer
# ---------------------------------------------------------------------------


class TestDefaultBehavior:
    def test_all_flows_use_default_inferencer(self, parameterized_topology):
        """Without overrides, all flows get the same default_inferencer."""
        cfg = load_config(
            str(parameterized_topology / "topology.yaml"),
            overrides={"_params.workspace_root": "/tmp"},
        )
        d = OmegaConf.to_container(cfg, resolve=True)
        flows = d["base_inferencer"]["worker_factory"]["flow_configs"]
        assert len(flows) == 2
        for flow in flows:
            assert flow["initial_inferencer"]["_target_"] == "DefaultInf"
            assert flow["followup_inferencer"]["_target_"] == "DefaultInf"

    def test_all_slots_use_default(self, parameterized_topology):
        """Every inferencer slot (breakdown, aggregator, review) uses default."""
        cfg = load_config(
            str(parameterized_topology / "topology.yaml"),
            overrides={"_params.workspace_root": "/tmp"},
        )
        d = OmegaConf.to_container(cfg, resolve=True)
        assert d["base_inferencer"]["breakdown_inferencer"]["_target_"] == "DefaultInf"
        assert d["base_inferencer"]["aggregator_inferencer"]["_target_"] == "DefaultInf"
        assert d["review_inferencer"]["_target_"] == "DefaultInf"

    def test_repeat_count_matches_num_flows(self, parameterized_topology):
        """_repeat_: ${_params.num_flows} expands to correct count."""
        cfg = load_config(
            str(parameterized_topology / "topology.yaml"),
            overrides={"_params.workspace_root": "/tmp"},
        )
        d = OmegaConf.to_container(cfg, resolve=True)
        flows = d["base_inferencer"]["worker_factory"]["flow_configs"]
        assert len(flows) == 2


# ---------------------------------------------------------------------------
# 2. Override via load_config overrides (simulates config_overrides)
# ---------------------------------------------------------------------------


class TestConfigOverrides:
    def test_override_flow_inferencers_distributes(self, parameterized_topology):
        """Overriding flow_inferencers distributes different inferencers per flow."""
        cfg = load_config(
            str(parameterized_topology / "topology.yaml"),
            overrides={
                "_params.workspace_root": "/tmp",
                "_params.flow_inferencers": ["ResearchInf", "MainInf"],
            },
        )
        d = OmegaConf.to_container(cfg, resolve=True)
        flows = d["base_inferencer"]["worker_factory"]["flow_configs"]
        assert flows[0]["initial_inferencer"]["_target_"] == "ResearchInf"
        assert flows[1]["initial_inferencer"]["_target_"] == "MainInf"

    def test_override_zips_initial_and_followup(self, parameterized_topology):
        """initial and followup inferencers zip — same index gets same value."""
        cfg = load_config(
            str(parameterized_topology / "topology.yaml"),
            overrides={
                "_params.workspace_root": "/tmp",
                "_params.flow_inferencers": ["ResearchInf", "MainInf"],
            },
        )
        d = OmegaConf.to_container(cfg, resolve=True)
        flows = d["base_inferencer"]["worker_factory"]["flow_configs"]
        for flow in flows:
            assert flow["initial_inferencer"]["_target_"] == flow["followup_inferencer"]["_target_"]

    def test_override_default_inferencer_affects_non_flow_slots(self, parameterized_topology):
        """Overriding default_inferencer changes breakdown/aggregator/review."""
        cfg = load_config(
            str(parameterized_topology / "topology.yaml"),
            overrides={
                "_params.workspace_root": "/tmp",
                "_params.default_inferencer": "CustomInf",
            },
        )
        d = OmegaConf.to_container(cfg, resolve=True)
        assert d["base_inferencer"]["breakdown_inferencer"]["_target_"] == "CustomInf"
        assert d["base_inferencer"]["aggregator_inferencer"]["_target_"] == "CustomInf"
        assert d["review_inferencer"]["_target_"] == "CustomInf"

    def test_override_both_default_and_flow_inferencers(self, parameterized_topology):
        """Override both: non-flow slots get default, flows get distribution."""
        cfg = load_config(
            str(parameterized_topology / "topology.yaml"),
            overrides={
                "_params.workspace_root": "/tmp",
                "_params.default_inferencer": "CustomDefault",
                "_params.flow_inferencers": ["Research", "Main"],
            },
        )
        d = OmegaConf.to_container(cfg, resolve=True)
        assert d["base_inferencer"]["breakdown_inferencer"]["_target_"] == "CustomDefault"
        assert d["review_inferencer"]["_target_"] == "CustomDefault"
        flows = d["base_inferencer"]["worker_factory"]["flow_configs"]
        assert flows[0]["initial_inferencer"]["_target_"] == "Research"
        assert flows[1]["initial_inferencer"]["_target_"] == "Main"

    def test_override_scalar_params(self, parameterized_topology):
        """Scalar _params overrides work alongside list overrides."""
        cfg = load_config(
            str(parameterized_topology / "topology.yaml"),
            overrides={
                "_params.workspace_root": "/tmp",
                "_params.plan_max_breakdown": 5,
                "_params.flow_max_steps": 7,
            },
        )
        d = OmegaConf.to_container(cfg, resolve=True)
        assert d["_params"]["plan_max_breakdown"] == 5
        flows = d["base_inferencer"]["worker_factory"]["flow_configs"]
        assert flows[0]["max_steps"] == 7

    def test_non_overridden_params_preserved(self, parameterized_topology):
        """Params not in overrides keep their defaults."""
        cfg = load_config(
            str(parameterized_topology / "topology.yaml"),
            overrides={
                "_params.workspace_root": "/tmp",
                "_params.flow_inferencers": ["A", "B"],
            },
        )
        d = OmegaConf.to_container(cfg, resolve=True)
        assert d["_params"]["plan_max_breakdown"] == 3
        assert d["_params"]["flow_max_steps"] == 3


# ---------------------------------------------------------------------------
# 3. Override with env vars (${oc.env:...} in override values)
# ---------------------------------------------------------------------------


class TestEnvVarOverrides:
    def test_env_var_interpolation_in_overrides(self, parameterized_topology, monkeypatch):
        """${oc.env:VAR} in override values resolves from environment."""
        monkeypatch.setenv("TEST_RESEARCH_INF", "EnvResearch")
        monkeypatch.setenv("TEST_MAIN_INF", "EnvMain")
        cfg = load_config(
            str(parameterized_topology / "topology.yaml"),
            overrides={
                "_params.workspace_root": "/tmp",
                "_params.flow_inferencers": [
                    "${oc.env:TEST_RESEARCH_INF}",
                    "${oc.env:TEST_MAIN_INF}",
                ],
            },
        )
        d = OmegaConf.to_container(cfg, resolve=True)
        flows = d["base_inferencer"]["worker_factory"]["flow_configs"]
        assert flows[0]["initial_inferencer"]["_target_"] == "EnvResearch"
        assert flows[1]["initial_inferencer"]["_target_"] == "EnvMain"

    def test_env_var_missing_raises(self, parameterized_topology):
        """Missing env var without default raises."""
        for var in ("_NONEXISTENT_A", "_NONEXISTENT_B"):
            os.environ.pop(var, None)
        with pytest.raises(Exception):
            load_config(
                str(parameterized_topology / "topology.yaml"),
                overrides={
                    "_params.workspace_root": "/tmp",
                    "_params.flow_inferencers": [
                        "${oc.env:_NONEXISTENT_A}",
                        "${oc.env:_NONEXISTENT_B}",
                    ],
                },
            )

    def test_env_var_with_default_fallback(self, env_topology, monkeypatch):
        """${oc.env:VAR,default} uses fallback when var missing."""
        (env_topology / "topology.yaml").write_text(textwrap.dedent("""\
            _params:
              default_inferencer: Fallback
              num_flows: 2
              flow_inferencers:
                - ${oc.env:_NONEXISTENT_TEST_INF,FallbackA}
                - ${oc.env:_NONEXISTENT_TEST_INF,FallbackB}
            items:
              - _repeat_: ${_params.num_flows}
                worker:
                  _target_: ${_params.flow_inferencers}
        """))
        cfg = load_config(str(env_topology / "topology.yaml"))
        d = OmegaConf.to_container(cfg, resolve=True)
        assert d["items"][0]["worker"]["_target_"] == "FallbackA"
        assert d["items"][1]["worker"]["_target_"] == "FallbackB"


# ---------------------------------------------------------------------------
# 4. _repeat_ with $ref sibling references
# ---------------------------------------------------------------------------


class TestRepeatWithRef:
    def test_ref_after_distribution(self, tmp_path):
        """$ref resolves AFTER distribution — sees per-copy value."""
        (tmp_path / "main.yaml").write_text(textwrap.dedent("""\
            _params:
              diverse:
                - A
                - B
            flows:
              - _repeat_: 2
                initial:
                  _target_: ${_params.diverse}
                followup:
                  _target_: $initial
        """))
        cfg = load_config(str(tmp_path / "main.yaml"))
        d = OmegaConf.to_container(cfg, resolve=True)
        assert d["flows"][0]["initial"]["_target_"] == "A"
        assert d["flows"][0]["followup"]["_target_"] == "A"
        assert d["flows"][1]["initial"]["_target_"] == "B"
        assert d["flows"][1]["followup"]["_target_"] == "B"

    def test_ref_without_distribution_uses_scalar(self, tmp_path):
        """$ref on scalar (non-distributed) field works normally."""
        (tmp_path / "main.yaml").write_text(textwrap.dedent("""\
            _params:
              inf: Scalar
            flows:
              - _repeat_: 3
                initial:
                  _target_: ${_params.inf}
                followup:
                  _target_: $initial
        """))
        cfg = load_config(str(tmp_path / "main.yaml"))
        d = OmegaConf.to_container(cfg, resolve=True)
        for flow in d["flows"]:
            assert flow["initial"]["_target_"] == "Scalar"
            assert flow["followup"]["_target_"] == "Scalar"


# ---------------------------------------------------------------------------
# 5. Deep nested override
# ---------------------------------------------------------------------------


class TestDeepNestedOverride:
    def test_three_level_deep_override(self, tmp_path):
        """Override at level.sublevel.subsublevel merges correctly."""
        (tmp_path / "base.yaml").write_text(textwrap.dedent("""\
            level1:
              level2:
                level3:
                  value: original
                  kept: preserved
                other: untouched
        """))
        (tmp_path / "child.yaml").write_text(textwrap.dedent("""\
            _import_: base.yaml
            level1:
              level2:
                level3:
                  value: overridden
        """))
        cfg = load_config(str(tmp_path / "child.yaml"))
        d = OmegaConf.to_container(cfg, resolve=True)
        assert d["level1"]["level2"]["level3"]["value"] == "overridden"
        assert d["level1"]["level2"]["level3"]["kept"] == "preserved"
        assert d["level1"]["level2"]["other"] == "untouched"

    def test_override_list_replaces_wholesale(self, tmp_path):
        """Lists in overrides replace the parent's list, not merge."""
        (tmp_path / "base.yaml").write_text(textwrap.dedent("""\
            parent:
              items:
                - a: 1
                - b: 2
        """))
        (tmp_path / "child.yaml").write_text(textwrap.dedent("""\
            _import_: base.yaml
            parent:
              items:
                - c: 3
        """))
        cfg = load_config(str(tmp_path / "child.yaml"))
        d = OmegaConf.to_container(cfg, resolve=True)
        assert len(d["parent"]["items"]) == 1
        assert d["parent"]["items"][0] == {"c": 3}


# ---------------------------------------------------------------------------
# 6. Dict distribution in overrides
# ---------------------------------------------------------------------------


class TestDictDistribution:
    def test_dict_count_distribution(self, tmp_path):
        """Dict-with-counts distributes values proportionally."""
        (tmp_path / "main.yaml").write_text(textwrap.dedent("""\
            items:
              - _repeat_: 3
                worker:
                  Research: 1
                  Main: 2
        """))
        cfg = load_config(str(tmp_path / "main.yaml"))
        d = OmegaConf.to_container(cfg, resolve=True)
        workers = [item["worker"] for item in d["items"]]
        assert workers.count("Research") == 1
        assert workers.count("Main") == 2


# ---------------------------------------------------------------------------
# 7. _factory_ + _repeat_ together
# ---------------------------------------------------------------------------


class TestFactoryRepeat:
    def test_factory_with_repeat_inside(self, tmp_path):
        """_factory_ field containing _repeat_ works correctly."""
        (tmp_path / "main.yaml").write_text(textwrap.dedent("""\
            _target_: Parent
            worker_factory:
              _factory_: MultiFlow
              flow_configs:
                - _repeat_: 2
                  inf:
                    - A
                    - B
        """))
        cfg = load_config(str(tmp_path / "main.yaml"))
        d = OmegaConf.to_container(cfg, resolve=True)
        flows = d["worker_factory"]["flow_configs"]
        assert len(flows) == 2
        assert flows[0]["inf"] == "A"
        assert flows[1]["inf"] == "B"


# ---------------------------------------------------------------------------
# 8. Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_repeat_distribution_mismatch_raises(self, tmp_path):
        """List distribution with wrong length raises ValueError."""
        (tmp_path / "bad.yaml").write_text(textwrap.dedent("""\
            items:
              - _repeat_: 3
                val:
                  - only_two
                  - elements
        """))
        with pytest.raises(ValueError, match="length 2"):
            load_config(str(tmp_path / "bad.yaml"))

    def test_import_chain_with_params(self, tmp_path):
        """Grandchild → child → parent chain with params override at each level."""
        (tmp_path / "grandparent.yaml").write_text(textwrap.dedent("""\
            _params:
              inf: GP
            worker:
              _target_: ${_params.inf}
        """))
        (tmp_path / "parent.yaml").write_text(textwrap.dedent("""\
            _import_: grandparent.yaml
            _params:
              inf: P
        """))
        (tmp_path / "child.yaml").write_text(textwrap.dedent("""\
            _import_: parent.yaml
            _params:
              inf: C
        """))
        cfg = load_config(str(tmp_path / "child.yaml"))
        d = OmegaConf.to_container(cfg, resolve=True)
        assert d["worker"]["_target_"] == "C"

    def test_interpolated_repeat_count(self, tmp_path):
        """_repeat_: ${_params.num} resolves correctly."""
        (tmp_path / "main.yaml").write_text(textwrap.dedent("""\
            _params:
              num: 3
            items:
              - _repeat_: ${_params.num}
                value: same
        """))
        cfg = load_config(str(tmp_path / "main.yaml"))
        d = OmegaConf.to_container(cfg, resolve=True)
        assert len(d["items"]) == 3
        assert all(item["value"] == "same" for item in d["items"])

    def test_interpolated_repeat_count_with_distribution(self, tmp_path):
        """_repeat_: ${_params.num} + list distribution of matching length."""
        (tmp_path / "main.yaml").write_text(textwrap.dedent("""\
            _params:
              num: 3
              types:
                - alpha
                - beta
                - gamma
            items:
              - _repeat_: ${_params.num}
                type: ${_params.types}
        """))
        cfg = load_config(str(tmp_path / "main.yaml"))
        d = OmegaConf.to_container(cfg, resolve=True)
        assert len(d["items"]) == 3
        assert [item["type"] for item in d["items"]] == ["alpha", "beta", "gamma"]

    def test_override_num_flows_and_flow_inferencers(self, parameterized_topology):
        """Overriding both num_flows and flow_inferencers (must match length)."""
        cfg = load_config(
            str(parameterized_topology / "topology.yaml"),
            overrides={
                "_params.workspace_root": "/tmp",
                "_params.num_flows": 3,
                "_params.flow_inferencers": ["A", "B", "C"],
            },
        )
        d = OmegaConf.to_container(cfg, resolve=True)
        flows = d["base_inferencer"]["worker_factory"]["flow_configs"]
        assert len(flows) == 3
        assert [f["initial_inferencer"]["_target_"] for f in flows] == ["A", "B", "C"]

    def test_override_flow_inferencers_mismatch_raises(self, parameterized_topology):
        """Overriding flow_inferencers to wrong length raises distribution error."""
        with pytest.raises(ValueError, match="length 3"):
            load_config(
                str(parameterized_topology / "topology.yaml"),
                overrides={
                    "_params.workspace_root": "/tmp",
                    "_params.flow_inferencers": ["A", "B", "C"],
                },
            )
