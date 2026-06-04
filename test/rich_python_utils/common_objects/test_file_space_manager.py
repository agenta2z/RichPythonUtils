"""Tests for FileSpaceManager — cascade, version, master_version, backends.

Covers T1-T32 from the template versioning formalization plan §6.1.
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock
from rich_python_utils.common_objects.file_space import (
    FileSpaceManager,
    ResolvedContent,
    FieldBackend,
    FileBackend,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def single_root(tmp_path):
    """Single root with plan/main/_variables/ structure."""
    plan_vars = tmp_path / "plan" / "main" / "_variables"
    plan_vars.mkdir(parents=True)
    (tmp_path / "plan" / "main" / "default.j2").write_text("dummy", encoding="utf-8")

    # task_preamble with master_version subdirs
    agg_preamble = plan_vars / "task_preamble" / "aggregation"
    agg_preamble.mkdir(parents=True)
    (agg_preamble / "default.jinja2").write_text("AGGREGATION_PREAMBLE")
    (agg_preamble / "create_role.jinja2").write_text("CREATE_ROLE_PREAMBLE")

    # task_preamble flat default
    (plan_vars / "task_preamble" / "default.jinja2").write_text("DEFAULT_PREAMBLE")

    # task_instructions with master_version subdirs
    agg_instr = plan_vars / "task_instructions" / "aggregation"
    agg_instr.mkdir(parents=True)
    (agg_instr / "default.jinja2").write_text("GENERIC_AGG_INSTRUCTIONS")
    (agg_instr / "create_role.jinja2").write_text("CREATE_ROLE_AGG_INSTRUCTIONS")

    # task_instructions flat (for Constraint H subdir fallback test)
    (plan_vars / "task_instructions" / "default.jinja2").write_text("DEFAULT_INSTRUCTIONS")

    # space-level variable (plan/_variables/)
    space_vars = tmp_path / "plan" / "_variables" / "task_preamble"
    space_vars.mkdir(parents=True)
    (space_vars / "space_level.jinja2").write_text("SPACE_LEVEL_CONTENT")

    # global variable (root/_variables/)
    global_vars = tmp_path / "_variables" / "task_preamble"
    global_vars.mkdir(parents=True)
    (global_vars / "global.jinja2").write_text("GLOBAL_CONTENT")
    (global_vars / "default.jinja2").write_text("GLOBAL_DEFAULT")

    return tmp_path


@pytest.fixture
def two_roots(tmp_path):
    """Two roots (consumer + framework) for multi-root tests."""
    consumer = tmp_path / "consumer"
    framework = tmp_path / "framework"

    # Consumer: plan/main/_variables/task_preamble/
    c_vars = consumer / "plan" / "main" / "_variables" / "task_preamble"
    c_vars.mkdir(parents=True)
    (c_vars / "default.jinja2").write_text("CONSUMER_DEFAULT")

    # Framework: plan/main/_variables/task_preamble/ with more variants
    f_vars = framework / "plan" / "main" / "_variables" / "task_preamble"
    f_vars.mkdir(parents=True)
    (f_vars / "default.jinja2").write_text("FRAMEWORK_DEFAULT")
    (f_vars / "aggregation.jinja2").write_text("FRAMEWORK_AGGREGATION")

    # Framework: global _variables/task_preamble/
    f_global = framework / "_variables" / "task_preamble"
    f_global.mkdir(parents=True)
    (f_global / "aggregation.jinja2").write_text("FRAMEWORK_GLOBAL_AGGREGATION")

    # Consumer: plan/_variables/ (space-level) with aggregation
    c_space = consumer / "plan" / "_variables" / "task_preamble"
    c_space.mkdir(parents=True)
    (c_space / "aggregation.jinja2").write_text("CONSUMER_SPACE_AGGREGATION")

    return consumer, framework


# ---------------------------------------------------------------------------
# T1-T6: build_cascade
# ---------------------------------------------------------------------------

class TestBuildCascade:
    def test_T1_three_levels(self, single_root):
        fsm = FileSpaceManager(roots=[str(single_root)])
        cascade = fsm.build_cascade(space="plan", type_="main")
        assert len(cascade) == 3
        assert "plan" in str(cascade[0]) and "main" in str(cascade[0])
        assert "plan" in str(cascade[1]) and "main" not in str(cascade[1])
        assert "plan" not in str(cascade[2])

    def test_T2_multi_root(self, two_roots):
        consumer, framework = two_roots
        fsm = FileSpaceManager(roots=[str(consumer), str(framework)])
        cascade = fsm.build_cascade(space="plan", type_="main")
        # Should interleave: L1_consumer, L1_framework, L2_consumer, L2_framework, L3_consumer, L3_framework
        assert len(cascade) == 6
        assert "consumer" in str(cascade[0])
        assert "framework" in str(cascade[1])

    def test_T3_empty_space(self, single_root):
        fsm = FileSpaceManager(roots=[str(single_root)])
        cascade = fsm.build_cascade(space="", type_="main")
        # No space -> Level 2 skipped, only L1 (type) and L3 (global)
        assert len(cascade) == 2

    def test_T4_empty_type(self, single_root):
        fsm = FileSpaceManager(roots=[str(single_root)])
        cascade = fsm.build_cascade(space="plan", type_="")
        # No type -> Level 1 = space-level, Level 2 skipped (same as L1), L3 = global
        assert len(cascade) == 2

    def test_T5_empty_subfolder(self, tmp_path):
        fsm = FileSpaceManager(
            roots=[str(tmp_path)], reserved_subfolder_canonical=""
        )
        cascade = fsm.build_cascade(space="plan", type_="main")
        # No subfolder -> paths don't have _variables
        for p in cascade:
            assert "_variables" not in str(p)

    def test_T6_deduplication(self, single_root):
        fsm = FileSpaceManager(roots=[str(single_root)])
        cascade = fsm.build_cascade(space="", type_="")
        # Empty space+type -> L1 collapses to global, should deduplicate
        assert len(cascade) == 1


# ---------------------------------------------------------------------------
# T7-T11: find_in_folder
# ---------------------------------------------------------------------------

class TestFindInFolder:
    def test_T7_direct_file(self, single_root):
        fsm = FileSpaceManager(roots=[str(single_root)])
        folder = single_root / "plan" / "main" / "_variables" / "task_preamble"
        result = fsm.find_in_folder(folder, "default")
        assert result is not None
        assert result.name == "default.jinja2"

    def test_T8_extension_priority(self, tmp_path):
        folder = tmp_path / "var"
        folder.mkdir()
        (folder / "x.txt").write_text("TXT")
        (folder / "x.jinja2").write_text("JINJA")
        fsm = FileSpaceManager(roots=[str(tmp_path)])
        result = fsm.find_in_folder(folder, "x")
        assert result is not None
        assert result.suffix == ".jinja2"

    def test_T9_config_alias(self, tmp_path):
        folder = tmp_path / "var"
        folder.mkdir()
        (folder / "real_name.jinja2").write_text("ALIASED")
        (folder / ".config.yaml").write_text("my_alias: real_name\n")
        fsm = FileSpaceManager(roots=[str(tmp_path)])
        result = fsm.find_in_folder(folder, "my_alias")
        assert result is not None
        assert "real_name" in result.name

    def test_T10_override_wins(self, tmp_path):
        folder = tmp_path / "var"
        folder.mkdir()
        (folder / "x.jinja2").write_text("NORMAL")
        (folder / "x.override.jinja2").write_text("OVERRIDE")
        fsm = FileSpaceManager(
            roots=[str(tmp_path)], enable_overrides=True
        )
        result = fsm.find_in_folder(folder, "x")
        assert result is not None
        assert "override" in result.name.lower()

    def test_T11_missing(self, tmp_path):
        fsm = FileSpaceManager(roots=[str(tmp_path)])
        result = fsm.find_in_folder(tmp_path / "nonexistent", "x")
        assert result is None


# ---------------------------------------------------------------------------
# T12-T13: resolve version basics
# ---------------------------------------------------------------------------

class TestResolveVersion:
    def test_T12_version_specific(self, single_root):
        fsm = FileSpaceManager(roots=[str(single_root)])
        r = fsm.resolve(
            space="plan", type_="main", name="task_preamble",
            version="aggregation",
        )
        # Constraint H: subdir fallback finds aggregation/default.jinja2
        assert r is not None
        assert r.read() == "AGGREGATION_PREAMBLE"

    def test_T13_version_default_fallback(self, single_root):
        fsm = FileSpaceManager(roots=[str(single_root)])
        r = fsm.resolve(
            space="plan", type_="main", name="task_preamble",
        )
        assert r is not None
        assert r.read() == "DEFAULT_PREAMBLE"


# ---------------------------------------------------------------------------
# T14-T17: master_version
# ---------------------------------------------------------------------------

class TestMasterVersion:
    def test_T14_basic(self, single_root):
        fsm = FileSpaceManager(roots=[str(single_root)])
        r = fsm.resolve(
            space="plan", type_="main", name="task_preamble",
            master_version="aggregation",
        )
        assert r is not None
        assert r.read() == "AGGREGATION_PREAMBLE"

    def test_T15_with_version(self, single_root):
        fsm = FileSpaceManager(roots=[str(single_root)])
        r = fsm.resolve(
            space="plan", type_="main", name="task_preamble",
            master_version="aggregation", version="create_role",
        )
        assert r is not None
        assert r.read() == "CREATE_ROLE_PREAMBLE"

    def test_T16_default_fallback(self, single_root):
        fsm = FileSpaceManager(roots=[str(single_root)])
        r = fsm.resolve(
            space="plan", type_="main", name="task_preamble",
            master_version="aggregation", version="nonexistent",
        )
        assert r is not None
        assert r.read() == "AGGREGATION_PREAMBLE"

    def test_T17_no_flat_fallback(self, single_root):
        """Constraint A: master_version set -> NO flat fallback."""
        fsm = FileSpaceManager(roots=[str(single_root)])
        r = fsm.resolve(
            space="plan", type_="main", name="task_preamble",
            master_version="nonexistent",
        )
        assert r is None


# ---------------------------------------------------------------------------
# T33-T43: list-based master_version fallback chain
# ---------------------------------------------------------------------------


@pytest.fixture
def chain_root(tmp_path):
    """Root with research_propose + aggregation master_version dirs for chain tests."""
    plan_vars = tmp_path / "plan" / "main" / "_variables"

    # task_response_format: research_propose has it, aggregation has a different one
    (plan_vars / "task_response_format" / "research_propose").mkdir(parents=True)
    (plan_vars / "task_response_format" / "research_propose" / "default.jinja2").write_text(
        "PROPOSAL_INDEX_FENCE"
    )
    (plan_vars / "task_response_format" / "aggregation").mkdir(parents=True)
    (plan_vars / "task_response_format" / "aggregation" / "default.jinja2").write_text(
        "GENERIC_AGGREGATION_FORMAT"
    )

    # task_preamble: ONLY in aggregation (not in research_propose)
    (plan_vars / "task_preamble" / "aggregation").mkdir(parents=True)
    (plan_vars / "task_preamble" / "aggregation" / "default.jinja2").write_text(
        "UPSTREAM_ARTIFACTS_CONTEXT"
    )

    # task_instructions: ONLY in aggregation
    (plan_vars / "task_instructions" / "aggregation").mkdir(parents=True)
    (plan_vars / "task_instructions" / "aggregation" / "default.jinja2").write_text(
        "MERGE_JSON_FENCES"
    )

    # task_response_format with version-specific files
    (plan_vars / "task_response_format" / "research_propose" / "modeling.jinja2").write_text(
        "MODELING_PROPOSAL_FENCE"
    )
    (plan_vars / "task_response_format" / "aggregation" / "special.jinja2").write_text(
        "SPECIAL_AGG_FORMAT"
    )

    return tmp_path


class TestMasterVersionChain:
    """List-based master_version: try each in order, first match wins."""

    def test_T33_list_first_found(self, chain_root):
        """When first version has the variable, use it."""
        fsm = FileSpaceManager(roots=[str(chain_root)])
        r = fsm.resolve(
            space="plan", type_="main", name="task_response_format",
            master_version=["research_propose", "aggregation"],
        )
        assert r is not None
        assert r.read() == "PROPOSAL_INDEX_FENCE"

    def test_T34_list_fallback_to_second(self, chain_root):
        """When first version lacks the variable, fall back to second."""
        fsm = FileSpaceManager(roots=[str(chain_root)])
        r = fsm.resolve(
            space="plan", type_="main", name="task_preamble",
            master_version=["research_propose", "aggregation"],
        )
        assert r is not None
        assert r.read() == "UPSTREAM_ARTIFACTS_CONTEXT"

    def test_T35_list_fallback_instructions(self, chain_root):
        """task_instructions: not in research_propose, falls back to aggregation."""
        fsm = FileSpaceManager(roots=[str(chain_root)])
        r = fsm.resolve(
            space="plan", type_="main", name="task_instructions",
            master_version=["research_propose", "aggregation"],
        )
        assert r is not None
        assert r.read() == "MERGE_JSON_FENCES"

    def test_T36_list_all_miss_returns_none(self, chain_root):
        """When no version in the chain has the variable, return None."""
        fsm = FileSpaceManager(roots=[str(chain_root)])
        r = fsm.resolve(
            space="plan", type_="main", name="nonexistent_var",
            master_version=["research_propose", "aggregation"],
        )
        assert r is None

    def test_T37_single_string_unchanged(self, chain_root):
        """Backward compat: single string still works exactly as before."""
        fsm = FileSpaceManager(roots=[str(chain_root)])
        r = fsm.resolve(
            space="plan", type_="main", name="task_response_format",
            master_version="aggregation",
        )
        assert r is not None
        assert r.read() == "GENERIC_AGGREGATION_FORMAT"

    def test_T38_list_with_version_first_found(self, chain_root):
        """Version-specific file found in first master_version."""
        fsm = FileSpaceManager(roots=[str(chain_root)])
        r = fsm.resolve(
            space="plan", type_="main", name="task_response_format",
            master_version=["research_propose", "aggregation"],
            version="modeling",
        )
        assert r is not None
        assert r.read() == "MODELING_PROPOSAL_FENCE"

    def test_T39_list_chain_order_beats_version_specificity(self, chain_root):
        """Chain order wins: first entry's default beats second entry's version-specific.

        With chain=["research_propose", "aggregation"] and version="special":
        research_propose/special → miss → research_propose/default → HIT.
        aggregation/special is never reached because research_propose has a default.
        """
        fsm = FileSpaceManager(roots=[str(chain_root)])
        r = fsm.resolve(
            space="plan", type_="main", name="task_response_format",
            master_version=["research_propose", "aggregation"],
            version="special",
        )
        assert r is not None
        assert r.read() == "PROPOSAL_INDEX_FENCE"  # first chain entry's default wins

    def test_T40_list_with_version_miss_falls_to_default(self, chain_root):
        """Version not found in any chain entry, falls to default in first match."""
        fsm = FileSpaceManager(roots=[str(chain_root)])
        r = fsm.resolve(
            space="plan", type_="main", name="task_response_format",
            master_version=["research_propose", "aggregation"],
            version="nonexistent_version",
        )
        assert r is not None
        # Falls back to default.jinja2 in research_propose (first in chain)
        assert r.read() == "PROPOSAL_INDEX_FENCE"

    def test_T41_single_element_list_same_as_string(self, chain_root):
        """List with one element behaves like a plain string."""
        fsm = FileSpaceManager(roots=[str(chain_root)])
        r_list = fsm.resolve(
            space="plan", type_="main", name="task_response_format",
            master_version=["aggregation"],
        )
        r_str = fsm.resolve(
            space="plan", type_="main", name="task_response_format",
            master_version="aggregation",
        )
        assert r_list is not None and r_str is not None
        assert r_list.read() == r_str.read()

    def test_T42_empty_list_same_as_none(self, chain_root):
        """Empty list behaves like master_version=None."""
        fsm = FileSpaceManager(roots=[str(chain_root)])
        r = fsm.resolve(
            space="plan", type_="main", name="task_response_format",
            master_version=[],
        )
        # No master_version → looks in flat task_response_format/ → no default there
        assert r is None

    def test_T43_three_level_chain(self, chain_root):
        """Three-level chain: first two miss, third hits."""
        # Add a third master_version dir
        plan_vars = chain_root / "plan" / "main" / "_variables"
        (plan_vars / "rare_var" / "deep_fallback").mkdir(parents=True)
        (plan_vars / "rare_var" / "deep_fallback" / "default.jinja2").write_text(
            "FOUND_AT_LEVEL_3"
        )

        fsm = FileSpaceManager(roots=[str(chain_root)])
        r = fsm.resolve(
            space="plan", type_="main", name="rare_var",
            master_version=["research_propose", "aggregation", "deep_fallback"],
        )
        assert r is not None
        assert r.read() == "FOUND_AT_LEVEL_3"


# ---------------------------------------------------------------------------
# T18-T19: cascade cross-level + cross-space
# ---------------------------------------------------------------------------

class TestCascadeCrossLevel:
    def test_T18_found_at_space_level(self, single_root):
        fsm = FileSpaceManager(roots=[str(single_root)])
        r = fsm.resolve(
            space="plan", type_="main", name="task_preamble",
            version="space_level",
        )
        assert r is not None
        assert r.read() == "SPACE_LEVEL_CONTENT"

    def test_T19_cross_space_via_multi_root(self, two_roots):
        consumer, framework = two_roots
        fsm = FileSpaceManager(roots=[str(consumer), str(framework)])
        r = fsm.resolve(
            space="plan", type_="main", name="task_preamble",
            version="aggregation",
        )
        # Consumer L1 doesn't have aggregation, framework L1 does
        assert r is not None
        assert r.read() == "FRAMEWORK_AGGREGATION"


# ---------------------------------------------------------------------------
# T20-T22: ResolvedContent shape + explain
# ---------------------------------------------------------------------------

class TestResolvedContent:
    def test_T20_uri_shape_file(self, single_root):
        fsm = FileSpaceManager(roots=[str(single_root)])
        r = fsm.resolve(
            space="plan", type_="main", name="task_preamble",
            master_version="aggregation", version="create_role",
        )
        assert r is not None
        assert r.kind == "file"
        assert r.uri.startswith("file://")
        assert r.field is None
        assert r.path.is_file()

    def test_T21_lazy_read(self, single_root):
        fsm = FileSpaceManager(roots=[str(single_root)])
        r = fsm.resolve(
            space="plan", type_="main", name="task_preamble",
            master_version="aggregation",
        )
        assert r is not None
        content1 = r.read()
        content2 = r.read()
        assert content1 == content2 == "AGGREGATION_PREAMBLE"

    def test_T22_explain(self, single_root):
        fsm = FileSpaceManager(roots=[str(single_root)])
        trace = fsm.explain(
            space="plan", type_="main", name="task_preamble",
            master_version="aggregation", version="create_role",
        )
        assert len(trace) > 0
        assert any(found for _, _, found in trace)
        # Each entry is (desc, uri, found)
        for desc, uri, found in trace:
            assert isinstance(desc, str)
            assert isinstance(uri, str)
            assert isinstance(found, bool)


# ---------------------------------------------------------------------------
# T23-T24: multi-root priority
# ---------------------------------------------------------------------------

class TestMultiRootPriority:
    def test_T23_specificity_beats_proximity(self, two_roots):
        """root2 L1 match beats root1 L2 match (version specificity beats proximity)."""
        consumer, framework = two_roots
        fsm = FileSpaceManager(roots=[str(consumer), str(framework)])
        # Consumer has aggregation at space level (L2)
        # Framework has aggregation at type level (L1)
        r = fsm.resolve(
            space="plan", type_="main", name="task_preamble",
            version="aggregation",
        )
        assert r is not None
        # Framework L1 should win (L1 beats L2)
        assert r.read() == "FRAMEWORK_AGGREGATION"

    def test_T24_first_root_wins_same_level(self, two_roots):
        """Within same cascade level, first root takes priority."""
        consumer, framework = two_roots
        fsm = FileSpaceManager(roots=[str(consumer), str(framework)])
        r = fsm.resolve(
            space="plan", type_="main", name="task_preamble",
        )
        assert r is not None
        assert r.read() == "CONSUMER_DEFAULT"


# ---------------------------------------------------------------------------
# T25-T26: prefix equivalence
# ---------------------------------------------------------------------------

class TestPrefixEquivalence:
    def test_T25_underscore_preferred(self, tmp_path):
        """_variables chosen when both _variables and .variables exist."""
        plan_main = tmp_path / "plan" / "main"
        under = plan_main / "_variables" / "x"
        under.mkdir(parents=True)
        (under / "default.jinja2").write_text("UNDERSCORE")
        dot = plan_main / ".variables" / "x"
        dot.mkdir(parents=True)
        (dot / "default.jinja2").write_text("DOT")

        fsm = FileSpaceManager(roots=[str(tmp_path)])
        r = fsm.resolve(space="plan", type_="main", name="x")
        assert r is not None
        assert r.read() == "UNDERSCORE"

    def test_T26_dot_fallback(self, tmp_path):
        """.variables chosen when _variables doesn't exist."""
        plan_main = tmp_path / "plan" / "main"
        dot = plan_main / ".variables" / "x"
        dot.mkdir(parents=True)
        (dot / "default.jinja2").write_text("DOT_CONTENT")

        fsm = FileSpaceManager(roots=[str(tmp_path)])
        r = fsm.resolve(space="plan", type_="main", name="x")
        assert r is not None
        assert r.read() == "DOT_CONTENT"


# ---------------------------------------------------------------------------
# T27: .config.yaml LOCAL-only
# ---------------------------------------------------------------------------

class TestConfigYamlLocalOnly:
    def test_T27_no_cascade(self, tmp_path):
        """.config.yaml in folder A does NOT affect sibling folder B."""
        vars_dir = tmp_path / "_variables"
        folder_a = vars_dir / "a"
        folder_a.mkdir(parents=True)
        (folder_a / ".config.yaml").write_text("my_alias: some_file\n")
        (folder_a / "some_file.jinja2").write_text("ALIASED_A")

        folder_b = vars_dir / "b"
        folder_b.mkdir(parents=True)

        fsm = FileSpaceManager(roots=[str(tmp_path)])
        # Alias should work in folder A
        result_a = fsm.find_in_folder(folder_a, "my_alias")
        assert result_a is not None
        # Alias should NOT leak to folder B
        result_b = fsm.find_in_folder(folder_b, "my_alias")
        assert result_b is None


# ---------------------------------------------------------------------------
# T28-T30: backend protocol
# ---------------------------------------------------------------------------

class TestBackendProtocol:
    def test_T28_file_backend_can_resolve(self, single_root):
        backend = FileBackend()
        folder = single_root / "plan" / "main" / "_variables" / "task_preamble"
        assert backend.can_resolve(folder=folder, name="default")

    def test_T29_chain_files_first(self, tmp_path):
        """When chain has [FileBackend, MockField], file always wins."""
        folder = tmp_path / "_variables" / "x"
        folder.mkdir(parents=True)
        (folder / "v.jinja2").write_text("FILE_CONTENT")

        mock_backend = MagicMock(spec=FieldBackend)
        mock_backend.scheme = "mock"
        mock_backend.resolve.return_value = ResolvedContent(
            name="v", kind="field", uri="mock://v", path=folder,
            field="v", mime=None, _backend=mock_backend,
        )

        fsm = FileSpaceManager(
            roots=[str(tmp_path)],
            backends=[FileBackend(), mock_backend],
        )
        r = fsm.resolve(name="x", version="v")
        assert r is not None
        assert r.kind == "file"

    def test_T30_chain_field_fallback(self, tmp_path):
        """When FileBackend misses, MockField hit returned."""
        folder = tmp_path / "_variables" / "x"
        folder.mkdir(parents=True)
        # No file — FileBackend will miss

        mock_backend = MagicMock()
        mock_backend.scheme = "mock"
        mock_resolved = ResolvedContent(
            name="v", kind="field", uri="mock://v", path=folder,
            field="v", mime=None, _backend=mock_backend,
        )
        mock_backend.resolve.return_value = mock_resolved

        fsm = FileSpaceManager(
            roots=[str(tmp_path)],
            backends=[FileBackend(), mock_backend],
        )
        r = fsm.resolve(name="x", version="v")
        assert r is not None
        assert r.kind == "field"


# ---------------------------------------------------------------------------
# T31-T32: subdirectory fallback (Constraint H)
# ---------------------------------------------------------------------------

class TestSubdirFallback:
    def test_T31_when_master_unset(self, single_root):
        """Subdir fallback: version='aggregation' with master_version=None finds
        aggregation/default.jinja2 after migration (transparent to callers)."""
        fsm = FileSpaceManager(roots=[str(single_root)])
        r = fsm.resolve(
            space="plan", type_="main", name="task_preamble",
            version="aggregation", master_version=None,
        )
        assert r is not None
        assert r.read() == "AGGREGATION_PREAMBLE"

    def test_T32_inactive_when_master_set(self, single_root):
        """Subdir fallback does NOT trigger when master_version is set
        (Constraint A: flat search disabled)."""
        fsm = FileSpaceManager(roots=[str(single_root)])
        r = fsm.resolve(
            space="plan", type_="main", name="task_preamble",
            version="aggregation", master_version="aggregation",
        )
        # master_version="aggregation" -> search in aggregation/ folder
        # version="aggregation" -> look for aggregation.jinja2 inside aggregation/
        # Not found -> fall back to aggregation/default.jinja2
        assert r is not None
        assert r.read() == "AGGREGATION_PREAMBLE"
