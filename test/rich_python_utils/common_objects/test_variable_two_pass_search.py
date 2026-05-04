"""Integration tests for the Refactor 12 two-pass search in
``FileBasedVariableManager._find_variable_file``.

Covers:
- Per-folder version convention: ``<name>/<version>.<ext>``
- ``.config.yaml`` alias map (version -> filename)
- Pass 2 default fallback when explicit version is missing
- Pass 1 version-anywhere beats Pass 3 unversioned-locally
- Existing flat-versioned (``<name>.<version>.<ext>``) still wins over per-folder version
- ``version=""`` skips Pass 1 + Pass 2 (preserves existing semantics)
"""

from pathlib import Path

import pytest

from rich_python_utils.common_objects.variable_manager import (
    FileBasedVariableManager,
    VariableManagerConfig,
)


@pytest.fixture
def manager(tmp_path: Path) -> FileBasedVariableManager:
    return FileBasedVariableManager(base_path=str(tmp_path))


# ---------------------------------------------------------------------------
# Per-folder version convention (NEW in Refactor 12)
# ---------------------------------------------------------------------------


class TestPerFolderVersionConvention:
    def test_per_folder_version_file(self, manager, tmp_path):
        # tmp_path/task_preamble/aggregation.j2
        folder = tmp_path / "task_preamble"
        folder.mkdir()
        target = folder / "aggregation.j2"
        target.write_text("aggregation preamble", encoding="utf-8")

        path, name = manager._find_variable_file(
            "task_preamble", [tmp_path], version="aggregation"
        )
        assert path == target
        assert name == "task_preamble"

    def test_config_yaml_alias_in_folder(self, manager, tmp_path):
        folder = tmp_path / "task_preamble"
        folder.mkdir()
        (folder / "agg_v2.j2").write_text("aliased preamble", encoding="utf-8")
        (folder / ".config.yaml").write_text(
            "aggregation: agg_v2\n", encoding="utf-8"
        )

        path, name = manager._find_variable_file(
            "task_preamble", [tmp_path], version="aggregation"
        )
        assert path is not None
        assert path.name == "agg_v2.j2"


# ---------------------------------------------------------------------------
# Pass 2 default fallback
# ---------------------------------------------------------------------------


class TestPass2DefaultFallback:
    def test_default_file_used_when_version_missing(self, manager, tmp_path):
        # Folder has only default.j2; caller asks for version=aggregation
        folder = tmp_path / "task_instructions"
        folder.mkdir()
        default_target = folder / "default.j2"
        default_target.write_text("default instructions", encoding="utf-8")

        path, _ = manager._find_variable_file(
            "task_instructions", [tmp_path], version="aggregation"
        )
        assert path == default_target

    def test_config_yaml_default_alias_used_when_version_missing(
        self, manager, tmp_path
    ):
        folder = tmp_path / "task_instructions"
        folder.mkdir()
        (folder / "generic.j2").write_text("generic content", encoding="utf-8")
        (folder / ".config.yaml").write_text(
            "default: generic\n", encoding="utf-8"
        )

        path, _ = manager._find_variable_file(
            "task_instructions", [tmp_path], version="aggregation"
        )
        assert path is not None
        assert path.name == "generic.j2"

    def test_pass2_skipped_when_version_empty(self, manager, tmp_path):
        # Even though default.j2 exists, version="" means Pass 1+2 skipped.
        # Pass 3 looks for unversioned task_instructions.j2 (which doesn't exist).
        folder = tmp_path / "task_instructions"
        folder.mkdir()
        (folder / "default.j2").write_text("default content", encoding="utf-8")

        path, _ = manager._find_variable_file(
            "task_instructions", [tmp_path], version=""
        )
        # Pass 3 searches for task_instructions.j2 directly, not folder/default.j2
        assert path is None


# ---------------------------------------------------------------------------
# Pass 1 version-anywhere vs Pass 3 unversioned-locally
# ---------------------------------------------------------------------------


class TestVersionAnywhereBeatsUnversionedLocally:
    def test_version_at_level_b_beats_unversioned_at_level_a(
        self, tmp_path
    ):
        # Cascade A (specific) has unversioned only.
        # Cascade B (general) has versioned aggregation file.
        # Pass 1 sweeps both levels for version BEFORE Pass 3 sweeps for unversioned.
        # Expected: B's aggregation wins.
        cascade_a = tmp_path / "specific"
        cascade_b = tmp_path / "general"
        cascade_a.mkdir()
        cascade_b.mkdir()

        (cascade_a / "task_preamble.j2").write_text(
            "A unversioned", encoding="utf-8"
        )
        b_folder = cascade_b / "task_preamble"
        b_folder.mkdir()
        b_versioned = b_folder / "aggregation.j2"
        b_versioned.write_text("B aggregation", encoding="utf-8")

        manager = FileBasedVariableManager(base_path=str(tmp_path))
        path, _ = manager._find_variable_file(
            "task_preamble", [cascade_a, cascade_b], version="aggregation"
        )
        assert path == b_versioned, (
            "Pass 1 across all cascades should beat Pass 3 at level A"
        )

    def test_default_anywhere_beats_unversioned(self, tmp_path):
        # Cascade A has unversioned. Cascade B has default in folder.
        # Pass 2 sweeps for default before Pass 3 sweeps for unversioned.
        cascade_a = tmp_path / "specific"
        cascade_b = tmp_path / "general"
        cascade_a.mkdir()
        cascade_b.mkdir()

        (cascade_a / "task_preamble.j2").write_text(
            "A unversioned", encoding="utf-8"
        )
        b_folder = cascade_b / "task_preamble"
        b_folder.mkdir()
        b_default = b_folder / "default.j2"
        b_default.write_text("B default", encoding="utf-8")

        manager = FileBasedVariableManager(base_path=str(tmp_path))
        path, _ = manager._find_variable_file(
            "task_preamble", [cascade_a, cascade_b], version="aggregation"
        )
        assert path == b_default

    def test_version_at_close_level_a_wins_over_default_at_a(self, tmp_path):
        # Both Pass 1 and Pass 2 hits at cascade A.
        # Pass 1 (version) runs first within the same level, so version wins.
        cascade_a = tmp_path / "level_a"
        cascade_a.mkdir()

        a_folder = cascade_a / "task_preamble"
        a_folder.mkdir()
        a_version = a_folder / "aggregation.j2"
        a_version.write_text("A aggregation", encoding="utf-8")
        (a_folder / "default.j2").write_text("A default", encoding="utf-8")

        manager = FileBasedVariableManager(base_path=str(tmp_path))
        path, _ = manager._find_variable_file(
            "task_preamble", [cascade_a], version="aggregation"
        )
        assert path == a_version


# ---------------------------------------------------------------------------
# Pass 1 phase ordering: flat-versioned still wins over per-folder version
# ---------------------------------------------------------------------------


class TestPass1PhaseOrdering:
    def test_flat_versioned_wins_over_per_folder(self, tmp_path):
        # Both task_preamble.aggregation.j2 (flat) and task_preamble/aggregation.j2 (per-folder) exist.
        # Phase 1.a (flat-versioned) runs before Phase 1.b (per-folder), so flat wins.
        flat_target = tmp_path / "task_preamble.aggregation.j2"
        flat_target.write_text("flat content", encoding="utf-8")
        folder = tmp_path / "task_preamble"
        folder.mkdir()
        (folder / "aggregation.j2").write_text(
            "per-folder content", encoding="utf-8"
        )

        manager = FileBasedVariableManager(base_path=str(tmp_path))
        path, _ = manager._find_variable_file(
            "task_preamble", [tmp_path], version="aggregation"
        )
        assert path == flat_target

    def test_folder_version_subdir_still_works(self, tmp_path):
        # Existing Phase 2.2 behavior: <path>/<version>/ subdir with single content
        folder = tmp_path / "task_preamble" / "aggregation"
        folder.mkdir(parents=True)
        target = folder / "default.j2"
        target.write_text("subdir content", encoding="utf-8")

        manager = FileBasedVariableManager(base_path=str(tmp_path))
        path, _ = manager._find_variable_file(
            "task_preamble", [tmp_path], version="aggregation"
        )
        assert path == target


# ---------------------------------------------------------------------------
# version="" semantics: skip Pass 1 + Pass 2
# ---------------------------------------------------------------------------


class TestEmptyVersionSemantics:
    def test_empty_version_skips_per_folder(self, manager, tmp_path):
        # Per-folder file exists, but version="" should NOT match it.
        folder = tmp_path / "task_preamble"
        folder.mkdir()
        (folder / "aggregation.j2").write_text("content", encoding="utf-8")

        path, _ = manager._find_variable_file(
            "task_preamble", [tmp_path], version=""
        )
        assert path is None

    def test_empty_version_finds_unversioned(self, manager, tmp_path):
        target = tmp_path / "task_preamble.j2"
        target.write_text("unversioned", encoding="utf-8")

        path, _ = manager._find_variable_file(
            "task_preamble", [tmp_path], version=""
        )
        assert path == target


# ---------------------------------------------------------------------------
# Pass 3 unversioned still works (regression: caller doesn't pass version)
# ---------------------------------------------------------------------------


class TestPass3Fallback:
    def test_unversioned_when_version_and_default_miss(self, manager, tmp_path):
        # Caller asks for version=aggregation but only unversioned exists.
        target = tmp_path / "task_preamble.j2"
        target.write_text("unversioned", encoding="utf-8")

        path, _ = manager._find_variable_file(
            "task_preamble", [tmp_path], version="aggregation"
        )
        assert path == target

    def test_returns_none_when_nothing_matches(self, manager, tmp_path):
        path, _ = manager._find_variable_file(
            "task_preamble", [tmp_path], version="aggregation"
        )
        assert path is None
