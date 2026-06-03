"""Integration test: auto-discovery (mechanism A) + _resolve_templated_feed (mechanism B).

Verifies that composed wrapper variables (like context.user_request_with_task_preamble)
correctly preserve file-based variable content (task_preamble) while resolving
feed-only variables (input) — even when task_preamble is NOT explicitly in the feed.

This is the bug that caused the aggregation preamble to disappear:
- Mechanism A (auto-discovery) correctly resolved {{ task_preamble }} via master_version
- Mechanism B (templated_feed Step 1) re-read the raw composition file and re-rendered
  {{ task_preamble }} against the feed — where task_preamble was missing → empty
"""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from rich_python_utils.string_utils.formatting.template_manager import TemplateManager
from rich_python_utils.string_utils.formatting.jinja2_format import format_template


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def _build_template_tree(root: Path) -> None:
    """Build a template tree that mimics the production aggregation scenario.

    Structure:
      plan/main/initial.jinja2  — wrapper template referencing {{ context.user_request_with_task_preamble }}
      _variables/context/user_request_with_task_preamble.jinja2  — composition file
      _variables/task_preamble/default.jinja2  — generic preamble
      _variables/task_preamble/aggregation/default.jinja2  — aggregation-specific preamble
      _variables/task_instructions/aggregation/default.jinja2  — aggregation task instructions
    """
    _write(
        root / "plan" / "main" / "initial.jinja2",
        "{{ context.user_request_with_task_preamble }}\n---\n{{ task_instructions }}",
    )
    _write(
        root / "_variables" / "context" / "user_request_with_task_preamble.jinja2",
        "{{ task_preamble }}\n\n## Original User Request\n{{ input }}",
    )
    _write(
        root / "_variables" / "task_preamble" / "default.jinja2",
        "GENERIC PLANNING PREAMBLE",
    )
    _write(
        root / "_variables" / "task_preamble" / "aggregation" / "default.jinja2",
        "AGGREGATION PREAMBLE: You are aggregating upstream artifacts.",
    )
    _write(
        root / "_variables" / "task_instructions" / "default.jinja2",
        "GENERIC TASK INSTRUCTIONS",
    )
    _write(
        root / "_variables" / "task_instructions" / "aggregation" / "default.jinja2",
        "AGGREGATION INSTRUCTIONS: integrate and consolidate.",
    )


class TestCompositionWithMasterVersion(unittest.TestCase):
    """The core integration test: auto-discovery with master_version +
    _resolve_templated_feed should produce correct composed content."""

    def test_aggregation_preamble_preserved_without_explicit_variable_names(self):
        """When master_version='aggregation' and NO explicit variable_names,
        the composed wrapper should contain the AGGREGATION preamble,
        not the generic one. And {{ input }} should be resolved from the feed."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _build_template_tree(root)

            tm = TemplateManager(
                templates=str(root),
                template_formatter=format_template,
                active_template_root_space="plan",
                active_template_type="main",
                predefined_variables=True,
                enable_templated_feed=True,
            )

            rendered = tm(
                "initial",
                active_template_root_space="plan",
                master_version="aggregation",
                input="Build a REST API",
            )

            self.assertIn("AGGREGATION PREAMBLE", rendered,
                          "Aggregation preamble should be preserved by auto-discovery")
            self.assertNotIn("GENERIC PLANNING PREAMBLE", rendered,
                             "Generic preamble should NOT appear when master_version=aggregation")
            self.assertIn("Build a REST API", rendered,
                          "Feed variable {{ input }} should be resolved")
            self.assertIn("AGGREGATION INSTRUCTIONS", rendered,
                          "Direct variable {{ task_instructions }} should use aggregation version")
            self.assertNotIn("{{ input }}", rendered,
                             "{{ input }} should not leak as literal text")
            self.assertNotIn("{{ task_preamble }}", rendered,
                             "{{ task_preamble }} should not leak as literal text")

    def test_generic_preamble_without_master_version(self):
        """Without master_version, the generic preamble should be used."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _build_template_tree(root)

            tm = TemplateManager(
                templates=str(root),
                template_formatter=format_template,
                active_template_root_space="plan",
                active_template_type="main",
                predefined_variables=True,
                enable_templated_feed=True,
            )

            rendered = tm(
                "initial",
                active_template_root_space="plan",
                input="Build a REST API",
            )

            self.assertIn("GENERIC PLANNING PREAMBLE", rendered)
            self.assertNotIn("AGGREGATION PREAMBLE", rendered)
            self.assertIn("Build a REST API", rendered)

    def test_explicit_feed_overrides_auto_discovery(self):
        """When task_preamble IS explicitly in the feed (from load_variables),
        it should override the auto-discovered value in the composition."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _build_template_tree(root)

            tm = TemplateManager(
                templates=str(root),
                template_formatter=format_template,
                active_template_root_space="plan",
                active_template_type="main",
                predefined_variables=True,
                enable_templated_feed=True,
            )

            rendered = tm(
                "initial",
                active_template_root_space="plan",
                master_version="aggregation",
                input="Build a REST API",
                task_preamble="EXPLICIT CUSTOM PREAMBLE",
            )

            self.assertIn("EXPLICIT CUSTOM PREAMBLE", rendered,
                          "Explicit feed value should override auto-discovered preamble")
            self.assertNotIn("AGGREGATION PREAMBLE", rendered)
            self.assertIn("Build a REST API", rendered)

    def test_no_template_variable_leakage(self):
        """No raw {{ variable }} syntax should leak into the final rendered output."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _build_template_tree(root)

            tm = TemplateManager(
                templates=str(root),
                template_formatter=format_template,
                active_template_root_space="plan",
                active_template_type="main",
                predefined_variables=True,
                enable_templated_feed=True,
            )

            rendered = tm(
                "initial",
                active_template_root_space="plan",
                master_version="aggregation",
                input="Test request",
            )

            self.assertNotIn("{{", rendered,
                             f"Raw template syntax leaked in output: {rendered[:200]}")


if __name__ == "__main__":
    unittest.main()
