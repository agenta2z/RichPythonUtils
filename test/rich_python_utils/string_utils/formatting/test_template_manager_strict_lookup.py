"""Regression tests for ``TemplateManager(strict_lookup=True)``.

Locks in the principled "fail loud" behavior added after the production
incident in ``OpenStartup/server_20260615_194631_8e0863a8`` turn_002, where
a misconfigured ``TemplateManager`` silently returned an empty rendered
string for a missing template (no exception, no warning) and the empty
output propagated to an LLM backend that then hung waiting for non-empty
input.

The previous behavior (``strict_lookup=False``, the historical default) is
preserved for backward compatibility with permissive partial-override use
cases. ``strict_lookup=True`` is the new principled opt-in for production
callers that pass an explicit ``template_key`` they expect to resolve.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from rich_python_utils.string_utils.formatting.template_manager.template_manager import (
    TemplateManager,
    TemplateNotFoundError,
    jinjia_template_format,
)


class TestStrictLookupRaisesOnMissingTemplate(unittest.TestCase):
    """``strict_lookup=True``: missing template_key → ``TemplateNotFoundError``."""

    def test_raises_when_strict_and_no_root_has_template(self) -> None:
        # ── Arrange: a non-empty templates dir that nonetheless has no
        # conversation/main/initial.* template — this is the realistic
        # production misconfiguration (a dir with OTHER templates but
        # not the one the caller asked for).
        d = Path(tempfile.mkdtemp(prefix="strict_lookup_no_conv_"))
        (d / "other_space" / "main").mkdir(parents=True, exist_ok=True)
        (d / "other_space" / "main" / "placeholder.jinja2").write_text(
            "ok", encoding="utf-8"
        )
        tm = TemplateManager(
            templates=str(d),
            active_template_root_space="conversation",
            active_template_type="main",
            template_formatter=jinjia_template_format,
            strict_lookup=True,
        )

        # ── Act + Assert: lookup of an explicit key raises loudly.
        with self.assertRaises(TemplateNotFoundError) as cm:
            tm("initial", name="Alice")

        message = str(cm.exception)
        # Diagnostic context must include enough to pinpoint the
        # misconfiguration without instrumenting the lookup chain.
        self.assertIn("strict_lookup=True", message)
        self.assertIn("'initial'", message)
        self.assertIn("'conversation'", message)
        self.assertIn("'main'", message)
        # Must include "Attempted template roots" so the reader sees which
        # roots were searched.
        self.assertIn("Attempted template roots", message)
        # Must include actionable guidance.
        self.assertIn("add_template_root", message)

    def test_does_not_raise_when_strict_and_template_found(self) -> None:
        """Sanity: with a valid template, strict_lookup is a no-op."""
        d = Path(tempfile.mkdtemp(prefix="strict_lookup_found_"))
        # Create conversation/main/initial.jinja2 with a known body.
        (d / "conversation" / "main").mkdir(parents=True, exist_ok=True)
        (d / "conversation" / "main" / "initial.jinja2").write_text(
            "Hello, {{name}}!", encoding="utf-8"
        )

        tm = TemplateManager(
            templates=str(d),
            active_template_root_space="conversation",
            active_template_type="main",
            template_formatter=jinjia_template_format,
            strict_lookup=True,
        )

        rendered = tm("initial", name="Alice")
        self.assertEqual(rendered, "Hello, Alice!")

    def test_does_not_raise_when_strict_and_fallback_root_has_template(self) -> None:
        """Multi-root: strict_lookup raises ONLY if EVERY root lacks the
        template. A fallback root that has it satisfies the lookup."""
        primary = Path(tempfile.mkdtemp(prefix="strict_lookup_primary_"))
        fallback = Path(tempfile.mkdtemp(prefix="strict_lookup_fallback_"))

        # Primary has no conversation/ subdir.
        # Fallback has the template.
        (fallback / "conversation" / "main").mkdir(parents=True, exist_ok=True)
        (fallback / "conversation" / "main" / "initial.jinja2").write_text(
            "From fallback: {{name}}", encoding="utf-8"
        )

        tm = TemplateManager(
            templates=[str(primary), str(fallback)],
            active_template_root_space="conversation",
            active_template_type="main",
            template_formatter=jinjia_template_format,
            strict_lookup=True,
        )

        rendered = tm("initial", name="Alice")
        self.assertEqual(rendered, "From fallback: Alice")


class TestStrictLookupBackwardCompatible(unittest.TestCase):
    """``strict_lookup=False`` (default): silent empty preserved."""

    def test_default_strict_lookup_is_false(self) -> None:
        # Construct with a real (non-empty) templates dir so we don't trip
        # the construction-time validation; we only inspect the attribute.
        d = Path(tempfile.mkdtemp(prefix="strict_default_attr_"))
        (d / "x" / "main").mkdir(parents=True, exist_ok=True)
        (d / "x" / "main" / "placeholder.jinja2").write_text("ok", encoding="utf-8")
        tm = TemplateManager(
            templates=str(d),
            template_formatter=jinjia_template_format,
        )
        self.assertFalse(tm.strict_lookup)

    def test_silent_empty_preserved_when_strict_lookup_false(self) -> None:
        """The historical permissive behavior: missing template → empty
        rendered output, no exception. Critical for callers that legitimately
        rely on partial-override semantics."""
        d = Path(tempfile.mkdtemp(prefix="strict_lookup_false_no_conv_"))
        (d / "other_space" / "main").mkdir(parents=True, exist_ok=True)
        (d / "other_space" / "main" / "placeholder.jinja2").write_text(
            "ok", encoding="utf-8"
        )
        tm = TemplateManager(
            templates=str(d),
            active_template_root_space="conversation",
            active_template_type="main",
            template_formatter=jinjia_template_format,
            # strict_lookup defaults to False
        )

        # Must NOT raise. Must return empty string (the historical contract).
        rendered = tm("initial", name="Alice")
        self.assertEqual(rendered, "")


if __name__ == "__main__":
    unittest.main(verbosity=2)
