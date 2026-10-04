# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

"""Tests for ``TemplateManager.with_variable_extensions`` -- the per-inferencer
variable-root primitive.

``with_variable_extensions(roots)`` returns an immutable copy-on-write fork whose
variable resolution consults *roots* ahead of the shared base tree, on **both**
cascade subsystems: the automatic embedded-variable path (``__call__`` -> per-root
loader) and the explicit ``load_variables`` path (the multi-root file space). The
ordered roots are threaded into the resolver so ``{{ __super__ }}`` composes an
extension override against the shadowed base value. These tests pin that
dual-subsystem resolution, the leak-free immutability of the source manager, the
identity fork for empty roots, and per-key disabling. The cascade-level rule (an
extension's global-level file beats a base file at a more-specific level) is a
resolver property, pinned in ``test_super_ref.py::ExtraRootCascadeLevelTest``.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from rich_python_utils.string_utils.formatting.jinja2_format import format_template
from rich_python_utils.string_utils.formatting.template_manager import TemplateManager


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def _make_base(base: Path) -> None:
    """Base tree: a ``main`` template embedding ``{{ notes_greeting }}`` plus a
    global ``notes/greeting`` variable resolving to ``BASE``."""
    _write(base / "main" / "default.jinja2", "{{ notes_greeting }}")
    _write(base / "_variables" / "notes" / "greeting.jinja2", "BASE")


def _make_super_extension(ext: Path) -> None:
    """Extension whose ``notes/greeting`` composes onto the base via super-ref."""
    _write(ext / "_variables" / "notes" / "greeting.jinja2", "{{ __super__ }}\nEXT")


def _manager(base: Path) -> TemplateManager:
    return TemplateManager(
        templates=str(base),
        template_formatter=format_template,
        predefined_variables=False,
        active_template_root_space=None,
        active_template_type="main",
    )


class WithVariableExtensionsIdentityTest(unittest.TestCase):
    """The fork is copy-on-write; the source manager is never mutated."""

    def test_empty_roots_returns_same_manager(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            _make_base(base)
            mgr = _manager(base)
            self.assertIs(mgr.with_variable_extensions([]), mgr)

    def test_fork_records_resolved_extension_roots(self) -> None:
        with (
            tempfile.TemporaryDirectory() as tmp_base,
            tempfile.TemporaryDirectory() as tmp_ext,
        ):
            base = Path(tmp_base)
            ext = Path(tmp_ext)
            _make_base(base)
            _make_super_extension(ext)
            fork = _manager(base).with_variable_extensions([ext])
            self.assertEqual(fork._variable_extension_roots, [ext.resolve()])

    def test_source_file_space_is_not_mutated(self) -> None:
        with (
            tempfile.TemporaryDirectory() as tmp_base,
            tempfile.TemporaryDirectory() as tmp_ext,
        ):
            base = Path(tmp_base)
            ext = Path(tmp_ext)
            _make_base(base)
            _make_super_extension(ext)
            mgr = _manager(base)
            original_file_space = mgr._file_space
            fork = mgr.with_variable_extensions([ext])
            self.assertIs(mgr._file_space, original_file_space)
            self.assertIsNot(fork._file_space, original_file_space)

    def test_source_loaders_dict_is_not_mutated(self) -> None:
        with (
            tempfile.TemporaryDirectory() as tmp_base,
            tempfile.TemporaryDirectory() as tmp_ext,
        ):
            base = Path(tmp_base)
            ext = Path(tmp_ext)
            _make_base(base)
            _make_super_extension(ext)
            mgr = _manager(base)
            original_loaders = mgr._variable_loaders_by_root
            fork = mgr.with_variable_extensions([ext])
            self.assertIs(mgr._variable_loaders_by_root, original_loaders)
            self.assertIsNot(fork._variable_loaders_by_root, original_loaders)

    def test_source_extension_fields_stay_empty(self) -> None:
        with (
            tempfile.TemporaryDirectory() as tmp_base,
            tempfile.TemporaryDirectory() as tmp_ext,
        ):
            base = Path(tmp_base)
            ext = Path(tmp_ext)
            _make_base(base)
            _make_super_extension(ext)
            mgr = _manager(base)
            mgr.with_variable_extensions([ext], disabled_keys={"notes_greeting"})
            self.assertEqual(mgr._variable_extension_roots, [])
            self.assertEqual(mgr._variable_extension_disabled_keys, frozenset())


class WithVariableExtensionsResolutionTest(unittest.TestCase):
    """Extensions shadow/compose the base on both cascade subsystems."""

    def test_super_composes_over_base_on_automatic_path(self) -> None:
        with (
            tempfile.TemporaryDirectory() as tmp_base,
            tempfile.TemporaryDirectory() as tmp_ext,
        ):
            base = Path(tmp_base)
            ext = Path(tmp_ext)
            _make_base(base)
            _make_super_extension(ext)
            fork = _manager(base).with_variable_extensions([ext])
            self.assertEqual(fork().strip(), "BASE\nEXT")

    def test_automatic_path_is_base_only_without_extensions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            _make_base(base)
            self.assertEqual(_manager(base)().strip(), "BASE")

    def test_super_composes_over_base_on_explicit_path(self) -> None:
        with (
            tempfile.TemporaryDirectory() as tmp_base,
            tempfile.TemporaryDirectory() as tmp_ext,
        ):
            base = Path(tmp_base)
            ext = Path(tmp_ext)
            _make_base(base)
            _make_super_extension(ext)
            fork = _manager(base).with_variable_extensions([ext])
            resolved = fork.load_variables({"notes.greeting": None}, root_space="")
            self.assertEqual(resolved, {"notes": {"greeting": "BASE\nEXT"}})

    def test_explicit_path_is_base_only_without_extensions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            _make_base(base)
            resolved = _manager(base).load_variables(
                {"notes.greeting": None}, root_space=""
            )
            self.assertEqual(resolved, {"notes": {"greeting": "BASE"}})

    def test_base_render_is_unaffected_after_deriving_fork(self) -> None:
        with (
            tempfile.TemporaryDirectory() as tmp_base,
            tempfile.TemporaryDirectory() as tmp_ext,
        ):
            base = Path(tmp_base)
            ext = Path(tmp_ext)
            _make_base(base)
            _make_super_extension(ext)
            mgr = _manager(base)
            before = mgr().strip()
            mgr.with_variable_extensions([ext])
            after = mgr().strip()
            self.assertEqual(before, "BASE")
            self.assertEqual(after, "BASE")


class WithVariableExtensionsDisableTest(unittest.TestCase):
    """A disabled key skips the extension roots and resolves base-only."""

    def test_disabled_key_falls_back_to_base_only(self) -> None:
        # disabled_keys matches the raw template token ``notes_greeting`` (what
        # the resolver extracts from ``{{ notes_greeting }}``), not the dotted
        # explicit-load form.
        with (
            tempfile.TemporaryDirectory() as tmp_base,
            tempfile.TemporaryDirectory() as tmp_ext,
        ):
            base = Path(tmp_base)
            ext = Path(tmp_ext)
            _make_base(base)
            _make_super_extension(ext)
            mgr = _manager(base)
            enabled = mgr.with_variable_extensions([ext])
            disabled = mgr.with_variable_extensions(
                [ext], disabled_keys={"notes_greeting"}
            )
            self.assertEqual(enabled().strip(), "BASE\nEXT")
            self.assertEqual(disabled().strip(), "BASE")


class WithVariableExtensionsWhitespaceSeamTest(unittest.TestCase):
    """A guarded base composed via super renders its guard at Pass-2 without
    concatenating across the super seam (the whitespace nuance): the newline the
    extension writes after ``{{ __super__ }}`` survives the composed value's Pass-2
    render because the base uses non-trimming block tags."""

    def test_guarded_base_composed_via_super_survives_pass2_without_seam_merge(
        self,
    ) -> None:
        with (
            tempfile.TemporaryDirectory() as tmp_base,
            tempfile.TemporaryDirectory() as tmp_ext,
        ):
            base = Path(tmp_base)
            ext = Path(tmp_ext)
            _write(base / "main" / "default.jinja2", "{{ notes_greeting }}")
            _write(
                base / "_variables" / "notes" / "greeting.jinja2",
                "{% if flag %}BASE{% endif %}",
            )
            _write(
                ext / "_variables" / "notes" / "greeting.jinja2",
                "{{ __super__ }}\nDELTA",
            )
            fork = _manager(base).with_variable_extensions([ext])
            composed = fork.load_variables({"notes.greeting": None}, root_space="")[
                "notes"
            ]["greeting"]
            # Pass-1: super pulled the (still-guarded) base body in; token is gone.
            self.assertNotIn("__super__", composed)
            self.assertIn("{% if flag %}", composed)
            # Pass-2, guard true: base body kept and the seam newline is preserved.
            rendered_on = format_template(composed, feed={"flag": True})
            self.assertEqual(rendered_on, "BASE\nDELTA")
            self.assertNotIn("BASEDELTA", rendered_on)
            # Pass-2, guard false: base gated out, the extension delta still renders.
            rendered_off = format_template(composed, feed={"flag": False})
            self.assertNotIn("BASE", rendered_off)
            self.assertIn("DELTA", rendered_off)


class WithVariableExtensionsByteIdentityTest(unittest.TestCase):
    """The empty-roots fork is the identity, and an extension override is additive:
    every key the extension does not ship resolves byte-for-byte the same on the
    fork as on the base, across a corpus of variable shapes (plain, nested-dir, and
    a Pass-2 guard). This pins the byte-neutrality invariant."""

    def test_identity_and_additive_override_preserve_byte_identity(self) -> None:
        corpus = {
            "notes.greeting": "BASE_G",
            "notes.farewell": "BASE_F",
            "instructions.behavior.file_reading": "BASE_I",
            "notes.guarded": "{% if show %}SHOWN{% endif %}",
        }
        with (
            tempfile.TemporaryDirectory() as tmp_base,
            tempfile.TemporaryDirectory() as tmp_ext,
        ):
            base = Path(tmp_base)
            ext = Path(tmp_ext)
            _write(base / "main" / "default.jinja2", "{{ notes_greeting }}")
            for key, value in corpus.items():
                _write(base / "_variables" / f"{key.replace('.', '/')}.jinja2", value)
            _write(ext / "_variables" / "notes" / "greeting.jinja2", "EXT_G")

            # Empty roots -> the same manager object (byte-identical by construction).
            mgr = _manager(base)
            self.assertIs(mgr.with_variable_extensions([]), mgr)

            baseline = _manager(base)
            fork = _manager(base).with_variable_extensions([ext])

            # The overridden key changes on the fork; the base resolves unchanged.
            self.assertEqual(
                fork.load_variables({"notes.greeting": None}, root_space="")["notes"][
                    "greeting"
                ],
                "EXT_G",
            )
            self.assertEqual(
                baseline.load_variables({"notes.greeting": None}, root_space="")[
                    "notes"
                ]["greeting"],
                "BASE_G",
            )

            # Every key the extension does not ship is byte-identical fork vs base.
            others = {key: None for key in corpus if key != "notes.greeting"}
            self.assertEqual(
                fork.load_variables(others, root_space=""),
                baseline.load_variables(others, root_space=""),
            )
