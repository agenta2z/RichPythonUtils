"""Tests for merge_dict_typed_attributes in the Hydra walker.

When enabled (default), dict-typed attrs fields with factory defaults are
merged (YAML wins per-key) instead of replaced during instantiation.
"""

from __future__ import annotations

import unittest

import attr
from attr import attrib, attrs

from omegaconf import OmegaConf

from rich_python_utils.config_utils import instantiate


@attrs
class _ModesExample:
    """Simulates TemplatedInferencerBase's modes field."""
    name: str = attrib(default="test")
    modes: dict = attrib(factory=lambda: {"deep_mode": True, "elegant_mode": True})


@attrs
class _NestedDictExample:
    """Tests nested dict merge."""
    config: dict = attrib(factory=lambda: {
        "level1": {"a": 1, "b": 2},
        "level2": {"c": 3},
    })


@attrs
class _LiteralDictDefault:
    """Tests dict literal default (not factory)."""
    options: dict = attrib(default={"x": 10, "y": 20})


@attrs
class _NoDictFields:
    """Tests that non-dict fields are unaffected."""
    name: str = attrib(default="original")
    count: int = attrib(default=5)


@attrs
class _TakesSelfFactory:
    """Tests factory with takes_self=True — should be skipped."""
    data: dict = attrib(default=attr.Factory(
        lambda self: {"derived": self.name}, takes_self=True
    ))
    name: str = attrib(default="base")


def _cfg(target_cls, **overrides):
    """Build a minimal Hydra-style OmegaConf config."""
    raw = {
        "_target_": f"{target_cls.__module__}.{target_cls.__qualname__}",
        **overrides,
    }
    return OmegaConf.create(raw)


class TestDictFactoryMerge(unittest.TestCase):

    def test_partial_override_merges_with_factory_default(self):
        cfg = _cfg(_ModesExample, modes={"deep_mode": False})
        result = instantiate(cfg)
        self.assertEqual(result.modes, {"deep_mode": False, "elegant_mode": True})

    def test_yaml_key_wins_over_default(self):
        cfg = _cfg(_ModesExample, modes={"deep_mode": False, "elegant_mode": False})
        result = instantiate(cfg)
        self.assertEqual(result.modes, {"deep_mode": False, "elegant_mode": False})

    def test_yaml_adds_new_key(self):
        cfg = _cfg(_ModesExample, modes={"verbose_mode": True})
        result = instantiate(cfg)
        self.assertEqual(result.modes, {
            "deep_mode": True, "elegant_mode": True, "verbose_mode": True,
        })

    def test_no_modes_in_yaml_uses_full_default(self):
        cfg = _cfg(_ModesExample)
        result = instantiate(cfg)
        self.assertEqual(result.modes, {"deep_mode": True, "elegant_mode": True})


class TestNestedDictMerge(unittest.TestCase):

    def test_nested_dict_deep_merged(self):
        cfg = _cfg(_NestedDictExample, config={"level1": {"a": 99}})
        result = instantiate(cfg)
        self.assertEqual(result.config["level1"], {"a": 99, "b": 2})
        self.assertEqual(result.config["level2"], {"c": 3})


class TestLiteralDictDefault(unittest.TestCase):

    def test_literal_dict_default_merged(self):
        cfg = _cfg(_LiteralDictDefault, options={"x": 99})
        result = instantiate(cfg)
        self.assertEqual(result.options, {"x": 99, "y": 20})


class TestNonDictFieldsUnaffected(unittest.TestCase):

    def test_scalar_fields_not_merged(self):
        cfg = _cfg(_NoDictFields, name="override", count=10)
        result = instantiate(cfg)
        self.assertEqual(result.name, "override")
        self.assertEqual(result.count, 10)


class TestDisableFlag(unittest.TestCase):

    def test_merge_disabled_replaces_entirely(self):
        cfg = _cfg(_ModesExample, modes={"deep_mode": False})
        result = instantiate(cfg, merge_dict_typed_attributes=False)
        self.assertEqual(result.modes, {"deep_mode": False})
        self.assertNotIn("elegant_mode", result.modes)


class TestTakesSelfSkipped(unittest.TestCase):

    def test_takes_self_factory_not_called(self):
        cfg = _cfg(_TakesSelfFactory, data={"override": True})
        result = instantiate(cfg)
        self.assertEqual(result.data, {"override": True})


@attrs
class _RequiredDictField:
    """Tests field with no default (required)."""
    data: dict = attrib()


class TestFieldWithNoDefault(unittest.TestCase):

    def test_required_field_passthrough(self):
        cfg = _cfg(_RequiredDictField, data={"key": "val"})
        result = instantiate(cfg)
        self.assertEqual(result.data, {"key": "val"})


# ---------------------------------------------------------------------------
# Regression: empty-dict factory defaults are unaffected
# ---------------------------------------------------------------------------


@attrs
class _EmptyDictDefaults:
    """Simulates template_variables/template_extra_feed (factory=dict)."""
    template_variables: dict = attrib(factory=dict)
    template_extra_feed: dict = attrib(factory=dict)
    modes: dict = attrib(factory=lambda: {"deep_mode": True, "elegant_mode": True})


class TestEmptyDictDefaultsUnaffected(unittest.TestCase):

    def test_empty_factory_produces_same_result_with_merge(self):
        cfg = _cfg(_EmptyDictDefaults,
                    template_variables={"task_preamble": "create_role"},
                    template_extra_feed={"employee": {"name": "Alice"}})
        result = instantiate(cfg)
        self.assertEqual(result.template_variables, {"task_preamble": "create_role"})
        self.assertEqual(result.template_extra_feed, {"employee": {"name": "Alice"}})

    def test_empty_factory_produces_same_result_without_merge(self):
        cfg = _cfg(_EmptyDictDefaults,
                    template_variables={"task_preamble": "create_role"})
        result = instantiate(cfg, merge_dict_typed_attributes=False)
        self.assertEqual(result.template_variables, {"task_preamble": "create_role"})

    def test_modes_merged_alongside_empty_dict_fields(self):
        cfg = _cfg(_EmptyDictDefaults,
                    template_variables={"task_preamble": None},
                    modes={"deep_mode": False})
        result = instantiate(cfg)
        self.assertEqual(result.template_variables, {"task_preamble": None})
        self.assertEqual(result.modes, {"deep_mode": False, "elegant_mode": True})


# ---------------------------------------------------------------------------
# SLOT_DEFAULTS + walker merge composition
# ---------------------------------------------------------------------------


@attrs
class _SlotDefaultsTarget:
    """Simulates an aggregator inferencer node after SLOT_DEFAULTS apply_to()."""
    name: str = attrib(default="aggregator")
    modes: dict = attrib(factory=lambda: {"deep_mode": True, "elegant_mode": True})


def _simulate_slot_defaults_merge(node: dict, slot_modes: dict) -> None:
    """Simulates InferencerTemplateDefaults.apply_to() _merge_dict for modes.

    SLOT_DEFAULTS base, YAML user values win (same as _merge_dict).
    """
    import copy
    existing = node.get("modes") or {}
    merged = copy.deepcopy(slot_modes)
    merged.update(existing)
    node["modes"] = merged


class TestSlotDefaultsComposition(unittest.TestCase):
    """Simulates the real flow: SLOT_DEFAULTS apply_to() sets a partial
    modes dict in the YAML node, then the walker merges it with the
    attrs factory default."""

    def test_slot_defaults_partial_modes_merged_with_factory(self):
        node = {"_target_": f"{_SlotDefaultsTarget.__module__}.{_SlotDefaultsTarget.__qualname__}"}
        _simulate_slot_defaults_merge(node, {"deep_mode": False})
        self.assertEqual(node["modes"], {"deep_mode": False})

        cfg = OmegaConf.create(node)
        result = instantiate(cfg)
        self.assertEqual(result.modes, {"deep_mode": False, "elegant_mode": True})

    def test_slot_defaults_plus_yaml_user_override(self):
        node = {
            "_target_": f"{_SlotDefaultsTarget.__module__}.{_SlotDefaultsTarget.__qualname__}",
            "modes": {"elegant_mode": False},
        }
        _simulate_slot_defaults_merge(node, {"deep_mode": False})
        self.assertEqual(node["modes"], {"deep_mode": False, "elegant_mode": False})

        cfg = OmegaConf.create(node)
        result = instantiate(cfg)
        self.assertEqual(result.modes, {"deep_mode": False, "elegant_mode": False})

    def test_three_way_merge_slot_plus_yaml_plus_attrs(self):
        """SLOT sets deep_mode=False, YAML adds verbose_mode=True,
        attrs default provides elegant_mode=True. All three should appear."""
        node = {
            "_target_": f"{_SlotDefaultsTarget.__module__}.{_SlotDefaultsTarget.__qualname__}",
            "modes": {"verbose_mode": True},
        }
        _simulate_slot_defaults_merge(node, {"deep_mode": False})
        self.assertEqual(node["modes"], {"deep_mode": False, "verbose_mode": True})

        cfg = OmegaConf.create(node)
        result = instantiate(cfg)
        self.assertEqual(result.modes, {
            "deep_mode": False,
            "elegant_mode": True,
            "verbose_mode": True,
        })


    def test_yaml_wins_over_slot_on_same_key(self):
        """YAML explicitly sets deep_mode=True, overriding SLOT's False.
        Priority: YAML > SLOT > attrs."""
        node = {
            "_target_": f"{_SlotDefaultsTarget.__module__}.{_SlotDefaultsTarget.__qualname__}",
            "modes": {"deep_mode": True},
        }
        _simulate_slot_defaults_merge(node, {"deep_mode": False})
        # After SLOT merge: YAML's True wins over SLOT's False
        self.assertTrue(node["modes"]["deep_mode"])

        cfg = OmegaConf.create(node)
        result = instantiate(cfg)
        self.assertEqual(result.modes, {"deep_mode": True, "elegant_mode": True})

    def test_yaml_restores_attrs_default_over_slot(self):
        """SLOT disables deep_mode, YAML re-enables it (matching attrs default).
        Proves YAML > SLOT even when YAML value equals attrs default."""
        node = {
            "_target_": f"{_SlotDefaultsTarget.__module__}.{_SlotDefaultsTarget.__qualname__}",
            "modes": {"deep_mode": True, "elegant_mode": False},
        }
        _simulate_slot_defaults_merge(node, {"deep_mode": False})
        # YAML's deep_mode=True wins over SLOT's False
        self.assertTrue(node["modes"]["deep_mode"])
        # YAML's elegant_mode=False wins (no SLOT override for this key)
        self.assertFalse(node["modes"]["elegant_mode"])

        cfg = OmegaConf.create(node)
        result = instantiate(cfg)
        # deep_mode: YAML True > SLOT False > attrs True → True
        # elegant_mode: YAML False > attrs True → False
        self.assertEqual(result.modes, {"deep_mode": True, "elegant_mode": False})


if __name__ == "__main__":
    unittest.main()
