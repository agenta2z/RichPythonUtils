"""Tests for FeedBase + build_feed + PhaseStatus."""

import unittest
from dataclasses import dataclass, field
from typing import Any

from rich_python_utils.common_objects.feed_base import FeedBase, build_feed
from rich_python_utils.common_objects.workflow.common.phase_status import PhaseStatus


# ── Test fixtures ────────────────────────────────────────────────────


@dataclass
class SimpleFeed(FeedBase):
    name: str = ""
    value: int = 0


@dataclass
class FilteredFeed(FeedBase):
    public_field: str = "visible"
    internal_obj: Any = field(default=None, repr=False)

    def to_feed(self) -> dict[str, Any]:
        return {"public_field": self.public_field}

    def to_dict(self) -> dict[str, Any]:
        d = super().to_dict()
        d.pop("internal_obj", None)
        return d


@dataclass
class NestedFeed(FeedBase):
    label: str = ""
    child: SimpleFeed | None = None


# ── FeedBase tests ───────────────────────────────────────────────────


class TestFeedBase(unittest.TestCase):
    def test_dict_conversion(self):
        f = SimpleFeed(name="test", value=42)
        assert dict(f) == {"name": "test", "value": 42}

    def test_spread(self):
        f = SimpleFeed(name="a", value=1)
        merged = {**f, "extra": True}
        assert merged == {"name": "a", "value": 1, "extra": True}

    def test_subscript(self):
        f = SimpleFeed(name="x")
        assert f["name"] == "x"

    def test_subscript_missing_key(self):
        f = SimpleFeed()
        with self.assertRaises(KeyError):
            _ = f["nonexistent"]

    def test_contains(self):
        f = SimpleFeed(name="y")
        assert "name" in f
        assert "missing" not in f

    def test_len(self):
        f = SimpleFeed()
        assert len(f) == 2

    def test_iter(self):
        f = SimpleFeed(name="z", value=9)
        assert sorted(f) == ["name", "value"]

    def test_to_dict_default(self):
        f = SimpleFeed(name="a", value=1)
        assert f.to_dict() == {"name": "a", "value": 1}

    def test_to_feed_default_equals_to_dict(self):
        f = SimpleFeed(name="b", value=2)
        assert f.to_feed() == f.to_dict()

    def test_to_feed_filtered(self):
        f = FilteredFeed(public_field="hello", internal_obj=object())
        feed = f.to_feed()
        assert feed == {"public_field": "hello"}
        assert "internal_obj" not in feed

    def test_to_dict_filtered(self):
        f = FilteredFeed(public_field="hi", internal_obj=object())
        d = f.to_dict()
        assert "internal_obj" not in d
        assert d["public_field"] == "hi"

    def test_dict_uses_to_feed(self):
        f = FilteredFeed(public_field="visible", internal_obj="hidden")
        assert dict(f) == {"public_field": "visible"}

    def test_from_dict(self):
        d = {"name": "restored", "value": 99}
        f = SimpleFeed.from_dict(d)
        assert f.name == "restored"
        assert f.value == 99

    def test_from_dict_ignores_extra_keys(self):
        d = {"name": "ok", "value": 1, "unknown": "ignored"}
        f = SimpleFeed.from_dict(d)
        assert f.name == "ok"

    def test_from_dict_roundtrip(self):
        original = SimpleFeed(name="rt", value=7)
        restored = SimpleFeed.from_dict(original.to_dict())
        assert restored.name == original.name
        assert restored.value == original.value

    def test_nested_to_dict(self):
        child = SimpleFeed(name="child", value=1)
        parent = NestedFeed(label="parent", child=child)
        d = parent.to_dict()
        assert d["child"] == {"name": "child", "value": 1}


# ── build_feed tests ────────────────────────────────────────────────


class TestBuildFeed(unittest.TestCase):
    def test_dicts_merge(self):
        result = build_feed({"a": 1}, {"b": 2})
        assert result == {"a": 1, "b": 2}

    def test_later_overrides(self):
        result = build_feed({"x": "old"}, {"x": "new"})
        assert result["x"] == "new"

    def test_none_skipped(self):
        result = build_feed({"a": 1}, None, {"b": 2})
        assert result == {"a": 1, "b": 2}

    def test_feedbase_flattened(self):
        f = SimpleFeed(name="test", value=42)
        result = build_feed({"other": True}, f)
        assert result["name"] == "test"
        assert result["value"] == 42
        assert result["other"] is True

    def test_feedbase_uses_to_feed(self):
        f = FilteredFeed(public_field="yes", internal_obj="no")
        result = build_feed(f)
        assert result == {"public_field": "yes"}
        assert "internal_obj" not in result

    def test_feedbase_override_by_dict(self):
        f = SimpleFeed(name="original")
        result = build_feed(f, {"name": "overridden"})
        assert result["name"] == "overridden"

    def test_all_none(self):
        result = build_feed(None, None)
        assert result == {}

    def test_empty(self):
        result = build_feed()
        assert result == {}

    def test_invalid_type_raises(self):
        with self.assertRaises(TypeError):
            build_feed("not a dict")

    def test_mixed_sources(self):
        f = SimpleFeed(name="sop", value=1)
        result = build_feed(
            {"template": "vars"},
            {"context": "data"},
            f,
            None,
            {"override": "yes"},
        )
        assert result["template"] == "vars"
        assert result["name"] == "sop"
        assert result["override"] == "yes"


# ── PhaseStatus tests ───────────────────────────────────────────────


class TestPhaseStatus(unittest.TestCase):
    def test_string_equality(self):
        assert PhaseStatus.IDLE == "idle"
        assert PhaseStatus.RUNNING == "running"
        assert PhaseStatus.COMPLETED == "completed"
        assert PhaseStatus.ERROR == "error"
        assert PhaseStatus.PAUSED == "paused"

    def test_is_str(self):
        assert isinstance(PhaseStatus.IDLE, str)

    def test_in_dict_key(self):
        d = {PhaseStatus.RUNNING: True}
        assert d["running"] is True

    def test_comparison_with_raw_string(self):
        status = PhaseStatus.COMPLETED
        assert status == "completed"
        assert "completed" == status


if __name__ == "__main__":
    unittest.main()
