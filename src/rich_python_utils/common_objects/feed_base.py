"""FeedBase — structured base class for template feed objects.

Provides dict protocol support so instances work with dict(), {**obj},
and obj["key"]. Subclasses override to_feed() to control which fields
are visible to templates, and to_dict() for serialization.

Usage::

    @dataclass
    class SOPState(FeedBase):
        name: str = ""
        _internal: Any = field(default=None, repr=False)

        def to_feed(self) -> dict[str, Any]:
            return {"name": self.name}  # _internal excluded from template

    state = SOPState(name="test")
    dict(state)     # {"name": "test"}
    {**state}       # {"name": "test"}
    state["name"]   # "test"
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any, Iterator


@dataclass
class FeedBase:
    """Base class for structured template feed objects.

    Subclasses should be @dataclass decorated. Provides:
      - to_feed() → dict of template-visible keys (override to filter)
      - to_dict() → dict of all serializable fields (for persistence)
      - from_dict() → reconstruct from serialized dict
      - Dict protocol: dict(obj), {**obj}, obj["key"]
    """

    def to_feed(self) -> dict[str, Any]:
        """Keys visible to the template. Override to filter internal fields."""
        return self.to_dict()

    def to_dict(self) -> dict[str, Any]:
        """All serializable fields. Recurses into nested FeedBase children."""
        result = {}
        for f in fields(self):
            val = getattr(self, f.name)
            if isinstance(val, FeedBase):
                result[f.name] = val.to_dict()
            elif hasattr(val, "to_dict") and callable(val.to_dict):
                result[f.name] = val.to_dict()
            else:
                result[f.name] = val
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FeedBase:
        """Reconstruct from a serialized dict. Only passes known fields."""
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in known})

    # ── Dict protocol ────────────────────────────────────────────────

    def keys(self) -> list[str]:
        return list(self.to_feed().keys())

    def __getitem__(self, key: str) -> Any:
        feed = self.to_feed()
        if key not in feed:
            raise KeyError(key)
        return feed[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.to_feed())

    def __len__(self) -> int:
        return len(self.to_feed())

    def __contains__(self, key: object) -> bool:
        return key in self.to_feed()


def build_feed(*sources: dict | FeedBase | None) -> dict[str, Any]:
    """Merge dicts and FeedBase objects into a single template feed.

    Later sources override earlier ones. None sources are skipped.
    FeedBase instances are converted via to_feed().

    Usage::

        feed = build_feed(
            template_vars,      # lowest priority
            prior_context,      # dict
            sop_state,          # FeedBase → to_feed(); or None → skipped
            {"key": "value"},   # highest priority
        )
    """
    result: dict[str, Any] = {}
    for source in sources:
        if source is None:
            continue
        if isinstance(source, FeedBase):
            result.update(source.to_feed())
        elif isinstance(source, dict):
            result.update(source)
        else:
            raise TypeError(
                f"build_feed: expected dict, FeedBase, or None — got {type(source).__name__}"
            )
    return result
