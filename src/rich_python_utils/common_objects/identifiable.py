"""
Identifiable base class for objects that need a unique identifier.
"""
import copy
import uuid
from abc import ABC
from typing import Optional, Union, Any, Callable

from attr import attrs, attrib


@attrs(slots=False)
class Identifiable(ABC):
    """
    A base class to provide unique identification for objects.

    Attributes:
        id (Union[int, str], optional):
            Unique identifier for this object instance. Auto-generated if None.
            Format: `{ClassName}-{uuid}` (e.g., "Agent-a3f2b1c4").
        auto_id_suffix (Union[bool, Callable], optional):
            Controls the suffix appended to the ID:
            - None or False: No automatic suffix (use the ID as-is)
            - True: Append a UUID suffix (e.g., "MyID-a3f2b1c4")
            - Callable: Call it to generate the suffix (e.g., lambda: "v2")
        enable_suffix_for_initial_id (bool, optional):
            Also apply ``auto_id_suffix`` to an explicitly-provided ``id`` (default
            False — an explicit id is used verbatim). Auto-generated ids (id=None)
            always get a UUID suffix regardless of this flag.

    Examples:
        Auto-generated id (always suffixed):
        >>> class MyObject(Identifiable):
        ...     pass
        >>> obj = MyObject()
        >>> obj.id  # doctest: +ELLIPSIS
        'MyObject-...'

        Explicit id, used verbatim (no suffix by default):
        >>> obj2 = MyObject(id="CustomID", auto_id_suffix=False)
        >>> obj2.id
        'CustomID'

        Explicit id with an automatic UUID suffix (opt in via enable_suffix_for_initial_id):
        >>> obj3 = MyObject(id="CustomID", auto_id_suffix=True, enable_suffix_for_initial_id=True)
        >>> obj3.id  # doctest: +ELLIPSIS
        'CustomID-...'

        Explicit id with a callable suffix:
        >>> obj4 = MyObject(id="CustomID", auto_id_suffix=lambda: "v2", enable_suffix_for_initial_id=True)
        >>> obj4.id
        'CustomID-v2'
    """
    id: Optional[Union[int, str]] = attrib(default=None, kw_only=True)
    _auto_id_suffix: Union[bool, Callable] = attrib(default=None, kw_only=True, alias='auto_id_suffix')
    _enable_suffix_for_initial_id : bool = attrib(default=False, kw_only=True, alias='enable_suffix_for_initial_id')
    _raw_id: Any = attrib(default=None, kw_only=True)

    def new_id(self):
        """
        Generate a new ID for this object.

        Behavior:
        - If _raw_id exists and _auto_id_suffix is enabled, appends suffix
        - If _raw_id exists but _auto_id_suffix is False, uses _raw_id as-is
        - If no _raw_id, generates {ClassName}_{uuid}
        """
        import uuid

        if self._raw_id:
            # We have a custom ID
            if self._auto_id_suffix is False:
                # Explicitly disabled suffix - use raw ID as-is
                self.id = f"{self._raw_id}"
            elif self._auto_id_suffix is True:
                # Explicitly enabled suffix - append UUID
                self.id = f"{self._raw_id}-{uuid.uuid4().hex[:8]}"
            elif callable(self._auto_id_suffix):
                # Custom suffix generator
                suffix = self._auto_id_suffix()
                self.id = f"{self._raw_id}-{suffix}"
            else:
                # _auto_id_suffix is None - default to appending UUID
                self.id = f"{self._raw_id}-{uuid.uuid4().hex[:8]}"
        else:
            # No custom ID - generate default with UUID
            self.id = f"{self.__class__.__name__}-{uuid.uuid4().hex[:8]}"


    def __attrs_post_init__(self):
        """Initialize the ID after object creation (delegates to :meth:`_resolve_id_value`)."""
        self._resolve_id_value()

    def _resolve_id_value(self):
        """Resolve ``id``/``_raw_id`` from the current ``self.id`` (auto-generate if None).

        Shared by construction *and* clone-time id regeneration so both paths stay
        consistent.

        Logic:
        - If id is None, generate a new auto ID (``{ClassName}-{uuid}``).
        - If id is provided: store it in _raw_id, normalize, and apply suffix logic only
          when ``_enable_suffix_for_initial_id`` is set.
        """
        # Auto-generate id if not provided
        if self.id is None:
            self._raw_id = None
            self.new_id()
        else:
            # Store the raw ID
            self._raw_id = self.id

            if isinstance(self._raw_id, str):
                self._raw_id = self._raw_id.replace("$class", self.__class__.__name__)
                self.id = self._raw_id
            elif isinstance(self._raw_id, int):
                self.id = self._raw_id
            else:
                self.id = f"{self._raw_id}"

            # DO NOT apply suffix logic initially if `_enable_suffix_for_initial_id` is set False
            if self._enable_suffix_for_initial_id:
                self.new_id()

    # ───────────────────────────────────────────────────────────────────────
    # Clone-with-fresh-identity (EXPLICIT — does NOT override copy.deepcopy)
    #
    # ``copy.deepcopy`` faithfully replicates the ``id``/``_raw_id`` attributes, so two
    # deep-copies share the same identity string — fine for "an exact replica", but wrong
    # when you want a genuinely NEW entity (e.g. a fresh worker per subtask), where shared
    # ids collide in logging and the parent/child object graph. These methods give that
    # "new entity" semantic WITHOUT touching ``__deepcopy__`` — so plain ``copy.deepcopy``
    # (and everything relying on it: serialization, purity snapshots, caches) is unchanged.
    # ───────────────────────────────────────────────────────────────────────

    # Attribute names skipped when regenerating ids across a clone's tree (back-references
    # that would otherwise walk UP the object graph). Subclasses extend this.
    _FRESH_ID_SKIP_TRAVERSE = frozenset()

    def _reset_fresh_id(self, new_id_value=None):
        """Reset this object's identity in place. ``new_id_value=None`` → a fresh auto id;
        otherwise that value becomes the raw id (same normalization/suffix rules as
        construction, via :meth:`_resolve_id_value`)."""
        self.id = new_id_value
        self._resolve_id_value()

    def copy_with_fresh_id(self, id=None):
        """Return a SHALLOW copy with a regenerated ``id`` (nested objects are SHARED).

        If ``id`` is given it becomes the new raw id; otherwise a fresh
        ``{ClassName}-{uuid}`` is generated. For a fully independent clone whose nested
        :class:`Identifiable` objects also get fresh ids, use :meth:`deepcopy_with_fresh_id`.
        """
        clone = copy.copy(self)
        clone._reset_fresh_id(id)
        return clone

    def deepcopy_with_fresh_id(self, id=None):
        """Return a DEEP, fully-independent copy whose ENTIRE tree gets fresh ids — a new
        entity, not a replica.

        The root uses ``id`` if given (else a fresh auto id); every nested
        :class:`Identifiable` reachable through attributes and dict/list/tuple/set
        containers gets a fresh auto id. ``copy.deepcopy`` is used internally but is NOT
        overridden, so global copy semantics are untouched.
        """
        clone = copy.deepcopy(self)
        clone._reset_fresh_id(id)
        for nested in clone._iter_nested_identifiables():
            nested._reset_fresh_id(None)
        return clone

    def _iter_nested_identifiables(self):
        """Yield every nested :class:`Identifiable` reachable from this object (excluding
        ``self`` and ``_FRESH_ID_SKIP_TRAVERSE`` back-refs), de-duped by object identity,
        traversing dict/list/tuple/set containers. Cycle-safe."""
        import builtins
        seen = {builtins.id(self)}
        out = []
        stack = list(self._fresh_id_traverse_values())
        while stack:
            obj = stack.pop()
            oid = builtins.id(obj)
            if oid in seen:
                continue
            seen.add(oid)
            if isinstance(obj, Identifiable):
                out.append(obj)
                stack.extend(obj._fresh_id_traverse_values())
            elif isinstance(obj, dict):
                stack.extend(obj.values())
            elif isinstance(obj, (list, tuple, set, frozenset)):
                stack.extend(obj)
        return out

    def _fresh_id_traverse_values(self):
        """Instance-attribute values to traverse for nested-id regeneration (skips the
        back-reference attrs named in ``_FRESH_ID_SKIP_TRAVERSE``)."""
        skip = type(self)._FRESH_ID_SKIP_TRAVERSE
        d = getattr(self, "__dict__", None) or {}
        return [v for k, v in d.items() if k not in skip]



