"""``Identifiable.copy_with_fresh_id`` / ``deepcopy_with_fresh_id`` — explicit
clone-with-fresh-identity.

These produce a genuinely NEW entity (fresh ids across the tree) WITHOUT overriding
``copy.deepcopy`` — so plain ``copy.deepcopy`` keeps its replica semantics (verified here),
which is what serialization / purity snapshots / caches rely on.
"""

import copy

from attr import attrs, attrib

from rich_python_utils.common_objects.identifiable import Identifiable


@attrs(slots=False)
class _Node(Identifiable):
    child = attrib(default=None, kw_only=True)
    peers = attrib(factory=list, kw_only=True)
    mapping = attrib(factory=dict, kw_only=True)


def test_deepcopy_with_fresh_id_refreshes_root_and_all_nested():
    root = _Node(child=_Node(), peers=[_Node(), _Node()], mapping={"k": _Node()})
    a = root.deepcopy_with_fresh_id()

    # root fresh
    assert a.id != root.id
    # nested fresh — via attribute, list, and dict
    assert a.child.id != root.child.id
    assert {p.id for p in a.peers}.isdisjoint({p.id for p in root.peers})
    assert a.mapping["k"].id != root.mapping["k"].id
    # genuinely independent objects (deep)
    assert a.child is not root.child
    assert all(ap is not rp for ap, rp in zip(a.peers, root.peers))
    assert a.mapping["k"] is not root.mapping["k"]


def test_two_clones_are_mutually_distinct():
    root = _Node(child=_Node())
    a = root.deepcopy_with_fresh_id()
    b = root.deepcopy_with_fresh_id()
    assert a.id != b.id
    assert a.child.id != b.child.id
    assert a.child is not b.child


def test_plain_deepcopy_is_unchanged_no_global_override():
    # The whole point: we did NOT override __deepcopy__, so copy.deepcopy still
    # replicates ids verbatim (no blast radius on serialization/purity/etc.).
    root = _Node(child=_Node())
    c = copy.deepcopy(root)
    assert c.id == root.id
    assert c.child.id == root.child.id


def test_explicit_id_param_sets_root_only():
    root = _Node(child=_Node())
    a = root.deepcopy_with_fresh_id(id="MyId")
    assert a.id == "MyId"               # root uses the explicit id (as-is, no suffix by default)
    assert a.child.id != root.child.id  # nested still auto-fresh


def test_copy_with_fresh_id_is_shallow():
    root = _Node(child=_Node())
    a = root.copy_with_fresh_id()
    assert a.id != root.id           # root fresh
    assert a.child is root.child     # nested SHARED (shallow)


def test_cycle_is_handled():
    a = _Node()
    b = _Node()
    a.child = b
    b.child = a  # reference cycle
    clone = a.deepcopy_with_fresh_id()  # must not infinite-loop
    assert clone.id != a.id
    assert clone.child.id != b.id


def test_auto_id_default_format():
    n = _Node()
    assert n.id.startswith("_Node-")
    fresh = n.deepcopy_with_fresh_id()
    assert fresh.id.startswith("_Node-") and fresh.id != n.id
