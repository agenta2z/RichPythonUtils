"""Part E (C7): the ``lazy_config_factory`` field-metadata opt-in.

A field carrying ``metadata={"lazy_config_factory": True}`` is wrapped in a
``LazyConfigFactory`` (fresh sub-tree per call) exactly like a ``*_factory``-suffix
field — but WITHOUT the magic suffix, so a clean field name (e.g. ``worker_inferencers``)
can be lazy. Regression: the legacy ``*_factory`` suffix path still works.
"""

from omegaconf import OmegaConf

from rich_python_utils.config_utils import instantiate
from rich_python_utils.config_utils._lazy_config_factory import LazyConfigFactory

_BTA = "test_slot_defaults_helpers.FakeBTA"
_SIMPLE = "test_helpers.SimpleAttrs"


def test_metadata_gated_field_becomes_lazy_factory():
    cfg = OmegaConf.create(
        {
            "_target_": _BTA,
            "worker_inferencers": {"_target_": _SIMPLE, "name": "w", "count": 1},
        }
    )
    obj = instantiate(cfg)
    # The metadata-gated field is a LazyConfigFactory, NOT an eager instance.
    assert isinstance(obj.worker_inferencers, LazyConfigFactory)
    # Fresh, independent instances per call (the fresh-per-subtask guarantee).
    a = obj.worker_inferencers()
    b = obj.worker_inferencers()
    assert a is not b
    assert a.name == "w" and a.count == 1
    assert b.name == "w"


def test_suffix_factory_still_lazy_regression():
    cfg = OmegaConf.create(
        {
            "_target_": _BTA,
            "worker_factory": {"_target_": _SIMPLE, "name": "f"},
        }
    )
    obj = instantiate(cfg)
    assert isinstance(obj.worker_factory, LazyConfigFactory)
    assert obj.worker_factory() is not obj.worker_factory()


def test_metadata_field_unset_stays_none():
    obj = instantiate(OmegaConf.create({"_target_": _BTA}))
    assert obj.worker_inferencers is None
