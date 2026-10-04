"""``Debuggable._log_display_name``: the name a record carries in place of the
default class-name ``log_name``."""

from rich_python_utils.common_objects.debuggable import Debuggable


class _Node(Debuggable):
    def __init__(self, name=None, **kwargs):
        super().__init__(**kwargs)
        self.name = name


class _PerCallNode(_Node):
    """Resolves its display name per call instead of writing ``name``."""

    current = None

    def _log_display_name(self):
        return self.current


def _node(cls, **kwargs):
    records = []
    node = cls(
        logger=records.append,
        always_add_logging_based_logger=False,
        log_time=False,
        **kwargs,
    )
    node.records = records
    return node


def _logged_names(node):
    node.records.clear()
    node.log_info("record", "Test")
    return [record["name"] for record in node.records]


def test_a_record_carries_the_name_attribute_else_the_class_name():
    assert _logged_names(_node(_Node, name="outer.worker_00")) == ["outer.worker_00"]
    assert _logged_names(_node(_Node)) == ["_Node"]


def test_an_explicit_log_name_wins_over_the_display_name():
    node = _node(_Node, name="outer.worker_00", log_name="explicit")
    assert _logged_names(node) == ["explicit"]


def test_an_override_supplies_the_display_name_without_writing_name():
    node = _node(_PerCallNode, name="configured")
    node.current = "outer.worker_01"
    assert _logged_names(node) == ["outer.worker_01"]
    node.current = None
    assert _logged_names(node) == ["_PerCallNode"]
    assert node.name == "configured"
