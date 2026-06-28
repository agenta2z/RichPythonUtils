"""Tests for the ${len:...} OmegaConf resolver.

``${len:...}`` derives an int count from a list so a count and its list cannot
drift apart (e.g. ``num_flows: ${len:${_params.flow_inferencers}}``). It returns
a primitive int, which OmegaConf stores cleanly into a scalar node — unlike a
list-returning resolver, which fails ``OmegaConf.resolve`` in place.
"""

from __future__ import annotations

import pytest
from omegaconf import OmegaConf

from rich_python_utils.config_utils._resolvers import ensure_resolvers


class TestLenResolver:
    @pytest.fixture(autouse=True)
    def _register(self):
        ensure_resolvers()

    def test_len_of_inline_list(self):
        cfg = OmegaConf.create({"n": "${len:${items}}", "items": ["a", "b", "c"]})
        assert cfg.n == 3
        assert isinstance(cfg.n, int)

    def test_len_locked_to_list_length(self):
        """The canonical use: a count derived from a list stays consistent."""
        cfg = OmegaConf.create(
            {
                "_params": {
                    "flow_inferencers": ["X", "Y"],
                    "num_flows": "${len:${_params.flow_inferencers}}",
                }
            }
        )
        assert cfg._params.num_flows == 2
        # mutate the list → derived count follows
        cfg._params.flow_inferencers = ["X", "Y", "Z"]
        assert cfg._params.num_flows == 3

    def test_len_of_empty_list(self):
        cfg = OmegaConf.create({"n": "${len:${items}}", "items": []})
        assert cfg.n == 0

    def test_len_of_string(self):
        cfg = OmegaConf.create({"n": "${len:abcd}"})
        assert cfg.n == 4
