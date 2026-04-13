"""Config loading, alias resolution, attrs preprocessing, and Hydra instantiation.

This module is the core of the config_utils package.  The public functions are
``load_config``, ``merge_configs``, and ``instantiate``.
"""

from __future__ import annotations

import importlib
import inspect
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from rich_python_utils.config_utils._registry import (
    _registry,
    resolve_target,
)
from rich_python_utils.config_utils._resolvers import (
    _current_config_dir,
    ensure_resolvers,
)

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_config(path: str, overrides: Optional[Dict[str, Any]] = None):
    """Load a YAML file into an OmegaConf ``DictConfig``.

    Parameters
    ----------
    path : str
        Path to the YAML file.
    overrides : dict, optional
        Key-value overrides merged on top of the loaded config via
        ``OmegaConf.merge``.

    Notes
    -----
    * Sets ``_current_config_dir`` (a ``ContextVar``) so that the
      ``${path:...}`` resolver can resolve paths relative to the YAML file.
    * Calls ``OmegaConf.resolve(cfg)`` **eagerly** so that the ``ContextVar``
      can be safely reset in ``finally``.  This means **all** interpolations
      (including ``${oc.env:...}``) are captured at load time.
    """
    from omegaconf import OmegaConf

    ensure_resolvers()
    token = _current_config_dir.set(str(Path(path).resolve().parent))
    try:
        cfg = OmegaConf.load(path)
        if overrides:
            cfg = OmegaConf.merge(cfg, overrides)
        # Eagerly resolve while the ContextVar is set.
        OmegaConf.resolve(cfg)
        return cfg
    finally:
        _current_config_dir.reset(token)


def merge_configs(*configs):
    """Merge multiple configs (dicts or ``DictConfig``).  Later configs win."""
    from omegaconf import OmegaConf

    return OmegaConf.merge(*configs)


# ---------------------------------------------------------------------------
# Instantiation
# ---------------------------------------------------------------------------

def instantiate(config, _convert_: str = "all", **kwargs) -> Any:
    """Instantiate a Python object from *config* using Hydra.

    This is a thin wrapper around ``hydra.utils.instantiate`` that first:
    1. Resolves target aliases → full import paths.
    2. Expands string shorthand (``field: Alias`` → ``field: {_target_: ...}``).
    3. Filters invalid attrs keys (``init=False`` fields) with a warning.

    Parameters
    ----------
    config : DictConfig or dict
        Configuration with ``_target_`` keys.
    _convert_ : str
        Hydra's ``_convert_`` parameter.  ``"all"`` (default) converts
        ``DictConfig``/``ListConfig`` to native Python types — **load-bearing**
        for attrs converters like ``dict_()``.
    **kwargs
        Extra keyword arguments forwarded to ``hydra.utils.instantiate``.
    """
    from hydra.utils import instantiate as _hydra_instantiate

    ensure_resolvers()
    config = _resolve_and_preprocess(config)
    return _hydra_instantiate(config, _convert_=_convert_, **kwargs)


# ---------------------------------------------------------------------------
# Internal: single-pass tree walk
# ---------------------------------------------------------------------------

def _resolve_and_preprocess(config):
    """Convert to plain dict, walk the tree, convert back to ``DictConfig``."""
    from omegaconf import OmegaConf

    raw = OmegaConf.to_container(config, resolve=False)
    _walk(raw)
    return OmegaConf.create(raw)


def _walk(node: Any) -> None:
    """Recursively process a mutable dict/list tree.

    For each dict node, three things happen in order:

    1. **Alias resolution** — if ``_target_`` is present and is a plain string
       (not an interpolation), resolve it via the registry.  Then import the
       target class and, for ``@attrs`` classes, filter out keys that are not
       valid ``__init__`` parameters.

    2. **String shorthand expansion** — for any non-Hydra key whose value is a
       plain string matching a registered alias, expand it into
       ``{"_target_": resolved_path}``.  This lets users write
       ``base_inferencer: ClaudeAPI`` instead of the verbose nested syntax.

    3. **Recursion** — recurse into all values (including freshly-expanded
       dicts from step 2, so they also get alias resolution + attrs filtering).
    """
    if isinstance(node, dict):
        # 1. Resolve _target_ alias + attrs preprocessing
        if "_target_" in node and isinstance(node["_target_"], str):
            target = node["_target_"]
            if not target.startswith("${"):
                node["_target_"] = resolve_target(target)
                cls = _import_target(node["_target_"])
                if cls is not None:
                    _filter_attrs_keys(node, cls)

        # 2. String shorthand expansion
        for key, val in list(node.items()):
            if key.startswith("_"):
                continue  # skip Hydra keys (_target_, _recursive_, etc.)
            if isinstance(val, str) and val in _registry:
                node[key] = {"_target_": resolve_target(val)}

        # 3. Recurse
        for v in node.values():
            _walk(v)

    elif isinstance(node, list):
        for item in node:
            _walk(item)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _import_target(target_path: str) -> Optional[type]:
    """Import and return the class/callable at *target_path*.

    Returns ``None`` on import failure — Hydra will give its own error later.
    """
    try:
        module_path, _, attr_name = target_path.rpartition(".")
        module = importlib.import_module(module_path)
        return getattr(module, attr_name, None)
    except (ImportError, ModuleNotFoundError):
        return None


def _filter_attrs_keys(node: dict, cls: type) -> None:
    """For ``@attrs`` classes, remove YAML keys that aren't valid ``__init__`` params.

    Uses ``inspect.signature(cls)`` which correctly handles:
    * Underscore stripping (``_secret_key`` → init param ``secret_key``)
    * Explicit aliases (``alias='reasoner'``)
    * ``init=False`` fields (excluded from signature)
    * ``kw_only=True`` fields
    * Inherited fields from the full MRO

    Non-attrs classes (dataclasses, Pydantic, plain) are skipped — Hydra
    handles those natively.
    """
    try:
        import attr
    except ImportError:
        return

    if not attr.has(cls):
        return  # not an attrs class

    valid_params = set(inspect.signature(cls).parameters.keys())
    hydra_keys = {"_target_", "_recursive_", "_convert_", "_partial_", "_args_"}

    invalid = [k for k in node if k not in valid_params and k not in hydra_keys]
    for k in invalid:
        _logger.warning(
            "Removing YAML key %r — not a valid __init__ param for %s. "
            "Valid params: %s",
            k,
            cls.__name__,
            sorted(valid_params),
        )
        del node[k]
