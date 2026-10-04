"""config_utils — YAML-based object instantiation with Hydra + target alias registry.

Public API
----------
Config loading::

    cfg = load_config("path/to/config.yaml", overrides={"key": "value"})
    cfg = merge_configs(base_cfg, override_cfg)

Instantiation::

    obj = instantiate(cfg)  # resolves aliases, filters attrs keys, calls Hydra

Registry — decorator::

    @register('ClaudeAPI', category='inferencer')
    class ClaudeApiInferencer: ...

Registry — imperative::

    register_class(ClaudeApiInferencer, 'ClaudeAPI', category='inferencer')

Registry — string-only (no class import needed)::

    register_alias('ClaudeAPI', 'module.path.ClaudeApiInferencer', 'inferencer')

Discoverability::

    list_registered()                  # all aliases
    list_registered('inferencer')      # filtered by category
    resolve_target('ClaudeAPI')        # alias → full import path
    import_target('ClaudeAPI')         # alias → imported class (raises on failure)
"""

from rich_python_utils.config_utils._registry import (
    _reset_registry,
    AliasResolutionError,
    import_target,
    list_registered,
    MissingTargetError,
    register,
    register_alias,
    register_class,
    RegistryError,
    resolve_target,
)

# Lazy — only imported when called, not at module load time.
# This keeps the package usable without hydra/omegaconf installed.


def load_config(path, overrides=None, env_prefix=None, config_defaults=None):
    from rich_python_utils.config_utils._instantiate import load_config as _load

    return _load(
        path, overrides, env_prefix=env_prefix, config_defaults=config_defaults
    )


def merge_configs(*configs):
    from rich_python_utils.config_utils._instantiate import merge_configs as _merge

    return _merge(*configs)


def instantiate(config, _convert_="all", merge_dict_typed_attributes=True, **kwargs):
    from rich_python_utils.config_utils._instantiate import instantiate as _inst

    return _inst(
        config,
        _convert_=_convert_,
        merge_dict_typed_attributes=merge_dict_typed_attributes,
        **kwargs,
    )


def collect_slot_defaults(cls):
    from rich_python_utils.config_utils._instantiate import (
        collect_slot_defaults as _collect,
    )

    return _collect(cls)


__all__ = [
    # Config loading
    "load_config",
    "merge_configs",
    # Instantiation
    "instantiate",
    "collect_slot_defaults",
    # Registry
    "register",
    "register_alias",
    "register_class",
    "resolve_target",
    "import_target",
    "list_registered",
    "_reset_registry",
    # Exceptions
    "AliasResolutionError",
    "MissingTargetError",
    "RegistryError",
]
