"""
Variable Manager module.

This module provides a file-based variable management system with:
- Abstract base class (VariableManager) with Mapping interface
- Full-featured file-based implementation (FileBasedVariableManager)
- Configurable variable syntax (Handlebars, Jinja2, Python, Template, or custom)
- Cascade resolution, scope modifiers, and variable composition

Example:
    >>> from rich_python_utils.common_objects.variable_manager import (
    ...     FileBasedVariableManager,
    ...     VariableManagerConfig,
    ...     KeyDiscoveryMode,
    ... )
    >>>
    >>> # Simple dict-like access
    >>> manager = FileBasedVariableManager(base_path="/config")
    >>> value = manager['database_host']
    >>> value = manager.get('database_port', '5432')
    >>>
    >>> # With cascade resolution
    >>> vars = manager.resolve_from_content(
    ...     "{{mindset}} and {{notes_safety}}",
    ...     variable_root_space="my_agent",
    ...     variable_type="main",
    ... )
"""

from rich_python_utils.common_objects.variable_manager.base import VariableManager
from rich_python_utils.common_objects.variable_manager.config import (
    VariableExtractor,
    VariableManagerConfig,
    VariableSyntax,
    VariableSyntaxMapping,
)
from rich_python_utils.common_objects.variable_manager.exceptions import (
    AmbiguousVariableError,
    CircularReferenceError,
    MaxDepthExceededError,
)
from rich_python_utils.common_objects.variable_manager.file_based import (
    FileBasedVariableManager,
    KeyDiscoveryMode,
)

__all__ = [
    # Base class
    "VariableManager",
    # Implementation
    "FileBasedVariableManager",
    "KeyDiscoveryMode",
    # Config
    "VariableManagerConfig",
    "VariableSyntax",
    "VariableExtractor",
    "VariableSyntaxMapping",
    # Exceptions
    "AmbiguousVariableError",
    "CircularReferenceError",
    "MaxDepthExceededError",
]
