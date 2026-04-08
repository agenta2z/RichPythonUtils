"""
Configuration classes for variable management.

This module provides configuration dataclasses for FileBasedVariableManager.
"""

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Callable, Dict, List, Optional, Set, Union


class VariableSyntax(StrEnum):
    """Syntax for variable references in content.

    Defines how variable placeholders are parsed from content.
    """

    HANDLEBARS = "handlebars"  # {{var}}
    JINJA2 = "jinja2"  # {{ var }}
    PYTHON_FORMAT = "python"  # {var}
    TEMPLATE = "template"  # $var or ${var}


# Type for custom variable extractors
VariableExtractor = Callable[[str], Set[str]]
"""Callable that extracts variable names from content string.

Args:
    content: The content string to extract variables from.

Returns:
    Set of variable names found in the content.
"""

# Type for pattern-based syntax mapping
VariableSyntaxMapping = Dict[str, Optional[Union["VariableSyntax", VariableExtractor]]]
"""Mapping from file patterns to variable syntax.

Example:
    {"*.hbs": VariableSyntax.HANDLEBARS, "*.j2": VariableSyntax.JINJA2, "*.txt": None}
"""


@dataclass
class VariableManagerConfig:
    """Configuration for variable managers.

    Attributes:
        variables_folder_name: Subfolder name for variables.
            Default: "" (empty = files directly in base_path, no subfolder)

        variable_syntax: Syntax for parsing variable references. Can be:
            - VariableSyntax enum: Use this syntax for all files
            - Custom callable: (str) -> Set[str] for custom extraction
            - Dict[pattern, syntax]: Map file patterns to different syntaxes
              e.g., {"*.hbs": HANDLEBARS, "*.j2": JINJA2, "*.txt": None}
            - None: Pure text mode - no composition, variable files treated as raw text
            Default: VariableSyntax.HANDLEBARS

        enable_overrides: Enable .override files for development.
            When True, looks for {name}.override.{ext} files first.
            Default: False

        override_suffix: Suffix for override files.
            Default: ".override"

        cache_content: Cache file content in memory.
            Default: True

        file_extensions: Extensions to check in priority order.
            Default: [".hbs", ".j2", ".txt", ""]

        max_recursion_depth: Maximum depth for variable composition.
            Prevents infinite loops in variable-to-variable references.
            Default: 50

        compose_on_access: Whether to resolve nested variable references
            when using dict-like access (manager["key"] or manager.get("key")).
            When True, variables containing {{other_var}} will be composed.
            When False, raw content is returned.
            Default: True
    """

    variables_folder_name: str = ""
    variable_syntax: Optional[
        Union[VariableSyntax, VariableExtractor, VariableSyntaxMapping]
    ] = VariableSyntax.HANDLEBARS
    enable_overrides: bool = False
    override_suffix: str = ".override"
    cache_content: bool = True
    file_extensions: List[str] = field(
        default_factory=lambda: [".hbs", ".j2", ".jinja2", ".jinja", ".txt", ""]
    )
    max_recursion_depth: int = 50
    compose_on_access: bool = True
    cross_space_root: Optional[str] = None
