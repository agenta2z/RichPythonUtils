from enum import StrEnum
import json

from rich_python_utils.string_utils.xml_helpers import xml_format


class KeyValueStringFormat(StrEnum):
    XML = "xml"
    JSON = "json"
    YAML = "yaml"
    HTML = "html"
    TSV = "tsv"
    CSV = "csv"
    MARKDOWN = "markdown"
    INI = "ini"
    Other = "other"


def format_key_value(value: str, key: str, format_type: KeyValueStringFormat = KeyValueStringFormat.YAML) -> str:
    """
    Formats a key-value pair according to the specified format type.

    Args:
        key (str): The key or title.
        value (str): The value associated with the key.
        format_type (KeyValueStringFormat): The format type, which can be one of XML, JSON, YAML, HTML, TSV, CSV, MARKDOWN, or INI. Defaults to YAML.

    Returns:
        str: A formatted string based on the format type.

    Raises:
        ValueError: If an unsupported format type is provided.

    Examples:
        >>> format_key_value("Alice","user_name",KeyValueStringFormat.JSON)
        '{"user_name": "Alice"}'

        >>> format_key_value("Alice","user_name",KeyValueStringFormat.XML)
        '<user_name>Alice</user_name>'

        >>> format_key_value("Alice","user_name",KeyValueStringFormat.YAML)
        'user_name: Alice'

        >>> format_key_value("Alice","user_name",KeyValueStringFormat.HTML)
        '<div><strong>user_name</strong>: Alice</div>'

        >>> format_key_value("Alice","user_name",KeyValueStringFormat.CSV)
        'user_name,Alice'

        >>> format_key_value("Alice","user_name",KeyValueStringFormat.TSV)
        'user_name\\tAlice'

        >>> format_key_value("Alice","user_name",KeyValueStringFormat.MARKDOWN)
        '**user_name**: Alice'

        >>> format_key_value("Alice","user_name",KeyValueStringFormat.INI)
        'user_name = Alice'
    """
    if format_type == KeyValueStringFormat.XML:
        return xml_format(tag=key, content=value)
    elif format_type == KeyValueStringFormat.JSON:
        return json.dumps({key: value})
    elif format_type == KeyValueStringFormat.YAML:
        return f"{key}: {value}"
    elif format_type == KeyValueStringFormat.HTML:
        return f"<div><strong>{key}</strong>: {value}</div>"
    elif format_type == KeyValueStringFormat.CSV:
        return f"{key},{value}"
    elif format_type == KeyValueStringFormat.TSV:
        return f"{key}\t{value}"
    elif format_type == KeyValueStringFormat.MARKDOWN:
        return f"**{key}**: {value}"
    elif format_type == KeyValueStringFormat.INI:
        return f"{key} = {value}"
    else:
        raise ValueError("Unsupported format type. Choose a valid FormatType.")
