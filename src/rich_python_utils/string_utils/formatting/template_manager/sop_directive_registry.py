"""SOP directive rendering registry.

Turns SOP ``[__directive__]`` tokens (and parser-extracted directive names) into
readable text for the human-facing next-step guidance, WITHOUT touching graph
semantics: structural directives are parsed into StateNode fields *before* any
rendering happens, so this module only affects display.

Two positional scenarios:
  - "head": the directive leads its line (e.g. a phase-body lead-in
    ``[__requires user input__] Do X``). Default => Title-Case words + ": "
    (``requires user input`` -> ``"Requires User Input: "``).
  - "mid": the directive appears inside a line (e.g. ``**Tools**[__required__]:``).
    Default => "(" + lowercase + ")" (``required`` -> ``"(required)"``).

Overrides may map a directive to a custom ``{"head":.., "mid":..}`` or to
``"waive"`` (render as empty). Structural / meta directives are waived (they are
graph edges / preamble, not reader-facing content).

CRITICAL: in-text matching is bracket+terminator STRICT -- only self-contained
``[__name__]`` tokens are transformed. Bare ``__x__`` in prose (MCP tool names
like ``mcp__atlassian__get_confluence_page``, Python dunders ``__init__``,
markdown bold ``__text__``) and operand-bearing structural expressions
(``[__goto__ Phase 3 __if__ `v`]``) are NEVER matched, so real content is safe.
"""

from __future__ import annotations

import re

_WAIVE = "waive"

# Explicit overrides. Value: "waive" OR {"head": str, "mid": str}.
# Unlisted directives fall back to the default patterns in render_directive().
_DIRECTIVE_OVERRIDES: dict[str, str | dict[str, str]] = {
    # structural -- parsed into graph edges, noise for the reader
    "depends on": _WAIVE,
    "goto": _WAIVE,
    "go to": _WAIVE,
    "branch": _WAIVE,
    "if": _WAIVE,
    "is": _WAIVE,
    "in": _WAIVE,
    "for each": _WAIVE,
    "sequentially": _WAIVE,
    "afterwards": _WAIVE,
    "wait": _WAIVE,
    # meta -- preamble / markers, not reader-facing guidance
    "initial": _WAIVE,
    "keywords": _WAIVE,
    "example_requests": _WAIVE,
    "example requests": _WAIVE,
    # display directives (requires user input, must, optional, ...) use defaults.
}

# Self-contained bracketed directive: BOTH the ``[__`` opener and the ``__]``
# closer are required. Deliberately does NOT match bare ``__x__`` or
# operand-bearing structural expressions (which do not end in ``__]``).
_INTEXT_RE = re.compile(r"\[__([\w][\w\s]*?)__\]")
_HEAD_RE = re.compile(r"^(\s*)\[__([\w][\w\s]*?)__\]\s*")


def _normalize(name: str) -> str:
    return name.strip().strip("[]").strip("_").replace("_", " ").strip().lower()


def render_directive(name: str, position: str = "head") -> str:
    """Render a single directive NAME to display text for ``position``.

    ``position``: "head" (line-leading label) or "mid" (inline annotation).
    """
    key = _normalize(name)
    if not key:
        return ""
    override = _DIRECTIVE_OVERRIDES.get(key)
    if override == _WAIVE:
        return ""
    if isinstance(override, dict):
        return override.get(position, override.get("head", ""))
    if position == "mid":
        return f"({key})"
    # head default: Title-Case words + ": "
    return " ".join(w.capitalize() for w in key.split()) + ": "


def render_directives(text: str) -> str:
    """Replace self-contained bracketed ``[__directive__]`` tokens in ``text``.

    Position-aware: a token that is the first non-whitespace on its line uses the
    "head" form; otherwise "mid". A line left empty by a waived head directive is
    dropped. Bare ``__x__`` tokens are never touched.
    """
    if not text or "[__" not in text:
        return text
    out: list[str] = []
    for line in text.split("\n"):
        head = _HEAD_RE.match(line)
        head_waived_empty = False
        if head:
            rep = render_directive(head.group(2), "head")
            rest = line[head.end() :]
            line = head.group(1) + rep + rest
            if not rep and not rest.strip():
                head_waived_empty = True
        line = _INTEXT_RE.sub(lambda m: render_directive(m.group(1), "mid"), line)
        if head_waived_empty and not line.strip():
            continue
        out.append(line)
    return "\n".join(out)
