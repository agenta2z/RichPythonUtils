

"""SOPManager — parse, validate, and render Standard Operating Procedures.

An SOP extends StateGraph with domain-specific semantics:
  - SOPPhase(StateNode): adds name, description, subsections, parent_id
  - SOP(StateGraph): collection of SOPPhases
  - SOPManager: parsing (markdown/YAML) + guidance rendering

Evaluation is handled by StateGraphTracker (from stategraph.py), not SOPManager.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from attr import attrib, attrs

from rich_python_utils.common_objects.workflow.stategraph import (
    StateGraph,
    StateGraphTracker,
    StateNode,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Regex patterns for markdown parsing
# ---------------------------------------------------------------------------

_PHASE_HEADING_RE = re.compile(
    r"^(#{2,3})\s+Phase\s+(\w+)"
    r"(?:\s+--\s+([^[\]:]+?))?"  # optional: -- PhaseName
    r"\s*(?:\[([^\]]*)\])?"      # optional: [directives]
    r"(?:\s*:\s*(.+))?$",         # optional: heading_rest (outputs)
    re.MULTILINE,
)

_OUTPUT_RE = re.compile(r"`(\w+)`")

DIRECTIVE_MUST = "must"

_SUBSECTION_RE = re.compile(
    r"^\*\*(\w+)\*\*"
    r"\s*(?:\[__(\w[\w\s]*)__\])?"
    r"\s*:\s*$",
    re.MULTILINE,
)

_DEPENDS_ON_RE = re.compile(
    r"__depends\s+on__\s+Phase\s+([\w\s,]+)", re.IGNORECASE
)

_FOR_EACH_RE = re.compile(
    r"__for\s+each__\s+`(\w+)`\s+__in__\s+`(\w+)`"
    r"(?:\s+__sequentially__)?",
    re.IGNORECASE,
)

_GOTO_RE = re.compile(
    r"__go\s+to__\s+Phase\s+(\w+)"
    r"(?:\s+__if__\s+`(\w+)`"
    r"(?:\s+__is__\s+`([^`]+)`)?"
    r")?",
    re.IGNORECASE,
)

_IF_RE = re.compile(
    r"__if__\s+`(\w+)`"
    r"(?:\s+__is__\s+`([^`]+)`)?",
    re.IGNORECASE,
)

# v2 grammar: separate-line tag detection
_TAG_LINE_RE = re.compile(r"^\[([^\]]+)\]\s*(.*)?$", re.MULTILINE)

# v2 grammar: __goto__ with __afterwards__ / __wait__ / __if__ modifiers
_GOTO_AFTERWARDS_RE = re.compile(
    r"__go\s*to__\s+Phase\s+(\w+)"
    r"(?:\s+__afterwards__)?"
    r"(?:\s+__wait__\s+(\d+[smhd]))?"
    r"(?:\s+__if__\s+(.+?))?$",
    re.IGNORECASE,
)

# SOP directive tag constants — canonical strings stored in phase.directives.
# Use these instead of hardcoding strings when checking directives.
DIRECTIVE_REQUIRES_USER_INPUT = "requires user input"

def normalize_tool_name(raw: str) -> str:
    """Normalize a tool name from SOP text to canonical form.

    ``"- /understand-codebase <path>"`` → ``"understand_codebase"``
    ``"understand-data"`` → ``"understand_data"``
    """
    token = raw.strip().lstrip("- ").split()[0] if raw.strip().lstrip("- ") else ""
    return token.lstrip("/").replace("-", "_")


_BRANCH_RE = re.compile(r"__branch__(?:\s+`(\w+)`)?", re.IGNORECASE)
_INITIAL_RE = re.compile(r"__initial__", re.IGNORECASE)
_REQUIRES_USER_INPUT_RE = re.compile(
    r"__requires\s+user\s+input__", re.IGNORECASE
)

# ---------------------------------------------------------------------------
# Guidance text templates — used by SOPManager.render_guidance()
#
# Each template is an f-string-style string.  Keeping them here makes it
# easy to tweak wording without hunting through render_guidance() logic.
# ---------------------------------------------------------------------------

_GUIDANCE_HEADER = "### Nextstep Guidance:"

_GUIDANCE_RUNNING = (
    "- **In progress:** Phase {phase_id} "
    "({phase_name}) is currently running. Wait for completion."
)

_GUIDANCE_ERROR = (
    "- **Error occurred:** Phase {phase_id} "
    "({phase_name}) had an error. Review, retry, or adjust."
)

_GUIDANCE_MISSING_OUTPUTS = (
    "- **Phase {phase_id} ({phase_name}) incomplete:** "
    "Missing outputs: {outputs}. "
    "Please provide the missing information."
)

_GUIDANCE_AVAILABLE_HEADER = "The following phases are available:"

_GUIDANCE_AVAILABLE_PHASE = "- **{phase_name}:** {description}"

_GUIDANCE_ALL_COMPLETE = (
    "- **All phases complete.** Suggest iterating or starting a new task."
)

_GUIDANCE_READY = "- **Ready:** Begin with the first available phase."

_GUIDANCE_FOOTER = (
    '\nWhen the user asks "what should I do next?", use the guidance '
    "above to give a concrete recommendation."
)


# ---------------------------------------------------------------------------
# SOP data classes (extend StateNode/StateGraph)
# ---------------------------------------------------------------------------


@dataclass
class SOPSubsection:
    """A subsection within a phase body (e.g., **Tools**, **Rules**)."""
    name: str
    directive: str | None = None
    content: str = ""


@attrs(slots=False, eq=False, hash=False)
class SOPPhase(StateNode):
    """A phase in an SOP — extends StateNode with description and subsections."""
    name: str = attrib(default="")
    description: str = attrib(default="")
    subsections: list = attrib(factory=list)
    parent_id: str = attrib(default=None)
    directives: list = attrib(factory=list)
    requires_user_input: bool = attrib(default=False)
    unknown_tags: list = attrib(factory=list)

    def __str__(self) -> str:
        return f"{self.id}: {self.name}" if self.name else self.id


class SOP(StateGraph):
    """A Standard Operating Procedure — a StateGraph of SOPPhases."""

    def __init__(self, nodes=None, keywords=None, example_requests=None, name=""):
        super().__init__(nodes)
        self.name: str = name
        self.keywords: list[str] = keywords or []
        self.example_requests: list[str] = example_requests or []

    @property
    def phases(self) -> list[SOPPhase]:
        return self.nodes

    @property
    def phase_ids(self) -> list[str]:
        return self.node_ids

    def get_phase(self, phase_id: str) -> SOPPhase | None:
        return self.get_node(phase_id)

    def get_next_pending_phase(
        self, completed_ids: set[str]
    ) -> SOPPhase | None:
        """Return the first phase whose dependencies are all satisfied and
        that itself is not yet completed. Returns None when no such phase
        exists (workflow complete or blocked by an unsatisfied dependency).
        """
        for phase in self.phases:
            if phase.id in completed_ids:
                continue
            deps = getattr(phase, "depends_on", []) or []
            if all(dep in completed_ids for dep in deps):
                return phase
        return None

    @property
    def phase_required_tools(self) -> dict[str, set[str]]:
        """Phase ID → set of required tool names (from ``Tools[__must__]`` only).

        Used by ``_check_phase_completion`` to require ALL must-tools to have
        executed before marking a phase complete — not just any single one.
        """
        result: dict[str, set[str]] = {}
        for phase in self.phases:
            tools: set[str] = set()
            for sub in phase.subsections:
                if sub.name.lower() in ("tools", "command") and sub.directive == DIRECTIVE_MUST:
                    for line in sub.content.split("\n"):
                        name = normalize_tool_name(line)
                        if name:
                            tools.add(name)
            if tools:
                result[phase.id] = tools
        return result

    @property
    def tool_to_phase_map(self) -> dict[str, str]:
        """Extract tool name → phase ID mapping from SOP subsections.

        Parses tool names from Tools/Command subsections and the phase body.
        Tool names are normalized: /understand-codebase → understand_codebase.

        Returns:
            Dict mapping tool names to phase IDs,
            e.g. {"understand_codebase": "1", "research_propose": "2", "task": "3"}
        """
        mapping: dict[str, str] = {}
        for phase in self.phases:
            # Extract from subsections (e.g., Tools[__must__]: - /tool-name)
            for sub in phase.subsections:
                if sub.name.lower() in ("tools", "command"):
                    for line in sub.content.split("\n"):
                        name = normalize_tool_name(line)
                        if name:
                            mapping[name] = phase.id
            # Also extract from phase body (e.g., "Command: `/research-propose <goal>`")
            for match in re.finditer(r"Command:\s*`/([a-zA-Z0-9_-]+)", phase.description):
                tool_name = normalize_tool_name(match.group(1))
                mapping[tool_name] = phase.id
        return mapping


# ---------------------------------------------------------------------------
# SOPManager
# ---------------------------------------------------------------------------


class SOPManager:
    """Parse SOP definitions and render guidance text."""

    # -- Loading -----------------------------------------------------------

    @staticmethod
    def load(path: Path | str) -> SOP:
        path = Path(path)
        content = path.read_text(encoding="utf-8")
        if path.suffix in (".yaml", ".yml"):
            data = yaml.safe_load(content)
            return SOPManager.parse_yaml(data)
        return SOPManager.parse_markdown(content)

    # -- Markdown parser ---------------------------------------------------

    @staticmethod
    def parse_markdown(content: str) -> SOP:
        phases: list[SOPPhase] = []
        matches = list(_PHASE_HEADING_RE.finditer(content))

        # Extract preamble meta-tags (before first ## Phase heading)
        preamble_end = matches[0].start() if matches else len(content)
        preamble = content[:preamble_end]
        keywords, example_requests = _extract_preamble_meta(preamble)

        sop_name = ""
        for pline in preamble.split("\n"):
            pline_s = pline.strip()
            if pline_s.startswith("# ") and not pline_s.startswith("## "):
                sop_name = pline_s[2:].strip()
                break

        for i, match in enumerate(matches):
            heading_level = len(match.group(1))
            phase_id = match.group(2)
            phase_name_raw = (match.group(3) or "").strip()
            directives_raw = match.group(4) or ""
            heading_rest = match.group(5) or ""

            outputs = _OUTPUT_RE.findall(heading_rest)

            # v2 format: heading_rest may contain bracket tags from a
            # separate line that the heading regex consumed (e.g.,
            # "## Phase 1:\n[__depends on__ Phase 0]" → heading_rest="[__depends on__ Phase 0]").
            # Detect and move bracket-tag content into directives_raw.
            heading_rest_clean = heading_rest
            if heading_rest and heading_rest.startswith("[") and heading_rest.endswith("]"):
                if not directives_raw:
                    directives_raw = heading_rest[1:-1]
                else:
                    directives_raw += "; " + heading_rest[1:-1]
                heading_rest_clean = ""
                outputs = _OUTPUT_RE.findall(heading_rest_clean)

            if phase_name_raw:
                name = phase_name_raw
            else:
                name = _OUTPUT_RE.sub("", heading_rest_clean).strip()
                name = re.sub(r"\s+and\s*$", "", name).strip()
                name = re.sub(r"^\s*and\s+", "", name).strip()
                name = name.rstrip(",").strip()

            raw_parts = [d.strip() for d in directives_raw.split(";") if d.strip()]

            depends_on = []
            remaining_directives = []
            foreach_item_var = None
            foreach_collection_var = None
            foreach_sequential = False
            gate_var = None
            gate_value = None
            goto_target = None
            goto_condition_var = None
            goto_condition_value = None

            # v2 additions
            goto_afterwards = False
            goto_wait_duration = None
            goto_condition_negate = False
            gate_negate = False
            branch = False
            branch_source_var = None
            requires_user_input = False
            unknown_tags = []

            for part in raw_parts:
                dep_match = _DEPENDS_ON_RE.search(part)
                if dep_match:
                    dep_ids = [d.strip() for d in dep_match.group(1).split(",") if d.strip()]
                    depends_on.extend(dep_ids)
                    continue

                fe_match = _FOR_EACH_RE.search(part)
                if fe_match:
                    foreach_item_var = fe_match.group(1)
                    foreach_collection_var = fe_match.group(2)
                    foreach_sequential = "__sequentially__" in part.lower()
                    continue

                # v2: __goto__ with __afterwards__/__wait__/__if__
                goto_aft_match = _GOTO_AFTERWARDS_RE.search(part)
                if goto_aft_match and ("__afterwards__" in part.lower() or "__wait__" in part.lower()):
                    goto_target = goto_aft_match.group(1)
                    if goto_aft_match.group(2):
                        goto_wait_duration = goto_aft_match.group(2)
                    if "__afterwards__" in part.lower():
                        goto_afterwards = True
                    if goto_aft_match.group(3):
                        cond = goto_aft_match.group(3).strip()
                        cond_result: dict[str, Any] = {}
                        _parse_condition_into(cond, cond_result, "goto_condition")
                        goto_condition_var = cond_result.get("goto_condition_var")
                        goto_condition_value = cond_result.get("goto_condition_value")
                        goto_condition_negate = cond_result.get("goto_condition_negate", False)
                    continue

                goto_match = _GOTO_RE.search(part)
                if goto_match:
                    goto_target = goto_match.group(1)
                    goto_condition_var = goto_match.group(2)
                    goto_condition_value = goto_match.group(3)
                    continue

                # v2: __branch__ [`var`]
                branch_match = _BRANCH_RE.search(part)
                if branch_match:
                    branch = True
                    if branch_match.group(1):
                        branch_source_var = branch_match.group(1)
                    continue

                if_match = _IF_RE.search(part)
                if if_match and not goto_match:
                    gate_var = if_match.group(1)
                    gate_value = if_match.group(2)
                    continue

                # v2: __requires user input__
                if _REQUIRES_USER_INPUT_RE.search(part):
                    requires_user_input = True
                    remaining_directives.append(DIRECTIVE_REQUIRES_USER_INPUT)
                    continue

                # v2: __initial__
                if _INITIAL_RE.search(part):
                    remaining_directives.append("initial")
                    continue

                remaining_directives.append(part.strip().lower())

            body_start = match.end()
            body_end = matches[i + 1].start() if i + 1 < len(matches) else len(content)
            body = content[body_start:body_end].strip()

            # v2 two-pass: scan body's leading lines for any remaining
            # separate-line tags not consumed by the heading regex
            tag_result = _extract_tag_lines(body)
            body = tag_result["remaining_body"]
            for dep in tag_result.get("depends_on", []):
                if dep not in depends_on:
                    depends_on.append(dep)
            if tag_result.get("goto_target") and not goto_target:
                goto_target = tag_result["goto_target"]
                goto_condition_var = tag_result.get("goto_condition_var")
                goto_condition_value = tag_result.get("goto_condition_value")
            if tag_result.get("gate_var") and not gate_var:
                gate_var = tag_result["gate_var"]
                gate_value = tag_result.get("gate_value")
            if tag_result.get("goto_afterwards"):
                goto_afterwards = True
            if tag_result.get("goto_wait_duration"):
                goto_wait_duration = tag_result["goto_wait_duration"]
            if tag_result.get("goto_condition_negate"):
                goto_condition_negate = True
            if tag_result.get("gate_negate"):
                gate_negate = True
            if tag_result.get("branch"):
                branch = True
            if tag_result.get("branch_source_var"):
                branch_source_var = tag_result["branch_source_var"]
            if tag_result.get("requires_user_input"):
                requires_user_input = True
            for d in tag_result.get("directives", []):
                if d not in remaining_directives:
                    remaining_directives.append(d)
            unknown_tags.extend(tag_result.get("unknown_tags", []))

            description, subsections = _parse_subsections(body)

            parent_id = None
            if heading_level == 3:
                for prev_phase in reversed(phases):
                    if prev_phase.parent_id is None:
                        parent_id = prev_phase.id
                        break

            phases.append(
                SOPPhase(
                    id=phase_id,
                    depends_on=depends_on,
                    outputs=outputs,
                    gate_var=gate_var,
                    gate_value=gate_value,
                    gate_negate=gate_negate,
                    goto_target=goto_target,
                    goto_condition_var=goto_condition_var,
                    goto_condition_value=goto_condition_value,
                    goto_condition_negate=goto_condition_negate,
                    goto_afterwards=goto_afterwards,
                    goto_wait_duration=goto_wait_duration,
                    foreach_item_var=foreach_item_var,
                    foreach_collection_var=foreach_collection_var,
                    foreach_sequential=foreach_sequential,
                    branch=branch,
                    branch_source_var=branch_source_var,
                    name=name,
                    description=description,
                    subsections=subsections,
                    parent_id=parent_id,
                    directives=remaining_directives,
                    requires_user_input=requires_user_input,
                    unknown_tags=unknown_tags,
                )
            )

        return SOP(phases, keywords=keywords, example_requests=example_requests, name=sop_name)

    # -- YAML parser -------------------------------------------------------

    @staticmethod
    def parse_yaml(data: dict[str, Any]) -> SOP:
        phases = []
        for entry in data.get("phases", []):
            subsections = [
                SOPSubsection(
                    name=s.get("name", ""),
                    directive=s.get("directive"),
                    content=s.get("content", ""),
                )
                for s in entry.get("subsections", [])
            ]
            phases.append(
                SOPPhase(
                    id=str(entry.get("id", "")),
                    depends_on=[str(d) for d in entry.get("depends_on", [])],
                    outputs=entry.get("outputs", []),
                    gate_var=entry.get("gate_var"),
                    gate_value=entry.get("gate_value"),
                    goto_target=entry.get("goto_target"),
                    goto_condition_var=entry.get("goto_condition_var"),
                    goto_condition_value=entry.get("goto_condition_value"),
                    foreach_item_var=entry.get("foreach_item_var"),
                    foreach_collection_var=entry.get("foreach_collection_var"),
                    foreach_sequential=entry.get("foreach_sequential", False),
                    name=entry.get("name", ""),
                    description=entry.get("description", ""),
                    subsections=subsections,
                    parent_id=entry.get("parent_id"),
                    directives=entry.get("directives", []),
                )
            )
        return SOP(phases, name=data.get("name", ""))


    # -- Guidance rendering ------------------------------------------------

    @staticmethod
    def render_guidance(
        tracker: StateGraphTracker,
        sop: SOP | None = None,
        context: dict[str, Any] | None = None,
        sop_config: dict[str, Any] | None = None,
    ) -> str:
        """Render next-step guidance from tracker state."""
        if context is None:
            context = {}
        parts: list[str] = [_GUIDANCE_HEADER]

        if tracker.status == "running" and tracker.current_state:
            phase = sop.get_phase(tracker.current_state) if sop else None
            phase_name = phase.name if phase else tracker.current_state
            parts.append(
                _GUIDANCE_RUNNING.format(
                    phase_id=tracker.current_state, phase_name=phase_name,
                )
            )
        elif tracker.status == "error" and tracker.current_state:
            phase = sop.get_phase(tracker.current_state) if sop else None
            phase_name = phase.name if phase else tracker.current_state
            parts.append(
                _GUIDANCE_ERROR.format(
                    phase_id=tracker.current_state, phase_name=phase_name,
                )
            )
        else:
            missing = tracker.get_missing_outputs()
            if missing:
                for phase_id, m in missing.items():
                    phase = sop.get_phase(phase_id) if sop else None
                    phase_name = phase.name if phase else phase_id
                    parts.append(
                        _GUIDANCE_MISSING_OUTPUTS.format(
                            phase_id=phase_id,
                            phase_name=phase_name,
                            outputs=", ".join(f"`{o}`" for o in m),
                        )
                    )
            else:
                available = tracker.get_available_next()
                if available:
                    parts.append(_GUIDANCE_AVAILABLE_HEADER)
                    for node in available:
                        phase = sop.get_phase(node.id) if sop else None
                        if phase:
                            desc = phase.description
                            parts.append(
                                _GUIDANCE_AVAILABLE_PHASE.format(
                                    phase_name=phase.name,
                                    description=desc.strip(),
                                )
                            )
                            for sub in phase.subsections:
                                instruction = _get_directive_instruction(
                                    sub.name, sub.directive, sop_config,
                                )
                                if instruction:
                                    parts.append(f"  {instruction}")
                                parts.append(f"  {sub.content.strip()}")
                        else:
                            parts.append(f"- **{node.id}**")
                elif tracker.status == "completed":
                    parts.append(_GUIDANCE_ALL_COMPLETE)
                else:
                    parts.append(_GUIDANCE_READY)

            # Task queue status (shown regardless of phase state)
            task_queue = context.get("task_queue", [])
            if task_queue:
                queue_summary = context.get("task_queue_summary", "")
                if queue_summary:
                    parts.append(f"\n**Task queue:** {queue_summary}")
                next_task = next(
                    (t for t in task_queue if t.get("status") == "queued"), None
                )
                if next_task:
                    h_id = next_task.get("id") or next_task.get("task_id", "")
                    parts.append(f"  Next: {h_id} — {next_task.get('title', '')}")

        parts.append(_GUIDANCE_FOOTER)
        return "\n".join(parts)

    @staticmethod
    def render_for_mode(content: str, mode: str = "default") -> str:
        """Return SOP markdown unchanged regardless of mode.

        Yolo mode no longer strips [__requires user input__] text —
        instead, conversation tools get synthetic auto-advance responses
        via ConversationalInferencer._synthesize_yolo_collected().
        The [__requires user input__] tag stays visible to the LLM
        so it still emits the conversation tool; the response is just
        synthesized instead of blocking on user input.
        """
        return content


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _extract_preamble_meta(preamble: str) -> tuple[list[str], list[str]]:
    """Extract SOP-level meta-tags from the text before the first phase heading.

    Returns (keywords, example_requests).
    """
    keywords: list[str] = []
    example_requests: list[str] = []
    current_meta: str | None = None

    for line in preamble.split("\n"):
        stripped = line.strip()

        # Match [__keywords__] or [__example_requests__] with optional trailing content
        tag_match = re.match(
            r"^\[?__(\w+)__\]?\s*(?::\s*)?(.*)$", stripped, re.IGNORECASE
        )
        if tag_match:
            tag_name = tag_match.group(1).lower()
            value = tag_match.group(2).strip()
            if tag_name == "keywords":
                current_meta = "keywords"
                if value:
                    keywords.extend(
                        k.strip() for k in value.split(",") if k.strip()
                    )
            elif tag_name == "example_requests":
                current_meta = "example_requests"
                if value:
                    example_requests.append(value)
            else:
                current_meta = None
            continue

        # Bullet list items under the current meta-tag
        if current_meta and stripped.startswith("- "):
            item = stripped[2:].strip()
            if item:
                if current_meta == "keywords":
                    keywords.append(item)
                elif current_meta == "example_requests":
                    example_requests.append(item)
            continue

        # Non-tag, non-list line resets current_meta
        if stripped and not stripped.startswith("#"):
            current_meta = None

    return keywords, example_requests


def _extract_tag_lines(body: str) -> dict[str, Any]:
    """Scan leading lines of a phase body for separate-line [tag] directives.

    Returns a dict with parsed directives + 'remaining_body' (body with
    tag lines stripped). Tag lines with trailing text (e.g.,
    '[__requires user input__] IMPORTANT: ...') preserve the trailing
    text in remaining_body.
    """
    result: dict[str, Any] = {
        "depends_on": [],
        "directives": [],
        "unknown_tags": [],
        "goto_target": None,
        "goto_condition_var": None,
        "goto_condition_value": None,
        "goto_afterwards": False,
        "goto_wait_duration": None,
        "goto_condition_negate": False,
        "gate_var": None,
        "gate_value": None,
        "gate_negate": False,
        "branch": False,
        "branch_source_var": None,
        "requires_user_input": False,
    }

    lines = body.split("\n")
    remaining_lines = []
    in_tag_section = True

    for line in lines:
        stripped = line.strip()
        if in_tag_section and not stripped:
            remaining_lines.append(line)
            continue

        if not in_tag_section:
            remaining_lines.append(line)
            continue

        tag_match = _TAG_LINE_RE.match(stripped)
        if not tag_match:
            in_tag_section = False
            remaining_lines.append(line)
            continue

        tag_content = tag_match.group(1)
        trailing = (tag_match.group(2) or "").strip()

        # Parse the tag content for known directives (may have semicolons)
        parts = [p.strip() for p in tag_content.split(";") if p.strip()]
        for part in parts:
            _parse_single_tag(part, result)

        if trailing:
            remaining_lines.append(trailing)

    result["remaining_body"] = "\n".join(remaining_lines).strip()
    return result


def _parse_single_tag(tag_text: str, result: dict[str, Any]) -> None:
    """Parse a single tag directive and update the result dict."""
    # __initial__ — normalize to "initial" (no double underscores)
    if _INITIAL_RE.search(tag_text):
        if "initial" not in result["directives"]:
            result["directives"].append("initial")
        return

    # __requires user input__
    if _REQUIRES_USER_INPUT_RE.search(tag_text):
        result["requires_user_input"] = True
        if DIRECTIVE_REQUIRES_USER_INPUT not in result["directives"]:
            result["directives"].append(DIRECTIVE_REQUIRES_USER_INPUT)
        return

    # __depends on__ Phase X, Y
    dep_match = _DEPENDS_ON_RE.search(tag_text)
    if dep_match:
        dep_ids = [d.strip() for d in dep_match.group(1).split(",") if d.strip()]
        for d in dep_ids:
            if d not in result["depends_on"]:
                result["depends_on"].append(d)
        return

    # __branch__ [`var`]
    branch_match = _BRANCH_RE.search(tag_text)
    if branch_match:
        result["branch"] = True
        if branch_match.group(1):
            result["branch_source_var"] = branch_match.group(1)
        return

    # __goto__/__go to__ with __afterwards__/__wait__/__if__
    goto_aft_match = _GOTO_AFTERWARDS_RE.search(tag_text)
    if goto_aft_match:
        result["goto_target"] = goto_aft_match.group(1)
        if goto_aft_match.group(2):
            result["goto_wait_duration"] = goto_aft_match.group(2)
        if "__afterwards__" in tag_text.lower():
            result["goto_afterwards"] = True
        if goto_aft_match.group(3):
            cond = goto_aft_match.group(3).strip()
            _parse_condition_into(cond, result, prefix="goto_condition")
        return

    # __go to__ Phase X [__if__ `var`] (v1 format)
    goto_match = _GOTO_RE.search(tag_text)
    if goto_match:
        result["goto_target"] = goto_match.group(1)
        result["goto_condition_var"] = goto_match.group(2)
        result["goto_condition_value"] = goto_match.group(3)
        return

    # __for each__ `item` __in__ `collection`
    fe_match = _FOR_EACH_RE.search(tag_text)
    if fe_match:
        return

    # __if__ `var` [__is__ `value`] (top-of-phase gate)
    if_match = _IF_RE.search(tag_text)
    if if_match:
        result["gate_var"] = if_match.group(1)
        result["gate_value"] = if_match.group(2)
        if "__is not__" in tag_text.lower() or "!=" in tag_text:
            result["gate_negate"] = True
        return

    # Unknown tag — preserve for forward compatibility
    result["unknown_tags"].append(tag_text.strip())


def _parse_condition_into(
    cond_text: str, result: dict[str, Any], prefix: str
) -> None:
    """Parse a condition expression (var, var == value, var != value)."""
    if "!=" in cond_text:
        parts = cond_text.split("!=", 1)
        result[f"{prefix}_var"] = parts[0].strip().strip("`\"'")
        result[f"{prefix}_value"] = parts[1].strip().strip("`\"'")
        result[f"{prefix}_negate"] = True
    elif "==" in cond_text:
        parts = cond_text.split("==", 1)
        result[f"{prefix}_var"] = parts[0].strip().strip("`\"'")
        result[f"{prefix}_value"] = parts[1].strip().strip("`\"'")
    else:
        result[f"{prefix}_var"] = cond_text.strip().strip("`\"'")


def _parse_subsections(body: str) -> tuple[str, list[SOPSubsection]]:
    matches = list(_SUBSECTION_RE.finditer(body))
    if not matches:
        return body, []

    description = body[: matches[0].start()].strip()
    subsections = []

    for i, match in enumerate(matches):
        name = match.group(1)
        directive = match.group(2)
        if directive:
            directive = directive.strip().lower()

        content_start = match.end()
        content_end = matches[i + 1].start() if i + 1 < len(matches) else len(body)
        content = body[content_start:content_end].strip()

        subsections.append(SOPSubsection(name=name, directive=directive, content=content))

    return description, subsections


def _get_directive_instruction(
    section_name: str, directive: str | None, sop_config: dict[str, Any] | None,
) -> str:
    if not directive or not sop_config:
        return ""
    subsections_config = sop_config.get("subsections", {})
    section_config = subsections_config.get(section_name, {})
    directives_map = section_config.get("directives", {})
    return directives_map.get(f"__{directive}__", directives_map.get(directive, ""))
