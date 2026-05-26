"""Verify aggregator task_preamble resolves to aggregation.jinja2."""
import sys
sys.path.insert(0, '/Users/tchen7/MyProjects/CoreProjects/AgentFoundation/src')
sys.path.insert(0, '/Users/tchen7/MyProjects/CoreProjects/RichPythonUtils/src')

from rich_python_utils.string_utils.formatting.template_manager.variable_manager import VariableLoader

loader = VariableLoader(
    template_dir='/Users/tchen7/MyProjects/CoreProjects/AgentFoundation/src/agent_foundation/resources/prompt_templates',
)

template_content = """
WRAPPER REF:
{{ context.user_request_with_task_preamble }}

DIRECT REF:
{{ task_preamble }}
"""

result = loader.resolve_from_template(
    template_content=template_content,
    template_root_space="plan",
    template_type="main",
    version="aggregation",
)

print("=" * 60)
print("RESOLVED VARS:")
print("=" * 60)
for k, v in result.items():
    print(f"\nKEY: {k!r}")
    s = str(v)
    print(f"FIRST 200 CHARS:")
    print(f"  {s[:200]!r}")
    if "aggregating" in s.lower() or "upstream_artifacts" in s.lower():
        print("  ✅ aggregation.jinja2 content detected")
    if "planning context" in s.lower():
        print("  ❌ default.jinja2 content detected (BUG)")
