"""MCP server exposing dagwright as a tool any MCP-aware LLM client
can call (Claude Code, Claude Desktop, Cursor, etc).

Why this exists: under the workflow framing in CHARTER.md, the AE
talks to Claude; Claude edits the spec and asks dagwright for plans.
The CLI (`dagwright plan`, `dagwright watch`) is internal
infrastructure for power users and CI. The MCP server is the actual
user-facing surface — invoked by Claude on the AE's behalf, with
plans + diffs surfaced inline in the chat.

Server-side state: a per-server-instance dict mapping spec_path to
the previous run's plan list. When the same spec is planned twice,
the response includes a diff vs the previous run. Reset on server
restart.
"""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from mcp.server.fastmcp import FastMCP

from dagwright.diff import diff_dc_plans
from dagwright.planner import run_plan
from dagwright.state import DefinitionalChange, MetricRequest

mcp = FastMCP("dagwright")

# Maps absolute spec_path → list of plans from the most recent
# `plan` call on that spec. Used to compute diffs across calls.
_previous_plans: dict[str, list] = {}


@mcp.tool()
def plan(
    spec_path: str,
    manifest_path: str,
    bi_path: str | None = None,
    top: int = 3,
) -> dict:
    """Generate ranked architectural change plans for a dagwright spec.

    Args:
        spec_path: Absolute path to the dagwright-spec YAML.
        manifest_path: Absolute path to the dbt manifest.json.
        bi_path: Optional path to a BI consumer graph JSON. If
            omitted, dagwright reads dbt exposures from the manifest.
        top: Number of top-ranked plans to return.

    Returns a dict with:
        spec: the parsed spec (kind, id, intent, etc.)
        plans: ranked list of plan dicts (operations, contracts,
            blast radius, scores, semantic summary, notes)
        rejections: candidates the planner ruled out and why
        diff: markdown summary of what changed vs the previous call
            on this spec_path. Empty string on the first call or for
            metric_request specs (diff not yet supported there).
    """
    spec_path_abs = str(Path(spec_path).resolve())
    args = SimpleNamespace(
        spec=Path(spec_path),
        manifest=Path(manifest_path),
        bi=Path(bi_path) if bi_path else None,
        top=top,
        format="markdown",
    )
    spec, plans, rejections = run_plan(args)

    diff_md = ""
    if isinstance(spec, DefinitionalChange):
        prev = _previous_plans.get(spec_path_abs)
        if prev is not None:
            diff_md = diff_dc_plans(prev, plans)
        _previous_plans[spec_path_abs] = list(plans)

    return {
        "spec": _to_jsonable(spec),
        "plans": [_to_jsonable(p) for p in plans],
        "rejections": [_to_jsonable(r) for r in rejections],
        "diff": diff_md,
    }


def _to_jsonable(obj: Any) -> Any:
    """Coerce dataclass-or-dict-or-primitive into JSON-friendly form.
    Handles nested dataclasses; falls back to str() for unknown types."""
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if hasattr(obj, "__dataclass_fields__"):
        return {k: _to_jsonable(v) for k, v in asdict(obj).items()}
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_to_jsonable(x) for x in obj]
    return str(obj)


def main() -> int:
    """Entry point for `dagwright mcp`. Runs the server over stdio."""
    mcp.run()
    return 0
