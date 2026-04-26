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

Hot-reload: each tool call mtime-checks dagwright's submodules
(planner, diff, loaders, state) and reloads any that have changed
since the last call. Lets the developer edit planner/diff logic and
have the next tool call pick up changes without restarting the MCP
server (i.e. without restarting Claude Code). Does NOT cover edits
to this file itself or new tool registrations — those still need a
real server restart.
"""

from __future__ import annotations

import importlib
import os
import sys
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("dagwright")

# Modules whose mtimes we track for hot-reload. Order matters:
# state and loaders have no internal deps; planner imports from
# state and loaders; diff imports from planner. Reloading in this
# order ensures dependents see freshly-reloaded dependencies.
_RELOADABLE = ("dagwright.state", "dagwright.loaders", "dagwright.planner", "dagwright.diff")
# Tracks (mtime_ns, size) per module path. Nanosecond mtime + size
# is more robust than mtime-in-seconds alone — catches rapid
# successive writes within the same second that still produce
# different file contents.
_module_stamps: dict[str, tuple[int, int]] = {}


def _maybe_reload() -> None:
    """Reload dagwright submodules whose source has changed since
    the last call. Called at the top of each tool. Silent on
    failure — a transient reload error shouldn't crash a tool call.
    Lazy imports inside tool bodies pick up the freshly reloaded
    module bindings."""
    for name in _RELOADABLE:
        mod = sys.modules.get(name)
        path = getattr(mod, "__file__", None)
        if mod is None or path is None:
            continue
        try:
            st = os.stat(path)
        except OSError:
            continue
        stamp = (st.st_mtime_ns, st.st_size)
        if _module_stamps.get(name) != stamp:
            _module_stamps[name] = stamp
            try:
                # Belt-and-suspenders: clear finder caches so Python
                # rechecks the filesystem rather than trusting any
                # stale bytecode cache.
                importlib.invalidate_caches()
                importlib.reload(mod)
            except Exception as e:
                print(
                    f"[dagwright mcp] reload failed for {name}: {e}",
                    file=sys.stderr,
                    flush=True,
                )

# Maps absolute spec_path → list of plans from the most recent
# `plan` call on that spec. Used to compute diffs across calls.
# Survives module reloads of planner/diff because mcp_server itself
# is not in the reload set.
_previous_plans: dict[str, list] = {}
# Class name of the previously-seen spec per spec_path. Stored as a
# string (not a type) so it survives `_maybe_reload()` swapping out
# `dagwright.state.DefinitionalChange` and friends for fresh class
# objects — `is` comparison would always be False after reload.
_previous_spec_kind: dict[str, str] = {}


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
            on this spec_path. Empty string on the first call, or
            when the spec kind changed between calls.
    """
    _maybe_reload()
    # Lazy-import so we get whatever versions just got (re-)loaded.
    from dagwright.planner import run_plan
    from dagwright.diff import diff_plans

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
    prev = _previous_plans.get(spec_path_abs)
    prev_kind = _previous_spec_kind.get(spec_path_abs)
    if prev is not None and prev_kind == type(spec).__name__:
        diff_md = diff_plans(prev, plans, spec)
    _previous_plans[spec_path_abs] = list(plans)
    _previous_spec_kind[spec_path_abs] = type(spec).__name__

    return {
        "spec": _to_jsonable(spec),
        "plans": [_to_jsonable(p) for p in plans],
        "rejections": [_to_jsonable(r) for r in rejections],
        "diff": diff_md,
    }


@mcp.tool()
def summarize_manifest(manifest_path: str) -> dict:
    """Compact summary of a dbt manifest — lets Claude orient in a
    new project without ingesting the full multi-MB JSON.

    Returns:
        dbt metadata: schema version, dbt version, project name.
        models_by_layer: count per (SOURCE|STAGING|INTERMEDIATE|MART).
        total_nodes / total_edges: structural size.
        marts: list of {name, schema_column_count, grain, materialization}.
        exposures: list of {name, type, owner, depends_on (model
            names only)} for in-tree dbt exposures (the BI graph).
    """
    _maybe_reload()
    import json
    from collections import Counter
    from dagwright.loaders import load_manifest

    p = Path(manifest_path)
    with open(p, encoding="utf-8") as f:
        raw = json.load(f)

    metadata = raw.get("metadata", {}) or {}
    dag = load_manifest(p)

    layers = Counter(n.layer for n in dag.nodes.values())

    marts = sorted(
        [
            {
                "name": n.name,
                "schema_column_count": len(n.schema),
                "grain": list(n.grain),
                "materialization": n.materialization,
            }
            for n in dag.nodes.values()
            if n.layer == "MART"
        ],
        key=lambda m: m["name"],
    )

    raw_exposures = raw.get("exposures", {}) or {}
    exposures = []
    for exp in raw_exposures.values():
        deps = (exp.get("depends_on") or {}).get("nodes", []) or []
        owner = exp.get("owner") or {}
        exposures.append({
            "name": exp.get("name"),
            "type": exp.get("type"),
            "owner": owner.get("email") or owner.get("name"),
            "depends_on": [d.split(".")[-1] for d in deps],
        })
    exposures.sort(key=lambda e: e["name"] or "")

    return {
        "dbt_schema_version": metadata.get("dbt_schema_version"),
        "dbt_version": metadata.get("dbt_version"),
        "generated_at": metadata.get("generated_at"),
        "project_name": metadata.get("project_name"),
        "models_by_layer": dict(layers),
        "total_nodes": len(dag.nodes),
        "total_edges": len(dag.edges),
        "marts": marts,
        "exposures": exposures,
    }


@mcp.tool()
def discover_specs(root_path: str) -> list[dict]:
    """Find dagwright specs under `root_path`. Walks the directory
    tree, looks at every .yaml / .yml file, returns those that have
    a recognised top-level `kind` field (`metric_request`,
    `definitional_change`, `domain`).

    Returns a list of dicts: {spec_path, kind, id, intent_excerpt}.
    The AE doesn't have to remember spec paths — the LLM finds them.
    """
    import yaml

    out: list[dict] = []
    root = Path(root_path)
    if not root.exists():
        return [{"error": f"path does not exist: {root_path}"}]

    recognised_kinds = {"metric_request", "definitional_change", "domain"}
    for p in root.rglob("*.yaml"):
        out.extend(_scan_one(p, recognised_kinds))
    for p in root.rglob("*.yml"):
        out.extend(_scan_one(p, recognised_kinds))
    out.sort(key=lambda x: x.get("spec_path", ""))
    return out


def _scan_one(path: Path, recognised_kinds: set[str]) -> list[dict]:
    import yaml

    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(raw, dict):
        return []
    kind = raw.get("kind")
    if kind not in recognised_kinds:
        return []
    intent = raw.get("intent") or ""
    if isinstance(intent, str):
        excerpt = intent.strip().split("\n")[0][:200]
    else:
        excerpt = ""
    return [{
        "spec_path": str(path).replace("\\", "/"),
        "kind": kind,
        "id": raw.get("id"),
        "intent_excerpt": excerpt,
    }]


@mcp.tool()
def validate_spec(spec_path: str) -> dict:
    """Check whether a dagwright spec at `spec_path` parses cleanly.

    Use this between an LLM-driven spec edit and a `plan` call so
    the LLM can self-correct an invalid edit before the planner sees
    it. Returns:
        ok: bool — True iff the spec parsed successfully.
        spec_kind: "metric_request" | "definitional_change" | None
        errors: list of error message strings (empty when ok).
        spec_id: str | None — the spec's id when parseable.
    """
    _maybe_reload()
    from dagwright.loaders import SpecError, load_spec
    from dagwright.state import DefinitionalChange, MetricRequest

    try:
        spec = load_spec(Path(spec_path))
    except SpecError as e:
        return {
            "ok": False,
            "spec_kind": None,
            "errors": [str(e)],
            "spec_id": None,
        }
    except FileNotFoundError as e:
        return {
            "ok": False,
            "spec_kind": None,
            "errors": [f"file not found: {e}"],
            "spec_id": None,
        }
    except Exception as e:
        return {
            "ok": False,
            "spec_kind": None,
            "errors": [f"{type(e).__name__}: {e}"],
            "spec_id": None,
        }

    if isinstance(spec, MetricRequest):
        kind = "metric_request"
    elif isinstance(spec, DefinitionalChange):
        kind = "definitional_change"
    else:
        kind = type(spec).__name__

    return {
        "ok": True,
        "spec_kind": kind,
        "errors": [],
        "spec_id": getattr(spec, "id", None),
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
