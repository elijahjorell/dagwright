import json
from dataclasses import asdict

from dagwright.planner import (
    DefinitionalChangePlan,
    Plan,
    Rejection,
)
from dagwright.state import DefinitionalChange, MetricRequest


def render_json(spec: MetricRequest, plans: list[Plan], rejections: list[Rejection]) -> str:
    payload = {
        "spec": asdict(spec),
        "plans": [asdict(p) for p in plans],
        "alternatives_rejected": [asdict(r) for r in rejections],
    }
    return json.dumps(payload, indent=2, default=str)


def render_markdown(spec: MetricRequest, plans: list[Plan], rejections: list[Rejection]) -> str:
    lines: list[str] = []
    lines.append(f"# Plan: `{spec.id}`")
    lines.append("")
    lines.append(f"**Intent.** {spec.intent.strip()}")
    lines.append("")
    lines.extend(_render_request_md(spec))
    lines.append("")

    if not plans:
        lines.append("## No viable plans")
        lines.append("")
        lines.append(
            "The planner could not construct any plan from the supplied "
            "manifest. See **Alternatives rejected** below for why each "
            "candidate was excluded. Most common cause: declared schemas "
            "missing the columns this metric needs (the planner is strict "
            "and does not infer columns from SQL)."
        )
    else:
        for i, plan in enumerate(plans, start=1):
            lines.extend(_render_plan_md(i, plan))

    if rejections:
        lines.append("## Alternatives rejected")
        lines.append("")
        for r in rejections:
            lines.append(f"- **`{r.candidate_parent}`** — {r.reason}")
        lines.append("")

    return "\n".join(lines)


def _render_plan_md(rank: int, plan: Plan) -> list[str]:
    out: list[str] = []
    out.append(f"## Plan {rank}: parent `{plan.parent}` → `{plan.new_construction[0]}`")
    out.append("")
    out.append(f"_Score: {plan.score:.2f}_  _Effort: {plan.effort} ops_")
    out.append("")
    out.append(f"**Semantics.** {plan.semantic_summary}")
    out.append("")

    out.append("### Operations")
    out.append("")
    for j, op in enumerate(plan.operations, start=1):
        out.append(f"{j}. `{op.op}`")
        for k, v in op.args.items():
            out.append(f"   - {k}: `{_compact(v)}`")
    out.append("")

    out.append("### Invariants checked")
    out.append("")
    for inv in plan.invariants:
        mark = "OK" if inv.holds else "FAIL"
        out.append(f"- **{inv.id}** [{mark}] — {inv.note}")
    out.append("")

    out.append("### Pathways")
    out.append("")
    out.append(f"- **Reused upstream:** {', '.join(f'`{n}`' for n in plan.pathways_reused) or '_(none)_'}")
    out.append(f"- **Newly constructed:** {', '.join(f'`{n}`' for n in plan.new_construction)}")
    out.append("")

    out.append("### Blast radius")
    out.append("")
    blast = plan.blast_radius
    out.append(
        f"- Existing artifacts whose contracts are at risk: "
        f"{', '.join(f'`{a}`' for a in blast['existing_artifacts_affected']) or '_(none)_'}"
    )
    if blast["parent_consumers_unchanged"]:
        out.append(
            "- Parent's existing consumers (unchanged by this plan, listed for context): "
            + ", ".join(f"`{a}`" for a in blast["parent_consumers_unchanged"])
        )
    out.append(f"- New artifact this plan binds a contract to: `{blast['new_artifact']}`")
    out.append("")

    if plan.risks:
        out.append("### Risks")
        out.append("")
        for r in plan.risks:
            out.append(f"- **{r.id}** [{r.severity.upper()}] — {r.summary}")
            out.append(f"  - *Mitigation:* {r.mitigation}")
        out.append("")

    if plan.notes:
        out.append("### Notes")
        out.append("")
        for n in plan.notes:
            out.append(f"- {n}")
        out.append("")

    return out


def _render_request_md(spec: MetricRequest) -> list[str]:
    grain = spec.output_shape.grain
    out: list[str] = []
    out.append("**Request.**")
    out.append("")
    out.append(
        f"- kind: `metric_request`  name: `{spec.name}`  "
        f"tier: `{spec.contract_tier}`  consumer: `{spec.consumer.tool}/{spec.consumer.artifact}`"
    )
    out.append(f"- grain.keys: `{list(grain.keys)}`")
    if grain.coverage:
        out.append("- grain.coverage:")
        for key, cov in grain.coverage.items():
            density = "dense" if cov.dense else "sparse"
            range_part = ""
            if cov.range:
                range_part = f", range `{cov.range.from_}` → `{cov.range.to}`"
            fill_part = f", fill `{cov.fill}`" if cov.fill is not None else ""
            out.append(f"  - `{key}`: {density}{range_part}{fill_part}")
    out.append("- columns:")
    for c in spec.output_shape.columns:
        if c.is_structured:
            expr = f"{c.aggregation}({c.column})"
        else:
            expr = c.expr
        out.append(f"  - `{c.name}` = `{expr}`")
    if spec.filters:
        out.append("- filters:")
        for f in spec.filters:
            out.append(f"  - `{f}`")
    return out


def _compact(v) -> str:
    if isinstance(v, dict):
        if not v:
            return "{}"
        return "{ " + ", ".join(f"{k}: {_compact(vv)}" for k, vv in v.items()) + " }"
    if isinstance(v, list):
        if not v:
            return "[]"
        return "[ " + ", ".join(_compact(x) for x in v) + " ]"
    return str(v)


# -----------------------------------------------------------------------------
# definitional_change rendering
# -----------------------------------------------------------------------------


def render_json_definitional_change(
    spec: DefinitionalChange,
    plans: list[DefinitionalChangePlan],
    rejections: list[Rejection],
) -> str:
    payload = {
        "spec": asdict(spec),
        "plans": [asdict(p) for p in plans],
        "alternatives_rejected": [asdict(r) for r in rejections],
    }
    return json.dumps(payload, indent=2, default=str)


def render_markdown_definitional_change(
    spec: DefinitionalChange,
    plans: list[DefinitionalChangePlan],
    rejections: list[Rejection],
) -> str:
    lines: list[str] = []
    lines.append(f"# Plan: `{spec.id}` (definitional_change)")
    lines.append("")
    lines.append(f"**Intent.** {spec.intent.strip()}")
    lines.append("")
    lines.append("**Change.**")
    lines.append("")
    lines.append(f"- target: `{spec.target_node}.{spec.target_column}`")
    lines.append(
        f"- old: `{spec.old_definition.expr}` "
        f"({spec.old_definition.basis})"
    )
    lines.append(
        f"- new: `{spec.new_definition.expr}` "
        f"({spec.new_definition.basis})"
    )
    must_migrate_str = (
        ", ".join(f"`{a}`" for a in spec.must_migrate) or "_(none)_"
    )
    lines.append(f"- must_migrate: {must_migrate_str}")
    lines.append(f"- allow_stale_consumers: `{spec.allow_stale_consumers}`")
    lines.append("")

    if not plans:
        lines.append("## No viable plans")
        lines.append("")
        lines.append(
            "The planner could not enumerate any plan shape against the "
            "supplied manifest. See **Alternatives rejected** for why."
        )
    else:
        for i, plan in enumerate(plans, start=1):
            lines.extend(_render_dc_plan_md(i, plan))

    if rejections:
        lines.append("## Alternatives rejected")
        lines.append("")
        for r in rejections:
            lines.append(f"- **`{r.candidate_parent}`** — {r.reason}")
        lines.append("")

    return "\n".join(lines)


def _render_dc_plan_md(rank: int, plan: DefinitionalChangePlan) -> list[str]:
    out: list[str] = []
    out.append(f"## Plan {rank}: `{plan.shape}`")
    out.append("")
    out.append(f"_Score: {plan.score:.2f}_  _Effort: {plan.effort} ops_")
    out.append("")
    out.append(f"**Semantics.** {plan.semantic_summary}")
    out.append("")

    out.append("### Operations")
    out.append("")
    for j, op in enumerate(plan.operations, start=1):
        out.append(f"{j}. `{op.op}`")
        for k, v in op.args.items():
            out.append(f"   - {k}: `{_compact(v)}`")
    out.append("")

    out.append("### Contract status")
    out.append("")
    for cs in plan.contract_status:
        mark = "OK" if cs.held else "FAIL"
        out.append(f"- **{cs.contract_id}** [{mark}] — {cs.note}")
    out.append("")

    out.append("### Blast radius")
    out.append("")
    blast = plan.blast_radius
    out.append(f"- scheme: {blast['scheme']}")
    must = blast.get("must_migrate_satisfied") or []
    out.append(
        f"- must_migrate satisfied: "
        f"{', '.join(f'`{a}`' for a in must) or '_(none)_'}"
    )
    affected = blast.get("existing_artifacts_affected") or []
    out.append(
        f"- artifacts at risk: "
        f"{', '.join(f'`{a}`' for a in affected) or '_(none)_'}"
    )
    downstream = blast.get("downstream_dbt_models") or []
    if downstream:
        head = ", ".join(f"`{n}`" for n in downstream[:8])
        suffix = "" if len(downstream) <= 8 else f", + {len(downstream) - 8} more"
        out.append(
            f"- downstream dbt models potentially affected "
            f"({len(downstream)}): {head}{suffix}"
        )
    else:
        out.append("- downstream dbt models potentially affected: _(none)_")
    out.append("")

    if plan.risks:
        out.append("### Risks")
        out.append("")
        for r in plan.risks:
            out.append(f"- **{r.id}** [{r.severity.upper()}] — {r.summary}")
            out.append(f"  - *Mitigation:* {r.mitigation}")
        out.append("")

    if plan.notes:
        out.append("### Notes")
        out.append("")
        for n in plan.notes:
            out.append(f"- {n}")
        out.append("")

    return out
