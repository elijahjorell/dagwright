"""Plan-level diffs between two ranked plan lists.

Why this exists: under watch mode, the AE doesn't see what the LLM
changed in the spec. They need the plan-side delta to bridge "I asked
for X" with "here's what got different in the plan." Re-reading two
full plans side by side defeats the iteration loop. This module
produces a compact markdown summary of what changed.

v0 supports DefinitionalChangePlan only. metric_request plans have
a different shape (no `shape` discriminator, ranked by parent + grain
resolution); diffing those is a follow-up.
"""

from __future__ import annotations

from dagwright.planner import DefinitionalChangePlan


def diff_dc_plans(
    prev: list[DefinitionalChangePlan],
    curr: list[DefinitionalChangePlan],
) -> str:
    """Render a markdown summary of what changed between two ranked
    DefinitionalChangePlan lists. Returns a one-line "no changes"
    message if scores, ranks, contracts, and downstream models are
    all identical. Returns empty string if `prev` is empty (initial
    run — caller should print full output instead)."""
    if not prev:
        return ""

    # Index by shape; in DC planning the shape is a stable
    # discriminator across runs (consumer_only, replace_in_place,
    # add_versioned_column, versioned_mart). When `_plan_consumer_only`
    # decides the shape isn't feasible (no bare-column match), it
    # disappears — handle that as a shape-removed case.
    prev_by_shape = {p.shape: (i, p) for i, p in enumerate(prev)}
    curr_by_shape = {p.shape: (i, p) for i, p in enumerate(curr)}
    shapes = sorted(set(prev_by_shape) | set(curr_by_shape))

    lines: list[str] = []
    found = False

    for shape in shapes:
        if shape not in prev_by_shape:
            _, p = curr_by_shape[shape]
            lines.append(
                f"- new shape `{shape}` (rank {p_rank(curr_by_shape, shape)}, "
                f"score {p.score:.2f})"
            )
            found = True
            continue
        if shape not in curr_by_shape:
            _, p = prev_by_shape[shape]
            lines.append(
                f"- removed shape `{shape}` (was rank "
                f"{p_rank(prev_by_shape, shape)}, score {p.score:.2f})"
            )
            found = True
            continue

        prev_rank, prev_plan = prev_by_shape[shape]
        curr_rank, curr_plan = curr_by_shape[shape]

        # Score delta — only flag if non-trivial.
        score_delta = curr_plan.score - prev_plan.score
        if abs(score_delta) > 0.01:
            sign = "+" if score_delta > 0 else ""
            lines.append(
                f"- `{shape}`: score {prev_plan.score:.2f} → "
                f"{curr_plan.score:.2f} ({sign}{score_delta:.2f})"
            )
            found = True

        # Rank change.
        if prev_rank != curr_rank:
            lines.append(
                f"- `{shape}`: rank {prev_rank + 1} → {curr_rank + 1}"
            )
            found = True

        # Contract status flips (held boolean changes).
        prev_held = {c.contract_id: (c.held, c.note) for c in prev_plan.contract_status}
        curr_held = {c.contract_id: (c.held, c.note) for c in curr_plan.contract_status}
        for cid in sorted(set(prev_held) | set(curr_held)):
            prev_h = prev_held.get(cid)
            curr_h = curr_held.get(cid)
            if prev_h is None:
                lines.append(f"- `{shape}` contract `{cid}` added (held: {curr_h[0]})")
                found = True
            elif curr_h is None:
                lines.append(f"- `{shape}` contract `{cid}` removed")
                found = True
            elif prev_h[0] != curr_h[0]:
                old = "OK" if prev_h[0] else "FAIL"
                new = "OK" if curr_h[0] else "FAIL"
                lines.append(f"- `{shape}` contract `{cid}`: [{old} → {new}]")
                found = True

        # Downstream dbt model adds/removes.
        prev_ds = set(prev_plan.blast_radius.get("downstream_dbt_models") or [])
        curr_ds = set(curr_plan.blast_radius.get("downstream_dbt_models") or [])
        added = curr_ds - prev_ds
        removed = prev_ds - curr_ds
        if added or removed:
            parts = []
            if added:
                parts.append("+" + ", ".join(f"`{n}`" for n in sorted(added)))
            if removed:
                parts.append("-" + ", ".join(f"`{n}`" for n in sorted(removed)))
            lines.append(f"- `{shape}` downstream: {' '.join(parts)}")
            found = True

    if not found:
        return "_(no semantic changes since last run)_"

    return "\n".join(lines)


def p_rank(by_shape: dict, shape: str) -> int:
    """1-indexed rank for a shape in a by-shape dict (i, plan)."""
    return by_shape[shape][0] + 1
