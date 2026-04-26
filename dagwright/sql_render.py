"""Deterministic plan -> SQL rendering for metric_request plans.

Why this exists: a dagwright plan's operations already encode
everything an AE needs to write the SQL — `column_lineage` gives
the SELECT fragments, `filters` give the WHERE clause, and
`join_to_spine` gives the LEFT JOIN axis for dense plans. Rendering
this deterministically lets us test outcome equivalence at the
data layer without an LLM in the loop on the dagwright side.

Scope (v0):
- Sparse metric_request plans (no dense axis) — full support.
- Dense metric_request plans (date_spine + LEFT JOIN) — full
  support, with a DuckDB-flavored date_spine emitter when range
  endpoints are ISO dates. Symbolic ranges (`earliest_event`,
  `current_period`) get rendered as a CTE that references the
  parent table — caller must execute against a warehouse where
  the parent has the relevant date column populated.
- definitional_change plans — out of scope for this module.

This is rendering, not validation. The planner already enforces
strict-schema mode (column references must exist on the parent's
declared schema), so the SQL is well-formed by construction.
Generated SQL is DuckDB dialect (the jaffle_shop fixtures use
DuckDB); other dialects need a small dialect layer.
"""

from __future__ import annotations

from dagwright.planner import Operation, Plan


def render_metric_request_plan(plan: Plan) -> str:
    """Render the plan as a single executable SQL query.

    Dense plans become a CTE chain (date_spine + agg + final
    LEFT JOIN). Sparse plans become a flat SELECT-GROUP BY.
    Output is a `SELECT ...;` query — wrap as `CREATE TABLE AS`
    or a dbt model file at the call site as needed.
    """
    agg_edge = _find_op(plan, "add_edge", transform_type="aggregation")
    if agg_edge is None:
        raise ValueError("plan has no aggregation edge")
    mart_node = _find_mart_add_node(plan)
    if mart_node is None:
        raise ValueError("plan has no MART add_node")

    if "join_to_spine" in agg_edge.args:
        return _render_dense(plan, agg_edge, mart_node)
    return _render_sparse(plan, agg_edge, mart_node)


# ---------------------------------------------------------------------------
# Sparse path
# ---------------------------------------------------------------------------


def _render_sparse(plan: Plan, agg_edge: Operation, mart_node: Operation) -> str:
    schema = mart_node.args["schema"]
    grain = mart_node.args["grain"]
    column_lineage: dict[str, str] = agg_edge.args["column_lineage"]
    parent: str = agg_edge.args["parent"]
    filters: list[str] = list(agg_edge.args.get("filters") or [])

    select_clauses = [f"  {column_lineage[c]} AS {c}" for c in schema]
    group_by_exprs = [column_lineage[k] for k in grain]

    sql = "SELECT\n" + ",\n".join(select_clauses) + f"\nFROM {parent}"
    if filters:
        sql += "\nWHERE " + " AND ".join(f"({f})" for f in filters)
    if group_by_exprs:
        sql += "\nGROUP BY " + ", ".join(group_by_exprs)
    return sql + ";\n"


# ---------------------------------------------------------------------------
# Dense path
# ---------------------------------------------------------------------------


def _render_dense(plan: Plan, agg_edge: Operation, mart_node: Operation) -> str:
    spine_info = agg_edge.args["join_to_spine"]
    spine_name: str = spine_info["spine"]
    join_on: dict[str, str] = spine_info["on"]  # {spine_qualified_col: source_expr}
    fill: dict = spine_info.get("fill") or {}

    spine_node = _find_op(plan, "add_node", name=spine_name)
    if spine_node is None:
        raise ValueError(f"dense plan references spine {spine_name!r} but no add_node found")
    spine_grain: list[str] = list(spine_node.args.get("grain") or [])
    if len(spine_grain) != 1:
        raise ValueError(f"v0 supports single-key spines; got {spine_grain}")
    spine_key = spine_grain[0]
    range_from = spine_node.args.get("range_from")
    range_to = spine_node.args.get("range_to")

    spine_edge = _find_op(plan, "add_edge", parent=spine_name)
    spine_lineage: dict[str, str] = (spine_edge.args.get("column_lineage") if spine_edge else {}) or {}

    schema = mart_node.args["schema"]
    parent: str = agg_edge.args["parent"]
    agg_lineage: dict[str, str] = agg_edge.args["column_lineage"]
    filters: list[str] = list(agg_edge.args.get("filters") or [])

    # Aggregation CTE: project the source-side join keys + the measure
    # columns, grouped by the join expressions. The measure column
    # entries in agg_lineage already carry the aggregation function
    # (e.g. count_distinct(customers.customer_id)).
    join_pairs = list(join_on.items())  # [(spine_qualified_col, source_expr)]
    join_alias: dict[str, str] = {}     # spine_qualified_col -> alias used in agg CTE
    agg_select: list[str] = []
    for spine_col, source_expr in join_pairs:
        alias = f"_join_{spine_col.replace('.', '__')}"
        join_alias[spine_col] = alias
        agg_select.append(f"  {source_expr} AS {alias}")
    for col, expr in agg_lineage.items():
        agg_select.append(f"  {expr} AS {col}")

    agg_group_by = [source_expr for _, source_expr in join_pairs]

    agg_cte = "_agg AS (\n  SELECT\n" + ",\n".join(agg_select) + f"\n  FROM {parent}"
    if filters:
        agg_cte += "\n  WHERE " + " AND ".join(f"({f})" for f in filters)
    if agg_group_by:
        agg_cte += "\n  GROUP BY " + ", ".join(agg_group_by)
    agg_cte += "\n)"

    # Spine CTE: render concrete DuckDB date_spine when both range
    # endpoints are ISO; otherwise emit a SELECT DISTINCT against the
    # source date column (degraded mode — covers the symbolic range
    # case without requiring a warehouse-side date_spine package).
    spine_cte = _render_spine_cte(
        spine_name, spine_key, range_from, range_to, parent, join_pairs
    )

    # Final SELECT: LEFT JOIN spine to agg
    final_select: list[str] = []
    for col in schema:
        if col in spine_lineage:
            final_select.append(f"  {spine_lineage[col]} AS {col}")
        elif col in agg_lineage:
            if col in fill:
                final_select.append(f"  COALESCE(_agg.{col}, {_lit(fill[col])}) AS {col}")
            else:
                final_select.append(f"  _agg.{col} AS {col}")
        else:
            raise ValueError(f"schema column {col!r} missing from spine and agg lineage")

    on_clauses = [
        f"{spine_col} = _agg.{join_alias[spine_col]}"
        for spine_col, _ in join_pairs
    ]

    sql = (
        "WITH " + spine_cte + ",\n" + agg_cte + "\n"
        "SELECT\n" + ",\n".join(final_select) + "\n"
        f"FROM {spine_name}\n"
        f"LEFT JOIN _agg ON " + " AND ".join(on_clauses) + ";\n"
    )
    return sql


def _render_spine_cte(
    spine_name: str,
    spine_key: str,
    range_from,
    range_to,
    parent: str,
    join_pairs: list[tuple[str, str]],
) -> str:
    """Emit the spine CTE.

    - Concrete ISO endpoints → `range()` generator (DuckDB).
    - Symbolic endpoints (earliest_event / current_period) → degraded
      mode: SELECT DISTINCT the source-side join expressions from
      the parent, so every distinct bucket present in the source
      becomes a spine row. Doesn't fill genuine gaps — caller should
      replace with a real date_spine when running for production.
    """
    iso_re = _is_iso

    if iso_re(range_from) and iso_re(range_to):
        # DuckDB range() emits one row per <interval> between two dates.
        interval = _grain_to_interval(spine_key)
        return (
            f"{spine_name} AS (\n"
            f"  SELECT generate_series AS {spine_key}\n"
            f"  FROM range(\n"
            f"    DATE '{range_from}',\n"
            f"    DATE '{range_to}',\n"
            f"    INTERVAL {interval}\n"
            f"  ) t(generate_series)\n"
            f")"
        )

    # Degraded fallback: select distinct buckets from the source.
    if not join_pairs:
        raise ValueError(f"cannot synthesize spine without join_pairs (spine={spine_name})")
    spine_qualified_col = join_pairs[0][0]  # e.g. "date_spine_month.month"
    source_expr = join_pairs[0][1]          # e.g. "date_trunc('month', customers.first_ordered_at)"
    return (
        f"{spine_name} AS (\n"
        f"  -- range from={range_from!r} to={range_to!r} — symbolic, fall back to source-distinct\n"
        f"  SELECT DISTINCT {source_expr} AS {spine_key}\n"
        f"  FROM {parent}\n"
        f"  WHERE {source_expr} IS NOT NULL\n"
        f")"
    )


def _grain_to_interval(grain_key: str) -> str:
    return {
        "day": "1 DAY",
        "week": "1 WEEK",
        "month": "1 MONTH",
        "quarter": "3 MONTH",
        "year": "1 YEAR",
    }.get(grain_key, "1 MONTH")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _find_op(plan: Plan, op_name: str, **filters) -> Operation | None:
    for op in plan.operations:
        if op.op != op_name:
            continue
        if all(op.args.get(k) == v for k, v in filters.items()):
            return op
    return None


def _find_mart_add_node(plan: Plan) -> Operation | None:
    for op in plan.operations:
        if op.op == "add_node" and op.args.get("layer") == "MART":
            return op
    return None


def _is_iso(s) -> bool:
    if not isinstance(s, str):
        return False
    if len(s) != 10:
        return False
    if s[4] != "-" or s[7] != "-":
        return False
    return s[:4].isdigit() and s[5:7].isdigit() and s[8:10].isdigit()


def _lit(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        escaped = v.replace("'", "''")
        return f"'{escaped}'"
    return f"'{v}'"
