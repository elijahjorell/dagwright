"""Column-level lineage extraction from raw dbt model SQL.

For each model, we want to know — per output column — which upstream
column(s) it was derived from. This lets the planner reason across
column renames so a metric_request asking for ``customer_id`` can
match a parent that exposes the same data under a different name.

Today's slice is regex-based and covers the dominant staging-model
rename pattern (``id AS customer_id``). It deliberately does NOT
handle:

- Multi-table JOINs where columns must be resolved to a specific
  source table (sqlglot with schemas does this; v1 work).
- Expressions wrapped in dbt macros (``{{ cents_to_dollars('subtotal') }}
  AS subtotal``) — the macro arg may be a column ref but extracting
  it reliably needs a real parser.
- Window functions and aggregates whose inputs we'd want to credit.
- SELECT ``*`` propagation through CTEs (we treat each CTE locally).

When extraction can't determine the upstream column for an output
column, the column is simply absent from the lineage map — meaning
the planner falls back to literal name matching for that column.
This is a coverage gap, not a correctness bug.

Compiled SQL would be ideal input but dbt's manifest only includes
``compiled_code`` if ``dbt compile`` was run (not ``dbt parse``). All
fixtures we ship today are parse-only, so we work from raw_code with
Jinja stripped to placeholders.
"""

from __future__ import annotations

import re

# {{ source('schema', 'table') }} → src_schema__table
_SOURCE_RE = re.compile(
    r"\{\{\s*source\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}"
)

# {{ ref('model_name') }} → model_name
_REF_RE = re.compile(r"\{\{\s*ref\s*\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}")

# Any other {{ expression }} → NULL placeholder. Must run after _SOURCE_RE
# and _REF_RE so we don't clobber recognized templates.
_JINJA_INLINE_RE = re.compile(r"\{\{[^{}]*\}\}")

# {% block tags %} → drop entirely.
_JINJA_BLOCK_RE = re.compile(r"\{%[^%]*%\}", re.DOTALL)

# Single-line and block SQL comments.
_SQL_LINE_COMMENT_RE = re.compile(r"--[^\n]*")
_SQL_BLOCK_COMMENT_RE = re.compile(r"/\*[\s\S]*?\*/")

# `<col> AS <alias>` where the source side is a bare identifier (not an
# expression, not a function call, not a literal). We pre-strip table
# aliases from FROM / JOIN clauses so any remaining `<word> AS <word>`
# is column-level by elimination.
_ALIAS_RE = re.compile(
    r"\b([a-zA-Z_]\w*)\s+as\s+([a-zA-Z_]\w*)\b",
    re.IGNORECASE,
)

# `from <table> [as <alias>]` and `join <table> [as <alias>]` — eliminate
# these so the remaining `<word> AS <word>` patterns are column aliases.
_TABLE_ALIAS_RE = re.compile(
    r"\b(from|join)\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)\s+(?:as\s+)?([a-zA-Z_]\w*)\b",
    re.IGNORECASE,
)
# Subquery alias: `(...) [as] <alias>`. After paren matching is intractable
# in regex, so we just blank `\) [as] <word>` patterns.
_SUBQUERY_ALIAS_RE = re.compile(r"\)\s*(?:as\s+)?([a-zA-Z_]\w*)\b", re.IGNORECASE)

# SQL keywords we must never treat as column names.
_SQL_KEYWORDS = frozenset({
    "select", "from", "where", "group", "order", "having", "with",
    "as", "and", "or", "not", "in", "is", "null", "true", "false",
    "case", "when", "then", "else", "end", "join", "on", "left",
    "right", "inner", "outer", "full", "cross", "union", "all",
    "distinct", "limit", "offset", "by", "asc", "desc", "between",
    "exists", "any", "some",
})


def strip_jinja(sql: str) -> str:
    """Replace Jinja templates with placeholders so SQL is structurally
    inspectable. Recognized ``source()`` and ``ref()`` calls become
    synthetic identifiers; other ``{{ ... }}`` becomes ``NULL`` so it
    doesn't confuse the alias regex; ``{% ... %}`` blocks are dropped."""
    sql = _SOURCE_RE.sub(r"src_\1__\2", sql)
    sql = _REF_RE.sub(r"\1", sql)
    sql = _JINJA_BLOCK_RE.sub("", sql)
    sql = _JINJA_INLINE_RE.sub("NULL", sql)
    return sql


def strip_comments(sql: str) -> str:
    sql = _SQL_BLOCK_COMMENT_RE.sub("", sql)
    sql = _SQL_LINE_COMMENT_RE.sub("", sql)
    return sql


def _strip_non_column_aliases(sql: str) -> str:
    """Eliminate FROM/JOIN/subquery aliasing so the remaining
    ``<word> AS <word>`` matches are guaranteed to be column-level."""
    # `from foo as f` → `from foo` (alias name stripped); same for join.
    sql = _TABLE_ALIAS_RE.sub(lambda m: f"{m.group(1)} {m.group(2)}", sql)
    # `) as sub` → `)` ; `) sub` → `)`. Loses subquery alias names; that's fine.
    sql = _SUBQUERY_ALIAS_RE.sub(")", sql)
    return sql


def extract_aliases(raw_code: str) -> list[tuple[str, str]]:
    """Extract (src_col, dst_col) pairs from ``<col> AS <alias>`` patterns
    in the SQL. Order is the source-text order; duplicates collapsed
    case-insensitively keeping the first occurrence."""
    if not raw_code:
        return []
    sql = strip_comments(strip_jinja(raw_code))
    sql = _strip_non_column_aliases(sql)
    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str]] = []
    for src, dst in _ALIAS_RE.findall(sql):
        src_l, dst_l = src.lower(), dst.lower()
        if src_l in _SQL_KEYWORDS or dst_l in _SQL_KEYWORDS:
            continue
        if src_l == dst_l:
            # `foo AS foo` — not a rename.
            continue
        key = (src_l, dst_l)
        if key in seen:
            continue
        seen.add(key)
        out.append((src, dst))
    return out


def attribute_aliases(
    aliases: list[tuple[str, str]],
    upstream_node_names: list[str],
) -> dict[str, list[tuple[str, str]]]:
    """Map each output column to a list of (upstream_node, upstream_col)
    references.

    With a single upstream node, every alias is attributed to it. With
    multiple upstream nodes, attribution is ambiguous via regex alone
    and we conservatively skip it; the sqlglot path resolves the
    JOIN-qualified case.
    """
    if not aliases or not upstream_node_names:
        return {}
    if len(upstream_node_names) > 1:
        return {}
    upstream = upstream_node_names[0]
    out: dict[str, list[tuple[str, str]]] = {}
    for src, dst in aliases:
        out.setdefault(dst, []).append((upstream, src))
    return out


def extract_lineage_sqlglot(
    raw_code: str,
    output_columns: list[str],
    upstream_schemas: dict[str, list[str]],
    dialect: str = "duckdb",
) -> dict[str, list[tuple[str, str]]]:
    """Use sqlglot's column-lineage walker to map each output column to
    its upstream source columns. Resolves JOIN-qualified columns,
    SELECT-* propagation through CTEs (when source schemas are known),
    and expression-derived columns the regex can't see.

    Returns ``{}`` when sqlglot can't parse the SQL or the requested
    output column doesn't appear in it. Per-column failures are silently
    skipped; the caller can fall back to regex for those.

    Performance: parses, qualifies, and builds a Scope **once** per
    model, then reuses the scope for each output column's lineage
    walk. Without this reuse, sqlglot re-parses and re-qualifies the
    SQL on every ``lineage()`` call — order-of-magnitude slowdown on
    real-world manifests with hundreds of models and ~10 columns each.

    ``upstream_schemas`` maps node name to the list of column names
    that node exposes. Pass ``[]`` for nodes whose columns aren't
    documented — sqlglot will fail to resolve ``*`` from them but
    other columns may still resolve fine.
    """
    if not raw_code or not output_columns:
        return {}
    try:
        import sqlglot
        from sqlglot import exp as _sg_exp
        from sqlglot import lineage as _sg_lineage
        from sqlglot.errors import SqlglotError
        from sqlglot.optimizer.qualify import qualify as _sg_qualify
        from sqlglot.optimizer.scope import build_scope as _sg_build_scope
    except ImportError:
        return {}

    sql = strip_comments(strip_jinja(raw_code))
    schema: dict[str, dict[str, str]] = {
        node: {col: "UNKNOWN" for col in cols}
        for node, cols in upstream_schemas.items()
    }

    # Parse once per model. Qualifying resolves table aliases, expands
    # SELECT *, and threads schema info through CTEs — expensive enough
    # to amortise across all output columns by sharing the scope.
    try:
        expression = sqlglot.parse_one(sql, dialect=dialect)
    except (SqlglotError, ValueError, AttributeError):
        return {}

    # Pull alias→underlying-table map from the parsed AST so leaves
    # like ``c.id`` from ``FROM customers AS c`` can be translated
    # back to ``customers.id`` when we filter against upstream_schemas.
    alias_to_table: dict[str, str] = {}
    try:
        for tbl in expression.find_all(_sg_exp.Table):
            tbl_name = tbl.name
            tbl_alias = tbl.alias
            if tbl_alias and tbl_alias != tbl_name:
                alias_to_table[tbl_alias] = tbl_name
    except (SqlglotError, AttributeError):
        pass

    # Qualify + build the Scope once. lineage(scope=...) skips its
    # internal parse + qualify + build_scope.
    try:
        qualified = _sg_qualify(
            expression.copy(), schema=schema, dialect=dialect
        )
        scope = _sg_build_scope(qualified)
    except (SqlglotError, ValueError, AttributeError, KeyError):
        return {}

    out: dict[str, list[tuple[str, str]]] = {}
    for col in output_columns:
        try:
            root = _sg_lineage.lineage(
                col, qualified, schema=schema, dialect=dialect, scope=scope
            )
        except (SqlglotError, KeyError, AttributeError, RecursionError, ValueError):
            continue
        leaves = _walk_to_leaves(root, alias_to_table)
        # Filter to (node, col) pairs where the node is a known upstream
        # AND the column is specific (not `*`). Sources without
        # documented schemas leave sqlglot stuck at `*`; the caller's
        # regex fallback handles those.
        filtered = [
            (n, c) for n, c in leaves
            if n in upstream_schemas and c != "*"
        ]
        if filtered:
            out[col] = filtered
    return out


def _walk_to_leaves(
    node,
    alias_to_table: dict[str, str] | None = None,
) -> list[tuple[str, str]]:
    """Collect (table, column) leaves from a sqlglot lineage tree.
    Leaves are nodes with no ``downstream`` entries; their ``name``
    attribute is usually ``"table.column"`` (or ``'"table"."column"'``
    after qualification). When ``alias_to_table`` is provided, table
    aliases are translated back to underlying table names so the
    result references real upstream nodes."""
    alias_to_table = alias_to_table or {}
    leaves: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def _unquote(s: str) -> str:
        # Qualified names come wrapped in double quotes (and sometimes
        # backticks for other dialects). Strip them so identifier
        # matching works against the upstream-schemas dict.
        if len(s) >= 2 and s[0] == s[-1] and s[0] in ('"', "'", "`"):
            return s[1:-1]
        return s

    def visit(n) -> None:
        if not n.downstream:
            name = getattr(n, "name", "") or ""
            if "." in name:
                # Handles `t.c`, `"t"."c"`, `schema.t.c`. Split on dots,
                # then strip quoting on each part. The last two parts
                # are the table-or-alias and the column name.
                parts = [p for p in name.split(".") if p]
                if len(parts) >= 2:
                    table_or_alias = _unquote(parts[-2])
                    column = _unquote(parts[-1])
                    real_table = alias_to_table.get(
                        table_or_alias, table_or_alias
                    )
                    ref = (real_table, column)
                    if ref not in seen:
                        seen.add(ref)
                        leaves.append(ref)
            return
        for d in n.downstream:
            visit(d)

    visit(node)
    return leaves


def extract_lineage(
    raw_code: str,
    output_columns: list[str],
    upstream_schemas: dict[str, list[str]],
) -> dict[str, list[tuple[str, str]]]:
    """Hybrid extractor: cheap regex path first, sqlglot only when the
    regex can't help.

    Per-output-column logic:
    - **Fast path** — single upstream and regex captures something:
      use regex only. Covers the dominant staging-rename pattern and
      keeps load time fast on real-world manifests with hundreds of
      models. sqlglot parsing is ~60 ms/model and adds up; we avoid
      it when the regex result is already enough.
    - **Slow path** — multiple upstreams (JOIN attribution needed) or
      no regex match at all (likely an expression-derived column):
      run sqlglot, fall back to regex for any column it can't resolve.
    - Otherwise the column is absent and the planner falls back to
      literal name matching.
    """
    upstream_names = list(upstream_schemas.keys())
    regex_aliases = extract_aliases(raw_code) if raw_code else []
    rg = attribute_aliases(regex_aliases, upstream_names)

    # Fast path: a single upstream node with at least one regex alias
    # match means the staging-rename pattern. sqlglot would give the
    # same answer at ~60 ms per model — skip it.
    if len(upstream_names) <= 1 and rg:
        return dict(rg)

    # Also skip sqlglot when there's a single upstream and it has no
    # documented schema. sqlglot can only resolve specific columns when
    # source schemas are known; with `select * from <undocumented>` it
    # stops at the table's `*` leaf which we filter out anyway.
    if (
        len(upstream_names) == 1
        and not upstream_schemas.get(upstream_names[0])
    ):
        return dict(rg)

    sg = extract_lineage_sqlglot(raw_code, output_columns, upstream_schemas)

    out: dict[str, list[tuple[str, str]]] = {}
    for col in output_columns:
        if col in sg:
            out[col] = sg[col]
        elif col in rg:
            out[col] = rg[col]
    # Also include regex-only columns the caller didn't list explicitly
    # (e.g., undocumented intermediate columns).
    for col, refs in rg.items():
        if col not in out:
            out[col] = refs
    return out
