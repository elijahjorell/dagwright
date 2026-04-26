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
    multiple upstream nodes, attribution is ambiguous and we conservatively
    skip it (the column is omitted from the lineage map, falling back
    to literal name matching). Resolving multi-source attribution
    requires a real SQL parser; see module docstring.
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
