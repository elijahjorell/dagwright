from dataclasses import dataclass, field
from typing import Optional

LAYERS = ("SOURCE", "STAGING", "INTERMEDIATE", "MART", "EXPOSURE")

# Grain keys whose coverage must be declared in the spec. Aligns with
# the time grains the planner can derive via date_trunc.
TIME_LIKE_KEYS = ("day", "week", "month", "quarter", "year")


@dataclass
class Node:
    name: str
    layer: str
    grain: tuple[str, ...]
    schema: tuple[str, ...]
    materialization: str
    # Per-column descriptions sourced from dbt .yml docs. Used for
    # heuristic date-column detection when data_type is not declared.
    column_descriptions: dict[str, str] = field(default_factory=dict)
    # Per-output-column upstream column refs extracted from the model's
    # raw SQL (`<src> AS <dst>` patterns). Maps output column name to a
    # list of (upstream_node_name, upstream_col_name). Empty when the
    # model has no aliases, ambiguous attribution (multi-source), or
    # SQL the regex can't parse — in which case the planner falls back
    # to literal name matching for those columns.
    column_lineage: dict[str, list[tuple[str, str]]] = field(default_factory=dict)


@dataclass(frozen=True)
class Edge:
    parent: str
    child: str


@dataclass
class DagState:
    nodes: dict[str, Node]
    edges: tuple[Edge, ...]
    # (node_name, col) → frozenset of (node_name, col) known to be the same
    # data across the DAG via alias chains. Built from per-node
    # ``column_lineage``. The planner consults this when literal name
    # matching of a required column fails — a parent that exposes a
    # synonymous column under a different name is still a valid candidate
    # (with an alias note in the plan).
    column_synonyms: dict[tuple[str, str], frozenset[tuple[str, str]]] = field(
        default_factory=dict
    )

    def aliases_of(self, node: str, col: str) -> frozenset[str]:
        """Set of column names this (node, col) is known by elsewhere
        in the DAG via aliasing. Includes ``col`` itself."""
        component = self.column_synonyms.get((node, col), frozenset({(node, col)}))
        return frozenset(c for (_, c) in component)

    def synonym_match(self, node: str, requested_col: str) -> str | None:
        """If the parent at ``node`` has any column synonymous with
        ``requested_col``, return the parent's column name. Used by the
        planner to accept alias hits when literal matching fails."""
        n = self.nodes.get(node)
        if not n or requested_col in n.schema:
            return None  # caller should handle direct hits separately
        for x in n.schema:
            if requested_col in self.aliases_of(node, x):
                return x
        return None

    def parents_of(self, child: str) -> tuple[str, ...]:
        return tuple(e.parent for e in self.edges if e.child == child)

    def children_of(self, parent: str) -> tuple[str, ...]:
        return tuple(e.child for e in self.edges if e.parent == parent)

    def ancestors(self, name: str) -> set[str]:
        seen: set[str] = set()
        stack = list(self.parents_of(name))
        while stack:
            n = stack.pop()
            if n in seen:
                continue
            seen.add(n)
            stack.extend(self.parents_of(n))
        return seen

    def descendants(self, name: str) -> set[str]:
        seen: set[str] = set()
        stack = list(self.children_of(name))
        while stack:
            n = stack.pop()
            if n in seen:
                continue
            seen.add(n)
            stack.extend(self.children_of(n))
        return seen


@dataclass(frozen=True)
class Consumes:
    node: str
    columns: tuple[str, ...]


@dataclass(frozen=True)
class Artifact:
    id: str
    kind: str
    consumes: tuple[Consumes, ...]


@dataclass
class ConsumerGraph:
    tool: str
    artifacts: dict[str, Artifact]

    def artifacts_consuming(self, node: str) -> tuple[Artifact, ...]:
        return tuple(
            a for a in self.artifacts.values()
            if any(c.node == node for c in a.consumes)
        )


@dataclass(frozen=True)
class CoverageRange:
    from_: str  # ISO date "YYYY-MM-DD" or symbolic ("earliest_event", "current_period")
    to: str


@dataclass(frozen=True)
class Coverage:
    dense: bool
    range: Optional[CoverageRange]  # required when dense; None otherwise
    fill: object = None


@dataclass(frozen=True)
class Grain:
    keys: tuple[str, ...]
    coverage: dict[str, Coverage]  # subset of keys; required entries for time-like keys


@dataclass(frozen=True)
class MeasureColumn:
    name: str
    # Exactly one of (column + aggregation) or expr is populated.
    column: Optional[str]
    aggregation: Optional[str]
    expr: Optional[str]

    @property
    def is_structured(self) -> bool:
        return self.expr is None


@dataclass(frozen=True)
class OutputShape:
    grain: Grain
    columns: tuple[MeasureColumn, ...]


@dataclass(frozen=True)
class Consumer:
    tool: str
    artifact: str


@dataclass(frozen=True)
class MetricRequest:
    id: str
    intent: str
    name: str
    output_shape: OutputShape
    filters: tuple[str, ...]
    consumer: Consumer
    contract_tier: str


@dataclass(frozen=True)
class Definition:
    # Human label for the basis (e.g. "post_tax", "pre_tax", "excludes_returns").
    # Used for human review and for plan-rendering disambiguation; the planner
    # does not interpret it.
    basis: str
    # SQL expression that produces the column under this definition. The
    # planner uses string-equality against existing column lineage to detect
    # whether an existing column already satisfies the new definition (the
    # consumer-only plan shape).
    expr: str


@dataclass(frozen=True)
class DefinitionalChange:
    id: str
    intent: str
    target_node: str
    target_column: str
    old_definition: Definition
    new_definition: Definition
    # Consumer artifact ids whose read of (target_node, target_column) has
    # semantic dependency on the change. Per `PLANNER_NOTES.md` (decision c):
    # plans satisfy this requirement only if every must_migrate consumer's
    # read either flows from the new definition upstream or is repointed via
    # an `update_consumer` op.
    must_migrate: tuple[str, ...]
    allow_stale_consumers: bool


@dataclass(frozen=True)
class Contract:
    # Synthetic id, e.g. "C_executive_overview__customers__lifetime_spend".
    # Stable across runs so plan diffs are readable.
    id: str
    consumer_artifact: str
    node: str
    column: str
    tier: str
