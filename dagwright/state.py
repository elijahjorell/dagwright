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


@dataclass(frozen=True)
class Edge:
    parent: str
    child: str


@dataclass
class DagState:
    nodes: dict[str, Node]
    edges: tuple[Edge, ...]

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
