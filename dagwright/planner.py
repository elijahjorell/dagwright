from dataclasses import dataclass, field
from pathlib import Path

from dagwright.loaders import load_consumer_graph, load_manifest, load_spec
from dagwright.state import (
    ConsumerGraph,
    DagState,
    MetricRequest,
    Node,
)

# Time grains the planner knows how to derive from a date-like column.
TIME_GRAINS = ("day", "week", "month", "quarter", "year")

# Column-name substrings that suggest a date/timestamp.
DATE_NAME_HINTS = ("date", "time", "timestamp", "_at", "_on")

# Description keywords that suggest a date/timestamp when the column
# name is uninformative (e.g. jaffle_shop's `first_order`).
DATE_DESC_HINTS = ("date", "timestamp", "datetime")

# Layers a parent of a new MART may live in. SOURCE and EXPOSURE are
# excluded by the layer transition matrix.
ELIGIBLE_PARENT_LAYERS = ("STAGING", "INTERMEDIATE", "MART")


@dataclass
class GrainResolution:
    grain_key: str
    via: str  # "direct" or "derived"
    source_column: str
    expr: str


@dataclass
class Operation:
    op: str
    args: dict


@dataclass
class InvariantCheck:
    id: str
    holds: bool
    note: str


@dataclass
class Plan:
    parent: str
    grain_resolutions: list[GrainResolution]
    operations: list[Operation]
    invariants: list[InvariantCheck]
    pathways_reused: list[str]
    new_construction: list[str]
    blast_radius: dict
    effort: int
    score: float
    semantic_summary: str
    notes: list[str] = field(default_factory=list)


@dataclass
class Rejection:
    candidate_parent: str
    candidate_source_columns: list[str]
    reason: str


def plan_command(args) -> int:
    dag = load_manifest(args.manifest)
    cg = (
        load_consumer_graph(args.bi)
        if args.bi
        else ConsumerGraph(tool="", artifacts={})
    )
    spec = load_spec(args.spec)

    plans, rejections = plan_metric_request(dag, cg, spec)
    plans.sort(key=lambda p: -p.score)
    plans = plans[: args.top]

    if args.format in ("json", "both"):
        from dagwright.output import render_json

        print(render_json(spec, plans, rejections))
    if args.format == "both":
        print()
    if args.format in ("markdown", "both"):
        from dagwright.output import render_markdown

        print(render_markdown(spec, plans, rejections))

    return 0 if plans else 2


def plan_metric_request(
    dag: DagState, cg: ConsumerGraph, spec: MetricRequest
) -> tuple[list[Plan], list[Rejection]]:
    plans: list[Plan] = []
    rejections: list[Rejection] = []

    for parent_name in sorted(dag.nodes):
        node = dag.nodes[parent_name]
        if node.layer not in ELIGIBLE_PARENT_LAYERS:
            continue

        # Measure column must be present in the parent's declared
        # schema (strict mode — see CHARTER + REGENERATE notes).
        if spec.measure.column and spec.measure.column not in node.schema:
            rejections.append(
                Rejection(
                    candidate_parent=parent_name,
                    candidate_source_columns=[],
                    reason=(
                        f"declared schema lacks measure column "
                        f"{spec.measure.column!r}"
                    ),
                )
            )
            continue

        # For an `expr` measure the planner cannot inspect column
        # lineage without parsing SQL; v0 trusts the AE that the expr
        # is satisfiable by the parent's columns.

        resolution_combos = enumerate_grain_resolutions(node, spec.grain)
        if not resolution_combos:
            rejections.append(
                Rejection(
                    candidate_parent=parent_name,
                    candidate_source_columns=[],
                    reason=(
                        f"grain {list(spec.grain)} not resolvable from "
                        "declared schema (no direct match, no derivable "
                        "date column for time grain)"
                    ),
                )
            )
            continue

        for combo in resolution_combos:
            plan = build_plan(dag, cg, spec, parent_name, combo)
            plans.append(plan)

    return plans, rejections


def enumerate_grain_resolutions(
    node: Node, grain_keys: tuple[str, ...]
) -> list[list[GrainResolution]]:
    """For each grain key, list every viable resolution; return the
    cross-product so each combo is one candidate plan.

    A grain key resolves either:
      - direct: column with the same name exists on the parent.
      - derived: the key is a recognized time grain (month, week, ...)
        and the parent has at least one date-like column from which
        date_trunc(grain, col) yields the key.
    """
    per_key: list[list[GrainResolution]] = []
    for key in grain_keys:
        options: list[GrainResolution] = []
        if key in node.schema:
            options.append(
                GrainResolution(
                    grain_key=key, via="direct", source_column=key, expr=key
                )
            )
        if key in TIME_GRAINS:
            for col in node.schema:
                if is_date_like(col, node.column_descriptions.get(col, "")):
                    options.append(
                        GrainResolution(
                            grain_key=key,
                            via="derived",
                            source_column=col,
                            expr=f"date_trunc('{key}', {col})",
                        )
                    )
        if not options:
            return []
        per_key.append(options)

    # Cartesian product.
    combos: list[list[GrainResolution]] = [[]]
    for opts in per_key:
        combos = [c + [o] for c in combos for o in opts]
    return combos


def is_date_like(column_name: str, description: str) -> bool:
    name = column_name.lower()
    desc = description.lower()
    if any(h in name for h in DATE_NAME_HINTS):
        return True
    if any(h in desc for h in DATE_DESC_HINTS):
        return True
    return False


def build_plan(
    dag: DagState,
    cg: ConsumerGraph,
    spec: MetricRequest,
    parent_name: str,
    grain_resolution: list[GrainResolution],
) -> Plan:
    parent = dag.nodes[parent_name]
    new_node_name = spec.name
    result_column = spec.name
    new_schema = tuple(list(spec.grain) + [result_column])

    # Structured-form measures get qualified with the parent name so
    # the column_lineage is unambiguous when rendered. Expr-form
    # measures are passed through verbatim — the AE wrote arbitrary
    # SQL and the planner does not parse it.
    measure_expr = (
        spec.measure.expr
        if spec.measure.expr
        else f"{spec.measure.aggregation}({parent_name}.{spec.measure.column})"
    )

    column_lineage = {
        gr.grain_key: f"{parent_name}.{gr.expr}" if gr.via == "direct"
        else f"{gr.expr.replace(gr.source_column, parent_name + '.' + gr.source_column)}"
        for gr in grain_resolution
    }
    column_lineage[result_column] = measure_expr

    operations: list[Operation] = [
        Operation(
            op="add_node",
            args={
                "name": new_node_name,
                "layer": "MART",
                "grain": list(spec.grain),
                "schema": list(new_schema),
                "materialization": "table",
            },
        ),
        Operation(
            op="add_edge",
            args={
                "parent": parent_name,
                "child": new_node_name,
                "transform_type": "aggregation",
                "column_lineage": column_lineage,
                "cardinality": "many_to_one",
                "filters": list(spec.filters),
            },
        ),
        Operation(
            op="add_contract",
            args={
                "node": new_node_name,
                "consumer": spec.consumer.artifact,
                "contract_id": "C1",
                "terms": {"columns": list(new_schema)},
                "tier": spec.contract_tier,
            },
        ),
        Operation(
            op="add_contract",
            args={
                "node": new_node_name,
                "consumer": spec.consumer.artifact,
                "contract_id": "C2",
                "terms": {"grain_entity": "_".join(spec.grain)},
                "tier": spec.contract_tier,
            },
        ),
    ]

    invariants = check_invariants(parent, grain_resolution)

    pathways_reused = [parent_name] + sorted(dag.ancestors(parent_name))
    new_construction = [new_node_name]

    blast = compute_blast_radius(dag, cg, parent_name, spec.consumer.artifact)

    effort = len(operations) + len(new_construction)

    score = score_plan(parent, grain_resolution, blast, effort)

    semantic_summary = describe_semantics(spec, parent, grain_resolution)

    notes: list[str] = []
    if any(gr.via == "derived" for gr in grain_resolution):
        derived = [gr for gr in grain_resolution if gr.via == "derived"]
        notes.append(
            "Time grain"
            + ("s " if len(derived) > 1 else " ")
            + ", ".join(f"{gr.grain_key} (from {parent_name}.{gr.source_column})" for gr in derived)
            + " — verify the source column is the intended one for this metric."
        )
    if blast["new_artifact"] not in [a.id for a in cg.artifacts.values()]:
        notes.append(
            f"Consumer {spec.consumer.artifact!r} is not yet in the "
            f"{cg.tool or 'BI'} consumer graph; the plan assumes it will be "
            "created externally as part of execution."
        )

    return Plan(
        parent=parent_name,
        grain_resolutions=grain_resolution,
        operations=operations,
        invariants=invariants,
        pathways_reused=pathways_reused,
        new_construction=new_construction,
        blast_radius=blast,
        effort=effort,
        score=score,
        semantic_summary=semantic_summary,
        notes=notes,
    )


def check_invariants(
    parent: Node, grain_resolution: list[GrainResolution]
) -> list[InvariantCheck]:
    """v0 covers only the invariants engaged by adding a new MART
    downstream of an existing eligible parent. Other invariants from
    catalog/invariants.yaml are not engaged by this plan shape and
    are reported as "not engaged" in render."""
    checks = [
        InvariantCheck(
            id="I1",
            holds=True,
            note="No cycle: new node has no children.",
        ),
        InvariantCheck(
            id="I2",
            holds=True,
            note=(
                "Grain reachable via "
                + ", ".join(
                    f"{gr.via} ({gr.source_column})" for gr in grain_resolution
                )
                + (
                    "; coarsen morphism with derived-grain extension."
                    if any(gr.via == "derived" for gr in grain_resolution)
                    else "; coarsen morphism."
                )
            ),
        ),
        InvariantCheck(
            id="I3",
            holds=True,
            note="All referenced columns exist on the parent's declared schema.",
        ),
        InvariantCheck(
            id="I5",
            holds=True,
            note=f"{parent.layer} -> MART permitted by edge transitions.",
        ),
        InvariantCheck(
            id="E2",
            holds=True,
            note="Cardinality many_to_one consistent with coarsen morphism.",
        ),
        InvariantCheck(
            id="E3",
            holds=True,
            note=f"Transform 'aggregation' valid for {parent.layer} -> MART.",
        ),
    ]
    return checks


def compute_blast_radius(
    dag: DagState,
    cg: ConsumerGraph,
    parent_name: str,
    consumer_artifact: str,
) -> dict:
    """Adding a new mart downstream of an existing parent does not
    modify the parent's schema. Therefore no existing artifact's
    contract is at risk for this plan shape. The dict still includes
    the parent's existing consumers as informational context."""
    parent_consumers = [a.id for a in cg.artifacts_consuming(parent_name)]
    return {
        "existing_artifacts_affected": [],
        "parent_consumers_unchanged": parent_consumers,
        "new_artifact": consumer_artifact,
    }


def score_plan(
    parent: Node,
    grain_resolution: list[GrainResolution],
    blast: dict,
    effort: int,
) -> float:
    """Higher = better. Pure heuristic; document each component.

    +10 base
    +5 if parent is at MART (closer to consumer, less translation)
    +3 if parent is at INTERMEDIATE
    +1 if parent is at STAGING
    -1 per derived grain (carries semantic ambiguity, surfaces a note)
    -1 per existing artifact at risk (zero for v0 add-only plans)
    -0.1 * effort (mild preference for fewer operations)
    """
    base = 10.0
    layer_bonus = {"MART": 5.0, "INTERMEDIATE": 3.0, "STAGING": 1.0}.get(parent.layer, 0.0)
    derived_penalty = sum(1.0 for gr in grain_resolution if gr.via == "derived")
    risk_penalty = float(len(blast["existing_artifacts_affected"]))
    effort_penalty = 0.1 * effort
    return base + layer_bonus - derived_penalty - risk_penalty - effort_penalty


def describe_semantics(
    spec: MetricRequest,
    parent: Node,
    grain_resolution: list[GrainResolution],
) -> str:
    """A short prose paraphrase of what this plan would actually
    compute. Distinct plans differ in which source column drives the
    grain, which often changes the metric's meaning entirely."""
    derived = [gr for gr in grain_resolution if gr.via == "derived"]
    if not derived:
        return (
            f"{spec.name} computed directly from {parent.name} grouped by "
            f"{', '.join(spec.grain)}."
        )
    parts = [
        f"{gr.grain_key} = date_trunc('{gr.grain_key}', {parent.name}.{gr.source_column})"
        for gr in derived
    ]
    return (
        f"{spec.name} computed from {parent.name} with "
        + "; ".join(parts)
        + ". The choice of source column determines what the metric counts."
    )
