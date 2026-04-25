from dataclasses import dataclass, field
from pathlib import Path

from dagwright.loaders import load_consumer_graph, load_manifest, load_spec
from dagwright.state import (
    ConsumerGraph,
    DagState,
    MeasureColumn,
    MetricRequest,
    Node,
    OutputShape,
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

    required_source_columns = _required_source_columns(spec.output_shape)

    for parent_name in sorted(dag.nodes):
        node = dag.nodes[parent_name]
        if node.layer not in ELIGIBLE_PARENT_LAYERS:
            continue

        # All structured measure columns must have their source column
        # present in the parent's declared schema (strict mode; see
        # PLANNER_NOTES.md boundary #4). expr-form columns are skipped
        # — the planner does not parse SQL.
        missing = [c for c in required_source_columns if c not in node.schema]
        if missing:
            rejections.append(
                Rejection(
                    candidate_parent=parent_name,
                    candidate_source_columns=[],
                    reason=(
                        f"declared schema lacks measure column(s) {missing}"
                    ),
                )
            )
            continue

        resolution_combos = enumerate_grain_resolutions(node, spec.output_shape.grain.keys)
        if not resolution_combos:
            rejections.append(
                Rejection(
                    candidate_parent=parent_name,
                    candidate_source_columns=[],
                    reason=(
                        f"grain {list(spec.output_shape.grain.keys)} not resolvable from "
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


def _required_source_columns(output_shape: OutputShape) -> list[str]:
    """Source columns the parent must expose for every structured
    measure column. expr columns are opaque (escape hatch) and don't
    contribute requirements."""
    needed: list[str] = []
    for c in output_shape.columns:
        if c.is_structured and c.column:
            needed.append(c.column)
    return needed


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
    grain_keys = list(spec.output_shape.grain.keys)
    column_names = [c.name for c in spec.output_shape.columns]
    new_schema = grain_keys + column_names

    column_lineage: dict[str, str] = {}
    for gr in grain_resolution:
        if gr.via == "direct":
            column_lineage[gr.grain_key] = f"{parent_name}.{gr.expr}"
        else:
            # Replace the bare source column with parent-qualified form.
            column_lineage[gr.grain_key] = gr.expr.replace(
                gr.source_column, f"{parent_name}.{gr.source_column}"
            )
    for c in spec.output_shape.columns:
        column_lineage[c.name] = _column_expr(c, parent_name)

    operations: list[Operation] = [
        Operation(
            op="add_node",
            args={
                "name": new_node_name,
                "layer": "MART",
                "grain": grain_keys,
                "schema": new_schema,
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
                "terms": {"columns": new_schema},
                "tier": spec.contract_tier,
            },
        ),
        Operation(
            op="add_contract",
            args={
                "node": new_node_name,
                "consumer": spec.consumer.artifact,
                "contract_id": "C2",
                "terms": {"grain_entity": "_".join(grain_keys)},
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
    # If any grain key was declared dense in the spec, the planner does
    # not yet emit a date-spine companion. PLANNER_NOTES.md "Planned
    # widenings" tracks this; surfacing it as a note keeps the human
    # reviewer honest until phase B lands.
    dense_keys = [k for k, cov in spec.output_shape.grain.coverage.items() if cov.dense]
    if dense_keys:
        notes.append(
            f"Spec requests dense coverage on {dense_keys}, but this plan "
            f"uses raw GROUP BY without a date-spine companion node — months "
            f"with no underlying events will be absent from the result. "
            f"See PLANNER_NOTES.md (planned widening: multi-parent "
            f"enumeration + scaffolding-node generation)."
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


def _column_expr(column: MeasureColumn, parent_name: str) -> str:
    """Render the SQL expression that produces this measure column.
    Structured columns get qualified with the parent name; expr
    columns are passed through verbatim (the AE wrote arbitrary SQL
    and the planner does not parse it)."""
    if column.expr:
        return column.expr
    return f"{column.aggregation}({parent_name}.{column.column})"


def check_invariants(
    parent: Node, grain_resolution: list[GrainResolution]
) -> list[InvariantCheck]:
    """v0 covers only the invariants engaged by adding a new MART
    downstream of an existing eligible parent. Other invariants from
    catalog/invariants.yaml are not engaged by this plan shape and
    are reported as "not engaged" in render."""
    return [
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
    grain_keys = list(spec.output_shape.grain.keys)
    if not derived:
        return (
            f"{spec.name} computed directly from {parent.name} grouped by "
            f"{', '.join(grain_keys)}."
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
