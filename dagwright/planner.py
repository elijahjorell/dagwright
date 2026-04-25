import re
from dataclasses import dataclass, field
from pathlib import Path

from dagwright.loaders import (
    load_consumer_graph,
    load_exposures_as_consumer_graph,
    load_manifest,
    load_spec,
)
from dagwright.state import (
    ConsumerGraph,
    Contract,
    DagState,
    DefinitionalChange,
    MeasureColumn,
    MetricRequest,
    Node,
    OutputShape,
)

# Time grains the planner knows how to derive from a date-like column.
TIME_GRAINS = ("day", "week", "month", "quarter", "year")

# Whole `_`-delimited tokens in a column name that mark it date-like.
# Token-equal (not substring) so `lifetime` / `runtime` / `datetime_col`
# don't false-positive via the `time` substring.
DATE_NAME_TOKENS = frozenset({"date", "time", "timestamp", "datetime"})

# Column-name suffixes that mark it date-like — the convention
# `<verb>ed_at` / `<noun>_on` is strong enough on its own. Suffix
# (not substring) so `meta_attribute` doesn't false-positive.
DATE_NAME_SUFFIXES = ("_at", "_on")

# Case-sensitive description match (word-boundary). Capitalized forms
# appear when the description STARTS with the temporal word ("Date of
# the customer's first order"). Lowercase mid-sentence mentions
# ("...to date.", "out of date") are typically idiomatic and excluded.
DATE_DESC_RE = re.compile(r"\b(Date|Timestamp|Datetime)\b")

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
    if args.bi:
        cg = load_consumer_graph(args.bi)
    else:
        # Fall back to dbt exposures embedded in the manifest. Empty
        # graph if no exposures declared.
        cg = load_exposures_as_consumer_graph(args.manifest)
    spec = load_spec(args.spec)

    if isinstance(spec, MetricRequest):
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

    if isinstance(spec, DefinitionalChange):
        dc_plans, rejections = plan_definitional_change(dag, cg, spec)
        dc_plans.sort(key=lambda p: -p.score)
        dc_plans = dc_plans[: args.top]
        if args.format in ("json", "both"):
            from dagwright.output import render_json_definitional_change
            print(render_json_definitional_change(spec, dc_plans, rejections))
        if args.format == "both":
            print()
        if args.format in ("markdown", "both"):
            from dagwright.output import render_markdown_definitional_change
            print(render_markdown_definitional_change(spec, dc_plans, rejections))
        return 0 if dc_plans else 2

    raise NotImplementedError(
        f"plan_command does not handle spec kind: {type(spec).__name__}"
    )


def plan_metric_request(
    dag: DagState, cg: ConsumerGraph, spec: MetricRequest
) -> tuple[list[Plan], list[Rejection]]:
    plans: list[Plan] = []
    rejections: list[Rejection] = []

    required_source_columns = _required_source_columns(spec.output_shape)
    dense_keys = _dense_grain_keys(spec)

    if len(dense_keys) > 1:
        # v0 supports a single dense key. The current fixture exercises
        # this and PLANNER_NOTES tracks the multi-spine widening.
        raise NotImplementedError(
            f"v0 supports at most one dense grain key; got {dense_keys}. "
            "Multi-spine support is a planned widening (PLANNER_NOTES.md)."
        )

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
            if dense_keys:
                plan = build_dense_plan(dag, cg, spec, parent_name, combo, dense_keys[0])
            else:
                plan = build_plan(dag, cg, spec, parent_name, combo)
            plans.append(plan)

    return plans, rejections


def _dense_grain_keys(spec: MetricRequest) -> list[str]:
    """Grain keys whose coverage is declared dense. Returned in
    grain.keys order so the spine name is deterministic."""
    coverage = spec.output_shape.grain.coverage
    return [k for k in spec.output_shape.grain.keys if k in coverage and coverage[k].dense]


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
    if any(t in DATE_NAME_TOKENS for t in name.split("_")):
        return True
    if any(name.endswith(s) for s in DATE_NAME_SUFFIXES):
        return True
    if DATE_DESC_RE.search(description):
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


def build_dense_plan(
    dag: DagState,
    cg: ConsumerGraph,
    spec: MetricRequest,
    parent_name: str,
    grain_resolution: list[GrainResolution],
    dense_key: str,
) -> Plan:
    """Like build_plan but emits a date-spine companion node and a
    LEFT JOIN edge so that every value of `dense_key` in the
    declared range appears as a row, even when no underlying event
    matches. The aggregation edge carries a `join_to_spine` arg
    capturing the spine column → source-derivation pairing and the
    fill values for missing buckets."""
    parent = dag.nodes[parent_name]
    new_node_name = spec.name
    grain_keys = list(spec.output_shape.grain.keys)
    column_names = [c.name for c in spec.output_shape.columns]
    new_schema = grain_keys + column_names
    coverage = spec.output_shape.grain.coverage[dense_key]

    spine_name = f"date_spine_{dense_key}"

    # Locate the grain resolution for the dense key so we know which
    # parent column to join the spine against.
    dense_resolution = next(gr for gr in grain_resolution if gr.grain_key == dense_key)
    if dense_resolution.via == "direct":
        join_source_expr = f"{parent_name}.{dense_key}"
    else:
        join_source_expr = (
            f"date_trunc('{dense_key}', {parent_name}.{dense_resolution.source_column})"
        )

    fill_values = {c.name: coverage.fill for c in spec.output_shape.columns if coverage.fill is not None}

    # Lineage on the aggregation edge: only the measure columns. The
    # grain key on the result comes from the spine via the LEFT JOIN.
    agg_lineage = {c.name: _column_expr(c, parent_name) for c in spec.output_shape.columns}
    spine_lineage = {dense_key: f"{spine_name}.{dense_key}"}
    # Non-dense grain keys (entity grains) come from the aggregation
    # source via column_lineage just like in the sparse path.
    for gr in grain_resolution:
        if gr.grain_key == dense_key:
            continue
        if gr.via == "direct":
            agg_lineage[gr.grain_key] = f"{parent_name}.{gr.expr}"
        else:
            agg_lineage[gr.grain_key] = gr.expr.replace(
                gr.source_column, f"{parent_name}.{gr.source_column}"
            )

    operations: list[Operation] = [
        Operation(
            op="add_node",
            args={
                "name": spine_name,
                "layer": "INTERMEDIATE",
                "grain": [dense_key],
                "schema": [dense_key],
                "materialization": "view",
                "range_from": coverage.range.from_,
                "range_to": coverage.range.to,
                "note": (
                    f"date_spine generator: one row per {dense_key} from "
                    f"{coverage.range.from_} to {coverage.range.to}. "
                    "Implemented in dbt via dbt_utils.date_spine or equivalent."
                ),
            },
        ),
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
                "parent": spine_name,
                "child": new_node_name,
                "transform_type": "left_join_axis",
                "column_lineage": spine_lineage,
                "cardinality": "one_to_one",
                "filters": [],
            },
        ),
        Operation(
            op="add_edge",
            args={
                "parent": parent_name,
                "child": new_node_name,
                "transform_type": "aggregation",
                "column_lineage": agg_lineage,
                "cardinality": "many_to_one",
                "filters": list(spec.filters),
                "join_to_spine": {
                    "spine": spine_name,
                    "on": {f"{spine_name}.{dense_key}": join_source_expr},
                    "fill": fill_values,
                },
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
    invariants.append(
        InvariantCheck(
            id="I5b",
            holds=True,
            note=f"INTERMEDIATE -> MART permitted by edge transitions ({spine_name} -> {new_node_name}).",
        )
    )

    pathways_reused = [parent_name] + sorted(dag.ancestors(parent_name))
    # Convention: target mart first, scaffolding nodes after. The
    # operations list still emits scaffolding first (execution order);
    # this list is for display ("what's new" — target leads).
    new_construction = [new_node_name, spine_name]

    blast = compute_blast_radius(dag, cg, parent_name, spec.consumer.artifact)

    effort = len(operations) + len(new_construction)

    score = score_plan(parent, grain_resolution, blast, effort)

    semantic_summary = (
        f"{spec.name} computed from {parent.name}, joined LEFT against a "
        f"date_spine on {dense_key} so every {dense_key} from "
        f"{coverage.range.from_} to {coverage.range.to} appears as a row "
        f"(missing buckets fill {column_names} with "
        f"{coverage.fill if coverage.fill is not None else 'null'})."
    )

    notes: list[str] = []
    if any(gr.via == "derived" for gr in grain_resolution):
        derived = [gr for gr in grain_resolution if gr.via == "derived"]
        notes.append(
            "Time grain"
            + ("s " if len(derived) > 1 else " ")
            + ", ".join(f"{gr.grain_key} (from {parent_name}.{gr.source_column})" for gr in derived)
            + " — verify the source column is the intended one for this metric."
        )
    notes.append(
        f"If a date spine for `{dense_key}` already exists in your project, "
        f"skip the first add_node operation and reference the existing spine "
        f"in the LEFT JOIN edge."
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


# -----------------------------------------------------------------------------
# definitional_change planning
# -----------------------------------------------------------------------------


@dataclass
class ContractStatus:
    contract_id: str
    consumer_artifact: str
    node: str
    column: str
    held: bool
    note: str


@dataclass
class DefinitionalChangePlan:
    shape: str  # replace_in_place | add_versioned_column | versioned_mart | consumer_only
    operations: list[Operation]
    contract_status: list[ContractStatus]
    blast_radius: dict
    effort: int
    score: float
    semantic_summary: str
    notes: list[str] = field(default_factory=list)


def plan_definitional_change(
    dag: DagState, cg: ConsumerGraph, spec: DefinitionalChange
) -> tuple[list[DefinitionalChangePlan], list[Rejection]]:
    """Enumerate plans satisfying a definitional change. Plan shapes:

    - replace_in_place: redefine the column on the target node.
    - add_versioned_column: add a new column with the new definition;
      old column preserved; must_migrate consumers repointed.
    - versioned_mart: build a parallel `<target>_v2` carrying the new
      definition; original retained.
    - consumer_only: when an existing column on the target node
      already carries the new definition, only consumer reads change.
    """
    plans: list[DefinitionalChangePlan] = []
    rejections: list[Rejection] = []

    target_node = dag.nodes.get(spec.target_node)
    if target_node is None:
        rejections.append(Rejection(
            candidate_parent=spec.target_node,
            candidate_source_columns=[],
            reason=f"target node {spec.target_node!r} not found in manifest",
        ))
        return plans, rejections

    if spec.target_column not in target_node.schema:
        rejections.append(Rejection(
            candidate_parent=spec.target_node,
            candidate_source_columns=[],
            reason=(
                f"target column {spec.target_column!r} not in declared "
                f"schema of {spec.target_node!r}"
            ),
        ))
        return plans, rejections

    contracts = derive_contracts(cg, [spec.target_node])

    plans.append(_plan_replace_in_place(spec, dag, cg, target_node, contracts))
    plans.append(_plan_add_versioned_column(spec, dag, cg, target_node, contracts))
    plans.append(_plan_versioned_mart(spec, dag, cg, target_node, contracts))

    consumer_only = _plan_consumer_only(spec, dag, cg, target_node, contracts)
    if consumer_only is not None:
        plans.append(consumer_only)

    return plans, rejections


def derive_contracts(cg: ConsumerGraph, in_scope_nodes: list[str]) -> list[Contract]:
    """Materialize Contracts for in-scope nodes. When the consumer
    declares column-level reads, emit one Contract per column. When
    the consumer declares only model-level reads (e.g. dbt exposures
    with empty columns tuple), emit a single Contract with column='*'
    indicating "any column on this node." The contract evaluator
    handles '*' as a model-level coarsening."""
    out: list[Contract] = []
    in_scope = set(in_scope_nodes)
    for artifact in cg.artifacts.values():
        for c in artifact.consumes:
            if c.node not in in_scope:
                continue
            if c.columns:
                for col in c.columns:
                    cid = f"C_{artifact.id}__{c.node}__{col}"
                    out.append(Contract(
                        id=cid, consumer_artifact=artifact.id,
                        node=c.node, column=col, tier="hard",
                    ))
            else:
                cid = f"C_{artifact.id}__{c.node}__*"
                out.append(Contract(
                    id=cid, consumer_artifact=artifact.id,
                    node=c.node, column="*", tier="hard",
                ))
    return out


def _evaluate_contracts(
    spec: DefinitionalChange,
    contracts: list[Contract],
    repoints: list[tuple[str, str, str]],
    redefines_in_place: bool,
) -> list[ContractStatus]:
    """One ContractStatus per derived contract. Implements decision (c):
    must_migrate consumers' contracts hold iff their read of the
    target column is either repointed by an `update_consumer` op or
    flows through an in-place upstream redefinition. Non-must_migrate
    consumers' column-level reads always hold; in-place redefinition
    surfaces a SEMANTIC RISK note for them."""
    must_migrate = set(spec.must_migrate)
    repointed = {(a, fc) for (a, fc, _) in repoints}

    out: list[ContractStatus] = []
    for k in contracts:
        # In scope iff the contract is on the target node AND either
        # the column matches or the contract is model-level ('*').
        in_target_node = k.node == spec.target_node
        column_in_scope = k.column == spec.target_column or k.column == "*"
        if not (in_target_node and column_in_scope):
            out.append(ContractStatus(
                contract_id=k.id,
                consumer_artifact=k.consumer_artifact,
                node=k.node,
                column=k.column,
                held=True,
                note="outside change scope",
            ))
            continue

        # Model-level contract: consumer reads this node but column-
        # level deps unspecified. We can't tell whether their read
        # includes the changing column, so we never mark these
        # held=False (would create false alarms); instead surface a
        # verbose warning the AE can act on.
        if k.column == "*":
            if redefines_in_place:
                note = (
                    "MODEL-LEVEL dependency; verify whether the consumer's "
                    f"reads of {k.node} include {spec.target_column!r} — "
                    "if so, their values change with the new definition"
                )
            elif k.consumer_artifact in must_migrate:
                note = (
                    "MODEL-LEVEL dependency; consumer is in must_migrate but "
                    "their column-level reads are unknown — verify whether "
                    f"{spec.target_column!r} is among them and the plan "
                    "covers it"
                )
            else:
                note = (
                    "MODEL-LEVEL dependency; consumer reads "
                    f"{k.node} but not flagged for migration; old definition "
                    "preserved at the original column name"
                )
            held = True
        elif k.consumer_artifact in must_migrate:
            if (k.consumer_artifact, k.column) in repointed:
                note = "must_migrate consumer repointed by an update_consumer op"
                held = True
            elif redefines_in_place:
                note = (
                    "must_migrate consumer reads the redefined column; "
                    "new definition flows through"
                )
                held = True
            else:
                note = (
                    "must_migrate consumer's read still points to the old "
                    "definition; plan does not satisfy the change for this consumer"
                )
                held = False
        else:
            if redefines_in_place:
                note = (
                    "column-level read holds; SEMANTIC RISK — meaning of "
                    "column changes silently for this consumer (not in must_migrate)"
                )
            else:
                note = (
                    "column-level read holds; old definition preserved at the "
                    "original column name"
                )
            held = True

        out.append(ContractStatus(
            contract_id=k.id,
            consumer_artifact=k.consumer_artifact,
            node=k.node,
            column=k.column,
            held=held,
            note=note,
        ))
    return out


def _plan_replace_in_place(
    spec: DefinitionalChange,
    dag: DagState,
    cg: ConsumerGraph,
    target_node: Node,
    contracts: list[Contract],
) -> DefinitionalChangePlan:
    operations = [
        Operation(
            op="modify_node",
            args={
                "name": spec.target_node,
                "properties": {
                    "column_definitions": {
                        spec.target_column: spec.new_definition.expr,
                    },
                },
            },
        ),
    ]
    contract_status = _evaluate_contracts(spec, contracts, repoints=[], redefines_in_place=True)

    silent_consumers = sorted({
        cs.consumer_artifact for cs in contract_status
        if cs.node == spec.target_node
        and cs.column == spec.target_column
        and "SEMANTIC RISK" in cs.note
    })
    downstream_models = sorted(dag.descendants(spec.target_node))
    blast = {
        "scheme": "redefine column in place; meaning changes for all readers",
        "must_migrate_satisfied": list(spec.must_migrate),
        "existing_artifacts_affected": silent_consumers,
        # Internal dbt propagation: every descendant model of the
        # target_node may compute different values after this plan
        # because target_column's meaning has changed. The planner
        # cannot know without SQL inspection which descendants
        # actually reference target_column — so the AE must audit.
        "downstream_dbt_models": downstream_models,
    }
    notes: list[str] = []
    if silent_consumers:
        notes.append(
            f"Consumer(s) {silent_consumers} read this column but are not in "
            "must_migrate. Their numbers change silently. Confirm intent before "
            "executing."
        )
    if downstream_models:
        notes.append(
            f"{len(downstream_models)} downstream dbt model(s) depend on "
            f"`{spec.target_node}` and may compute different values after "
            "this plan. The planner does not parse SQL — audit each "
            f"descendant for whether it references `{spec.target_column}` "
            "before merging."
        )
    return DefinitionalChangePlan(
        shape="replace_in_place",
        operations=operations,
        contract_status=contract_status,
        blast_radius=blast,
        effort=len(operations),
        score=_score_change_plan("replace_in_place", operations, contract_status, blast),
        semantic_summary=(
            f"Redefine `{spec.target_node}.{spec.target_column}` from "
            f"`{spec.old_definition.expr}` ({spec.old_definition.basis}) to "
            f"`{spec.new_definition.expr}` ({spec.new_definition.basis}). "
            "All consumers see the new value at the same column name."
        ),
        notes=notes,
    )


def _plan_add_versioned_column(
    spec: DefinitionalChange,
    dag: DagState,
    cg: ConsumerGraph,
    target_node: Node,
    contracts: list[Contract],
) -> DefinitionalChangePlan:
    new_col = f"{spec.target_column}_v2"
    operations: list[Operation] = [
        Operation(
            op="modify_node",
            args={
                "name": spec.target_node,
                "properties": {
                    "schema_add": [new_col],
                    "column_definitions": {new_col: spec.new_definition.expr},
                },
            },
        ),
    ]
    repoints: list[tuple[str, str, str]] = []
    for artifact_id in spec.must_migrate:
        operations.append(Operation(
            op="update_consumer",
            args={
                "artifact": artifact_id,
                "from_read": {"node": spec.target_node, "column": spec.target_column},
                "to_read": {"node": spec.target_node, "column": new_col},
            },
        ))
        repoints.append((artifact_id, spec.target_column, new_col))

    contract_status = _evaluate_contracts(
        spec, contracts, repoints=repoints, redefines_in_place=False
    )
    blast = {
        "scheme": (
            f"add new column `{new_col}` carrying the new definition; "
            "old column preserved"
        ),
        "must_migrate_satisfied": list(spec.must_migrate),
        "existing_artifacts_affected": [],
        # Adding a column doesn't change existing column semantics;
        # existing downstream models that read existing columns are
        # unaffected. The new column has no readers yet.
        "downstream_dbt_models": [],
    }
    notes = [
        f"Old column `{spec.target_column}` retains its current definition. "
        "Consumers not in must_migrate continue to read the old value."
    ]
    if new_col in target_node.schema:
        notes.append(
            f"WARNING: `{new_col}` already exists on `{spec.target_node}`. "
            "Pick a different name when executing."
        )
    return DefinitionalChangePlan(
        shape="add_versioned_column",
        operations=operations,
        contract_status=contract_status,
        blast_radius=blast,
        effort=len(operations),
        score=_score_change_plan("add_versioned_column", operations, contract_status, blast),
        semantic_summary=(
            f"Add new column `{spec.target_node}.{new_col}` carrying the "
            f"{spec.new_definition.basis} definition (`{spec.new_definition.expr}`). "
            f"Repoint must_migrate consumer(s) to the new column. Old column "
            f"`{spec.target_column}` is unchanged."
        ),
        notes=notes,
    )


def _plan_versioned_mart(
    spec: DefinitionalChange,
    dag: DagState,
    cg: ConsumerGraph,
    target_node: Node,
    contracts: list[Contract],
) -> DefinitionalChangePlan:
    new_node = f"{spec.target_node}_v2"
    parents = list(dag.parents_of(spec.target_node))
    operations: list[Operation] = [
        Operation(
            op="add_node",
            args={
                "name": new_node,
                "layer": target_node.layer,
                "grain": list(target_node.grain),
                "schema": list(target_node.schema),
                "materialization": target_node.materialization,
                "column_definitions": {
                    spec.target_column: spec.new_definition.expr,
                },
            },
        ),
    ]
    for parent in parents:
        operations.append(Operation(
            op="add_edge",
            args={
                "parent": parent,
                "child": new_node,
                "transform_type": "passthrough",
                "cardinality": "one_to_one",
                "filters": [],
            },
        ))

    repoints: list[tuple[str, str, str]] = []
    for artifact_id in spec.must_migrate:
        operations.append(Operation(
            op="update_consumer",
            args={
                "artifact": artifact_id,
                "from_read": {"node": spec.target_node, "column": spec.target_column},
                "to_read": {"node": new_node, "column": spec.target_column},
            },
        ))
        # Repoint key uses (artifact, from_column); the contract evaluator
        # only inspects column identity. Versioned-mart still satisfies
        # must_migrate because the consumer's reading is moved to a node
        # carrying the new definition.
        repoints.append((artifact_id, spec.target_column, spec.target_column))

    contract_status = _evaluate_contracts(
        spec, contracts, repoints=repoints, redefines_in_place=False
    )
    blast = {
        "scheme": (
            f"build parallel mart `{new_node}` with the new definition; "
            f"original `{spec.target_node}` retained for stale consumers"
        ),
        "must_migrate_satisfied": list(spec.must_migrate),
        "existing_artifacts_affected": [],
        # Original node is unchanged; existing downstream of
        # spec.target_node continues to compute the old definition.
        "downstream_dbt_models": [],
    }
    notes = [
        "Versioned-mart pattern. Heaviest plan; choose this when many "
        "consumers depend on the original node and migration must be staged. "
        f"Sets up an explicit deprecation path for `{spec.target_node}`."
    ]
    return DefinitionalChangePlan(
        shape="versioned_mart",
        operations=operations,
        contract_status=contract_status,
        blast_radius=blast,
        effort=len(operations),
        score=_score_change_plan("versioned_mart", operations, contract_status, blast),
        semantic_summary=(
            f"Build `{new_node}` as a parallel of `{spec.target_node}` with "
            f"`{spec.target_column}` redefined to `{spec.new_definition.expr}` "
            f"({spec.new_definition.basis}). Repoint must_migrate consumers; "
            f"original `{spec.target_node}` retained for any consumer staying "
            "on the old definition."
        ),
        notes=notes,
    )


def _plan_consumer_only(
    spec: DefinitionalChange,
    dag: DagState,
    cg: ConsumerGraph,
    target_node: Node,
    contracts: list[Contract],
) -> DefinitionalChangePlan | None:
    """Feasible when the new definition is a bare column reference to
    a column already on the target node."""
    expr = spec.new_definition.expr.strip()
    if expr not in target_node.schema:
        return None
    if expr == spec.target_column:
        return None  # trivial; new definition equals existing column

    operations: list[Operation] = []
    repoints: list[tuple[str, str, str]] = []
    for artifact_id in spec.must_migrate:
        operations.append(Operation(
            op="update_consumer",
            args={
                "artifact": artifact_id,
                "from_read": {"node": spec.target_node, "column": spec.target_column},
                "to_read": {"node": spec.target_node, "column": expr},
            },
        ))
        repoints.append((artifact_id, spec.target_column, expr))

    if not operations:
        return None

    contract_status = _evaluate_contracts(
        spec, contracts, repoints=repoints, redefines_in_place=False
    )
    blast = {
        "scheme": (
            f"existing column `{spec.target_node}.{expr}` already carries the "
            "new definition; no dbt change needed"
        ),
        "must_migrate_satisfied": list(spec.must_migrate),
        "existing_artifacts_affected": [],
        # No dbt change; existing downstream models are entirely
        # unaffected.
        "downstream_dbt_models": [],
    }
    notes = [
        f"Smallest possible plan. The {spec.new_definition.basis} basis is "
        f"already computed in `{spec.target_node}.{expr}`. Only the consumer's "
        "saved query / dashboard column reference needs to change."
    ]
    return DefinitionalChangePlan(
        shape="consumer_only",
        operations=operations,
        contract_status=contract_status,
        blast_radius=blast,
        effort=len(operations),
        score=_score_change_plan("consumer_only", operations, contract_status, blast),
        semantic_summary=(
            f"Repoint must_migrate consumer(s) to read "
            f"`{spec.target_node}.{expr}`, which already carries the "
            f"{spec.new_definition.basis} definition. No dbt model change."
        ),
        notes=notes,
    )


def _score_change_plan(
    shape: str,
    operations: list[Operation],
    contract_status: list[ContractStatus],
    blast_radius: dict,
) -> float:
    """Higher = better.

    +10 base
    -1 per operation (effort)
    -5 per held=False contract (a must_migrate consumer left on old definition)
    -2 per SEMANTIC RISK note (silent meaning change for non-must_migrate consumer)
    -0.3 per downstream dbt model (capped at -3.0): in-place plans
       silently propagate to every descendant, and each descendant
       is real audit work for the AE. Shapes that don't modify
       existing nodes (consumer_only, add_versioned_column,
       versioned_mart) report empty downstream_dbt_models and pay
       nothing — so any non-zero descendant count is enough to tip
       the ranking toward consumer_only when it's feasible.
    Shape preferences:
    +1 consumer_only (no dbt change is genuinely cheaper)
    +0.5 replace_in_place (single op, single-meaning end state)
    -0.5 versioned_mart (heaviest pattern)
    """
    base = 10.0
    effort_penalty = 1.0 * len(operations)
    violations = sum(1 for cs in contract_status if not cs.held)
    silent = sum(1 for cs in contract_status if "SEMANTIC RISK" in cs.note)
    downstream = blast_radius.get("downstream_dbt_models") or []
    downstream_penalty = min(3.0, 0.3 * len(downstream))
    shape_bonus = {
        "consumer_only": 1.0,
        "replace_in_place": 0.5,
        "add_versioned_column": 0.0,
        "versioned_mart": -0.5,
    }.get(shape, 0.0)
    return (
        base
        - effort_penalty
        - 5.0 * violations
        - 2.0 * silent
        - downstream_penalty
        + shape_bonus
    )
