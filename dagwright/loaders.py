import json
import re
from collections import defaultdict
from pathlib import Path

import yaml

from dagwright.column_lineage import extract_lineage
from dagwright.state import (
    Artifact,
    Consumer,
    ConsumerGraph,
    Consumes,
    Coverage,
    CoverageRange,
    DagState,
    Definition,
    DefinitionalChange,
    Edge,
    Grain,
    MeasureColumn,
    MetricRequest,
    Node,
    OutputShape,
    TIME_LIKE_KEYS,
)

SLUG_RE = re.compile(r"^[a-z][a-z0-9_]*$")
ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
ALLOWED_AGGREGATIONS = {"sum", "count", "count_distinct", "avg", "min", "max"}
ALLOWED_TIERS = {"critical", "standard", "best_effort"}
SUPPORTED_BI_TOOLS = {"metabase"}
SYMBOLIC_RANGE_VALUES = {"earliest_event", "current_period"}


class SpecError(ValueError):
    pass


def load_manifest(path: Path) -> DagState:
    with open(path, encoding="utf-8") as f:
        manifest = json.load(f)

    raw_nodes = manifest.get("nodes", {})
    raw_sources = manifest.get("sources", {})

    # Pass 1: collect unique-test column hits per attached model.
    # dbt unique tests are the cleanest grain signal available without
    # parsing SQL or reading meta tags.
    unique_columns: dict[str, list[str]] = defaultdict(list)
    for key, node in raw_nodes.items():
        if node.get("resource_type") != "test":
            continue
        meta = node.get("test_metadata") or {}
        if meta.get("name") != "unique":
            continue
        attached = node.get("attached_node")
        col = node.get("column_name")
        if attached and col:
            unique_columns[attached].append(col)

    nodes: dict[str, Node] = {}
    edges: list[Edge] = []

    for key, n in raw_nodes.items():
        rt = n.get("resource_type")
        if rt not in {"model", "seed"}:
            continue
        name = n["name"]
        layer = _infer_layer(rt, n.get("original_file_path", ""))
        cols = n.get("columns", {})
        schema = tuple(cols.keys())
        descs = {c: (info.get("description") or "") for c, info in cols.items()}
        grain = tuple(unique_columns.get(key, []))
        materialization = (n.get("config") or {}).get("materialized") or rt

        nodes[name] = Node(
            name=name,
            layer=layer,
            grain=grain,
            schema=schema,
            materialization=materialization,
            column_descriptions=descs,
        )
        depends_on_keys = (n.get("depends_on") or {}).get("nodes", [])
        for parent_key in depends_on_keys:
            parent_name = _key_to_name(parent_key)
            if parent_name:
                edges.append(Edge(parent=parent_name, child=name))

    # dbt sources (separate from seeds). Treat as SOURCE layer.
    for key, s in raw_sources.items():
        name = s["name"]
        nodes[name] = Node(
            name=name,
            layer="SOURCE",
            grain=(),
            schema=tuple(s.get("columns", {}).keys()),
            materialization="source",
        )

    # Second pass: extract column-level lineage. Runs after all nodes
    # exist so each model can be told its upstream models' schemas
    # (sqlglot needs them to resolve JOIN-qualified columns and SELECT *
    # propagation). Sources and seeds have no SQL so nothing to extract.
    for key, n in raw_nodes.items():
        if n.get("resource_type") not in {"model", "seed"}:
            continue
        name = n["name"]
        if name not in nodes:
            continue
        raw_code = n.get("compiled_code") or n.get("raw_code") or ""
        if not raw_code:
            continue
        output_columns = list((n.get("columns") or {}).keys())
        upstream_names = [
            _key_to_name(k)
            for k in (n.get("depends_on") or {}).get("nodes", [])
            if k.startswith(("model.", "source.", "seed."))
        ]
        upstream_schemas = {
            upn: list(nodes[upn].schema) if upn in nodes else []
            for upn in upstream_names if upn
        }
        nodes[name].column_lineage = extract_lineage(
            raw_code, output_columns, upstream_schemas
        )

    column_synonyms = _build_column_synonyms(nodes, edges)

    return DagState(
        nodes=nodes, edges=tuple(edges), column_synonyms=column_synonyms
    )


def _build_column_synonyms(
    nodes: dict[str, Node],
    edges: list[Edge] | tuple[Edge, ...],
) -> dict[tuple[str, str], frozenset[tuple[str, str]]]:
    """Compute connected components over the column-lineage graph: every
    (node, col) reachable from another via aliasing ends up in the same
    component. Singletons (documented columns with no aliases) are
    included so the lookup never returns ``None``.

    Two sources of edges:

    1. **Explicit aliases** from each model's ``column_lineage``
       (regex-extracted ``<src> AS <dst>`` patterns).
    2. **Passthrough heuristic**: when a child node and one of its
       upstream parents both have the same column name in their
       declared schema, treat them as the same data. This covers the
       common pattern where a downstream model selects a column without
       aliasing it. Over-unions in the rare case where the same name
       means different things across an edge — acceptable v0 trade-off,
       documented in PLANNER_NOTES.md.
    """
    parent: dict[tuple[str, str], tuple[str, str]] = {}

    def find(x: tuple[str, str]) -> tuple[str, str]:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: tuple[str, str], b: tuple[str, str]) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    # Seed every documented column as its own component.
    for node_name, node in nodes.items():
        for col in node.schema:
            key = (node_name, col)
            parent.setdefault(key, key)

    # Edges from explicit aliases.
    for node_name, node in nodes.items():
        for output_col, refs in node.column_lineage.items():
            out_key = (node_name, output_col)
            parent.setdefault(out_key, out_key)
            for up_node, up_col in refs:
                up_key = (up_node, up_col)
                parent.setdefault(up_key, up_key)
                union(out_key, up_key)

    # Passthrough heuristic: same-named columns across a parent-child
    # edge are treated as the same data.
    parents_of_child: dict[str, list[str]] = {}
    for e in edges:
        parents_of_child.setdefault(e.child, []).append(e.parent)

    for child_name, child_node in nodes.items():
        child_cols = set(child_node.schema)
        for p_name in parents_of_child.get(child_name, ()):
            p = nodes.get(p_name)
            if p is None:
                continue
            shared = child_cols & set(p.schema)
            for col in shared:
                union((child_name, col), (p_name, col))

    components: dict[tuple[str, str], set[tuple[str, str]]] = {}
    for k in parent:
        components.setdefault(find(k), set()).add(k)

    out: dict[tuple[str, str], frozenset[tuple[str, str]]] = {}
    for members in components.values():
        frozen = frozenset(members)
        for m in members:
            out[m] = frozen
    return out


def _infer_layer(resource_type: str, file_path: str) -> str:
    if resource_type == "seed":
        return "SOURCE"
    p = file_path.replace("\\", "/").lower()
    if "/staging/" in p or "/stg/" in p:
        return "STAGING"
    if "/intermediate/" in p or "/int/" in p:
        return "INTERMEDIATE"
    if "/marts/" in p or "/mart/" in p:
        return "MART"
    # jaffle_shop convention: top-level models in models/ are marts.
    return "MART"


def _key_to_name(key: str) -> str | None:
    # dbt manifest keys look like "model.jaffle_shop.customers" or
    # "seed.jaffle_shop.raw_customers". The third segment is the name.
    parts = key.split(".")
    if len(parts) < 3:
        return None
    return parts[-1]


def load_consumer_graph(path: Path) -> ConsumerGraph:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    tool = data["tool"]
    artifacts: dict[str, Artifact] = {}
    for raw in data.get("artifacts", []):
        consumes = tuple(
            Consumes(node=c["node"], columns=tuple(c["columns"]))
            for c in raw.get("consumes", [])
        )
        a = Artifact(id=raw["id"], kind=raw["kind"], consumes=consumes)
        artifacts[a.id] = a
    return ConsumerGraph(tool=tool, artifacts=artifacts)


def load_exposures_as_consumer_graph(manifest_path: Path) -> ConsumerGraph:
    """Read dbt exposures from a manifest.json and convert to a
    ConsumerGraph. Each exposure becomes an Artifact; each model in
    its `depends_on.nodes` becomes a Consumes with empty columns
    (model-level coarsening — exposures rarely declare column-level
    deps). Used as the BI consumer graph when no separate BI export
    is supplied; aligns with how mature dbt projects already declare
    consumers in-tree."""
    with open(manifest_path, encoding="utf-8") as f:
        manifest = json.load(f)
    raw_exposures = manifest.get("exposures", {}) or {}
    artifacts: dict[str, Artifact] = {}
    for raw in raw_exposures.values():
        node_keys = (raw.get("depends_on") or {}).get("nodes", []) or []
        consumes_list: list[Consumes] = []
        for dep_key in node_keys:
            n = _key_to_name(dep_key)
            if n:
                consumes_list.append(Consumes(node=n, columns=()))
        if not consumes_list:
            continue
        name = raw.get("name") or raw.get("unique_id")
        a = Artifact(
            id=name,
            kind=raw.get("type", "exposure"),
            consumes=tuple(consumes_list),
        )
        artifacts[a.id] = a
    return ConsumerGraph(tool="dbt_exposures", artifacts=artifacts)


def load_spec(path: Path):
    """Dispatch on top-level `kind`. Returns the spec dataclass for
    that kind. Callers branch on isinstance to dispatch to the
    matching planner."""
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise SpecError("spec must be a YAML mapping at the top level")

    kind = raw.get("kind")
    if kind == "metric_request":
        return _parse_metric_request(raw)
    if kind == "definitional_change":
        return _parse_definitional_change(raw)
    raise SpecError(
        f"unsupported kind: {kind!r}. v0 supports: metric_request, definitional_change"
    )


def _parse_metric_request(raw: dict) -> MetricRequest:
    _require_keys(
        raw,
        required={"kind", "id", "intent", "metric", "consumer"},
        optional={"filters", "contract_tier"},
        where="spec",
    )

    spec_id = raw["id"]
    if not isinstance(spec_id, str) or not SLUG_RE.match(spec_id):
        raise SpecError(f"id must match {SLUG_RE.pattern}; got {spec_id!r}")

    intent = raw["intent"]
    if not isinstance(intent, str) or not intent.strip():
        raise SpecError("intent must be a non-empty string")

    metric = raw["metric"]
    if not isinstance(metric, dict):
        raise SpecError("metric must be a mapping")
    _require_keys(metric, required={"name", "output_shape"}, optional=set(), where="metric")

    name = metric["name"]
    if not isinstance(name, str) or not SLUG_RE.match(name):
        raise SpecError(f"metric.name must match {SLUG_RE.pattern}; got {name!r}")

    output_shape = _parse_output_shape(metric["output_shape"])

    filters_raw = raw.get("filters", []) or []
    if not isinstance(filters_raw, list):
        raise SpecError("filters must be a list")
    filters = tuple(str(f) for f in filters_raw)

    consumer_raw = raw["consumer"]
    if not isinstance(consumer_raw, dict):
        raise SpecError("consumer must be a mapping")
    _require_keys(consumer_raw, required={"tool", "artifact"}, optional=set(), where="consumer")
    if consumer_raw["tool"] not in SUPPORTED_BI_TOOLS:
        raise SpecError(
            f"consumer.tool must be one of {sorted(SUPPORTED_BI_TOOLS)}; "
            f"got {consumer_raw['tool']!r}"
        )
    consumer = Consumer(tool=consumer_raw["tool"], artifact=consumer_raw["artifact"])

    tier = raw.get("contract_tier", "standard")
    if tier not in ALLOWED_TIERS:
        raise SpecError(f"contract_tier must be one of {sorted(ALLOWED_TIERS)}; got {tier!r}")

    return MetricRequest(
        id=spec_id,
        intent=intent,
        name=name,
        output_shape=output_shape,
        filters=filters,
        consumer=consumer,
        contract_tier=tier,
    )


def _parse_output_shape(raw) -> OutputShape:
    if not isinstance(raw, dict):
        raise SpecError("metric.output_shape must be a mapping")
    _require_keys(raw, required={"grain", "columns"}, optional=set(), where="metric.output_shape")
    grain = _parse_grain(raw["grain"])
    columns = _parse_columns(raw["columns"], grain.keys)
    return OutputShape(grain=grain, columns=columns)


def _parse_grain(raw) -> Grain:
    if not isinstance(raw, dict):
        raise SpecError("metric.output_shape.grain must be a mapping")
    _require_keys(raw, required={"keys"}, optional={"coverage"}, where="metric.output_shape.grain")

    keys_raw = raw["keys"]
    if not isinstance(keys_raw, list) or not keys_raw:
        raise SpecError("metric.output_shape.grain.keys must be a non-empty list")
    if len(keys_raw) != len(set(keys_raw)):
        raise SpecError("metric.output_shape.grain.keys must contain unique entries")
    for k in keys_raw:
        if not isinstance(k, str) or not SLUG_RE.match(k):
            raise SpecError(f"metric.output_shape.grain.keys entries must be slugs; got {k!r}")
    keys = tuple(keys_raw)

    coverage_raw = raw.get("coverage", {}) or {}
    if not isinstance(coverage_raw, dict):
        raise SpecError("metric.output_shape.grain.coverage must be a mapping")

    # Reject coverage entries for keys that aren't in grain.keys.
    unknown = set(coverage_raw.keys()) - set(keys)
    if unknown:
        raise SpecError(
            f"metric.output_shape.grain.coverage references keys not in "
            f"grain.keys: {sorted(unknown)}"
        )

    # Every time-like grain key must have a coverage entry.
    time_like_in_grain = [k for k in keys if k in TIME_LIKE_KEYS]
    missing_time_coverage = [k for k in time_like_in_grain if k not in coverage_raw]
    if missing_time_coverage:
        raise SpecError(
            f"metric.output_shape.grain.coverage missing required entries for "
            f"time-like keys: {missing_time_coverage}. Time-like keys "
            f"({sorted(TIME_LIKE_KEYS)}) require explicit dense/sparse choice."
        )

    coverage = {k: _parse_coverage(spec, where=f"coverage.{k}") for k, spec in coverage_raw.items()}
    return Grain(keys=keys, coverage=coverage)


def _parse_coverage(raw, where: str) -> Coverage:
    if not isinstance(raw, dict):
        raise SpecError(f"metric.output_shape.grain.{where} must be a mapping")
    _require_keys(
        raw,
        required={"dense"},
        optional={"range", "fill"},
        where=f"metric.output_shape.grain.{where}",
    )
    dense = raw["dense"]
    if not isinstance(dense, bool):
        raise SpecError(f"metric.output_shape.grain.{where}.dense must be a boolean")

    range_obj: CoverageRange | None = None
    if dense:
        if "range" not in raw:
            raise SpecError(
                f"metric.output_shape.grain.{where}: range is required when dense: true"
            )
        range_obj = _parse_range(raw["range"], where=f"{where}.range")
    elif "range" in raw:
        # Sparse with a range is not meaningful; reject to keep semantics tight.
        raise SpecError(
            f"metric.output_shape.grain.{where}: range is only valid when dense: true"
        )

    fill = raw.get("fill", None)
    return Coverage(dense=dense, range=range_obj, fill=fill)


def _parse_range(raw, where: str) -> CoverageRange:
    if not isinstance(raw, dict):
        raise SpecError(f"metric.output_shape.grain.{where} must be a mapping")
    _require_keys(raw, required={"from", "to"}, optional=set(), where=f"metric.output_shape.grain.{where}")
    return CoverageRange(
        from_=_validate_range_endpoint(raw["from"], f"{where}.from"),
        to=_validate_range_endpoint(raw["to"], f"{where}.to"),
    )


def _validate_range_endpoint(value, where: str) -> str:
    if not isinstance(value, str):
        raise SpecError(
            f"metric.output_shape.grain.{where} must be a string "
            f"(ISO date or one of {sorted(SYMBOLIC_RANGE_VALUES)})"
        )
    if value in SYMBOLIC_RANGE_VALUES:
        return value
    if ISO_DATE_RE.match(value):
        return value
    raise SpecError(
        f"metric.output_shape.grain.{where}: {value!r} is neither an ISO date "
        f"(YYYY-MM-DD) nor one of {sorted(SYMBOLIC_RANGE_VALUES)}"
    )


def _parse_columns(raw, grain_keys: tuple[str, ...]) -> tuple[MeasureColumn, ...]:
    if not isinstance(raw, list) or not raw:
        raise SpecError("metric.output_shape.columns must be a non-empty list")
    seen_names: set[str] = set()
    out: list[MeasureColumn] = []
    for i, entry in enumerate(raw):
        col = _parse_column(entry, where=f"metric.output_shape.columns[{i}]")
        if col.name in seen_names:
            raise SpecError(f"metric.output_shape.columns[{i}].name {col.name!r} is duplicated")
        if col.name in grain_keys:
            raise SpecError(
                f"metric.output_shape.columns[{i}].name {col.name!r} collides with a grain key; "
                "grain keys are implicit columns and cannot be redeclared"
            )
        seen_names.add(col.name)
        out.append(col)
    return tuple(out)


def _parse_column(raw, where: str) -> MeasureColumn:
    if not isinstance(raw, dict):
        raise SpecError(f"{where} must be a mapping")
    has_struct = "column" in raw or "aggregation" in raw
    has_expr = "from" in raw
    if has_struct and has_expr:
        raise SpecError(f"{where}: must specify either {{column, aggregation}} or from, not both")
    if not has_struct and not has_expr:
        raise SpecError(f"{where}: must specify either {{column, aggregation}} or from")

    name = raw.get("name")
    if not isinstance(name, str) or not SLUG_RE.match(name):
        raise SpecError(f"{where}.name must be a slug; got {name!r}")

    if has_expr:
        _require_keys(raw, required={"name", "from"}, optional=set(), where=where)
        expr = raw["from"]
        if not isinstance(expr, str) or not expr.strip():
            raise SpecError(f"{where}.from must be a non-empty SQL expression")
        return MeasureColumn(name=name, column=None, aggregation=None, expr=expr)

    _require_keys(raw, required={"name", "column", "aggregation"}, optional=set(), where=where)
    col = raw["column"]
    agg = raw["aggregation"]
    if not isinstance(col, str) or not SLUG_RE.match(col):
        raise SpecError(f"{where}.column must be a slug; got {col!r}")
    if agg not in ALLOWED_AGGREGATIONS:
        raise SpecError(
            f"{where}.aggregation must be one of {sorted(ALLOWED_AGGREGATIONS)}; got {agg!r}"
        )
    return MeasureColumn(name=name, column=col, aggregation=agg, expr=None)


def _parse_definitional_change(raw: dict) -> DefinitionalChange:
    _require_keys(
        raw,
        required={
            "kind", "id", "intent",
            "target", "old_definition", "new_definition", "migration",
        },
        optional=set(),
        where="spec",
    )

    spec_id = raw["id"]
    if not isinstance(spec_id, str) or not SLUG_RE.match(spec_id):
        raise SpecError(f"id must match {SLUG_RE.pattern}; got {spec_id!r}")

    intent = raw["intent"]
    if not isinstance(intent, str) or not intent.strip():
        raise SpecError("intent must be a non-empty string")

    target = raw["target"]
    if not isinstance(target, dict):
        raise SpecError("target must be a mapping")
    _require_keys(target, required={"node", "column"}, optional=set(), where="target")
    target_node = target["node"]
    target_column = target["column"]
    if not isinstance(target_node, str) or not SLUG_RE.match(target_node):
        raise SpecError(f"target.node must be a slug; got {target_node!r}")
    if not isinstance(target_column, str) or not SLUG_RE.match(target_column):
        raise SpecError(f"target.column must be a slug; got {target_column!r}")

    old_def = _parse_definition(raw["old_definition"], where="old_definition")
    new_def = _parse_definition(raw["new_definition"], where="new_definition")

    migration = raw["migration"]
    if not isinstance(migration, dict):
        raise SpecError("migration must be a mapping")
    _require_keys(
        migration,
        required={"must_migrate", "allow_stale_consumers"},
        optional=set(),
        where="migration",
    )
    must_migrate_raw = migration["must_migrate"]
    if not isinstance(must_migrate_raw, list):
        raise SpecError("migration.must_migrate must be a list")
    for a in must_migrate_raw:
        if not isinstance(a, str) or not a:
            raise SpecError(
                f"migration.must_migrate entries must be non-empty strings; got {a!r}"
            )
    must_migrate = tuple(must_migrate_raw)
    allow_stale = migration["allow_stale_consumers"]
    if not isinstance(allow_stale, bool):
        raise SpecError("migration.allow_stale_consumers must be a boolean")

    return DefinitionalChange(
        id=spec_id,
        intent=intent,
        target_node=target_node,
        target_column=target_column,
        old_definition=old_def,
        new_definition=new_def,
        must_migrate=must_migrate,
        allow_stale_consumers=allow_stale,
    )


def _parse_definition(raw, where: str) -> Definition:
    if not isinstance(raw, dict):
        raise SpecError(f"{where} must be a mapping")
    _require_keys(raw, required={"basis", "expr"}, optional=set(), where=where)
    basis = raw["basis"]
    expr = raw["expr"]
    if not isinstance(basis, str) or not basis.strip():
        raise SpecError(f"{where}.basis must be a non-empty string")
    if not isinstance(expr, str) or not expr.strip():
        raise SpecError(f"{where}.expr must be a non-empty string")
    return Definition(basis=basis, expr=expr)


def _require_keys(d: dict, required: set[str], optional: set[str], where: str) -> None:
    keys = set(d.keys())
    missing = required - keys
    if missing:
        raise SpecError(f"{where}: missing required keys {sorted(missing)}")
    unknown = keys - required - optional
    if unknown:
        raise SpecError(f"{where}: unknown keys {sorted(unknown)} (schema rejects unknown keys)")
