import json
import re
from collections import defaultdict
from pathlib import Path

import yaml

from dagwright.state import (
    Artifact,
    Consumer,
    ConsumerGraph,
    Consumes,
    DagState,
    Edge,
    Measure,
    MetricRequest,
    Node,
)

SLUG_RE = re.compile(r"^[a-z][a-z0-9_]*$")
ALLOWED_AGGREGATIONS = {"sum", "count", "count_distinct", "avg", "min", "max"}
ALLOWED_TIERS = {"critical", "standard", "best_effort"}
SUPPORTED_BI_TOOLS = {"metabase"}


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
        for parent_key in (n.get("depends_on") or {}).get("nodes", []):
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

    return DagState(nodes=nodes, edges=tuple(edges))


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


def load_spec(path: Path) -> MetricRequest:
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise SpecError("spec must be a YAML mapping at the top level")

    _require_keys(raw, required={"kind", "id", "intent", "metric", "consumer"},
                  optional={"filters", "contract_tier"}, where="spec")

    if raw["kind"] != "metric_request":
        raise SpecError(f"v0 only supports kind: metric_request (got {raw['kind']!r})")

    spec_id = raw["id"]
    if not isinstance(spec_id, str) or not SLUG_RE.match(spec_id):
        raise SpecError(f"id must match {SLUG_RE.pattern}; got {spec_id!r}")

    intent = raw["intent"]
    if not isinstance(intent, str) or not intent.strip():
        raise SpecError("intent must be a non-empty string")

    metric = raw["metric"]
    if not isinstance(metric, dict):
        raise SpecError("metric must be a mapping")
    _require_keys(metric, required={"name", "grain", "measure"}, optional=set(), where="metric")

    name = metric["name"]
    if not isinstance(name, str) or not SLUG_RE.match(name):
        raise SpecError(f"metric.name must match {SLUG_RE.pattern}; got {name!r}")

    grain = metric["grain"]
    if not isinstance(grain, list) or not grain:
        raise SpecError("metric.grain must be a non-empty list")
    if len(grain) != len(set(grain)):
        raise SpecError("metric.grain must contain unique columns")
    for g in grain:
        if not isinstance(g, str) or not SLUG_RE.match(g):
            raise SpecError(f"metric.grain entries must be slugs; got {g!r}")

    measure = _parse_measure(metric["measure"])

    filters_raw = raw.get("filters", []) or []
    if not isinstance(filters_raw, list):
        raise SpecError("filters must be a list")
    filters = tuple(str(f) for f in filters_raw)

    consumer_raw = raw["consumer"]
    if not isinstance(consumer_raw, dict):
        raise SpecError("consumer must be a mapping")
    _require_keys(consumer_raw, required={"tool", "artifact"}, optional=set(), where="consumer")
    if consumer_raw["tool"] not in SUPPORTED_BI_TOOLS:
        raise SpecError(f"consumer.tool must be one of {sorted(SUPPORTED_BI_TOOLS)}; got {consumer_raw['tool']!r}")
    consumer = Consumer(tool=consumer_raw["tool"], artifact=consumer_raw["artifact"])

    tier = raw.get("contract_tier", "standard")
    if tier not in ALLOWED_TIERS:
        raise SpecError(f"contract_tier must be one of {sorted(ALLOWED_TIERS)}; got {tier!r}")

    return MetricRequest(
        id=spec_id,
        intent=intent,
        name=name,
        grain=tuple(grain),
        measure=measure,
        filters=filters,
        consumer=consumer,
        contract_tier=tier,
    )


def _parse_measure(raw) -> Measure:
    if not isinstance(raw, dict):
        raise SpecError("metric.measure must be a mapping")
    has_struct = "column" in raw or "aggregation" in raw
    has_expr = "expr" in raw
    if has_struct and has_expr:
        raise SpecError("metric.measure must specify either {column, aggregation} or expr, not both")
    if not has_struct and not has_expr:
        raise SpecError("metric.measure must specify either {column, aggregation} or expr")

    if has_expr:
        _require_keys(raw, required={"expr"}, optional=set(), where="metric.measure")
        expr = raw["expr"]
        if not isinstance(expr, str) or not expr.strip():
            raise SpecError("metric.measure.expr must be a non-empty string")
        return Measure(column=None, aggregation=None, expr=expr)

    _require_keys(raw, required={"column", "aggregation"}, optional=set(), where="metric.measure")
    col = raw["column"]
    agg = raw["aggregation"]
    if not isinstance(col, str) or not SLUG_RE.match(col):
        raise SpecError(f"metric.measure.column must be a slug; got {col!r}")
    if agg not in ALLOWED_AGGREGATIONS:
        raise SpecError(f"metric.measure.aggregation must be one of {sorted(ALLOWED_AGGREGATIONS)}; got {agg!r}")
    return Measure(column=col, aggregation=agg, expr=None)


def _require_keys(d: dict, required: set[str], optional: set[str], where: str) -> None:
    keys = set(d.keys())
    missing = required - keys
    if missing:
        raise SpecError(f"{where}: missing required keys {sorted(missing)}")
    unknown = keys - required - optional
    if unknown:
        raise SpecError(f"{where}: unknown keys {sorted(unknown)} (schema rejects unknown keys)")
