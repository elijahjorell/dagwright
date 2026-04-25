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
    Coverage,
    CoverageRange,
    DagState,
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

    _require_keys(
        raw,
        required={"kind", "id", "intent", "metric", "consumer"},
        optional={"filters", "contract_tier"},
        where="spec",
    )

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


def _require_keys(d: dict, required: set[str], optional: set[str], where: str) -> None:
    keys = set(d.keys())
    missing = required - keys
    if missing:
        raise SpecError(f"{where}: missing required keys {sorted(missing)}")
    unknown = keys - required - optional
    if unknown:
        raise SpecError(f"{where}: unknown keys {sorted(unknown)} (schema rejects unknown keys)")
