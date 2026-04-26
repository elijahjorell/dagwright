"""Smoke test for the column-level lineage feature.

Verifies that:

1. Per-model alias extraction picks up the canonical jaffle_shop_modern
   staging-rename pattern (``id AS customer_id`` etc.).
2. The DAG-wide synonym index unions across alias chains AND the
   passthrough heuristic (same-named column across a parent-child edge).
3. The planner accepts a metric_request that asks for an upstream
   column name (``id``), via a synonym match on the parent.

Run:

  uv run --no-sync python experiments/column_lineage.py

Exits 0 on pass, non-zero on the first assertion failure. Standalone:
no API keys, no DuckDB, no network.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

from dagwright.column_lineage import attribute_aliases, extract_aliases
from dagwright.loaders import load_consumer_graph, load_manifest, load_spec
from dagwright.planner import plan_metric_request


REPO_ROOT = Path(__file__).resolve().parent.parent


def _check(condition: bool, message: str) -> None:
    if not condition:
        print(f"  FAIL: {message}")
        sys.exit(1)
    print(f"  PASS: {message}")


def test_extract_aliases_basic() -> None:
    print("\n[1] extract_aliases — staging-rename pattern")
    raw = """with source as (select * from {{ source('ecom', 'raw_orders') }}),
renamed as (
    select
        id as order_id,
        store_id as location_id,
        customer as customer_id,
        subtotal as subtotal_cents,
        {{ cents_to_dollars('subtotal') }} as subtotal
    from source
)
select * from renamed"""
    aliases = extract_aliases(raw)
    expected = [
        ("id", "order_id"),
        ("store_id", "location_id"),
        ("customer", "customer_id"),
        ("subtotal", "subtotal_cents"),
    ]
    _check(aliases == expected, f"aliases match expected order: got {aliases}")


def test_extract_aliases_no_false_positives() -> None:
    print("\n[2] extract_aliases — table aliases must NOT count as columns")
    cases = {
        "from <tbl> as <alias>": "select id from customers as c",
        "join <tbl> <alias>": "select * from a join b o on o.x = a.x",
        "subquery alias": "select x from (select 1 as y) as sub",
    }
    for label, sql in cases.items():
        # No `<col> AS <col>` pairs that aren't from these structural aliases.
        aliases = extract_aliases(sql)
        _check(
            aliases == [] or all(src not in {"customers", "b", "sub"} for src, _ in aliases),
            f"no table-alias false positives: [{label}] -> {aliases}",
        )


def test_synonym_index_jaffle_shop_modern() -> None:
    print("\n[3] DAG-wide synonym index on jaffle_shop_modern")
    dag = load_manifest(REPO_ROOT / "tests/jaffle_shop_modern/manifest.json")

    # Direct alias edge: raw_customers.id should be in the same component
    # as stg_customers.customer_id (extracted from the staging SQL).
    aliases_for_stg_customer_id = dag.aliases_of("stg_customers", "customer_id")
    _check(
        "id" in aliases_for_stg_customer_id,
        "stg_customers.customer_id has 'id' as a synonym (via raw_customers.id alias)",
    )

    # Passthrough heuristic: customers.customer_id selects from
    # stg_customers.customer_id via SELECT *. The synonym should
    # transitively include 'id'.
    aliases_for_customers_customer_id = dag.aliases_of("customers", "customer_id")
    _check(
        "id" in aliases_for_customers_customer_id,
        "customers.customer_id has 'id' as a synonym (via passthrough + alias chain)",
    )

    # Negative: customers.customer_name should NOT be synonymous with 'id'.
    aliases_for_customer_name = dag.aliases_of("customers", "customer_name")
    _check(
        "id" not in aliases_for_customer_name,
        "customers.customer_name is NOT synonymous with 'id'",
    )

    # synonym_match probe: parent=customers, requested='id' -> 'customer_id'.
    _check(
        dag.synonym_match("customers", "id") == "customer_id",
        "synonym_match(customers, 'id') resolves to 'customer_id'",
    )
    _check(
        dag.synonym_match("customers", "unknown_zzz") is None,
        "synonym_match(customers, 'unknown_zzz') returns None",
    )


def test_planner_accepts_alias_match() -> None:
    print("\n[4] planner accepts a metric_request that uses an upstream column name")
    dag = load_manifest(REPO_ROOT / "tests/jaffle_shop_modern/manifest.json")
    cg = load_consumer_graph(REPO_ROOT / "tests/jaffle_shop_modern/metabase.json")

    spec_yaml = """
kind: metric_request
id: count_via_alias
intent: Count distinct ids — testing the column-lineage synonym path.
metric:
  name: count_via_alias
  output_shape:
    grain:
      keys: [month]
      coverage:
        month:
          dense: false
    columns:
      - {name: n, column: id, aggregation: count_distinct}
consumer:
  tool: metabase
  artifact: growth_dashboard
"""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False, encoding="utf-8"
    ) as f:
        f.write(spec_yaml)
        spec_path = Path(f.name)
    spec = load_spec(spec_path)

    plans, _rejections = plan_metric_request(dag, cg, spec)

    parents_with_alias = {
        p.parent for p in plans
        if any("via DAG alias chain" in n for n in p.notes)
    }

    _check(
        "customers" in parents_with_alias,
        "customers accepted as parent via alias (id -> customer_id)",
    )
    _check(
        "orders" in parents_with_alias,
        "orders accepted as parent via alias (id -> order_id)",
    )

    # And the alias note carries the actual mapping so an AE can read it.
    notes_for_customers = [
        n for p in plans for n in p.notes
        if p.parent == "customers" and "alias" in n.lower()
    ]
    _check(
        any("customers.customer_id" in n for n in notes_for_customers),
        "alias note for 'customers' parent names the actual column "
        "(customers.customer_id)",
    )


def main() -> int:
    print("=" * 78)
    print("Column-lineage smoke test")
    print("=" * 78)

    test_extract_aliases_basic()
    test_extract_aliases_no_false_positives()
    test_synonym_index_jaffle_shop_modern()
    test_planner_accepts_alias_match()

    print()
    print("=" * 78)
    print("ALL CHECKS PASSED")
    print("=" * 78)
    return 0


if __name__ == "__main__":
    sys.exit(main())
