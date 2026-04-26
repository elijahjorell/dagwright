# Business request types

Taxonomy of stakeholder requests AEs receive against an analytics
DAG. Each entry is a candidate `kind` for a forward requirement
inside the dagwright-spec (the domain spec). One domain spec
typically bundles several requirements of various kinds alongside
the domain's scope and contracts to preserve. This is the **input
ontology** (what stakeholders ask). The **output ontology** (DAG
operations the planner emits — add/remove/rewire nodes, change
grain, etc.) is separate and lives in `catalog/operations.yaml`.

Listed for design context. A type is only official once it has a
concrete fixture example and a payload schema in `specs/schema.md`.

## Catalog

| #  | `kind`                | Example request                              | dagwright-shaped?       |
|----|-----------------------|----------------------------------------------|-------------------------|
| 1  | `metric_request`      | "Track ARPU by month"                        | Yes                     |
| 2  | `dimension_request`   | "Slice revenue by acquisition channel"       | Yes                     |
| 3  | `dashboard_request`   | "Build a customer health dashboard"          | Yes (compound)          |
| 4  | `definitional_change` | "Active customer means X now, not Y"         | Strongly yes            |
| 5  | `deprecation`         | "Kill the legacy revenue report"             | Strongly yes            |
| 6  | `reconciliation`      | "Looker number ≠ finance number"             | Maybe (diagnostic)      |
| 7  | `backfill_request`    | "Need this metric going back 2 years"        | Marginal                |
| 8  | `diagnostic`          | "Why did X drop last week?"                  | No — analyst work       |
| 9  | `performance_request` | "This dashboard is slow / stale"             | Marginal                |
| 10 | `access_request`      | "Give marketing this view"                   | No                      |
| 11 | `migration`           | "Move dashboards from Looker to Metabase"    | Yes (large)             |
| 12 | `source_integration`  | "Add Stripe to the warehouse"                | Marginal                |
| 13 | `compliance`          | "Strip PII from this exposure"               | Yes                     |

"dagwright-shaped" = the request involves non-trivial DAG change with
contract / invariant / blast-radius implications that benefit from
ranked plan generation. "No" types belong in other tools.

## Shape sketches

Illustrative — not schema commitments. Concrete payloads are defined
per-type in `specs/schema.md` as types are implemented.

### `metric_request` (v0 target)
Identifier, prose intent, grain, measure, optional filters and
consumers. The metric does not yet exist; planner proposes the DAG
additions to make it exist.

### `dimension_request`
Existing metric/measure to extend, the new dimension to slice on,
the source entity/column for that dimension. Distinct from
`metric_request` because the metric is already defined.

### `dashboard_request`
Compound: one or more metric payloads + an exposure declaration
(consumer name, layout intent). Planner may propose shared
intermediate aggregations across metrics.

### `definitional_change`
Pointer to the concept whose definition is changing (model, column,
metric name, business term), the old and new definitions,
constraints on which consumers must be migrated vs. allowed to stay
on the old version. Highest-blast-radius type.

### `deprecation`
Target node(s) to retire, deadline, allowed migration paths for
existing consumers. Planner enumerates every consumer and proposes
migration order.

### `migration`
Source system → target system, artifacts to move, fidelity
requirements (must dashboards look identical, must metric
definitions stay byte-equivalent, etc.).

### `compliance`
Target column / concept (PII), action (mask, drop, hash, restrict),
consumers that may need contract updates.

Types not sketched above are out of scope until they earn a fixture
example.

## v0 selection

First implemented type: **`metric_request`**.

Tradeoff acknowledged: this is the most commoditized request type
(MetricFlow / Cube / LookML already help here), so it does not
showcase dagwright's distinctive value — blast-radius analysis,
contract preservation, ranked alternatives — as well as
`definitional_change` or `deprecation` would. Chosen first because:

- Cleanest input boundary (grain + measure captures most cases).
- Cleanest output (a new model in the mart layer).
- Most testable on the `jaffle_shop` fixture.
- De-risks the end-to-end pipeline before harder types.

Second implemented type (planned May 2026): TBD. Likely
`definitional_change` or `deprecation` to demonstrate the
distinctive value.

## Charter alignment

`CHARTER.md` (artifact-property revision) frames dagwright as a
deterministic, fast, free planning-artifact layer rather than a
domain-scoped reasoning engine. The dagwright-spec remains the input
to that layer. Each spec belongs to one of the kinds catalogued
above; the kinds describe *what stakeholders ask for*, not what
shape dagwright "wants." v0 implements `metric_request` and
`definitional_change`. Broader kinds earn implementation when a
fixture demands them — but only if the kind benefits from the
artifact-property thesis (deterministic, structured, fast, free)
rather than purely from richer reasoning, which AE+LLM already
provides.
