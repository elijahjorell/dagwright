# dagwright-spec — Schema

YAML schemas for the dagwright-spec types. Each spec is a single YAML
document describing one stakeholder request. The planner reads this
spec plus a unified DAG state (dbt manifest + BI consumer graph) and
emits ranked plans.

v0 implements `metric_request` only. Other types listed in
`REQUEST_TYPES.md` are deferred until they earn a fixture example.

## Common envelope

Every spec, regardless of `kind`, carries:

| field    | type   | required | purpose |
|----------|--------|----------|---------|
| `kind`   | string | yes      | Discriminator. One of the types in `REQUEST_TYPES.md`. |
| `id`     | slug   | yes      | Stable identifier for this spec. Used in filenames, logs, and the planner's output. |
| `intent` | string | yes      | Prose description of what the stakeholder wants. The LLM-in-the-loop fills this from ticket / Slack / interview notes. The planner does not parse it; it exists for human review. |

A slug matches `^[a-z][a-z0-9_]*$`.

## `metric_request`

Captures: a metric that **does not yet exist** in the DAG, that
should. The planner proposes the DAG additions to make it exist,
ranked by reuse of existing pathways and contract preservation.

### Schema

```yaml
kind: metric_request
id: <slug>
intent: <prose>

metric:
  name: <slug>                 # proposed name for the new mart
  grain: [<col>, ...]          # ordered grain columns; non-empty
  measure:                     # exactly one of the two forms below
    column: <col>
    aggregation: <sum|count|count_distinct|avg|min|max>
  # --- OR ---
  measure:
    expr: <SQL expression>

filters:                       # optional; list of SQL boolean expressions
  - <expression>

consumer:                      # required
  tool: metabase               # v0 supports metabase only
  artifact: <name>             # named dashboard / question / collection

contract_tier: standard        # optional; one of critical | standard | best_effort
```

### Field reference

| field             | required | notes |
|-------------------|----------|-------|
| `metric.name`     | yes      | The proposed dbt model name. The planner may suggest variants in alternative plans, but this is the caller's preferred name. |
| `metric.grain`    | yes      | Ordered list of grain columns. Matches dag-simulator's grain model and SQL `GROUP BY`. Time grain is just another column (e.g. `month`). Must be non-empty. |
| `metric.measure`  | yes      | Exactly one form. Structured (`column` + `aggregation`) lets the planner introspect column lineage cheaply. `expr` is the escape hatch for ratios, windowed measures, and other derived expressions. |
| `filters`         | no       | SQL boolean expressions ANDed together. Empty / omitted means no filter. |
| `consumer.tool`   | yes      | The BI tool that will consume the metric. v0 accepts `metabase` only. |
| `consumer.artifact` | yes    | Named consumer artifact (dashboard, question, collection) that the contract will bind to. Required so the planner can compute blast radius and place a C1/C2 contract. |
| `contract_tier`   | no       | Defaults to `standard`. The planner's ranker weights `critical` contracts more heavily. |

### Validation

- `kind` must be exactly `metric_request`.
- `id` and `metric.name` must match `^[a-z][a-z0-9_]*$`.
- `metric.grain` must be a non-empty list of unique slugs.
- `metric.measure` must specify exactly one of `{column, aggregation}` or `expr` — never both, never neither.
- `metric.measure.aggregation`, when present, must be one of: `sum`, `count`, `count_distinct`, `avg`, `min`, `max`.
- `consumer.tool` must be a supported BI integration. v0: `metabase`.
- `contract_tier`, when present, must be one of `critical`, `standard`, `best_effort`.
- Unknown top-level or nested keys are rejected. Catches typos and prevents speculative-field sprawl. To add a field, amend this schema first.

### Worked example

```yaml
kind: metric_request
id: arpu_monthly_2026q2
intent: >
  Finance wants to track average revenue per active user, by month,
  on the executive dashboard. ARPU = monthly revenue / monthly active
  users.

metric:
  name: arpu_monthly
  grain: [month]
  measure:
    expr: sum(revenue) / count(distinct active_user_id)

filters:
  - "customer_status = 'active'"

consumer:
  tool: metabase
  artifact: executive_dashboard

contract_tier: critical
```

A simpler example using the structured measure form:

```yaml
kind: metric_request
id: monthly_revenue
intent: Track total revenue by month for the finance dashboard.

metric:
  name: revenue_by_month
  grain: [month]
  measure:
    column: amount
    aggregation: sum

consumer:
  tool: metabase
  artifact: finance_dashboard
```

### What the planner consumes from this

- `metric.name` + `metric.grain` + `metric.measure` → target node
  shape passed to the planner (analogous to dag-simulator's
  `targeted-construction-planning` input).
- `filters` → additional `WHERE` predicates the proposed model must
  apply.
- `consumer` → the contract binding. The planner attaches a C1
  (schema) and C2 (grain) contract on the new node, owned by
  `consumer.artifact`, at `contract_tier`.
- `intent` → carried through to the human-readable plan output for
  reviewer context. Not parsed.

## Decisions worth remembering

These are the design choices made when the schema was first written.
Revisit before changing any of them.

- **Consumer is required.** dagwright's distinctive value is
  blast-radius analysis and contract preservation. A metric_request
  without a consumer cannot exercise either, so the LLM should refuse
  to file one. Overrides the "optional consumer" sketch in
  `REQUEST_TYPES.md`.
- **Single consumer per spec, not a list.** Multiple consumers →
  file multiple specs. Add a list later when a fixture demands it.
- **`measure` is XOR.** Structured form is preferred when expressible;
  `expr` is the escape hatch. Forbidding both-at-once prevents
  ambiguity about which one the planner should trust.
- **Grain is a flat list of column names.** Matches dag-simulator's
  grain model and SQL `GROUP BY`. No `{entity, time}` structuring —
  premature ontology that the planner doesn't need.
- **No `materialization_hint`, `source_hints`, `target_layer`, or
  `priority`.** Planning decisions belong to the planner. Workflow
  metadata belongs in the caller's tracker. Add fields here only
  when a concrete planning use case demands them.

## Future types

`dimension_request`, `definitional_change`, `deprecation`,
`dashboard_request`, `migration`, `compliance` — listed in
`REQUEST_TYPES.md`, not yet schematized. Add their sections here
when each earns a fixture example.
