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
  output_shape:                # required — describes the row shape of the new mart
    grain:
      keys: [<col>, ...]       # ordered grain columns; non-empty
      coverage:                # required for time-like keys; optional for entity keys
        <key>:
          dense: <true|false>
          range:                # required when dense: true
            from: <YYYY-MM-DD | earliest_event | current_period>
            to:   <YYYY-MM-DD | earliest_event | current_period>
          fill: <scalar>        # optional; default null
    columns:                   # measure columns of the result; grain keys are implicit
      - name: <slug>
        column: <col>
        aggregation: <sum|count|count_distinct|avg|min|max>
      # --- OR ---
      - name: <slug>
        from: <SQL expression>

filters:                       # optional; list of SQL boolean expressions
  - <expression>

consumer:                      # required
  tool: metabase               # v0 supports metabase only
  artifact: <name>             # named dashboard / question / collection

contract_tier: standard        # optional; one of critical | standard | best_effort
```

### Field reference

| field                  | required | notes |
|------------------------|----------|-------|
| `metric.name`          | yes      | The proposed dbt model name. The planner may suggest variants in alternative plans, but this is the caller's preferred name. |
| `metric.output_shape`  | yes      | The row shape of the result table. See **Output shape** below. |
| `filters`              | no       | SQL boolean expressions ANDed together. Empty / omitted means no filter. |
| `consumer.tool`        | yes      | The BI tool that will consume the metric. v0 accepts `metabase` only. |
| `consumer.artifact`    | yes      | Named consumer artifact (dashboard, question, collection) that the contract will bind to. Required so the planner can compute blast radius and place a C1/C2 contract. |
| `contract_tier`        | no       | Defaults to `standard`. The planner's ranker weights `critical` contracts more heavily. |

### Output shape

`metric.output_shape` describes the row shape of the result table.
It unifies what naive specs scatter across grain (keys), measure
(values), and unstated assumptions (coverage, fill). The result
table's full schema is `grain.keys` ∪ `[c.name for c in columns]`.

**`grain.keys`** — ordered list of grain columns, matching SQL
`GROUP BY`. Non-empty.

**`grain.coverage`** — mapping from grain key to a coverage spec.
**Required for every time-like key** (`day`, `week`, `month`,
`quarter`, `year`); optional for entity keys.

A coverage spec has:

- `dense` (bool, required) — whether every value in the range must
  appear as a row, even when no underlying event matches. `true` is
  the dashboard-line-chart case (the planner adds a date-spine
  companion node and a LEFT JOIN); `false` is the sparse-by-nature
  event-log case (raw `GROUP BY` without densification).
- `range` (required when `dense: true`) — `{from, to}`. Each
  endpoint is either an ISO date (`2018-01-01`) or a symbolic
  reference:
  - `earliest_event` — earliest date present in any source the
    planner uses.
  - `current_period` — the current period at the grain. For monthly
    grain, the current month.
- `fill` (scalar, optional) — value to use in the rows the dense
  axis fills in. Defaults to `null`. For numeric measures, set to
  `0` to make the gap explicit and avoid null-vs-zero ambiguity in
  the BI tool.

Why coverage is required for time keys: gaps in time-axis dashboards
are the most common defect of a naive `GROUP BY date_trunc(...)`.
Forcing the AE to choose dense-vs-sparse upfront converts a silent
defect into an explicit decision. The LLM-in-the-loop should default
to `dense: true` for dashboard consumers and ask the AE to confirm.

Why coverage is optional for entity keys: the combinatorial
explosion of `(customer × day × ...)` cells makes universal
densification wasteful. Most entity-keyed marts are sparse-by-nature
and the consumer expects to handle absence semantically.

**`columns`** — the measure columns of the result. Grain keys are
implicit; do not list them here. Each entry is exactly one of:

- `{name, column, aggregation}` — structured form. Lets the planner
  introspect column lineage cheaply and bind contracts precisely.
- `{name, from}` — `from` is a SQL expression. Escape hatch for
  ratios, windowed measures, and other derived expressions.

`name` is the column's name in the result table. Must be a slug,
unique within `columns`, and not equal to any `grain.keys` entry.

### Validation

- `kind` must be exactly `metric_request`.
- `id` and `metric.name` must match `^[a-z][a-z0-9_]*$`.
- `metric.output_shape.grain.keys` must be a non-empty list of unique slugs.
- `metric.output_shape.grain.coverage` must be a mapping from a subset of `grain.keys` to coverage specs. Every time-like grain key (`day`, `week`, `month`, `quarter`, `year`) **must** have an entry. Coverage keys not in `grain.keys` are rejected.
- For each coverage spec: `dense` is required boolean. When `dense: true`, `range` is required and must specify both `from` and `to`. Each endpoint is an ISO date (`YYYY-MM-DD`) or one of `earliest_event`, `current_period`. `fill` is optional; default `null`.
- `metric.output_shape.columns` must be a non-empty list. Each entry must specify exactly one of `{column, aggregation}` or `from` — never both, never neither.
- Each column `name` must be a slug, unique within `columns`, and not collide with any `grain.keys` entry.
- `aggregation`, when present, must be one of: `sum`, `count`, `count_distinct`, `avg`, `min`, `max`.
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
  output_shape:
    grain:
      keys: [month]
      coverage:
        month:
          dense: true
          range: {from: earliest_event, to: current_period}
          fill: 0
    columns:
      - name: arpu
        from: sum(revenue) / count(distinct active_user_id)

filters:
  - "customer_status = 'active'"

consumer:
  tool: metabase
  artifact: executive_dashboard

contract_tier: critical
```

A simpler example using the structured column form:

```yaml
kind: metric_request
id: monthly_revenue
intent: Track total revenue by month for the finance dashboard.

metric:
  name: revenue_by_month
  output_shape:
    grain:
      keys: [month]
      coverage:
        month: {dense: true, range: {from: earliest_event, to: current_period}, fill: 0}
    columns:
      - name: total_revenue
        column: amount
        aggregation: sum

consumer:
  tool: metabase
  artifact: finance_dashboard
```

### What the planner consumes from this

- `metric.name` + `metric.output_shape` → target node shape passed
  to the planner. `grain.keys` + `coverage` tell the planner whether
  to add a date-spine companion node; `columns` describes what to
  compute.
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
- **Output shape is one block, not three scattered fields.** Earlier
  drafts split grain and measure at the top of `metric`. The result
  table's row shape is a single coherent thing the AE knows up front;
  splitting it leaks the question of "what does this thing look like
  on the dashboard?" across multiple places. Coverage was the missing
  piece — without it the planner cannot tell whether to add a date
  spine, and the resulting line chart silently has gaps.
- **Coverage is required for time-like grain keys, optional for
  entity keys.** Time-axis gaps are the most common silent defect of
  naive aggregations; forcing the AE to declare dense-vs-sparse
  surfaces the choice. Entity grains are usually sparse-by-nature and
  universal densification is combinatorially wasteful.
- **Per-column measure XOR.** Each entry in `columns` is exactly one
  of structured (`column` + `aggregation`) or `from` (SQL expression).
  Same reasoning as the original `measure` XOR — forbidding both-at-
  once prevents ambiguity about which one the planner should trust.
- **Grain keys are implicit columns of the result.** Don't list them
  in `columns`. The result table's full schema is `grain.keys` ∪
  `[c.name for c in columns]`.
- **Grain is a flat list of column names.** Matches the catalog's
  grain model and SQL `GROUP BY`. No `{entity, time}` structuring —
  premature ontology. Time-likeness is determined by the key being
  one of `{day, week, month, quarter, year}`, not by structural
  typing.
- **No `materialization_hint`, `source_hints`, `target_layer`, or
  `priority`.** Planning decisions belong to the planner. Workflow
  metadata belongs in the caller's tracker. Add fields here only
  when a concrete planning use case demands them.

## Future types

`dimension_request`, `definitional_change`, `deprecation`,
`dashboard_request`, `migration`, `compliance` — listed in
`REQUEST_TYPES.md`, not yet schematized. Add their sections here
when each earns a fixture example.
