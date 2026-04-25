# Planner Notes

A working-state map of what the v0 planner can and cannot do.
Audience: future Claudes and the author. Read before adding a
fixture or trusting a "no plans" result.

## Algorithm in one paragraph

The v0 planner is **hand-coded enumeration, not a constraint
solver**. For each spec it loops every node in the DAG, filters to
single eligible parents, enumerates grain resolutions per parent
(direct or derived-from-date-column heuristic), Cartesian-products
across grain keys, and emits one plan per (parent, resolution)
tuple. Each plan is built from a fixed operation template
(`add_node`, `add_edge`, `add_contract C1`, `add_contract C2`).
Annotations are formulaic — the `[OK]` invariant lines are
**by-construction true** for the narrow slice the planner handles,
not the result of evaluating the vendored catalog. Scoring is a
hardcoded weighted sum with arbitrary coefficients.

Per CHARTER, the eventual rewrite uses Z3 for rule evaluation and
plan ranking. v0 is hand-coded so the pipeline ships before the
engine gets sophisticated.

## Today's slice

"Today's slice" is the bounded subset of *all conceivable plans
that could produce the desired state* the planner will actually
consider. Plans outside the slice are not rejected — they are
**invisible**.

The slice is defined by six constraints. A plan must satisfy ALL of
them to be enumerated:

1. **Single parent.** The new mart depends on exactly one existing
   structural parent. Multi-parent joins are outside the slice.
2. **Single hop.** No intermediate nodes are constructed between an
   existing parent and the new mart. Chains are outside the slice.
3. **Add-only.** No operations modify or drop existing nodes. Only
   `add_node` / `add_edge` / `add_contract`.
4. **Strict declared schema.** A column is "available on a parent"
   only if it appears in the `.yml` columns block. SQL-only columns
   (e.g. `stg_orders.customer_id` in jaffle_shop, which the SQL
   selects but the docs don't list) are invisible.
5. **Date detection by heuristic.** A column is "date-like" only if
   its name contains one of `{date, time, timestamp, _at, _on}` OR
   its description contains one of `{date, timestamp, datetime}`.
   Date columns the heuristic misses are invisible.
6. **Fixed operation template.** Each plan emits exactly four
   operations in a fixed order: `add_node` (the mart) → `add_edge`
   (parent → mart) → `add_contract C1` (schema) → `add_contract C2`
   (grain). No tests, no materialization choice, no filter
   placement variants, no naming alternatives.

## Worked example: `new_customers_monthly` on jaffle_shop

| candidate plan | in slice? | why / why not |
|---|---|---|
| `customers --aggregation→ new_mart`, grain via `first_order` | inside | Single parent, declared schema has all needed cols, date heuristic catches `first_order` from its description. |
| Same but grain via `most_recent_order` | inside | Same reasons. |
| `orders --aggregation→ new_mart`, grain via `order_date` | inside | Same reasons. |
| `stg_orders --aggregation→ new_mart`, grain via `order_date` | outside (#4) | `customer_id` and `order_date` exist in `stg_orders.sql` but not in the `.yml`. Strict-declared-schema boundary. |
| `date_spine + customers --LEFT JOIN→ new_mart` | outside (#1) | Two structural parents. Single-parent boundary. This is the date-spine plan needed for `dense: true`. |
| Add `first_order_month` column to `customers`, then aggregate | outside (#3) | Modifies an existing node. Add-only boundary. |
| Build `customer_acquisition_log` intermediate, then aggregate | outside (#2) | Multi-step chain. Single-hop boundary. |
| Use a window function over `orders` to derive first-order, then aggregate | outside (#6) | Single-hop, but the SQL is richer than the fixed template handles. |

## What "no plans" means

When the planner returns zero plans, the only honest reading is
**"no plan in today's slice reaches the desired state."** It does
*not* mean the desired state is unreachable. A wider slice might
find one.

The "Alternatives rejected" section in the output identifies
candidates that fell *within* the slice but failed a filter
(typically schema strictness). Plans that fall *outside* the slice
never appear at all — neither as winners nor as rejections.

## Planned widenings

Each widening is independent and can be added one at a time, driven
by a fixture that needs it. Order isn't fixed; pick what the next
fixture forces.

| widening | unlocks | trigger fixture |
|---|---|---|
| Multi-parent enumeration | Date spines, calendar dims, joins of two existing entities | `new_customers_monthly` with `dense: true` (in flight) |
| Scaffolding-node generation | Helper nodes (date spine, calendar) constructed alongside the requested mart | Same as above |
| SQL-aware column inference | Plans using columns that exist in SQL but not in `.yml` | A fixture where the staging schema is undocumented but the SQL is readable |
| Modify-existing support | Adding a column to an existing mart instead of building a new one | `definitional_change` fixture |
| Multi-hop chains | New INTERMEDIATE → new MART arrangements when no single eligible parent has the needed columns | A fixture where no single parent is feasible |
| Operation-template variants | Tests, materialization choice, filter placement, naming alternatives | When a fixture's plan reads more clearly with a richer template |

## When the hand-coded engine gets retired

CHARTER calls for a Z3-based engine: "Rewrite the engine (Z3
encoding, rule evaluation, plan ranking) from scratch in this repo,
using dag-simulator's definitions as the source of truth for *what*
the rules are, not *how* to execute them."

The hand-coded planner stretches further than expected because v0
plan shapes are structurally simple (add a MART downstream of one
valid parent). Z3 becomes necessary when **two or more** of these
are simultaneously true:

- Plans involve modifying or dropping existing **contracted** nodes
  (contracts as hard constraints to satisfy).
- Multi-parent enumeration explodes combinatorially and ranking by
  hardcoded coefficients stops correlating with plan quality.
- Plan candidates can violate invariants in non-obvious ways and
  the catalog needs actual evaluation, not by-construction trust.

Until then, prefer widening the hand-coded planner one boundary at
a time. When the threshold is crossed, prefer a clean rewrite over
patching the hand-coded planner further.
