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
5. **Date detection by heuristic.** A column is "date-like" iff:
   one of `{date, time, timestamp, datetime}` appears as a whole
   `_`-delimited token in its name (so `event_time` matches but
   `count_lifetime_orders` does not); OR the name ends with `_at`
   or `_on`; OR `Date` / `Timestamp` / `Datetime` appears
   case-sensitively as a whole word in its description (catches
   "Date of customer's first order" but not idiomatic "...to
   date."). Date columns the heuristic misses are invisible.
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

## Contract semantics under change (decided)

When a forward requirement changes the *meaning* of a column or
expression an existing consumer reads, the engine must decide
whether the consumer's contract is preserved. Three semantics were
considered:

- (a) Contract = column-level read only. Definitional change
  preserves the contract syntactically; risk surfaces as a note.
- (b) Contract = column read + its currently-implied semantics.
  Any definitional change to the target violates the contract for
  every consumer.
- (c) Contract semantics declared by the spec via the
  `migration.must_migrate` list. Consumers in the list have
  semantic dependency; the contract is broken for them unless the
  plan migrates their read. Consumers outside the list retain only
  column-level dependency, with a note on definitional change.

**Decision: (c).** The AE knows whether a consumer's number is
supposed to track the new meaning or stay on the old. The engine
doesn't infer. The `must_migrate` list is the spec field that
declares semantic dependency.

Implication: a plan satisfies a `definitional_change` requirement
only if, for every consumer in `must_migrate`, the plan either
(i) updates the source so the new definition flows through to that
consumer's read, or (ii) emits an `update_consumer` operation that
re-points the read.

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

## Next slice (in design)

The slice planned for the next engine iteration, scoped to the
June 30 multi-spec milestone in `METRIC.md`. Target fixture:
`tests/jaffle_shop_modern/specs/customer_domain.target.yaml`.

What it adds beyond today's slice:

1. **Domain envelope.** Top-level spec becomes `kind: domain`,
   with `scope.models`, `contracts`, and `forward_requirements`.
   Existing per-request shapes embed unchanged inside the list.
2. **Contract derivation from the BI graph.** When
   `contracts.derive_from_bi: true`, the engine materializes a
   contract per (artifact, node, column) read on in-scope models.
   `additional` and `relax` adjust the derived set.
3. **`definitional_change` as a forward-requirement kind.** Plan
   shapes the engine enumerates:
   - Replace in place — violates contracts in `must_migrate` per
     (c); surfaces as contract-broken, not invisible.
   - Add a new column with the new definition; emit
     `update_consumer` per consumer in `must_migrate`.
   - Versioned mart (e.g. `customers_v2`); old node stays for
     consumers not in `must_migrate`.
   - Consumer-only change — when a column already on the target
     node satisfies the new definition, plan is just
     `update_consumer` ops, no dbt change.
4. **`update_consumer` plan operation.** Catalog addition.
5. **Multi-spec orchestration.** Each forward requirement planned
   independently against the same domain state; outputs bundled.
   Cross-requirement plan optimization deferred.

What it explicitly does NOT add (deferred):

- Auto-extraction of domain scope or contracts beyond what the AE
  declares or what's already in the BI fixture.
- Modify-existing for kinds other than `definitional_change`.
- Cross-requirement plan interactions (shared intermediates,
  conflict detection).
- The Z3 rewrite. Still hand-coded enumeration.

### Surgery (file-by-file)

- `dagwright/state.py` — add `DomainSpec`, `Contract`,
  `DefinitionalChange`, `ForwardRequirement` (tagged union).
  Existing `MetricRequest` unchanged; nests inside
  `DomainSpec.forward_requirements`.
- `dagwright/loaders.py` — `load_domain_spec` dispatches on
  top-level `kind`. Bare-`metric_request` loader stays as
  fallback for backwards compatibility with the existing fixture.
- `dagwright/planner.py` — `plan_domain` orchestrator;
  `plan_definitional_change` enumerating the four plan shapes;
  contract-derivation helper.
- `dagwright/output.py` — bundle rendering; option-(c) violation
  rendering.
- `catalog/operations.yaml` — add `update_consumer`.
- `dagwright/cli.py` — dispatch on top-level `kind`; no new
  flags.

### Order of work

1. State types + loader for the domain envelope.
2. `update_consumer` operation in catalog.
3. `plan_definitional_change` against a minimal hand-built
   fixture. **Load-bearing step** — if the four plan shapes don't
   read as executable, the rest is wasted plumbing.
4. Domain orchestrator + multi-spec rendering.
5. Promote target spec from `.target` to a real fixture.
6. End-to-end run on jaffle_shop_modern; log to METRIC.md.

## Plan diff implementation state

`dagwright/diff.py` implements `diff_dc_plans(prev, curr)` for
`DefinitionalChangePlan` lists. Per shape (`consumer_only`,
`replace_in_place`, `add_versioned_column`, `versioned_mart`) it
surfaces:

- Score deltas (any change > 0.01)
- Rank changes (1-indexed)
- New / removed plan shapes
- Contract status: held flips (OK ↔ FAIL), contract adds/removes,
  note shifts (classified into a small label set:
  `MODEL-LEVEL must_migrate` ↔ `MODEL-LEVEL not-flagged` etc.)
- Operations: adds and removes by canonical-JSON signature
  (modifications surface as a remove + add pair)
- Downstream dbt models: adds and removes from
  `blast_radius.downstream_dbt_models`

### Not yet implemented

- **`metric_request` plan diff.** The `Plan` dataclass has a
  different shape (no `shape` discriminator; ranked by parent +
  grain resolution). Closing this gap means watch + diff work for
  both kinds; symmetry work, ~half-day.

### Audience reframe (dogfood finding, April 25, 2026)

The MCP-driven dogfood revealed that when an LLM is the consumer,
the diff field is partially redundant — Claude derives the same
signals (op adds/removes, note shifts, score deltas) from the raw
`plans` and `contract_status` fields. The diff comparator's primary
audience is therefore **non-LLM consumers**: `dagwright watch`,
CI / sweep scripts, and humans reading raw artifacts. See
`CHARTER.md` for the full discussion.

## When the hand-coded engine gets retired

CHARTER calls for a Z3-based engine: rule evaluation and plan
ranking implemented from scratch, with the catalog as the source of
truth for *what* the rules are and the engine as the source of truth
for *how* to execute them.

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
