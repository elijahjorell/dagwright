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
4. **Strict declared schema, with column-lineage synonyms.** A column
   is "available on a parent" if it appears in the `.yml` columns
   block (literal match) OR if the parent exposes a synonymous column
   under a different name. The synonym index is built from a hybrid
   extractor in `dagwright/column_lineage.py`:
   - **Fast path** (single-upstream models with extractable aliases):
     regex-match `<src> AS <dst>` patterns in the model's raw SQL
     after Jinja stripping. Covers the canonical staging-rename
     pattern (`raw_customers.id` → `stg_customers.customer_id` →
     `customers.customer_id`). ~0.1 ms/model.
   - **Slow path** (multi-parent JOINs, expression-derived columns,
     anything regex can't attribute): sqlglot's column-lineage
     walker with upstream schemas fed in. Resolves
     `customers.id AS cust_id` vs `orders.id AS order_id` when both
     parents expose `id`. Translates table aliases (`FROM customers
     AS c` → `c.id`) back to underlying table names. ~30–100 ms/
     model on real-world SQL.
   - **Passthrough heuristic**: same-named column across a
     parent-child edge is unioned into the same component. Catches
     downstream marts that select a column without aliasing.
   What's still invisible:
   - dbt-macro arguments (`{{ cents_to_dollars('subtotal') }} AS
     subtotal`) — Jinja stripping replaces the macro with a
     placeholder so neither extractor can see the underlying column
     reference. Documented limitation.
   - Output columns the model emits but never names in `<src> AS
     <dst>` and that aren't documented in `.yml` either. The hybrid
     extractor never asks sqlglot about them; regex also can't
     surface them.
   - Columns flowing through SOURCE-layer tables that have no
     documented schema in the manifest (the dominant pattern in
     test fixtures). sqlglot stops at `<source>.*`; the regex
     fallback recovers the dominant rename pattern but misses
     anything more elaborate at the source boundary.
   - Cost: full lineage build on Mattermost (302 models, 6 MB
     manifest) takes ~500 ms vs ~70 ms for the regex-only baseline.
     The slow path (multi-parent / expression-derived columns)
     parses + qualifies + builds a single sqlglot Scope per model,
     then reuses it across all output columns — without the scope-
     reuse, the same workload took ~5 s. Watch mode reloads only on
     spec changes so the cost is paid once per session.
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
`tests/jaffle_shop_modern/specs/customer_bundle.target.yaml`.

The unit of input becomes a **change_bundle** — a list of forward
requirements scoped to a list of in-scope dbt models. The scope is
not constrained to "a domain" in any structural sense; it can be a
single model, a sub-domain slice, a whole nominal domain, or a
cross-domain set. Domain-shaped scopes are the most common useful
case but not a requirement.

What it adds beyond today's slice:

1. **change_bundle envelope.** Top-level spec becomes
   `kind: change_bundle`, with `scope.models`, `contracts`, and
   `forward_requirements`. Existing per-request shapes embed
   unchanged inside the list.
2. **Contract derivation from the BI graph.** When
   `contracts.derive_from_bi: true`, the engine materializes a
   contract per (artifact, node, column) read on in-scope models.
   `additional` and `relax` adjust the derived set.
3. **`definitional_change` as a forward-requirement kind.** Plan
   shapes the engine enumerates: replace_in_place,
   add_versioned_column, versioned_mart, consumer_only.
   (Already implemented as a standalone spec; reused inside
   bundles unchanged.)
4. **`update_consumer` plan operation.** Catalog entry already
   present; orchestrator wiring needed.
5. **Multi-spec orchestration.** Each forward requirement planned
   independently against the same scope-level derived contracts;
   outputs bundled. Cross-requirement plan optimization deferred.

What it explicitly does NOT add (deferred):

- Auto-extraction of scope or contracts beyond what the AE declares
  or what's already in the BI fixture.
- Modify-existing for kinds other than `definitional_change`.
- Cross-requirement plan interactions (shared intermediates,
  conflict detection).
- The Z3 rewrite. Still hand-coded enumeration.

### Surgery (file-by-file)

- `dagwright/state.py` — add `ChangeBundle`, `Contract`,
  `Scope`, `ContractsConfig`, `ForwardRequirement` (tagged union).
  Existing `MetricRequest` and `DefinitionalChange` unchanged; both
  nest unchanged inside `ChangeBundle.forward_requirements`.
- `dagwright/loaders.py` — extend `load_spec` to dispatch
  `kind: change_bundle`. Bare-kind loaders stay as is.
- `dagwright/planner.py` — `plan_change_bundle` orchestrator;
  `derive_contracts` lifted from inside `plan_definitional_change`
  to scope-level (so all forward requirements see the same derived
  contracts).
- `dagwright/output.py` — bundle rendering with one section per
  forward requirement plus a header showing scope + derived contract
  count.
- `dagwright/cli.py` — dispatch on top-level `kind`; no new flags.

### Order of work

1. State types + loader for the bundle envelope.
2. `plan_change_bundle` orchestrator against the renamed target
   fixture. **Load-bearing step** — if the bundle output doesn't
   read coherently across multiple forward requirements, the rest
   is wasted plumbing.
3. Bundle rendering in `output.py`.
4. Promote target spec from `.target` to a real fixture.
5. End-to-end run on jaffle_shop_modern; log to METRIC.md.
6. Bundle-level diff helper in `diff.py`.

## Plan diff implementation state

`dagwright/diff.py` covers both spec kinds. `diff_plans(prev, curr,
spec)` dispatches on spec type:

`diff_dc_plans` (definitional_change) — identity is the `shape`
discriminator (`consumer_only`, `replace_in_place`,
`add_versioned_column`, `versioned_mart`). Surfaces:

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

`diff_mr_plans` (metric_request) — identity is `(parent, grain
signature)`, where the grain signature names the source column for
each derived resolution and `direct` for direct ones. Surfaces
score deltas, rank changes, plan adds/removes, ops adds/removes
(modifications surface as a remove + add pair), parent-consumer
adds/removes from `blast_radius.parent_consumers_unchanged`, and
consumer artifact rename via `blast_radius.new_artifact`.

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

## Coverage of real AE work — empirical signal

`experiments/pr_classification.md` classifies 50 recent merged PRs
from `mattermost/mattermost-data-warehouse` (`transform/mattermost-
analytics/`, ~Aug 2024–Apr 2025) by which dagwright spec kinds the
change touches. Headline:

- **52%** fit a single existing dagwright kind cleanly (mostly
  `metric_request`, which alone touches 54% of PRs).
- **16%** are genuinely compositional within today's two kinds —
  the case `change_bundle` is supposed to cover.
- **32%** sit outside today's slice, distributed across a long tail
  of small kinds: source/seed additions (16%), dependency repoints
  (10%), materialisation changes (8%), structural splits (6%),
  renames and drops (4% each), exposures-only edits, freshness
  config, etc.

Implications for widening priorities:

1. The two existing kinds are **good picks**: `metric_request`
   alone is the single most common pattern. Investment to deepen
   metric_request and definitional_change pays off immediately.
2. `change_bundle` is **a worthwhile widening, not a transformative
   one** — it extends coverage from ~52% to ~68%. The remaining
   32% requires new kinds, not just composition.
3. The long-tail kinds are individually small (≤16% each) and
   collectively large (~32%). Adding all of them is several quarters
   of planner work; prioritising by use-case importance matters
   more than count.
4. Mean kinds per AE PR is **1.42** — most changes are single-kind.
   The "compositions everywhere" intuition the dau_desktop_only
   case suggested isn't supported at scale; compositions are real
   but median PRs are simpler.

Caveats in `experiments/pr_classification.md`: judgment-driven
classification with ~6–8 disputable calls; the dominant ambiguity
is whether new sources count as independent kinds when they ship
alongside a new metric. Treating source-additions as part-of-metric
drops the multi-kind share from 16% to ~6%.

This empirical signal post-dates the design choices the planner is
built around; it largely validates them. It also says the path to
broader coverage is more about new kinds than richer composition.
