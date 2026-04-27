# Experiments

Empirical tests of dagwright's value claims. Each experiment in this
directory targets a specific testable hypothesis from `METRIC.md` and
the CHARTER's value framing — not "is dagwright useful in general"
(too vague to falsify) but "does this specific property hold under
this specific protocol."

Results land under `experiments/results/` (gitignored — they're
sample-specific and re-run on a fresh machine should produce
different absolute numbers but the same ratios).

## Inventory

### Experiment B — `iteration_cost.py`

Cost-per-iteration microbenchmark. The cleanest test of the headline
thesis: per-iteration cost is lower with dagwright in the loop than
with prose-plan regeneration.

Two agents, same task, six iterations each (initial ask + five
refinements):

- **control**: prose-plan regeneration. The LLM holds full
  conversation history; each refinement requests a complete new
  plan as prose. This is the AE+LLM workflow today.
- **treatment**: LLM-edits-spec + dagwright-compiles. The LLM
  authors a YAML spec on iteration 0 and edits it on each
  refinement. dagwright compiles spec → ranked plans
  deterministically.

The harness records LLM input/output tokens, LLM wall-clock, and
dagwright wall-clock per iteration, into a CSV.

Run:

```bash
# dry-run (no API calls; verify harness wiring)
uv run --no-sync python experiments/iteration_cost.py --dry-run
uv run --no-sync python experiments/iteration_cost.py --task all --dry-run

# real run — single task
export ANTHROPIC_API_KEY=sk-ant-...
uv pip install -e ".[experiments]"
uv run --extra experiments python experiments/iteration_cost.py \
  --task new_customers_monthly \
  --model claude-sonnet-4-6 \
  --out experiments/results/iteration_cost.csv

# real run — all wired tasks (single CSV; per-task ratios printed)
uv run --extra experiments python experiments/iteration_cost.py \
  --task all \
  --model claude-sonnet-4-6 \
  --out experiments/results/iteration_cost.csv
```

Three tasks are wired today:

| Task | Spec kind | Manifest | Why |
|---|---|---|---|
| `new_customers_monthly` | metric_request | jaffle_shop_modern (small) | Original headline task; dense aggregation. |
| `lifetime_spend_pretax` | definitional_change | jaffle_shop_modern (small) | Tests that the cost shape holds for a different spec kind. |
| `dau_desktop_only` | definitional_change | mattermost (302 models) | Tests that the cost shape holds when manifest context is ~10× larger (real-world scale). |

The CSV has one row per (agent, iteration). Columns:

```
task_id, agent_kind, iteration, refinement,
llm_input_tokens, llm_output_tokens,
llm_wall_ms, dagwright_wall_ms, total_wall_ms,
notes
```

Console summary prints totals per agent and the control/treatment
ratio. Multiply tokens by your model's per-Mtok pricing to derive
USD cost — pricing is intentionally not baked into the harness.

#### What "winning" looks like

If the thesis holds, the CSV should show:

- Control input tokens grow with iteration (conversation history
  accumulates).
- Treatment input tokens stay roughly flat (each call sends only
  the current spec + the refinement).
- Control output ≈ a full plan each turn (~3–5K tokens of prose).
- Treatment output ≈ a small spec edit each turn (~500–1000 tokens).
- Total token ratio control:treatment around 4–10× across six
  iterations; widens with longer iteration counts.
- Wall-clock dominated by LLM calls in both agents; dagwright
  compile is ~5–30 ms regardless.

#### Today's data

Two runs against `claude-sonnet-4-6` on 2026-04-26. CSV:
`experiments/results/iteration_cost.csv`.

**Aggregate across 3 tasks × 6 iterations × 2 agents (54 API
calls):**

| | control (LLM-only) | treatment (dagwright) | ratio |
|---|---|---|---|
| Total tokens | 346,325 (249,470 in / 96,855 out) | 25,980 (19,318 in / 6,662 out) | **13.3×** |
| Total wall-clock | 1,578.5 s (~26 min) | 77.8 s (~1.3 min) | **20.3×** |
| Spend at Sonnet 4.6 rates | ~$2.20 | ~$0.16 | 13.7× |

**Per-task ratios (control / treatment):**

| Task | Spec kind | Manifest | Tokens | Wall |
|---|---|---|---|---|
| `new_customers_monthly` | metric_request | jaffle_shop_modern | **8.8×** | **10.3×** |
| `lifetime_spend_pretax` | definitional_change | jaffle_shop_modern | **21.6×** | **31.9×** |
| `dau_desktop_only` | definitional_change | mattermost (302 models) | **11.7×** | **20.1×** |

The thesis holds across spec kinds and manifest scales:

- **Control input grows monotonically** in every task. By iter 5,
  control is sending 13K–38K input tokens per call (depending on
  manifest size in the prompt history). Each refinement re-feeds
  the full conversation; the curve is super-linear.
- **Treatment is flat after iter 0** in every task (~400–700 input
  tokens per refinement; iter 0 is heavier because it carries the
  schema + manifest summary). dagwright's own compile scales with
  manifest size: ~7 ms on jaffle_shop_modern, ~70–150 ms on
  Mattermost — still negligible vs. the LLM call.
- **The ratio gap is widest on `lifetime_spend_pretax`** (21.6×
  tokens) because the prose plan for a definitional change is
  longer than for a metric_request — the LLM enumerates migration
  shapes and contract analysis in prose every iteration. Treatment
  edits the same 4-line `migration:` block.

#### Honest finding from this run: schema-rejection failures

This run also surfaced a real failure mode in the treatment that
wasn't visible in the first run of B: **15 of 18 treatment
refinement iterations produced specs that dagwright rejected as
schema-invalid.** The LLM made up YAML keys not in the spec
schema:

- `metric.output_shape: unknown keys ['filters']` (filters belong
  at the spec root, not nested under output_shape)
- `migration: unknown keys ['deprecation_window']` (no such field)
- `spec: unknown keys ['contract', 'preserved_column']` (extra
  fields the LLM invented to express the refinement)
- `spec: missing required keys ['new_definition']` (LLM dropped a
  required field while restructuring)

Two readings of this finding:

1. **The cost ratio is still informative.** Even when the LLM
   makes schema mistakes, treatment uses ~13× fewer tokens than
   control because the LLM is producing small structured edits
   instead of multi-thousand-token prose plans. The mistakes are
   detectable (dagwright's `validate_spec` returns a pointed
   error) and recoverable in production via the MCP server's
   round-trip loop.
2. **The harness doesn't measure end-to-end iteration cost
   honestly** because it lacks a retry-on-validate-error step.
   The docstring claims "the harness retries once with the
   validate_spec error appended" but that wasn't actually
   implemented. A real AE-in-the-loop or MCP-driven session would
   catch the schema error and re-prompt; that costs ~1 extra LLM
   call per failed iteration. With retries, treatment tokens go
   up ~2-3× and the headline ratio drops to maybe ~5× — still a
   real saving, but not 13×.

**Action:** wire a single retry step in `run_treatment` (call
validate_spec; if it errors, append the error to the prompt and
ask the LLM for a corrected YAML; re-validate once). Re-run B
and report both numbers (with vs. without retry). Until then, the
13× headline is the no-retry token-only ratio; treat it as an
upper bound on dagwright's cost saving.

#### Inter-run variance

The same task (`new_customers_monthly`) was run twice today:

| Run | Control tokens | Treatment tokens | Ratio | Treatment iters compiled? |
|---|---|---|---|---|
| 1 (single-task) | 80,275 | 6,275 | 12.8× | 6/6 |
| 2 (multi-task) | 54,710 | 6,202 | 8.8× | 1/6 |

Treatment was nearly identical (6,275 vs 6,202 tokens). Control
was meaningfully different (80K vs 55K) because Anthropic's API
at temperature 0 isn't perfectly deterministic across runs and
the prose plans varied in length. The LLM in run 2 also made
schema mistakes that run 1 didn't make — same prompt, different
behaviour. **The order of magnitude is robust; specific ratios
are sample-dependent.** Quote ranges, not point estimates,
unless the receipt covers many seeds.

#### Methodological note: SDK vs Claude Code orchestration

The harness uses the Anthropic SDK directly (`from anthropic
import Anthropic`) rather than orchestrating Claude Code subagents
via `claude -p` or the Agent tool. Honest reasoning:

- **Measurement contamination.** Claude Code wraps every call in
  its own multi-thousand-token system prompt + tool schemas. The
  control in B is supposed to be "raw LLM on a planning task" —
  if it ran through Claude Code, the input-token count would
  include all that scaffolding and the comparison would no longer
  isolate dagwright's effect from Claude Code's own framing.
- **Token accounting.** The SDK returns `response.usage.input_tokens`
  and `output_tokens` exactly. Headless `claude -p` returns final
  text only; per-call usage is harder to extract reliably.
- **Temperature / seed control.** SDK calls pin `temperature=0`
  directly; subagent orchestration doesn't expose that knob.

A complementary experiment **B′** would orchestrate Claude Code
subagents (with vs without the dagwright MCP server) and measure
the same per-iteration cost shape under realistic deployment
framing. That tests "Claude Code agent + dagwright vs Claude Code
agent alone" rather than "raw LLM + dagwright vs raw LLM alone."
Both are valid; B′ is closer to what an investor cares about, but
its signal is noisier (system prompt drift from Claude Code
releases will shift absolute numbers). Run B′ if asked.

#### Caveats

- LLM output is non-deterministic at temperature > 0. The harness
  pins temperature=0 but Anthropic does not currently expose a
  seed; back-to-back runs can still vary slightly.
- The control's input growth is realistic, not a deliberate
  handicap. Trimming history changes what the experiment is
  testing — prose plans do require accumulated context to refine
  coherently.
- One task is wired today (`new_customers_monthly`). Add more by
  appending entries to the `TASKS` dict in `iteration_cost.py`.
  Today's headline ratios (12.8×, 21.1×) are N=1; replicate across
  2–3 more fixtures before treating them as load-bearing.
- Sonnet 4.6 specifically. Different models will give different
  absolute numbers; the ratio is what generalises. Re-run on any
  model that ships and update the receipt.
- Pricing is not baked in. Sonnet and Opus rates differ ~5×; quote
  the exact rates alongside any cost claim derived from this CSV.

### Experiment C — `determinism.py`

Determinism receipts. The trivial slam-dunk: anyone can re-run this
and reproduce the same hashes.

Runs each fixture N times (default 3), in both `json` and `markdown`
formats, hashes each output, asserts byte-identical across runs.
Today's data: 24 compiles across 4 fixtures × 2 formats × 3 runs,
every single run byte-identical. Mattermost compile averages ~75ms;
the small fixtures average ~5–8ms.

Run:

```bash
uv run --no-sync python experiments/determinism.py
uv run --no-sync python experiments/determinism.py --runs 5
```

The complementary claim — that an LLM run twice on the same prompt
produces different output — is asserted but not measured here.
That's a separate experiment (and costs money). What this proves is
the dagwright half of the asymmetry.

### Experiment E — `sweep.py`

Bulk-benchmark for the CHARTER's "Bulk analysis" claim — generate
plans across a parameter sweep in seconds, not hours and dollars.

Generates N copies of a base spec by varying the spec id and (where
present) the consumer artifact. The plans end up nearly identical —
that's intentional. This is throughput, not plan-quality coverage.

Run:

```bash
uv run --no-sync python experiments/sweep.py                   # 100 plans on jaffle_shop_modern
uv run --no-sync python experiments/sweep.py --manifest mattermost -n 200
```

Today's data:

| Fixture | N | total wall | mean / spec | LLM-only projection (tokens) | LLM Sonnet | LLM Opus |
|---|---|---|---|---|---|---|
| jaffle_shop_modern | 100 | 0.70 s | ~7 ms | 3.8 M | ~$25 | ~$125 |
| mattermost (302 models, real-world) | 50 | 3.88 s | ~77 ms | 1.9 M | ~$12 | ~$63 |

The LLM cost projection assumes 38K tokens per plan (the CHARTER's
stated figure for prose-plan regeneration). Override via
`--tokens-per-llm-plan <n>` once Experiment B has produced a
measured value. Pricing rates baked into the harness are
approximate Sonnet 4.6 / Opus 4.7 rates as of April 2026; verify
against current published rates before quoting.

### Experiment H1 — `sql_equivalence.py`

The first measurement of outcome equivalence. Tests whether
dagwright's plan, deterministically rendered to SQL, structurally
matches what a competent AE would write by hand for the same task.

H1 is paired with a deterministic plan→SQL renderer
(`dagwright/sql_render.py`) that handles:

- Sparse metric_request plans (no dense axis): SELECT-GROUP BY.
- Dense metric_request plans with ISO-dated range endpoints:
  `WITH date_spine AS (range(...))` + LEFT JOIN.
- Dense plans with symbolic range endpoints (`earliest_event`,
  `current_period`): degraded fallback to `SELECT DISTINCT
  <source_expr>` from the parent table.

Run:

```bash
uv run --no-sync python experiments/sql_equivalence.py
uv run --no-sync python experiments/sql_equivalence.py --emit  # always print rendered SQL
```

Today's data: 1 / 1 fixtures structurally equivalent on
`new_customers_monthly` (jaffle_shop_modern). The dagwright plan
renders to SQL that, after whitespace + comment normalization,
matches a hand-written canonical for the same task.

#### Honest finding from H1: symbolic-range gap

The `new_customers_monthly` fixture has
`range: {from: earliest_event, to: current_period}` — symbolic
endpoints. dagwright cannot resolve them to concrete dates without
warehouse access. The renderer falls back to `SELECT DISTINCT
date_trunc(...)` from the source, which technically only emits
months that have data. A "true" date_spine that fills genuine gaps
requires either:

- an AE-side post-processing step to substitute concrete dates into
  `range()`, or
- dagwright extension to read warehouse metadata at compile time
  (would break the zero-cost / zero-dependency property).

**This is a real gap, not a rendering bug.** The H1 canonical SQL
matches dagwright's degraded fallback; the experiment validates
that the renderer is *deterministic and matches what dagwright
emits today*. It does not validate that what dagwright emits is
identical to what a senior AE would have written. That's the
remaining gap.

The canonical SQL in `sql_equivalence.py` is annotated to make
this gap visible to anyone reading the test.

### Experiment H2 — data-level outcome equivalence (scaffolded, not wired)

Continuation of H1: execute both the dagwright-rendered SQL and a
canonical hand-written SQL against a populated jaffle_shop DuckDB,
diff the resulting datasets row-by-row.

Status: **not yet runnable.** The `tests/jaffle_shop_modern/`
fixture only ships the manifest.json (DAG state); it doesn't
include a populated `.duckdb` file with seeded data. Setup needed:

```bash
git clone --depth=1 https://github.com/dbt-labs/jaffle-shop ~/jaffle_shop_modern_src
cd ~/jaffle_shop_modern_src
uv venv --python 3.12
uv pip install dbt-core dbt-duckdb
# (set up profiles.yml — see tests/jaffle_shop_modern/REGENERATE.md)
DBT_PROFILES_DIR=. .venv/Scripts/dbt seed
DBT_PROFILES_DIR=. .venv/Scripts/dbt run
# now jaffle_shop.duckdb exists with all the source + intermediate + mart tables
```

Once that exists, H2's harness needs to:

1. Run the dagwright-rendered SQL against the DuckDB; capture results.
2. Run the canonical SQL against the same DB; capture results.
3. Diff: same row count? same columns? same values? sample-level diff
   when divergent.
4. Report per-fixture: `[MATCH]`, `[DIFF rows]`, `[DIFF values]`.

Effort: ~half a day on top of the populated DB setup. Genuinely
strong receipt — moves outcome equivalence from "anecdotal" to
"data-level on jaffle_shop, untested elsewhere."

### PR-classification study — `pr_classification.md`

Empirical study answering: *what fraction of real AE PRs would
require composition (multiple spec kinds) versus fit a single
dagwright kind?* See the linked file for the full report (50 PRs,
~Aug 2024–Apr 2025, mattermost-data-warehouse).

Headline finding:

| Category | % of AE-relevant PRs |
|---|---|
| dagwright_single_kind (covered today) | **52%** |
| dagwright_composable_multi_kind (needs `change_bundle`) | **16%** |
| outside_slice (single or multi) | **32%** |

Mean kinds per AE PR: **1.42**. Median: **1**.

Per-kind frequency: `metric_request` 54%, `definitional_change` 24%,
`add_source_or_seed` 16%, `dependency_repoint` 10%, materialisation
changes 8%, structural splits 6%, renames/drops 4% each, etc.

Implications:

- dagwright **today** plausibly covers ~half of routine AE work.
- `change_bundle` (June 30 milestone) extends coverage to ~68%.
- The remaining ~32% is a long tail of small kinds (sources,
  materialisation, dependency repoints, structural splits/merges,
  drops, renames). To cover them all would need 6–8 new spec kinds.
- "Compositions are how real AE work happens" is **partially**
  supported: the median PR is single-kind, but ~16% of AE PRs are
  genuinely compositional within dagwright's existing two kinds —
  enough to make `change_bundle` a worthwhile widening, not enough
  to justify framing single-kind specs as a corner case.

Caveats: judgment-driven classification (~6–8 PRs of 50 are
disputable); the dominant ambiguity is "is `add_source` independent
of the metric_request that uses it?" Treating source-additions as
part-of-metric drops multi-kind from 16% to ~6%.

## Future experiments

- **A** — head-to-head with a quality rubric (2–3 days, including
  rubric design). **Unblocked now that B has produced iteration
  data**; A scores convergence quality across the same iteration
  count to test whether the cost saving comes at a quality cost.
  *Run as v1/v2/v3, see commits 29ef6f8 / 697ee97 / 6ceb40b.*
- **B′** — same per-iteration measurement, but orchestrated through
  Claude Code subagents (with vs without the dagwright MCP server)
  instead of raw SDK calls. Closer to deployment framing; noisier
  signal. See B's methodological note above.
- **D** — stability under spec rephrasing (half-day).
- **F** — manifest drift replay (1–2 days).
- **G** — AE-in-the-loop user study (post-Aug-31).

See the design discussion that proposed this program for the full
write-up of each.
