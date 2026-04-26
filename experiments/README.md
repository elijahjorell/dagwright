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

First real run, 2026-04-26, `claude-sonnet-4-6`, task
`new_customers_monthly` (jaffle_shop_modern), 6 iterations per
agent. CSV: `experiments/results/iteration_cost.csv`.

| | control (LLM-only) | treatment (dagwright) | ratio |
|---|---|---|---|
| Total tokens (6 iters) | 80,275 (55,145 in / 25,130 out) | 6,275 (4,413 in / 1,862 out) | **12.8×** |
| Total wall-clock | 455.5 s | 21.6 s | **21.1×** |
| Iter 5 input tokens | 19,995 | 424 | **47×** |
| Iter 5 wall-clock | 119.7 s | 3.0 s | **40×** |
| Spend at Sonnet 4.6 rates | ~$0.54 | ~$0.04 | 13.2× |

Per-iteration shape matched the thesis exactly:

- **Control input grows monotonically** (734 → 2,891 → 6,350 →
  10,429 → 14,746 → 19,995 tokens) as conversation history
  accumulates. The cost-per-iteration curve is super-linear in
  iteration count because each turn re-feeds all prior turns.
- **Treatment is flat after iter 0** (~400 input tokens per
  refinement; iter 0 is heavier at 2,411 because it carries the
  full spec schema + manifest summary). dagwright's own compile is
  5–41 ms per iteration.
- All 6 treatment iterations compiled cleanly through dagwright —
  the LLM-edited specs were structurally valid every time.

Total run spend: ~$0.58. Total wall-clock: ~8 minutes (single-
threaded; both agents run sequentially).

The headline ratios (12.8×, 21.1×) are weaker than the absolute
delta on later iterations (47×, 40× by iter 5) because iter 0 is
where treatment pays its setup cost. The ratio-vs-iteration-count
curve only gets steeper as iterations extend — by iter 10 the
control is sending ~50K input tokens per call and the treatment
is still at ~400.

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

## Future experiments

- **A** — head-to-head with a quality rubric (2–3 days, including
  rubric design). **Unblocked now that B has produced iteration
  data**; A scores convergence quality across the same iteration
  count to test whether the cost saving comes at a quality cost.
- **B′** — same per-iteration measurement, but orchestrated through
  Claude Code subagents (with vs without the dagwright MCP server)
  instead of raw SDK calls. Closer to deployment framing; noisier
  signal. See B's methodological note above.
- **D** — stability under spec rephrasing (half-day).
- **F** — manifest drift replay (1–2 days).
- **G** — AE-in-the-loop user study (post-Aug-31).

See the design discussion that proposed this program for the full
write-up of each.
