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

# real run
export ANTHROPIC_API_KEY=sk-ant-...
uv pip install -e ".[experiments]"
uv run --extra experiments python experiments/iteration_cost.py \
  --task new_customers_monthly \
  --model claude-sonnet-4-6 \
  --out experiments/results/iteration_cost.csv
```

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
- Pricing is not baked in. Sonnet and Opus rates differ ~5×; quote
  the exact rates alongside any cost claim derived from this CSV.

## Future experiments

- **C** — determinism receipts (trivial, half-day).
- **E** — sweep / bulk benchmark (half-day).
- **A** — head-to-head with a quality rubric (2–3 days, including
  rubric design).
- **D** — stability under spec rephrasing (half-day).
- **F** — manifest drift replay (1–2 days).
- **G** — AE-in-the-loop user study (post-Aug-31).

See the design discussion that proposed this program for the full
write-up of each.
