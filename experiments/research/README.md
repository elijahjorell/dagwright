# Research / exploratory studies

This directory holds **exploratory empirical work** that informed
design thinking but **isn't pitched externally**. Treat findings here
as priors and intuitions, not as receipts for the dagwright value
claim. The author intends to validate these against a real-world dbt
project they have day-job context on; until then, they're not
load-bearing for any external pitch.

The receipt-grade experiments live one directory up
(`experiments/determinism.py`, `experiments/iteration_cost.py`,
`experiments/sql_equivalence.py`, `experiments/quality_rubric.py`).

## What's here

### `pr_classification_mattermost.md`

50 recent merged PRs from `mattermost/mattermost-data-warehouse`
(`transform/mattermost-analytics/`, ~Aug 2024 – Apr 2025), classified
by which dagwright spec kind(s) the change touches.

### `pr_classification_cal_itp.md`

Replication on `cal-itp/data-infra` (California Integrated Travel
Project, public dbt warehouse). 50 recent merged PRs covering
roughly Dec 2025 – Apr 2026. Same rubric, same sample size.
(Original target was `gitlab-data/analytics`; that repo is no
longer publicly accessible.)

## Aggregate of the two studies

| Category | Mattermost | cal-itp |
|---|---|---|
| dagwright_single_kind (covered today) | 52% | 36% |
| dagwright_composable_multi_kind (`change_bundle`) | 16% | 10% |
| outside_slice (today's two kinds don't reach) | 32% | 54% |
| **In-slice today + composable** | **68%** | **46%** |

Mean kinds per AE PR: 1.42 / 1.34. Median in both: 1.

Per-kind ordering — `metric_request` and `definitional_change`
hold the top two slots in both projects regardless of business
domain, just with different weights:

| Kind | Mattermost | cal-itp |
|---|---|---|
| `metric_request` | 54% | 30% |
| `definitional_change` | 24% | 38% |
| `materialization_change` | 8% | 22% |
| `add_source_or_seed` | 16% | 14% |
| `dependency_repoint` | 10% | 2% |

## Tentative read

Useful as priors, **not as a pitch number**:

- Coverage range looks like **46–68%** as single-kind, **56–84%**
  with composition. Single-number coverage claims aren't honest;
  quote a range.
- The two existing kinds (`metric_request`, `definitional_change`)
  are robust top picks across both projects.
- `change_bundle` is justified but modest — 10–16% of PRs.
- `materialization_change` is a stronger candidate for the next new
  kind than its Mattermost share alone suggested (8% there, 22% in
  cal-itp; mean ~15%, persistent across both).
- "Compositions are how AE work happens" is weakened at scale.
  Median PR is single-kind in both projects.

## The framing this analysis is missing — degradation weighting

PR count is **unweighted**. Different kinds contribute very
differently to **DAG degradation** (silent drift, accumulated mess,
dead ends). The right framing isn't "what % of PRs does dagwright
cover" but "what % of *degradation contribution* does dagwright
cover."

Reasoned weighting (priors, not measured):

| Kind | Reversibility | Blast | Silent failure | Defect rate | Degradation per change |
|---|---|---|---|---|---|
| `metric_request` | easy | low | visible | low | **LOW** |
| `definitional_change` | hard | high | **silent** | high | **HIGH** |
| `structural_split` | hard | high | medium | medium | **HIGH** |
| `rename` | medium | high | partly visible | medium | **MEDIUM** |
| `drop` | hard | high | visible | low | **MEDIUM-LOW** |
| `materialization_change` | medium | medium | medium | medium | **MEDIUM** |
| `add_source_or_seed` | easy | low | visible | low | **LOW** |
| `dependency_repoint` | medium | high | partly silent | medium | **MEDIUM** |
| `test_or_contract` | n/a | n/a | n/a | n/a | **NEGATIVE** (reduces) |

Implication: if degradation-weighted, dagwright's coverage of
`definitional_change` (a HIGH-degradation kind) is worth
proportionally more than its 24–38% PR count suggests — silent
semantic drift is exactly the failure mode dagwright's contract
analysis is designed to prevent. `metric_request` (LOW-degradation)
contributes mostly iteration speed value, not degradation
prevention. `structural_split` is a more important coverage gap
than its 4–6% PR count would imply.

## Validation checklist for a real-world project

When validating against an actual dbt project (whether a personal
day-job platform or any other production dbt repo), the questions
that would convert these priors into evidence:

1. **Reproduce the kind classification** on ~30–50 PRs from that
   project. Same rubric. See whether single-kind / composable /
   outside-slice shares fall inside or outside the 46–68% / 10–16%
   / 32–54% bands seen here.
2. **Per-kind defect tagging.** For each PR, was it reverted
   within 6 months? How many follow-up "fix" PRs touched the same
   files? That's the degradation rate. Tag and cross-reference
   against kind. If `definitional_change` PRs have 3× the follow-up
   rate of `metric_request` PRs, the HIGH/LOW framing above is
   empirically supported.
3. **Blast-radius measurement.** For each PR, count consumers
   affected (downstream model count + exposure count). Multiply by
   defect rate per kind to get a real degradation-weight.
4. **Silent vs. loud failure split.** Of the PRs that caused
   downstream issues, how many were caught by tests vs. surfaced
   later by data review? `definitional_change` should skew silent;
   `metric_request` should skew loud. Confirm or refute.
5. **Time-to-detection.** Median days between merge and the first
   "fix" PR addressing fallout. Higher = more degradation per
   change. Should split sharply by kind.

If a real project shows the same shape — top two kinds are
metric_request and definitional_change, materialization_change
varies but is persistent, definitional_change has high silent-
failure rate — the priors above are supported. If the real project
diverges substantially, the dagwright design would need rethinking
against that data, not the synthetic rubric here.

## Honest scope of what these studies are NOT

- Not a comparison vs. competing tools (no comparison group exists
  for this design space).
- Not a prediction of individual PR fate (N=50 per project; way
  too small for that).
- Not a substitute for actual product use. Two synthetic
  classifications don't replace running dagwright against a real
  AE workflow.
- Not currently load-bearing in any external pitch material — by
  design, until validated against real-project context.
