# PR Classification: cal-itp/data-infra (fallback from gitlab-data/analytics)

## 1. Sample method, date range, project

**Project actually used: `cal-itp/data-infra`** (GitHub) — the California Integrated Travel Project's data infrastructure repository. Their dbt project lives under `warehouse/`.

**Why not `gitlab-data/analytics`:** The original target is no longer publicly accessible on gitlab.com. Both `https://gitlab.com/api/v4/projects/gitlab-data%2Fanalytics` and `https://gitlab.com/gitlab-data/analytics` return 404 / Cloudflare-block / redirect to sign-in for unauthenticated requests. GitLab's public project search confirms it does not appear in public listings. Per the brief's fallback clause I picked another active medium-large public dbt analytics project: cal-itp/data-infra has 246 merged PRs in the last ~6 months, an active multi-team analytics dbt project (GTFS schedule, GTFS-RT, payments, NTD reporting, transit database, benefits) with a structure comparable to mattermost-data-warehouse (single dbt root under `warehouse/`, mart/intermediate/staging layering, multiple domains, and clear AE-style PRs).

**Sampling method:** Listed all merged PRs from the most recent 3 pages of `gh api repos/cal-itp/data-infra/pulls?state=closed&per_page=100` (246 total). For each, fetched the file list via `gh api .../pulls/{n}/files` and kept only PRs whose file list includes paths under `warehouse/` (the dbt project). That yielded 81 candidates. I excluded pure non-AE PRs (CI/workflow changes, Python script deletions, dependency bumps, poetry→uv migration, deploy workflow review, Airflow-warning yml fixes, service-account deprecations, GTFS download/parse Airflow refactors). I then classified the **50 most recent remaining AE-relevant PRs**, exactly matching the Mattermost study's sample size of 50.

**Date range:** 2025-12-22 to 2026-04-24 (PR numbers 4721–5141)

**Excluded as non-AE during sampling (not classified):** #5106 (deploy-dbt workflow review), #4941 (Airflow warning yml fixes), #4932 (dbt-artifacts deploy ordering), #4914 (poetry→uv), #4822 (delete publish.py), #4802 (sqlparse dependabot bump), #4723 (dbt deps quarterly bump), #4562 (deprecate service-account macro), #4559 (revert v7.1 validator). These PRs touched warehouse/ paths but were operational/CI/script work rather than AE work. The revert (#4559) was excluded because the matching forward PR #4813 is in the sample.

**Notes on scope:**
- I read each PR's title, body (truncated to ~800 chars), and the full warehouse-scope file list.
- I followed the same conservative multi-kind rule used in the Mattermost study: source/seed additions are flagged as independent (multi-kind) only when a new external system is being onboarded or when the metric work also redefines existing models. Tests/docs/contract additions accompanying a metric change are *part of* shipping the metric.

## 2. Aggregate

| Category | Count | % of total | % of AE-only |
|---|---|---|---|
| dagwright_single_kind | 18 | 36% | 36% |
| dagwright_composable_multi_kind | 5 | 10% | 10% |
| outside_slice_single_kind | 16 | 32% | 32% |
| outside_slice_multi_kind | 11 | 22% | 22% |
| (excluded as non_ae) | 0 | 0% | — |

(All 50 sampled PRs were AE-relevant after the up-front filter; non-AE PRs were dropped during sampling rather than classified.)

Mean kinds per AE PR: 1.34
Median kinds per AE PR: 1

## 3. Per-kind frequency

| Kind | Appears in N PRs | % of AE PRs |
|---|---|---|
| metric_request | 15 | 30% |
| definitional_change | 19 | 38% |
| materialization_change | 11 | 22% |
| add_source_or_seed | 7 | 14% |
| drop / retire | 4 | 8% |
| other_ae_change | 3 | 6% |
| structural_split | 2 | 4% |
| rename | 2 | 4% |
| test_or_contract | 2 | 4% |
| structural_merge | 1 | 2% |
| dependency_repoint | 1 | 2% |

(A PR can appear under multiple kinds; column does not sum to 100%.)

## 4. Sampled PRs (50 most-recent AE-relevant, newest first)

| # | Title (truncated) | Kinds | Summary category | Rationale |
|---|---|---|---|---|
| 5141 | Migrate final GTFS schedule incremental models to microbatch | materialization_change, drop | outside_slice_multi_kind | Migrates two intermediate models to microbatch; deprecates old historic incremental macros. Materialization + macro retirement. |
| 5133 | Payments: substitute new RCTA gtfs feed in mapping | add_source_or_seed | outside_slice_single_kind | Edits `payments_entity_mapping.csv` seed only — feed-id remap. |
| 5114 | Payments(Enghouse): scrub expiry from pipeline | drop, definitional_change | outside_slice_multi_kind | Removes `expiry` column end-to-end (drop) AND changes surrogate-key generation in the staging model (definitional_change). |
| 5107 | Add product_type to final CTE for fct_payment_rides | metric_request | dagwright_single_kind | Adds `products.activation_type` column to `fct_payments_rides_v2`. New measure on existing fact. |
| 5097 | Create Littlepay parse audit table | metric_request | dagwright_single_kind | New `dim_littlepay_parse_job_results` mart for audit dashboards. Single net-new dim. |
| 5092 | Create new incremental macro and handle models at the dt/service_date interface | materialization_change, rename | outside_slice_multi_kind | Introduces new ranged-incremental macros, renames `PROD_GTFS_RT_START` → `GTFS_RT_START`, removes redundant where clauses across many models. |
| 5082 | Replace yaml tests with data_tests | other_ae_change | outside_slice_single_kind | Pure `tests:` → `data_tests:` rename in many YAML files. dbt-syntax deprecation fix; no semantic change. |
| 5080 | Payments(SLORTA): Littlepay technical onboarding | add_source_or_seed | outside_slice_single_kind | Onboards SLORTA: new entity-mapping seed entries + row-access-policy macro updates. Operational onboarding. |
| 5077 | Convert remaining GTFS RT incremental models to microbatch | materialization_change | outside_slice_single_kind | Converts ~10 models to microbatch incremental strategy. Pure materialization change. |
| 5076 | Set schema change behavior for trip updates message age model & move event_time into SQL files | materialization_change | outside_slice_single_kind | Sets `on_schema_change` config and moves `event_time` from YAML into model SQL — config/materialization tweak. |
| 5075 | Set event_time on RT message models | materialization_change | outside_slice_single_kind | One-line `event_time` config addition on a fact model — microbatch follow-up. |
| 5065 | Convert GTFS RT message age models to microbatch & fix old message age calculation bug | materialization_change, definitional_change, drop | outside_slice_multi_kind | Three changes: (a) microbatch migration, (b) fixes long-standing message-age calculation bug (definitional_change), (c) deprecates `fct_daily_vehicle_positions_message_age_summary`. |
| 5062 | Deduplicating the organization name | definitional_change | dagwright_single_kind | Refactors `fct_create_expiring_gtfs_issues` to dedupe earlier with `ARRAY_AGG(... LIMIT 1)`; changes how `organization_name` is resolved. Pure redefinition. |
| 5052 | Payments(RABA + Camarillo): Enghouse technical onboarding | add_source_or_seed | outside_slice_single_kind | Same shape as #5080 but for Enghouse + two new operators. Operational onboarding. |
| 5051 | Fix RT index join | dependency_repoint, definitional_change | outside_slice_multi_kind | Changes int model to use GTFS download configs instead of GTFS datasets (dependency_repoint) AND removes a `data_quality_pipeline` filter in downstream models (definitional_change). |
| 5042 | Adding organization_name to TDQ issues | metric_request | dagwright_single_kind | Adds `organization_name` to `fct_create_expiring_gtfs_issues`. New column on existing fact. |
| 5037 | Staging/airtable expiring issues | metric_request | dagwright_single_kind | dbt-side change is adding new fields/logic to existing fct_create_expiring_gtfs_issues. (Airflow-side operator refactor is out of scope.) |
| 5036 | Reenable dbt tests for Payments | test_or_contract | outside_slice_single_kind | Re-enables dbt tests for the payments domain via `dbt_project.yml` config. Pure test toggling. |
| 5011 | Add fct_create_expiring_gtfs_issues model and Airtable source | metric_request, add_source_or_seed | dagwright_composable_multi_kind | Net-new mart AND new Airtable external source declaration. Source addition independent (new system being onboarded). |
| 5010 | Littlepay Sync Results Audit Table | metric_request, add_source_or_seed | dagwright_composable_multi_kind | New `dim_littlepay_sync_job_results` mart + new external source for `raw_littlepay_sync_job_result` + new staging models. Source addition independent. |
| 5009 | Convert GTFS RT Day Map Grouping Models to Microbatch | materialization_change | outside_slice_single_kind | Microbatch migration of several intermediate models, plus a doc fix. |
| 4990 | Add Airtable Issue Management DAG with custom operators and tests | metric_request | dagwright_single_kind | Adds `fct_close_expired_issues` mart that supports new Airflow DAG. Single net-new fact on the dbt side. |
| 4982 | Consolidate rollup route-direction summary and fix operator summaries | structural_merge, definitional_change | outside_slice_multi_kind | Consolidates 3 `mart_gtfs_rollup` route-direction tables into one (structural_merge) AND switches `fct_daily_schedule_rt_route_direction_summary` from inner to left join (definitional_change). |
| 4981 | Migrate GTFS Schedule Dimension Tables To Microbatch Strategy | materialization_change, drop | outside_slice_multi_kind | Migrates ~20 `dim_*` schedule models to microbatch + deletes `dim_stop_times_orig` testing model + deletes the old shared incremental macro. |
| 4968 | Reverse changes to fields, stop publishing an array field | definitional_change | dagwright_single_kind | Reverts a downstream column type and stops publishing one field — re-defines the model's contract. |
| 4967 | Changed model arrays to strings to comply with ckan csv needs | definitional_change | dagwright_single_kind | Converts array columns to string columns for CKAN-CSV compatibility on two latest dim models. Pure redefinition. |
| 4959 | Updates to ckan publishing | metric_request, definitional_change | dagwright_composable_multi_kind | Adds new fields and changes existing field types/sources across 3+ latest dim models for CKAN publishing. Mix of new metrics + redefinitions. |
| 4943 | Benefits: Add row-level security to benefits mart table | other_ae_change | outside_slice_single_kind | Adds `benefits_row_access_policy` macro and applies it as post-hook on `fct_benefits_events`. Row-level access policy = AE-relevant operational, not metric/definition. |
| 4942 | Hotfix to stop duplicates on dim_stop_latest | definitional_change | dagwright_single_kind | De-dupe fix in `dim_stops_latest`. Changes the existing model's row-identity logic. |
| 4926 | int_gtfs_rt__vehicle_positions_trip_day_map_grouping microbatch | materialization_change | outside_slice_single_kind | Single-model microbatch migration. |
| 4904 | Benefits: update enrollment_method from `digital` to `self_service` | rename | outside_slice_single_kind | Renames a categorical value in event data (string-rename, applied historically). Effectively a value rename in `fct_benefits_events`. |
| 4896 | Adjust dbt_short_name so it can handle… | definitional_change | dagwright_single_kind | Strips `__dbt_tmp_*` suffixes in `fct_bigquery_data_access`. Changes semantics of an existing model field. |
| 4894 | dim_stop_times microbatch | materialization_change | outside_slice_single_kind | Single-model microbatch migration with new helper macro. |
| 4889 | Create Payments product data dimension model to correct missing product info | structural_split, metric_request | outside_slice_multi_kind | Creates new `int_payments__dim_product_data` to model SCD product data (structural_split: factoring product slowly-changing data out of mainline payments models) AND surfaces it on `fct_payments_rides_v2` (metric_request). |
| 4863 | feat: microbatch strategy for dim stop arrivals | materialization_change | outside_slice_single_kind | Single-model microbatch migration. |
| 4860 | Fix: add feed_version to Payments joins to dedupe Micropayments | definitional_change | dagwright_single_kind | Adds `feed_version` to joins to fix fanout — changes semantics of how the int model dedupes. |
| 4857 | RT trip updates stop / trip / operator aggregations | metric_request, definitional_change | dagwright_composable_multi_kind | (a) Adds new array columns (`prediction_error_sec`, `scaled_prediction_error_sec`) and percentile aggregations (metric_request); (b) changes `int_gtfs_schedule__stop_order_by_route` filter logic (definitional_change). |
| 4841 | docs: document validator 7.1 rules seed | test_or_contract | outside_slice_single_kind | Pure docs/tests addition for seed YAML. Documentation/contract only. |
| 4828 | Move Payments reconciliation tables into warehouse | metric_request | dagwright_single_kind | Adds new `fct_payments_aggregations_reconciliation` mart and new columns to `int_payments__customers_vaults_to_aggregations`. Cohesive metric_request bundle. |
| 4827 | Remove any empty line and space from macros to fix dim_translations | other_ae_change | outside_slice_single_kind | Fixes whitespace in macros so generated SQL parses correctly. Operational. |
| 4813 | Upgrade Schedule Validator to 7.1.0 | definitional_change, add_source_or_seed | outside_slice_multi_kind | Switches validator version (definitional_change to validator-derived data) AND adds a new seed file with v7.1 rules. (Note: this PR was reverted in #4559.) |
| 4788 | Payments(enghouse): mart table reorganization and enrichment | metric_request | dagwright_single_kind | Adds `organization_name`, `route_long_name`, `route_short_name`, `agency_id`, `agency_name` to existing enghouse mart facts. New columns on existing models. |
| 4785 | Fix settlements dedupe (follow up to #4755) | definitional_change | dagwright_single_kind | Removes a `qualify` dedupe statement in unioned settlements model, alters dedupe semantics. |
| 4777 | Daily route-direction summary + refactor operator summaries | metric_request, definitional_change | dagwright_composable_multi_kind | Adds new daily route-direction summary fact (metric_request) AND switches join from inner to full-outer in `fct_scheduled_trips`/`fct_observed_trips` (definitional_change). |
| 4755 | Dedupe payments settlements | structural_split, definitional_change | outside_slice_multi_kind | Adds new dedicated intermediate `int_payments__settlements_deduped` model — factors dedupe out of unioned model (structural_split) AND changes semantics of the deduped result (definitional_change). |
| 4750 | Payments (enghouse): mart and reliability table revisions | metric_request | dagwright_single_kind | Adds new mart tables (`fct_payments_settlements_enghouse`, `v2_payments_reliability_weekly_unlabeled_routes_enghouse`, `payments_tests_weekly_date_spine_enghouse`). Cohesive new-mart bundle. |
| 4743 | Payments (enghouse): seed revisions handling changes | definitional_change, add_source_or_seed | outside_slice_multi_kind | Updates seed YAML data types (definitional_change to seed contract) AND adjusts seed handling in mart fact. |
| 4741 | Payments(enghouse): data handling for warehouse use | definitional_change | dagwright_single_kind | Quick fixes to enghouse staging + mart for "data more workable" — touches existing staging columns and mart joins. Definitional. |
| 4728 | Fix GTFS downloading invalid Zip file | definitional_change | dagwright_single_kind | Fixes parse outcomes in `dim_gtfs_schedule_unzip_outcomes` after fixing zip-download header issue. Single-file model fix; alters which rows the dim shows. (Borderline — could be other_ae_change.) |
| 4721 | Include Fare Media in join tables | metric_request | dagwright_single_kind | Adds Fare Media parse-result rows to existing parse-outcomes models. New rows/columns on existing facts. |

## 5. Honest caveats

- **Classification is judgment-driven.** Another reviewer might disagree on roughly 7–10 PRs out of 50. The largest variance axes are listed below.
- **`materialization_change` dominates the outside-slice bucket.** Cal-ITP is in the middle of a multi-quarter migration to dbt's microbatch incremental strategy. 11 of 50 PRs (22%) involve that migration in some form. If this team had finished the migration before my sample window, the outside-slice share would drop substantially. Mattermost did not have an analogous in-flight materialization migration. This is a real distribution difference, but it is partly a snapshot artifact.
- **`add_source_or_seed` independence call.** As in the Mattermost study, I called source/seed additions multi-kind only when (a) a new external system is being onboarded (e.g., #5011 Airtable, #5010 Littlepay sync) or (b) the metric work also redefines existing models. If you treat every "new source + new metric" PR as single-kind, the multi-kind dagwright count drops from 5 to ~2, and the dagwright_single_kind share rises to ~42%.
- **Operational PRs (row-access-policy macros, payments entity mappings, validator-version bumps, macro whitespace cleanups) are heavier in cal-itp than in Mattermost.** Mattermost had analogous "freshness check" / "exposure update" PRs but in smaller numbers. Both fall into `other_ae_change` or `add_source_or_seed` outside the slice.
- **`metric_request` vs `definitional_change` ambiguity.** When a PR labeled "fix" actually adds new fields, I called it metric_request. When a fix changes existing semantics, I called it definitional_change. PRs #4860 (add `feed_version` to dedupe), #4942 (hotfix duplicates), #4728 (fix invalid zip handling), and #4785 (fix settlements dedupe) sit on this fence — all four I tagged as definitional_change. Cal-ITP's "fixes" lean toward redefining existing semantics rather than adding new columns; that's why definitional_change (38%) outranks metric_request (30%) here, the opposite ordering from Mattermost.
- **`drop`/`retire` calls.** I separated `drop` from `materialization_change` in #5141, #5065, and #4981 because each PR explicitly retired old models or macros along with the migration. A stricter reading would lump them as a single materialization PR.
- **Most ambiguous classifications I'm least confident on:**
  1. **#4889** (payments product data dim) — I called it two-kind (structural_split + metric_request). Defensible single-kind metric_request because the new dim is the load-bearing change and the rest is downstream propagation.
  2. **#4982** (consolidate rollup tables) — I called the consolidation `structural_merge + definitional_change`. The body says "consolidate 3 tables into 1" plus a separate join-fix (inner→left); I pulled the join-fix out as a definitional_change. Defensible single-kind structural_merge.
  3. **#4813** (validator upgrade) — tagged `definitional_change + add_source_or_seed` because the new seed introduces v7.1 rules and changes the rule set used downstream. Could be argued single-kind add_source_or_seed.
  4. **#5051** (RT index join fix) — called `dependency_repoint + definitional_change`. The repoint is clear; the definitional_change rests on whether removing the `data_quality_pipeline` filter actually changes downstream output (the body says it shouldn't, because the filter would always be true). Stricter reading would drop the definitional_change tag.

## 6. Headline finding

In **cal-itp/data-infra**, only **36% of recent AE-relevant PRs** fit a single dagwright kind (vs Mattermost's **52%**), and **10%** are composable-multi-kind (vs Mattermost's **16%**) — so dagwright as scoped today, with composability, would directly cover **46% of routine AE PRs in cal-itp vs 68% in Mattermost**. The gap is absorbed by a much larger **outside-slice tail (54% vs Mattermost's 32%)**, dominated by `materialization_change` (11/50 PRs = 22%, all microbatch migrations) plus operational PRs (row-access-policy macros, payments entity-mapping seeds, validator version bumps, macro whitespace cleanups). The shape of the dominant in-slice kinds is broadly similar across both projects — `metric_request` and `definitional_change` are the top two in both — but cal-itp leans more toward `definitional_change` (38% vs Mattermost's 24%) because cal-itp's payment-pipeline "fixes" tend to redefine existing dedupe and join semantics rather than add new fields. Strip out the in-flight materialization migration and the in-slice share rebounds to ~58% — close to Mattermost. **The 52%/16%/32% Mattermost breakdown does not generalise tightly:** across two samples, the in-slice (single + composable) share ranges from 46% to 68%, while the outside-slice share roughly doubles from ~32% to ~54% under realistic project-shape variation.
