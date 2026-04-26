"""Per-plan risk detection.

The planner has all the data it needs to surface a structured risk
register — derived-grain NULL exposure, retroactive mutability,
symbolic-range fallback, contract-tier escalation, and contract-tier
escalation policy implications. The previous version of the renderer
left these as a flat ``Notes`` list; that didn't read as a "risk
register" to a quality rubric, and it lost the actionable mitigation
guidance.

Each detector is a pure function of the spec, the chosen parent, and
the grain resolution combo. No randomness, no I/O — fits the existing
determinism receipt.

The output is a list of :class:`Risk` items the renderer turns into
a ``### Risks`` section per plan. Severity tags help the AE prioritise:

- ``critical`` — the SQL won't ship correctly without addressing this.
- ``moderate`` — the SQL will ship but produces silently-wrong or
  fragile data unless mitigated.
- ``advisory`` — known property of the plan the AE should be aware
  of; usually the right answer is "you knew that, OK."
"""

from __future__ import annotations

from dataclasses import dataclass

from typing import TYPE_CHECKING

from dagwright.state import (
    DefinitionalChange,
    MetricRequest,
)

if TYPE_CHECKING:
    # GrainResolution is defined in planner.py to avoid a circular
    # import; we only need the type for annotations here.
    from dagwright.planner import GrainResolution


# Symbolic range endpoints the planner can't resolve to ISO dates
# without warehouse access. SQL render falls back to SELECT DISTINCT
# against the source rather than emitting a true date_spine fill.
SYMBOLIC_RANGE_VALUES = frozenset({"earliest_event", "current_period"})

# Time-like grain keys; metrics aggregated over these are retroactively
# mutable as new rows arrive in the parent.
TIME_GRAIN_KEYS = frozenset({"day", "week", "month", "quarter", "year"})

# Aggregations whose result depends on the row set, hence retroactively
# mutable when the row set grows.
MUTABLE_AGGREGATIONS = frozenset({"count_distinct", "count", "sum", "avg"})


@dataclass(frozen=True)
class Risk:
    id: str
    severity: str  # "critical" | "moderate" | "advisory"
    summary: str
    mitigation: str


def detect_metric_risks(
    spec: MetricRequest,
    parent_name: str,
    grain_resolutions: "list[GrainResolution]",
) -> list[Risk]:
    """All risks the planner can identify for a single metric_request
    plan, ordered by severity."""
    risks: list[Risk] = []

    # Symbolic-range fallback (moderate): the renderer can't produce
    # a true date_spine when the range endpoints are symbolic. The
    # SELECT DISTINCT fallback only emits periods that already have
    # underlying events — not genuine zero-fill across all months.
    for key, cov in spec.output_shape.grain.coverage.items():
        if not (cov.dense and cov.range):
            continue
        r = cov.range
        if r.from_ in SYMBOLIC_RANGE_VALUES or r.to in SYMBOLIC_RANGE_VALUES:
            risks.append(Risk(
                id="symbolic_range_fallback",
                severity="moderate",
                summary=(
                    f"Range endpoints for grain key '{key}' are symbolic "
                    f"({r.from_!r} → {r.to!r}); the SQL render uses a "
                    f"degraded fallback (SELECT DISTINCT against the parent) "
                    f"instead of a true date_spine."
                ),
                mitigation=(
                    f"Substitute concrete ISO dates in the spec before "
                    f"deployment, OR accept that periods without underlying "
                    f"events won't appear as zero-fill rows in the result."
                ),
            ))
            break  # one is enough

    # Derived-grain NULL exposure (advisory): when grain is derived
    # via date_trunc on a parent column, rows where that column is
    # NULL are silently dropped from the metric.
    for gr in grain_resolutions:
        if gr.via == "derived":
            risks.append(Risk(
                id=f"derived_grain_null__{gr.source_column}",
                severity="advisory",
                summary=(
                    f"Grain '{gr.grain_key}' is derived from "
                    f"{parent_name}.{gr.source_column}; rows with NULL "
                    f"{gr.source_column} are silently excluded from the metric."
                ),
                mitigation=(
                    f"Confirm {gr.source_column} is non-null for all relevant "
                    f"rows. If not, add a filter to handle NULLs explicitly "
                    f"(exclude / coalesce / surface as 'unknown' bucket)."
                ),
            ))

    # Retroactive mutation (moderate): aggregations over time-grained
    # metrics are not stable as new parent rows arrive. Past periods
    # mutate. Audit / finance consumers usually want immutable history.
    is_time_grained = any(
        k in TIME_GRAIN_KEYS for k in spec.output_shape.grain.keys
    )
    has_mutable_agg = any(
        c.is_structured and c.aggregation in MUTABLE_AGGREGATIONS
        for c in spec.output_shape.columns
    )
    if is_time_grained and has_mutable_agg:
        risks.append(Risk(
            id="retroactive_mutation",
            severity="moderate",
            summary=(
                f"Aggregations over a time-grained metric are retroactively "
                f"mutable — as new rows arrive in {parent_name}, past period "
                f"values can change. The result is not a snapshot of "
                f"history; it's a recomputed view of it."
            ),
            mitigation=(
                "If consumers (audit, finance, exec dashboards) require "
                "immutable historical numbers, add a snapshot pattern "
                "(dbt snapshot or scheduled materialisation with as_of "
                "column) downstream of this metric."
            ),
        ))

    # Critical-tier policy (advisory): tier 'critical' has explicit
    # operational implications the AE often forgets to wire up.
    if spec.contract_tier == "critical":
        risks.append(Risk(
            id="critical_tier_policy",
            severity="advisory",
            summary=(
                "Contract tier is 'critical': downstream consumers treat "
                "this artifact as audit-relevant. Implicit policy applies."
            ),
            mitigation=(
                "Required practice for critical-tier artifacts: data tests "
                "(not_null, unique, accepted_values) on every contracted "
                "column; schema changes require AE + stakeholder sign-off "
                "before merge; breaking changes (column drop/rename, type "
                "change) need a deprecation window with explicit comms to "
                "every must-migrate consumer."
            ),
        ))

    return _by_severity(risks)


def detect_definitional_change_risks(
    spec: DefinitionalChange,
    plan_shape: str,
) -> list[Risk]:
    """Risks specific to a definitional_change plan shape."""
    risks: list[Risk] = []

    # Replace-in-place semantic drift (critical for non-must_migrate consumers).
    if plan_shape == "replace_in_place":
        risks.append(Risk(
            id="replace_in_place_semantic_drift",
            severity="critical",
            summary=(
                f"Replacing {spec.target_node}.{spec.target_column} in place "
                f"changes the meaning for ALL readers, including consumers "
                f"not in must_migrate. They will silently see "
                f"'{spec.new_definition.basis}' values where they expected "
                f"'{spec.old_definition.basis}'."
            ),
            mitigation=(
                "Either move to add_versioned_column / versioned_mart so the "
                "old definition stays addressable, OR explicitly accept the "
                "drift after auditing every reader and getting their consent."
            ),
        ))

    # Versioned-column migration coordination (moderate).
    if plan_shape == "add_versioned_column":
        risks.append(Risk(
            id="versioned_column_drift",
            severity="moderate",
            summary=(
                f"Two columns now coexist: the original "
                f"{spec.target_node}.{spec.target_column} carrying "
                f"'{spec.old_definition.basis}' and a new versioned column "
                f"carrying '{spec.new_definition.basis}'. Drift between the "
                f"two is a real risk if the underlying data evolves."
            ),
            mitigation=(
                "Set a sunset date for the old column, or wire a data test "
                "that asserts the relationship between the two definitions "
                "holds (within tolerance) on every dbt run."
            ),
        ))

    # Stale consumers allowed (advisory).
    if spec.allow_stale_consumers:
        risks.append(Risk(
            id="stale_consumers_allowed",
            severity="advisory",
            summary=(
                "allow_stale_consumers=true means readers outside must_migrate "
                "may continue consuming the old definition for some window."
            ),
            mitigation=(
                "Track those readers in a deprecation log with a sunset "
                "date; without an explicit sunset, the 'temporary' window "
                "becomes permanent."
            ),
        ))

    return _by_severity(risks)


def _by_severity(risks: list[Risk]) -> list[Risk]:
    """Stable sort by severity (critical first), preserving order
    within ties."""
    rank = {"critical": 0, "moderate": 1, "advisory": 2}
    return sorted(risks, key=lambda r: rank.get(r.severity, 99))
