"""S1: data-quality checks on stg_trips (nulls, ranges, duplicates, row counts).

Decision point (decisions/006-blocking-vs-warning.md):
  - unknown_pickup_zones is BLOCKING (ERROR): taxi zones 1..263 are a fixed physical reality.
    An unknown zone indicates malformed upstream source data that will break downstream mart joins.
  - plausible_trip_durations is NON-BLOCKING (WARN): non-positive durations and trips over 3 hours
    occur in real TLC data due to GPS glitches or driver meter errors (typically <0.5%).
    Downstream marts already filter these out (where duration_min > 0 and duration_min <= 180),
    so we warn if the glitch rate exceeds 1% rather than aborting the pipeline run.
  - passenger_counts is NON-BLOCKING (WARN): null passenger counts occasionally happen in credit card
    meter records; we warn if missing rates exceed 5%.
"""

import dagster as dg

from ..partitions import check_month, trip_months
from ..resources import RedshiftData


def _stg_trip_stats(redshift: RedshiftData, month: str) -> dict:
    check_month(month)
    sql = (
        "select count(*) as rows, "
        "sum(case when duration_min <= 0 then 1 else 0 end) as nonpositive_duration, "
        "sum(case when duration_min > 180 then 1 else 0 end) as over_3h, "
        "sum(case when passenger_count is null then 1 else 0 end) as null_passengers, "
        "sum(case when pickup_zone_id not between 1 and 263 then 1 else 0 end) as unknown_zone "
        f"from staging.stg_trips where trip_month = '{month}'"
    )
    res = redshift.query(sql)
    if not res:
        return {"rows": 0, "nonpositive_duration": 0, "over_3h": 0, "null_passengers": 0, "unknown_zone": 0}
    return {k: (int(v) if v is not None else 0) for k, v in res[0].items()}


@dg.asset_check(
    asset="stg_trips",
    partitions_def=trip_months,
    blocking=True,
    description="No unknown pickup zone IDs (outside 1..263). Blocking check.",
)
def unknown_pickup_zones(
    context: dg.AssetCheckExecutionContext,
    redshift: RedshiftData,
) -> dg.AssetCheckResult:
    month = context.partition_key
    stats = _stg_trip_stats(redshift, month)
    unknown = stats["unknown_zone"]
    passed = unknown == 0

    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.ERROR,
        description="All pickup zones valid" if passed else f"{unknown} trip(s) with unknown pickup zone",
        metadata={
            "rows": dg.MetadataValue.int(stats["rows"]),
            "unknown_zones": dg.MetadataValue.int(unknown),
        },
    )


@dg.asset_check(
    asset="stg_trips",
    partitions_def=trip_months,
    blocking=False,
    description="Trip durations are plausible (>0 and <=180 min). Warns if bad rate > 1%.",
)
def plausible_trip_durations(
    context: dg.AssetCheckExecutionContext,
    redshift: RedshiftData,
) -> dg.AssetCheckResult:
    month = context.partition_key
    stats = _stg_trip_stats(redshift, month)
    total = stats["rows"]
    bad = stats["nonpositive_duration"] + stats["over_3h"]
    rate = (bad / total) if total > 0 else 0.0
    passed = rate <= 0.01

    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.WARN,
        description=(
            f"Duration glitch rate {rate:.2%} is within 1% threshold"
            if passed
            else f"Duration glitch rate {rate:.2%} exceeds 1% threshold ({bad}/{total} trips)"
        ),
        metadata={
            "rows": dg.MetadataValue.int(total),
            "nonpositive_duration": dg.MetadataValue.int(stats["nonpositive_duration"]),
            "over_3h": dg.MetadataValue.int(stats["over_3h"]),
            "bad_rate": dg.MetadataValue.float(rate),
        },
    )


@dg.asset_check(
    asset="stg_trips",
    partitions_def=trip_months,
    blocking=False,
    description="Passenger count null rate is within tolerance (<5%). Warns if null rate > 5%.",
)
def passenger_counts(
    context: dg.AssetCheckExecutionContext,
    redshift: RedshiftData,
) -> dg.AssetCheckResult:
    month = context.partition_key
    stats = _stg_trip_stats(redshift, month)
    total = stats["rows"]
    nulls = stats["null_passengers"]
    rate = (nulls / total) if total > 0 else 0.0
    passed = rate <= 0.05

    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.WARN,
        description=(
            f"Passenger null rate {rate:.2%} is within 5% threshold"
            if passed
            else f"Passenger null rate {rate:.2%} exceeds 5% threshold ({nulls}/{total} trips)"
        ),
        metadata={
            "rows": dg.MetadataValue.int(total),
            "null_passengers": dg.MetadataValue.int(nulls),
            "null_rate": dg.MetadataValue.float(rate),
        },
    )
