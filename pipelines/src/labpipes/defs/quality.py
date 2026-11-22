"""S1: data-quality checks on stg_trips (nulls, ranges, duplicates, row counts).

Decision point (decisions/006-blocking-vs-warning.md):
  - zone_ids_in_range is BLOCKING (ERROR): TLC zone IDs are 1..265, where 264 = "Unknown" and
    265 = "Outside of NYC" are official codes. A null or any other ID on a pickup or dropoff means the
    source file is malformed. 2024-01..04 have zero such rows.
  - unknown_location_share is NON-BLOCKING (WARN): the share of trips at 264/265 is normal at
    0.37-0.41% (pickups) and 0.86-0.95% (dropoffs) for 2024-01..04; warn above 1% / 2%, which
    signals an upstream location (GPS) problem. trip_features drops 264/265 before training.
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
        # A null ID is invalid too: `null not between ...` is null, which a plain case would skip.
        "sum(case when pickup_zone_id is null or pickup_zone_id not between 1 and 265 then 1 else 0 end) as invalid_pickup, "
        "sum(case when dropoff_zone_id is null or dropoff_zone_id not between 1 and 265 then 1 else 0 end) as invalid_dropoff, "
        "sum(case when pickup_zone_id in (264, 265) then 1 else 0 end) as unknown_pickup, "
        "sum(case when dropoff_zone_id in (264, 265) then 1 else 0 end) as unknown_dropoff "
        f"from staging.stg_trips where trip_month = '{month}'"
    )
    res = redshift.query(sql)
    row = res[0] if res else {}
    return {k: int(row.get(k) or 0) for k in _STATS}


_STATS = ("rows", "nonpositive_duration", "over_3h", "null_passengers",
          "invalid_pickup", "invalid_dropoff", "unknown_pickup", "unknown_dropoff")

# Decision 006: warn when the share of trips at zone 264/265 ("Unknown", "Outside of NYC") is above
# these, about 2.5x the 2024-01..04 baseline.
UNKNOWN_PICKUP_WARN = 0.01
UNKNOWN_DROPOFF_WARN = 0.02


@dg.asset_check(
    asset="stg_trips",
    partitions_def=trip_months,
    blocking=True,
    description="Every pickup and dropoff zone ID is a TLC ID (1..265, not null). Blocking check.",
)
def zone_ids_in_range(
    context: dg.AssetCheckExecutionContext,
    redshift: RedshiftData,
) -> dg.AssetCheckResult:
    month = context.partition_key
    stats = _stg_trip_stats(redshift, month)
    bad_pu, bad_do = stats["invalid_pickup"], stats["invalid_dropoff"]
    passed = bad_pu == 0 and bad_do == 0

    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.ERROR,
        description=(
            "All zone IDs within 1..265"
            if passed
            else f"{bad_pu} pickup and {bad_do} dropoff zone ID(s) null or outside 1..265"
        ),
        metadata={
            "rows": dg.MetadataValue.int(stats["rows"]),
            "invalid_pickup": dg.MetadataValue.int(bad_pu),
            "invalid_dropoff": dg.MetadataValue.int(bad_do),
        },
    )


@dg.asset_check(
    asset="stg_trips",
    partitions_def=trip_months,
    blocking=False,
    description="Share of trips at zone 264/265 (Unknown, Outside NYC). Warns above 1% pickups / 2% dropoffs.",
)
def unknown_location_share(
    context: dg.AssetCheckExecutionContext,
    redshift: RedshiftData,
) -> dg.AssetCheckResult:
    month = context.partition_key
    stats = _stg_trip_stats(redshift, month)
    total = stats["rows"]
    pu = stats["unknown_pickup"] / total if total else 0.0
    do = stats["unknown_dropoff"] / total if total else 0.0
    passed = pu <= UNKNOWN_PICKUP_WARN and do <= UNKNOWN_DROPOFF_WARN

    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.WARN,
        description=(
            f"Unknown-location share: pickups {pu:.2%} (warn > {UNKNOWN_PICKUP_WARN:.0%}), "
            f"dropoffs {do:.2%} (warn > {UNKNOWN_DROPOFF_WARN:.0%})"
        ),
        metadata={
            "rows": dg.MetadataValue.int(total),
            "unknown_pickup": dg.MetadataValue.int(stats["unknown_pickup"]),
            "unknown_dropoff": dg.MetadataValue.int(stats["unknown_dropoff"]),
            "unknown_pickup_rate": dg.MetadataValue.float(pu),
            "unknown_dropoff_rate": dg.MetadataValue.float(do),
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
