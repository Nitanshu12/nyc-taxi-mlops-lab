"""S2: the feature_contract check on trip_features.

Enforces contracts/features.yaml:
- Columns in staging.trip_features match the contract (keys, inputs, label).
- None of the forbidden post-trip columns (e.g. trip_distance, fare_amount) are present.
- Values are within the specified ranges (zones 1..263, hours 0..23, dow 0..6, duration 0..180).
contracts/mlflow.yaml says this check blocks.
"""

import dagster as dg

from ..contracts import features_contract
from ..partitions import check_month, trip_months
from ..resources import RedshiftData


@dg.asset_check(
    asset="trip_features",
    partitions_def=trip_months,
    blocking=True,
    description="Validates staging.trip_features schema, forbidden columns, and feature value ranges.",
)
def feature_contract(
    context: dg.AssetCheckExecutionContext,
    redshift: RedshiftData,
) -> dg.AssetCheckResult:
    month = context.partition_key
    check_month(month)
    contract = features_contract()

    # 1. Check columns for forbidden fields and unknown fields
    # Query one row to inspect column names
    sample = redshift.query(f"select * from staging.trip_features where trip_month = '{month}' limit 1")
    cols = set(sample[0].keys()) if sample else set()

    forbidden = set(contract.get("forbidden", []))
    found_forbidden = cols & forbidden

    allowed_cols = {
        *(k["name"] for k in contract.get("keys", [])),
        *(i["name"] for i in contract.get("inputs", [])),
        contract["label"]["name"],
    }

    mismatches: list[str] = []
    if found_forbidden:
        mismatches.append(f"Forbidden post-trip columns found: {sorted(found_forbidden)}")
    if cols and (cols - allowed_cols):
        mismatches.append(f"Unexpected columns found: {sorted(cols - allowed_cols)}")

    # 2. Check value ranges on the partition
    stats = redshift.query(
        f"select count(*) as rows, "
        f"sum(case when pickup_zone_id not between 1 and 263 then 1 else 0 end) as bad_pickup, "
        f"sum(case when dropoff_zone_id not between 1 and 263 then 1 else 0 end) as bad_dropoff, "
        f"sum(case when pickup_hour not between 0 and 23 then 1 else 0 end) as bad_hour, "
        f"sum(case when pickup_dow not between 0 and 6 then 1 else 0 end) as bad_dow, "
        f"sum(case when duration_min <= 0 or duration_min > 180 then 1 else 0 end) as bad_duration "
        f"from staging.trip_features where trip_month = '{month}'"
    )

    bad_rows = 0
    if stats:
        row = stats[0]
        bad_rows = sum(int(row[k]) for k in ("bad_pickup", "bad_dropoff", "bad_hour", "bad_dow", "bad_duration") if row.get(k) is not None)
        if bad_rows > 0:
            mismatches.append(f"{bad_rows} row(s) violate value ranges")

    passed = len(mismatches) == 0
    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.ERROR,
        description="Feature contract verified" if passed else "; ".join(mismatches),
        metadata={
            "columns": dg.MetadataValue.json(sorted(cols)),
            "mismatches": dg.MetadataValue.json(mismatches),
        },
    )
