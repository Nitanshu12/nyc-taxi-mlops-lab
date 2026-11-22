"""schema_contract: blocking asset check on trip_files.

Compares the Parquet footer of a landed file against contracts/trips.schema.yaml before
raw_trips copies anything. Redshift COPY maps Parquet columns by position, so a column-count
or column-type mismatch would load wrong data silently. This check prevents that.

Decision point: what counts as a mismatch?
  - Strict (names + types + count + order): any change is a failure. Safe, but extra columns
    at the end (e.g. 2025's cbd_congestion_fee) would block ingestion until you evolve the
    contract — that is a decision, not a fix.
  - Name-only (count + case-insensitive names, order-sensitive): types can drift if the
    physical representation stays compatible (e.g. INT32 → INT64 for VendorID); counts and
    order must match because COPY is positional.
  - Count-only: the lightest check, fails only on added/removed columns; renamed or retyped
    columns pass silently — risky.

The implementation here uses strict matching (names, types, count, order): any difference
surfaces immediately so the student decides whether to evolve the contract. The severity is
ERROR so the check blocks downstream assets (platform.yaml: blocking_checks.trip_files).
"""

import dagster as dg

from ..contracts import trips_schema
from ..partitions import trip_months
from ..resources import DataBucket


@dg.asset_check(
    asset="trip_files",
    partitions_def=trip_months,
    blocking=True,
    description="Parquet footer matches contracts/trips.schema.yaml (names, types, order).",
)
def schema_contract(
    context: dg.AssetCheckExecutionContext,
    data_bucket: DataBucket,
) -> dg.AssetCheckResult:
    """Compare the file's Parquet footer with the frozen schema contract.

    Passes only when the footer's columns match the contract exactly: same count, same names
    (case-insensitive), same Parquet storage types, in the same order. Extra trailing columns
    (e.g. a new TLC field) are a failure — they are a schema evolution that the student records
    and decides how to handle.
    """
    month = context.partition_key
    footer = data_bucket.footer(month)
    expected = trips_schema()

    mismatches: list[str] = []

    if len(footer.columns) != len(expected):
        mismatches.append(
            f"column count: got {len(footer.columns)}, expected {len(expected)}"
        )
    else:
        for i, (got, exp) in enumerate(zip(footer.columns, expected, strict=True)):
            if got.name.lower() != exp.name.lower():
                mismatches.append(
                    f"col[{i}] name: got '{got.name}', expected '{exp.name}'"
                )
            if got.type != exp.type:
                mismatches.append(
                    f"col[{i}] ({got.name}) type: got '{got.type}', expected '{exp.type}'"
                )

    passed = len(mismatches) == 0
    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.ERROR,
        description=(
            "Schema matches the contract."
            if passed
            else f"{len(mismatches)} mismatch(es): " + "; ".join(mismatches[:3])
        ),
        metadata={
            "num_rows": dg.MetadataValue.int(footer.num_rows),
            "num_columns": dg.MetadataValue.int(len(footer.columns)),
            "mismatches": dg.MetadataValue.int(len(mismatches)),
            "details": dg.MetadataValue.json(mismatches),
        },
    )
