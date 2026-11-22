"""raw_trips: one month of TLC trip data, COPYed from S3 into ingest.raw_trips.

One partition per month (partitions.trip_months), depending on trip_files. The landing table
mirrors the Parquet schema (contracts/trips.schema.yaml) column-for-column so positional COPY
works. Re-running a month deletes its rows first (delete + COPY in one transaction) so there
are no duplicates. LabRole is the cluster's default IAM role, so COPY says `iam_role default`.

Decision points (record in decisions/):
  - COPY vs Spectrum: COPY materialises into Redshift managed storage; Spectrum queries S3 in
    place. COPY: storage cost per GB, but staging queries are fast local joins with no S3
    round-trips and no per-query scan charge. Spectrum: no storage cost, but each staging run
    re-reads S3 (cost: $ per TB scanned) and latency is higher. For repeated staging runs on
    a small dataset COPY is simpler and predictably cheap.
  - Dist key: pulocationid (pickup zone) is the join key with taxi_zones in the mart and in
    stg_trips. Distributing on it co-locates that join. Alternative: EVEN distribution avoids
    skew if one zone dominates; profile before choosing.
  - Sort key: tpep_pickup_datetime lets Redshift skip blocks when stg_trips filters rows by
    month (a datetime range). Alternative: source_file, to cluster rows for the per-file delete.
    The datetime sort wins if reads outweigh deletes.
"""

import dagster as dg

from ..contracts import redshift_columns, trips_schema
from ..partitions import check_month, source_file, trip_months
from ..resources import DataBucket, RedshiftData

# Column definitions (lowercase names, Redshift types) from the frozen contract.
_PARQUET_COLS = redshift_columns(trips_schema())
# Comma-separated lowercase column names, same order as the Parquet file.
_COL_NAMES = ", ".join(c.name.lower() for c in trips_schema())


@dg.asset(
    partitions_def=trip_months,
    deps=["trip_files"],
    group_name="ingest",
    kinds={"redshift", "s3"},
    pool="redshift",
)
def raw_trips(
    context: dg.AssetExecutionContext,
    redshift: RedshiftData,
    data_bucket: DataBucket,
) -> dg.MaterializeResult:
    """COPY one month's Parquet file from S3 into ingest.raw_trips (one transaction).

    The table also carries a source_file column (not in the Parquet file) so stg_trips can
    filter by file without scanning raw_trips in full. Redshift COPY maps Parquet columns by
    position, so source_file is appended to the table after the Parquet columns and left NULL
    during COPY, then updated in the same transaction.
    """
    month = context.partition_key
    check_month(month)
    uri = data_bucket.trip_uri(month)
    file = source_file(month)

    redshift.execute(
        # 1. Schema and table (idempotent DDL).
        "create schema if not exists ingest",
        (
            f"create table if not exists ingest.raw_trips ({_PARQUET_COLS}, "
            "source_file varchar(64)) "
            "distkey(pulocationid) sortkey(tpep_pickup_datetime)"
        ),
        # 2. Delete this month's rows so a re-run never duplicates data.
        f"delete from ingest.raw_trips where source_file = '{file}'",
        # 3. COPY the Parquet file by position (column names are ignored by Redshift COPY for
        #    Parquet; source_file gets NULL because there is no matching Parquet column).
        f"copy ingest.raw_trips ({_COL_NAMES}, source_file) "
        f"from '{uri}' iam_role default format as parquet",
        # 4. Stamp every newly-loaded row with its source file name (source_file is NULL from
        #    step 3 because Parquet COPY can only map by name, not inject constants).
        f"update ingest.raw_trips set source_file = '{file}' where source_file is null",
    )

    rows = redshift.scalar(
        f"select count(*) from ingest.raw_trips where source_file = '{file}'"
    )
    return dg.MaterializeResult(
        metadata={
            "rows": dg.MetadataValue.int(int(rows)),
            "uri": uri,
            "source_file": file,
        }
    )
