"""S1: the S3 sensor — watches the data bucket's trips/ prefix and starts runs.

When a month's file appears (or its ETag changes — the instructor could replace a file),
the sensor adds the month to the trip_months partitions and requests one run that goes
from trip_files all the way through to zone_hour_stats.

The cursor is a JSON dict: {month: etag}. Using the ETag (not just presence) means that
if the instructor uploads a corrected file the sensor catches it and re-runs the month.

Patterns: pipelines/reference/patterns.py.
"""

import json

import dagster as dg

from ..partitions import trip_months
from ..resources import DataBucket

# The job covers every asset from trip_files to zone_hour_stats. We select by key so that
# adding new assets later doesn't silently widen the job beyond ingest.
_INGEST_ASSETS = dg.AssetSelection.assets(
    "trip_files", "raw_trips", "stg_trips", "zone_hour_stats"
) | dg.AssetSelection.checks_for_assets("trip_files")

trip_files_job = dg.define_asset_job(
    "trip_files_job",
    selection=_INGEST_ASSETS,
)


@dg.sensor(job=trip_files_job, minimum_interval_seconds=60)
def trip_files_sensor(
    context: dg.SensorEvaluationContext,
    data_bucket: DataBucket,
) -> dg.SensorResult | dg.SkipReason:
    """Add new/changed months to trip_months and request a run per month.

    The cursor is a JSON dict mapping month → etag. A month is (re-)processed when:
      - it has never been seen before, or
      - its ETag has changed (instructor replaced the file).

    run_key = f"{month}:{etag}" makes a repeated sensor tick for the same (month, etag)
    pair a no-op — Dagster deduplicates by run_key so we never double-submit.
    """
    seen: dict[str, str] = json.loads(context.cursor or "{}")

    files = data_bucket.trip_files()
    new_months: list[str] = []
    run_requests: list[dg.RunRequest] = []

    for f in files:
        if seen.get(f.month) != f.etag:
            new_months.append(f.month)
            run_requests.append(
                dg.RunRequest(
                    partition_key=f.month,
                    run_key=f"{f.month}:{f.etag}",
                )
            )

    if not run_requests:
        return dg.SkipReason("No new or changed trip files.")

    new_cursor = {**seen, **{f.month: f.etag for f in files}}

    return dg.SensorResult(
        run_requests=run_requests,
        dynamic_partitions_requests=[trip_months.build_add_request(new_months)],
        cursor=json.dumps(new_cursor),
    )
