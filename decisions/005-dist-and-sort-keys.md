# 005: Distribution and sort keys for trip tables

Stage: S1 · Date: 2026-11-22 · Author: Nitanshu12 · Drafted by agent: yes

## Choice

Distribute `ingest.raw_trips` on `pulocationid` (`distkey(pulocationid)`) and sort on `tpep_pickup_datetime` (`sortkey(tpep_pickup_datetime)`).

## Rejected alternative

- **Distribution:** `DISTSTYLE EVEN` (round-robin distribution across all cluster slices).
- **Sort Key:** `sortkey(source_file)` (clustering rows by file name to optimize per-month partition deletion).

## Why

- **Distribution on `pulocationid`:** The primary consumers of trip data are the downstream joins against `taxi_zones` (on `location_id = pulocationid`) and the aggregation in `zone_hour_stats` (grouped by `pickup_zone_id`). Distributing rows by `pulocationid` co-locates rows for the same pickup zone on the same slice, eliminating expensive cross-slice network data redistribution during join and aggregation steps.
- **Sort Key on `tpep_pickup_datetime`:** Staging queries (`stg_trips`) filter by date/time ranges. Sorting by pickup timestamp allows Redshift zone maps (min/max metadata per 1MB disk block) to skip irrelevant disk blocks during range scans.
- `source_file` as sort key was rejected because month-level DELETE operations only occur once per reload, whereas datetime-filtered queries and extracts occur frequently during staging and feature generation.

Trade-offs accepted:
- Potential data skew: high-volume pickup zones (such as Midtown Manhattan or JFK Airport) may cause non-uniform distribution across slices compared to `DISTSTYLE EVEN`.
- Slower `DELETE WHERE source_file = ...` than if `source_file` were the sort key.

## What would show this was wrong

- `svv_diskusage` or query execution alerts show severe slice skew (>2x row count on the most loaded slice compared to average) due to dominant taxi zones, degrading parallel performance.
- Staging and reload operations spend more time in DELETE block cleaning than saved in subsequent datetime range scans.
