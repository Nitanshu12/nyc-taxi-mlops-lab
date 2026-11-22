# 006: Data-quality checks on stg_trips: blocking vs warning

Stage: S1 · Date: 2026-11-22 · Author: Nitanshu12 · Drafted by agent: yes

## Choice

On `staging.stg_trips`, categorize data-quality checks into two tiers:
1. **Blocking (`severity=ERROR`, `blocking=True`):** `unknown_pickup_zones` (fails if any trip has `pickup_zone_id` outside 1..263).
2. **Warning (`severity=WARN`, `blocking=False`):**
   - `plausible_trip_durations`: warns if trips with duration $\le 0$ or $> 180$ minutes exceed 1% of total monthly trips.
   - `passenger_counts`: warns if null passenger counts exceed 5% of monthly rows.

## Rejected alternative

- **All-blocking:** Failing and stopping the pipeline if even a single trip has non-positive duration, >3h duration, or null passenger count.
- **All-warning:** Marking even unknown pickup zones as non-blocking warnings.

## Why

- **Unknown pickup zones must block:** TLC defines exactly 263 official taxi zones in New York City. An unknown zone ID indicates corruption in raw source ingestion or an unrecognized spatial identifier. If allowed downstream, these rows break the join against `ingest.taxi_zones` and corrupt the `zone_hour_stats` mart.
- **Duration glitches are inherent in raw TLC data:** Meter reset errors and GPS signal loss produce small numbers of negative or zero-duration trips in every real-world TLC release (typically 0.05% - 0.2%). Downstream transformations in `zone_hour_stats.sql` explicitly filter these rows out (`where s.duration_min > 0 and s.duration_min <= 180 and s.pickup_zone_id between 1 and 263`). Blocking the monthly ingestion on a handful of sensor glitches would halt data delivery unnecessarily; setting a 1% threshold ensures abnormal corruption trips an alert while normal operational noise passes.
- **Null passenger counts are non-critical for duration modeling:** Trip duration models primarily rely on pickup/dropoff zones, time of day, and day of week. While high missingness (>5%) indicates a logging malfunction worth flagging, isolated nulls do not justify blocking staging.

Trade-offs accepted:
- If duration glitch rates are between 0% and 1%, bad rows land in staging, though they are pruned by mart DDL queries.
- Operational burden of reviewing WARN-level check results in Dagster asset check logs.

## What would show this was wrong

- A gameday corruption scenario introduces silent feature distribution shift below the 1% threshold that severely impacts model training metrics.
- TLC updates the zone boundaries and introduces official zone 264, causing `unknown_pickup_zones` to block valid new data until the contract and check boundaries are formally evolved.
