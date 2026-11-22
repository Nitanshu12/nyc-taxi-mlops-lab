# 006: Data-quality checks on stg_trips: blocking vs warning

Stage: S1 · Date: 2026-11-22 · Author: Nitanshu12 · Drafted by agent: yes

## Choice

On `staging.stg_trips`, categorize data-quality checks into two tiers:
1. **Blocking (`severity=ERROR`, `blocking=True`):** `zone_ids_in_range` (fails if any trip has a null `pickup_zone_id` or `dropoff_zone_id`, or one outside TLC's 1..265).
2. **Warning (`severity=WARN`, `blocking=False`):**
   - `unknown_location_share`: warns if trips at zone 264 ("Unknown") or 265 ("Outside of NYC") exceed 1% of pickups or 2% of dropoffs.
   - `plausible_trip_durations`: warns if trips with duration $\le 0$ or $> 180$ minutes exceed 1% of total monthly trips.
   - `passenger_counts`: warns if null passenger counts exceed 5% of monthly rows.

> **Revised in S2 (2026-11-22).** The S1 version blocked on any pickup outside 1..263. Ingest of 2024-01 stopped on it: 12,018 of 2,964,624 pickups (0.41%) are zone 264 or 265, which TLC uses on purpose for unknown and out-of-city locations. The rule treated valid codes as corruption, so `trip_features` and training never ran.

## Rejected alternative

- **All-blocking:** Failing and stopping the pipeline if even a single trip has non-positive duration, >3h duration, or null passenger count.
- **All-warning:** Marking even unknown pickup zones as non-blocking warnings.
- **(S2) Filter 264/265 out of `stg_trips` and keep the 1..263 rule.** Rejected: `stg_trips.sql` is unfiltered by design so checks can see bad rows; filtering would hide a 264 spike (a GPS outage) from every check and shrink the denominators of the other checks and the `zone_hour_stats` mart.
- **(S2) One blocking check above a rate (e.g. > 1% outside 1..263).** Rejected: it can't tell an official "unknown" code from a garbage ID, so real corruption below the rate would pass.

## Why

- **Impossible zone IDs must block; official "unknown" codes must not.** TLC's lookup has IDs 1..265: 263 physical zones plus 264 "Unknown" and 265 "Outside of NYC". Measured in the source files for 2024-01..04: zero null or out-of-range IDs (pickup or dropoff), so the strict check passes on real data yet stops the moment a malformed file arrives. A null ID now counts as invalid; the S1 query's `not between` silently skipped nulls.
- **The unknown share is stable, so a deviation is the signal.** Pickups at 264/265: 0.41%, 0.37%, 0.38%, 0.37%; dropoffs: 0.95%, 0.86%, 0.86%, 0.88%. Warning at 1% / 2% (about 2.5x baseline) ignores month-to-month wobble but flags a clear upstream location problem. It warns rather than blocks because `trip_features` drops 264/265 for both pickup and dropoff, so the model never trains on them either way.
- **Duration glitches are inherent in raw TLC data:** Meter reset errors and GPS signal loss produce small numbers of negative or zero-duration trips in every real-world TLC release (typically 0.05% - 0.2%). Downstream transformations in `zone_hour_stats.sql` explicitly filter these rows out (`where s.duration_min > 0 and s.duration_min <= 180 and s.pickup_zone_id between 1 and 263`). Blocking the monthly ingestion on a handful of sensor glitches would halt data delivery unnecessarily; setting a 1% threshold ensures abnormal corruption trips an alert while normal operational noise passes.
- **Null passenger counts are non-critical for duration modeling:** Trip duration models primarily rely on pickup/dropoff zones, time of day, and day of week. While high missingness (>5%) indicates a logging malfunction worth flagging, isolated nulls do not justify blocking staging.

Trade-offs accepted:
- If duration glitch rates are between 0% and 1%, bad rows land in staging, though they are pruned by mart DDL queries.
- Operational burden of reviewing WARN-level check results in Dagster asset check logs.

## What would show this was wrong

- A gameday corruption scenario introduces silent feature distribution shift below the 1% threshold that severely impacts model training metrics.
- TLC adds zones beyond 265, and `zone_ids_in_range` blocks valid new data until the check and `contracts/features.yaml` are evolved.
- The unknown share drifts slowly from 0.4% toward 1% without ever warning, while slice MAE (airport, outer boroughs) worsens: then the threshold is too loose, or the check should compare against the trailing months instead of a fixed number.
- Nobody reads the WARN results in Dagster, so a warning protects nothing in practice.
