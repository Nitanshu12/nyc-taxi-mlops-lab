# 004: COPY vs Spectrum for trip data ingestion

Stage: S1 · Date: 2026-11-22 · Author: Nitanshu12 · Drafted by agent: yes

## Choice

Load monthly trip Parquet files from S3 directly into Redshift managed storage using the `COPY` command via the Redshift Data API, running inside an idempotent transaction that deletes any existing rows for that `source_file` before copying.

## Rejected alternative

Redshift Spectrum (querying external S3 tables directly without loading). Spectrum leaves the Parquet files in S3 and uses an external catalog, avoiding physical data movement and Redshift storage charges.

## Why

- **Query Performance & Latency:** Once data is copied into Redshift managed storage, downstream staging transformations (`stg_trips`) and mart aggregations (`zone_hour_stats`) run as local columnar joins without repeated S3 scans or network round-trips.
- **Predictable Cost:** In AWS Learner Lab, data volume is small (<5 GB/month across 4 months). Redshift cluster storage is already paid for as part of the allocated `ra3.large` instance, whereas Spectrum incurs a per-query scan charge ($5 per TB scanned). With frequent re-runs and pipeline development, Spectrum scan costs can accumulate.
- **Operational Simplicity:** Avoids external schema, glue catalog, and extra external table IAM plumbing. Idempotency is cleanly handled with a `DELETE WHERE source_file = ...` followed by `COPY` in a single Data API batch transaction.

Trade-offs accepted:
- Data is duplicated between S3 and Redshift storage.
- Loading takes an explicit step during ingestion rather than immediate zero-ETL querying.

## What would show this was wrong

- Redshift cluster storage capacity approaches limits (>80% disk utilization), making local storage too costly or unviable.
- Ingestion times become a bottleneck compared to instantaneous external table registration.
- S3 data volume scales up to hundreds of gigabytes where per-query Spectrum pricing would be cheaper than provisioning additional Redshift cluster storage.
