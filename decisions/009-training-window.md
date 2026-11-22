# 009: Training window and holdout split

Stage: S2 · Date: 2026-11-22 · Author: Nitanshu12 · Drafted by agent: yes

## Choice

Use an expanding multi-month chronological window for training (e.g. 2024-01 through 2024-03) and hold out the immediately following chronological month (2024-04) as the evaluation test set.

## Rejected alternative

Random train/test split across all available months (e.g., shuffling 2024-01..04 and taking a random 80% train / 20% test split).

## Why

- **Temporal Validity (No Data Leakage):** Taxi demand, weather patterns, school schedules, congestion pricing, and city construction shift over time. In production, a model is always trained on historical data to predict future trips. Random splitting leaks future temporal structure into the training set, giving falsely optimistic offline evaluation metrics.
- **True Holdout Simulation:** Evaluating strictly on the subsequent month tests the model's ability to generalize to temporal distribution shift and seasonality changes.
- **Distance Lookup Alignment:** The zone-pair distance lookup (`est_distance_miles`) is built strictly from the training window to prevent information leakage from the holdout period into the feature lookup.

Trade-offs accepted:
- If the holdout month has unique seasonal events (e.g. spring holidays in April) not seen in winter training months (Jan-Mar), performance metrics will drop.
- Training set size is constrained to preceding months rather than utilizing 100% of historical rows for fitting.

## What would show this was wrong

- Traffic patterns undergo abrupt structural breaks (e.g., pandemic restrictions or new congestion tolling), rendering models trained on older months completely unrepresentative of future months without shorter rolling windows.
