# 008: Promotion metric and margin

Stage: S2 · Date: 2026-11-22 · Author: Nitanshu12 · Drafted by agent: yes

## Choice

Promote a model to `challenger` if its holdout Mean Absolute Error (`mae_min`) strictly beats the current champion (or v1 baseline if no champion exists) on the same holdout month (`candidate_mae < champion_mae`), while satisfying a p95 single-row inference latency budget of $\le 50\text{ ms}$.

## Rejected alternative

- Using RMSE (Root Mean Squared Error) as the promotion metric.
- Requiring an aggressive margin threshold (e.g. at least 0.5 minutes better) before qualifying as challenger.
- Evaluating only accuracy without an explicit latency ceiling.

## Why

- **MAE vs RMSE:** NYC taxi trips have a heavy right tail (traffic jams, bridge closures, and weather anomalies causing trips up to 180 minutes). RMSE squares errors, causing models to over-index on rare extreme outliers at the expense of typical daily trips. MAE reflects the passenger's intuitive expectation: "how many minutes off is my ETA on an average trip?"
- **Strict Improvement (`<`):** Any genuine improvement in holdout MAE without latency regression warrants challenger consideration. Setting a high fixed margin (e.g. 0.5 min) would reject incremental gains in an already mature model.
- **Latency Budget:** Production serving must respond in real time (within user request SLAs). A model with slightly lower MAE but 500ms latency cannot be served; the 50ms p95 latency gate protects production service SLOs.

Trade-offs accepted:
- Tiny improvements (e.g. 0.01 min MAE) qualify as challenger, potentially causing frequent alias updates.
- Extreme outliers are not penalized heavily by the L1 objective.

## What would show this was wrong

- A candidate model beats champion on average MAE but suffers severe error spikes on airport or night trips that degrade user experience.
- Single-row latency passes the 50ms budget on the platform host but causes CPU credit exhaustion or timeout under concurrent serving load.
