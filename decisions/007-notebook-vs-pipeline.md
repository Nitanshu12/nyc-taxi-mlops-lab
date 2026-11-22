# 007: What stays in the notebook vs moves into the pipeline

Stage: S2 · Date: 2026-11-22 · Author: Nitanshu12 · Drafted by agent: yes

## Choice

Exploratory data analysis, prototype model comparisons, visual distributions, and interactive debugging stay in the SageMaker notebook (`notebooks/explore.ipynb`). Feature generation, feature contract validation, model fitting, holdout evaluation, gating, and MLflow registration move into the automated Dagster pipeline (`training.py`).

## Rejected alternative

Training and registering models directly from Jupyter notebook cells, using notebooks as the execution engine for production model artifacts.

## Why

- **Reproducibility & Lineage:** Notebooks have non-linear execution state; cells run out of order can produce unrepeatable model weights. Dagster asset graphs track exact code commits, data inputs, and execution lineage.
- **Contract Enforcement & Safety:** Ingestion of post-trip forbidden columns (target leakage) is strictly guarded by the pipeline's blocking `feature_contract` check. A manual notebook registration could bypass these checks and register an invalid model directly.
- **Automation & Aliasing:** Production deployment requires automated evaluation against the current champion and setting the `challenger` alias automatically. Manual alias tagging is error-prone.

Trade-offs accepted:
- Slower iteration cycle when modifying pipeline code (requires code commit and Dagster execution vs immediate in-memory cell execution in Jupyter).
- Debugging pipeline code requires inspecting Dagster asset logs rather than interactive cell state.

## What would show this was wrong

- Fast-paced experimentation is blocked because routine hyperparameter sweeps require full pipeline rebuilds rather than quick interactive iterations.
