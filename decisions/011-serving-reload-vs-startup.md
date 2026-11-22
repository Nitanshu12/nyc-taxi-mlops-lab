# 011: How serving picks up a new champion

Stage: S3 · Date: 2026-11-22 · Author: Nitanshu12 · Drafted by agent: yes

## Choice

Serving **reloads on an alias change**. `promote.yaml` moves the `champion` alias, then calls `POST /reload` on `serving-champion` (`deploy/web.sh reload champion`). The old model keeps answering until the new one has loaded, then the swap happens. The workflow finishes by checking that `/predict` through the app answers with `model.alias == "champion"` and the promoted version.

## Rejected alternative

Load only at container start, and restart `serving-champion` after promotion.

## Why

- A restart leaves a window (the model has to download and load) where champion has no model and answers 503. The app then falls back to the static median, so riders get a worse quote during every promotion. Reload has no such window.
- The alias is the source of truth and serving follows it, so promotion and rollback of a model are the same move (alias, then reload).

Trade-offs accepted:
- Two steps can disagree: the alias may point at version N while serving still holds N-1 if `/reload` fails. The promote workflow checks `/predict` and fails loudly, but nothing re-reconciles later; a retry is manual.
- Serving holds two models in memory while it swaps.
- I trust `/reload` to do what it says; a restart would make "what's loaded" obvious from boot.

## What would show this was wrong

- `/predict` shows a version different from the `champion` alias in MLflow after a promotion run reported success.
- A reload fails halfway and the served model is neither version.
- Memory pressure on the app host during the swap.
