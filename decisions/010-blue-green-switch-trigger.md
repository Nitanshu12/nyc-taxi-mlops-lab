# 010: What triggers the blue-green switch

Stage: S3 · Date: 2026-11-22 · Author: Nitanshu12 · Drafted by agent: yes

## Choice

CD deploys the new image to the idle colour and smoke-tests it through Traefik (`deploy/web.sh smoke`: `/version` must show the commit CI built, one `/predict` must answer). It then **waits for a reviewer on the `production` environment** before `deploy/web.sh switch` sends traffic to the new colour. The old colour keeps running. Rollback (`rollback.yaml`) switches back with no approval and no deploy.

Exception: the first run on this stack has no live colour (the S0 stack is gone), so it switches straight after the smoke test. There is nothing live to protect.

## Rejected alternative

Switch automatically as soon as the smoke test passes.

## Why

- The smoke test is shallow: it proves the container starts, reports the right commit and answers one valid request. It cannot tell that a change made quotes worse, or that the model fallback is quietly answering instead of the champion. A person looking at the deploy log before riders see the new version catches things the test can't.
- Rollback is cheap and unreviewed, which is what makes a human gate on the way in affordable: a wrong approval costs one command, not an outage.
- The approval step is the same `production` environment promotion uses, so there is one place where a person says "yes, this goes to riders".

Trade-offs accepted:
- Every release waits for me. A push at a bad moment sits with the new colour deployed but idle until I approve, so "merged" and "live" are different things.
- The reviewer is also the author (`prevent_self_review: false`), so this is a deliberate pause, not a second pair of eyes.

## What would show this was wrong

- I approve every deploy within seconds without reading the log: the gate is ritual, and the checks should be made stricter (latency, the answering alias) and the approval dropped.
- A release that passed the smoke test and my approval still degraded quotes: the smoke test needs a real check on the answer.
- Approvals pile up unattended and fixes reach riders late.
