# 013: SLO targets

Stage: S5 · Date: 2026-11-22 · Author: Nitanshu12 · Drafted by agent: yes

## Choice

Two SLIs, each with a 99.5% objective over 30 days (an error budget of 0.5%):

1. **predict-availability**: the share of `/predict` requests the app did not answer with a 5xx.
2. **predict-model-answered**: the share of quotes the model gave rather than the static-median fallback.

Each pages on a **fast burn**: the error ratio above 14.4x the budget (7.2%) on the last hour **and** the last 5 minutes, held for 1 minute, and only when there were at least 10 requests (or quotes) in the last 5 minutes. Rules and unit tests: `observability/rules/slo.rules.yaml`, `slo.rules.test.yaml`. No latency SLI.

## Rejected alternative

- **Availability alone.** When serving is down the app falls back to the static median and still answers 200, so this SLI stays green while no rider gets a model's quote. Unit test 4 shows exactly that: only the model-answered alert fires.
- **A 99.9% objective** (43 minutes of budget a month). This lab's traffic is tiny, so a single failed request in a quiet hour would break it, and a bad deploy or a model reload would burn most of the budget.
- **A latency SLI** (the three-SLI option). Not chosen to keep the rule set small: a slow serving shows up anyway, because past the app's 0.3 s timeout the quote becomes a fallback and counts against SLI 2.
- **Burn windows of 5m + 1m**, which page in about a minute but also on small blips; and the **two-stage** page plus ticket, which needs twice the rules and tests.

## Why

- 99.5% leaves about 3 h 36 min a month: room for a deploy or two going wrong, not for a day of it. It is strict enough to matter and loose enough that normal reloads don't page.
- The second SLI exists because the first cannot see the failure this system is most likely to have. The fallback is what keeps riders answered, and it is also what hides serving problems behind a 200.
- Two windows must agree: the 1h window needs about 4.3 minutes of total outage to cross 7.2%, so a 3-minute spike (5% of the hour) does not page (unit test 2), while the 5m window makes the alert stop once the problem is fixed.
- The traffic guard avoids a page at 1 failed request out of 2 (unit test 3). Cost: with fewer than 10 requests in 5 minutes, a real outage stays silent.

Trade-offs accepted:
- No latency SLO: a slowdown that stays under the 0.3 s timeout and returns a model answer is invisible to the alerts (the dashboard still shows it).
- A page takes about 5 to 6 minutes after a total outage starts; a partial failure of under 7.2% never pages, however long it lasts.
- The `$value` shown in the alert summary is the 1-hour ratio (the first term of the expression), not the 5-minute one. A unit test caught my first wording ("over 5m") being wrong.

## What would show this was wrong

- An incident a rider noticed that no alert caught: for instance a slowdown under the timeout, or a partial failure under 7.2%. That argues for a latency SLI or a slow-burn alert.
- The alert fires on deploys or model reloads with no real problem, so the objective or the windows are too tight.
- The traffic guard hides a real outage, because traffic really is below 10 requests in 5 minutes at the time it matters.
- The 30-day budget is never touched, so 99.5% is looser than needed.
