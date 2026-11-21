# 002: Runner credentials

Stage: S0 · Date: 2026-11-21 · Author: Nitanshu12 · Drafted by agent: yes

## Choice

The self-hosted runners use the host's instance profile (`LabInstanceProfile`, i.e. `LabRole`) for
AWS. No AWS keys are stored in GitHub secrets or on the hosts; jobs get short-lived credentials from
the instance metadata service (IMDSv2), and the AWS CLI and SDKs renew them automatically.

## Rejected alternative

Static Learner Lab session credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_SESSION_TOKEN`) in GitHub secrets, via `scripts/gh-secrets.sh`. They give the same `LabRole`
permissions, but:
- they expire when the lab session ends (about every 4 hours), so every Start Lab means re-running
  `gh-secrets.sh`, and any deploy in between fails with an expired-token error (the SDK does not fall
  back to the instance profile when expired keys are in the environment);
- they put a copy of the keys in a public repo's secrets, readable by any workflow job that
  references them;
- they would not remove the instance profile: the host keeps it either way, so they add a second
  set of keys rather than replacing the first.

## Why

- Survives session refreshes: the profile stays attached to the host, which keeps getting fresh
  keys after Start Lab, so CD keeps working with nothing for me to refill.
- Nothing secret is copied or stored anywhere, so there is nothing to leak from GitHub or the repo.
- It matches the given platform and serving workflows, whose self-hosted deploy jobs use no AWS
  secrets.

Trade-off accepted: anything that runs on a host can get `LabRole` keys, including every job on its
runner and, because the metadata hop limit is 2, every container (the app container answers internet
traffic). What limits who can run there:
- fork-PR workflows need my approval for all external contributors;
- `make check` forbids self-hosted jobs on `pull_request`, and `cd.yaml` only deploys successful
  pushes from this repo (`workflow_run.event` and `head_repository` checked);
- IMDSv2 is required, so a simple request forgery in the app can't read the keys.

## What would show this was wrong

- AWS API calls from a host that none of my workflows or services made (CloudTrail, if available).
- A job that needs fewer permissions than `LabRole` and should not have the rest: one role per
  host can't give different jobs different access.
- A vulnerability or compromised dependency in a container on a host, since it could reach the keys.
- Moving off Learner Lab: there, OIDC federation (per-repo, per-branch role, nothing stored) would
  be the option to revisit.
