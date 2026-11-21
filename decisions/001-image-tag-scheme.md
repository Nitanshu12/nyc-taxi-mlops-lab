# 001: Image tag scheme

Stage: S0 · Date: 2026-11-21 · Author: Nitanshu12 · Drafted by agent: yes

## Choice

Every app image CI pushes to GHCR is tagged `<CI run number>-<7-char commit sha>`, e.g. `42-5a5c93e`.
CI hands the tag to CD as the `image-tag` artifact, and a rollback redeploys an older tag of this form
(`gh workflow run cd.yaml -f image_tag=41-1a2b3c4`). No moving tag (`latest`, `main`) is pushed.

## Rejected alternative

`sha-<7>` alone (what the given platform and serving workflows use). It is just as immutable and CD could compute it from the commit without a hand-off,
but the tag says nothing about build order, so finding "the previous good image" needs `git log`.

## Why

- Rollback is simple: the number shows build order at a glance, so the previous image is easy to find
  under pressure. The sha part still ties each tag to an exact commit, and `/version` shows both.
- CI passes the tag to CD instead of CD recomputing it, so CD deploys exactly what CI built and
  reported.
- Tags never move: a re-run of CI keeps its run number, so CI skips the build when the tag already
  exists rather than overwriting it.

Trade-offs accepted:
- Numbers have gaps: the counter rises for every CI run (other branches, PRs), not only for images.
- The app's tags differ from the `sha-<7>` tags of the pipelines and serving images.
- CD depends on CI's artifact (kept 30 days); a rollback names the tag by hand instead.

## What would show this was wrong

- The `ci` workflow is renamed or recreated: the run number restarts at 1 and tags stop sorting.
- A rollback picks the wrong image because someone read "41" as "the one before 42".
- CD fails because CI's `image-tag` artifact is missing or expired.
