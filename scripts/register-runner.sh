#!/usr/bin/env bash
# Register a lab host as a self-hosted GitHub Actions runner, through SSM (no SSH in this lab).
#
#   export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_SESSION_TOKEN=...   (AWS Details)
#   scripts/register-runner.sh            # the app host, label lab-app (S0)
#   scripts/register-runner.sh platform   # lab-platform (S1); llm -> lab-llm (S6)
#   scripts/register-runner.sh app --force   # wipe the host's runner config and register again
#
# The runner gets the labels self-hosted + lab-<role> (contracts/runtime.yaml) and runs as the
# `runner` user (created by the host's user-data, in the docker group). This script installs no AWS
# credentials on the host: jobs use the host's instance profile, LabInstanceProfile
# (decisions/002-runner-credentials.md).
#
# Refuses to run unless fork-PR workflows need approval for all external contributors: the repo
# is public and the runner is a door into your AWS account.
#
# Your laptop's AWS credentials are used only to send the SSM command. The registration token
# (single use, expires in an hour) goes to the host inside that command, so it is visible in this
# account's SSM command history until it expires; it is never printed here or written to the repo.
set -euo pipefail
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1 AWS_PAGER=""

RUNNER_VERSION=2.338.2
SHA_ARM64=670adb2f268fe21dcbb58b3db0e0aa9d5b3bddfe2c651481336e561ff86de945
SHA_X64=baebf204307bac6193480852732f0aba2ae97b6c5597c244f4d5558390147c40

role=${1:-app}
force=${2:-}
[[ "$role" =~ ^(app|platform|llm)$ ]] || { echo "usage: $0 [app|platform|llm] [--force]" >&2; exit 2; }
[[ -z "$force" || "$force" == "--force" ]] || { echo "usage: $0 [app|platform|llm] [--force]" >&2; exit 2; }
label="lab-$role"
root="$(cd "$(dirname "$0")/.." && pwd)"

for tool in aws gh jq terraform; do
  command -v "$tool" >/dev/null || { echo "$tool is not installed" >&2; exit 1; }
done

# Your copy of the repo, from origin (not gh's default repo, which may be the upstream template).
repo=${LAB_REPO:-$(git -C "$root" remote get-url origin | sed -E 's#(git@github.com:|https://github.com/)##; s#\.git$##')}
[[ "$repo" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || { echo "can't tell the repo from origin; set LAB_REPO=owner/name" >&2; exit 1; }
echo "repo: $repo"

# 1. Fork-PR approval must be on before a runner exists.
policy=$(gh api "repos/$repo/actions/permissions/fork-pr-contributor-approval" --jq .approval_policy 2>/dev/null || true)
if [[ "$policy" != "all_external_contributors" ]]; then
  echo "refusing: fork-PR workflow approval on $repo is '${policy:-unknown}', not all_external_contributors." >&2
  echo "Set it in Settings > Actions > General > 'Require approval for all external contributors', or:" >&2
  echo "  gh api -X PUT repos/$repo/actions/permissions/fork-pr-contributor-approval -f approval_policy=all_external_contributors" >&2
  exit 1
fi
echo "fork-PR approval: $policy"

# 2. The host, and SSM can reach it.
instance=${INSTANCE_ID:-$(terraform -chdir="$root/infra/live" output -raw "${role}_instance_id")}
aws sts get-caller-identity >/dev/null || { echo "AWS credentials missing or expired (Learner Lab > AWS Details)" >&2; exit 1; }
ping=$(aws ssm describe-instance-information --filters "Key=InstanceIds,Values=$instance" \
  --query 'InstanceInformationList[0].PingStatus' --output text)
[[ "$ping" == "Online" ]] || { echo "$instance is not online in SSM ($ping); wait for boot and retry" >&2; exit 1; }
echo "host: lab-$role ($instance), SSM online"

# 3. A registration token, kept in a variable and a mode-600 temp file only.
token=$(gh api -X POST "repos/$repo/actions/runners/registration-token" --jq .token)
[[ -n "$token" ]] || { echo "could not get a registration token (gh needs admin on $repo)" >&2; exit 1; }

params=$(mktemp)
chmod 600 "$params"
trap 'rm -f "$params"' EXIT

# Runs as root on the host. Values are passed as single-quoted assignments (none contains a quote).
read -r -d '' host_body <<'EOF' || true
[ -f /var/lib/lab-setup-done ] || { echo "first-boot setup not finished (see /var/log/lab-setup.log)"; exit 1; }
cd /opt/actions-runner
if [ -f .runner ]; then
  if [ "$FORCE" != "--force" ]; then
    echo "a runner is already configured on this host:"; ./svc.sh status || true
    echo "re-run with --force to register it again"; exit 0
  fi
  ./svc.sh stop || true
  ./svc.sh uninstall || true
  rm -f .runner .credentials .credentials_rsaparams .service
fi
case "$(uname -m)" in
  aarch64) a=arm64 sha=$SHA_ARM64 ;;
  x86_64) a=x64 sha=$SHA_X64 ;;
  *) echo "unsupported arch $(uname -m)"; exit 1 ;;
esac
if [ ! -x ./config.sh ]; then  # first time only; afterwards the runner updates itself
  tgz="actions-runner-linux-$a-$V.tar.gz"
  curl -fsSL -o "/tmp/$tgz" "https://github.com/actions/runner/releases/download/v$V/$tgz"
  echo "$sha  /tmp/$tgz" | sha256sum -c -
  sudo -u runner tar xzf "/tmp/$tgz" -C /opt/actions-runner
  rm -f "/tmp/$tgz"
fi
sudo -u runner ./config.sh --unattended --replace --url "https://github.com/$REPO" \
  --token "$TOKEN" --name "$LABEL" --labels "$LABEL"
./svc.sh install runner
./svc.sh start
./svc.sh status
EOF
host_script="set -euo pipefail
REPO='$repo' LABEL='$label' TOKEN='$token' FORCE='$force'
V='$RUNNER_VERSION' SHA_ARM64='$SHA_ARM64' SHA_X64='$SHA_X64'
$host_body"

jq -n --arg id "$instance" --arg script "$host_script" '{
  InstanceIds: [$id],
  DocumentName: "AWS-RunShellScript",
  Comment: "register GitHub Actions runner",
  TimeoutSeconds: 600,
  Parameters: {commands: [$script], executionTimeout: ["600"]}
}' >"$params"
unset token host_script host_body

cmd=$(aws ssm send-command --cli-input-json "file://$params" --query Command.CommandId --output text)
rm -f "$params"
echo "SSM command $cmd sent; waiting for it to finish"

status=Pending
for _ in $(seq 1 120); do
  sleep 5
  status=$(aws ssm get-command-invocation --command-id "$cmd" --instance-id "$instance" \
    --query Status --output text 2>/dev/null || echo Pending)
  [[ "$status" =~ ^(Pending|InProgress|Delayed)$ ]] || break
done
aws ssm get-command-invocation --command-id "$cmd" --instance-id "$instance" \
  --query '[StandardOutputContent, StandardErrorContent]' --output text | tail -n 40
[[ "$status" == "Success" ]] || { echo "registration on the host ended with status $status" >&2; exit 1; }

# 4. GitHub sees it.
for _ in $(seq 1 12); do
  state=$(gh api "repos/$repo/actions/runners" --jq ".runners[] | select(.name == \"$label\") | .status" || true)
  [[ "$state" == "online" ]] && { echo "runner $label is online for $repo"; exit 0; }
  sleep 5
done
echo "runner $label registered but not online yet (state: ${state:-missing}); check: gh api repos/$repo/actions/runners" >&2
exit 1
