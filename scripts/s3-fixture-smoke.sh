#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

RUN_ID=""
ROOT="${REPO_ROOT}/target/fixtures"
REPORT_PATH="${REPO_ROOT}/target/tmp/s3-fixture-smoke-evidence.md"
IMAGE="quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z"
TIMEOUT=90
TAIL_LINES=80
PULL=0

usage() {
  cat <<'EOF'
Usage: scripts/s3-fixture-smoke.sh [options]

Starts a disposable local MinIO fixture, exercises S3 plugin capabilities,
writes redacted evidence, and tears the fixture down.

Options:
  --run-id <id>         Stable run id. Generated when omitted.
  --root <dir>          Fixture state root. Default: target/fixtures.
  --report <path>       Evidence report path. Default: target/tmp/s3-fixture-smoke-evidence.md.
  --image <image>       Docker image for the fixture. Default: pinned MinIO release tag.
  --timeout <seconds>   Health wait timeout. Default: 90.
  --tail <lines>        Log lines to keep. Default: 80.
  --pull                Pull the fixture image when it is missing.
  -h, --help            Show this help.

The script prints resource names and variable names only. It does not print
fixture credentials, endpoint URLs, object contents, or scratch values.

Set VOIDB_S3_CLIENT_IMAGE to override the MinIO client image used by the local
fixture helper for bucket setup.
EOF
}

die() {
  echo "error: $*" >&2
  exit 2
}

generate_run_id() {
  printf 's3-cap-%s-%s\n' "$(date -u +%Y%m%dT%H%M%SZ)" "$$"
}

sanitize_id() {
  local value="$1"
  [[ "${value}" =~ ^[A-Za-z0-9_.-]+$ ]] || die "invalid id: ${value}"
}

fixture_dir() {
  printf '%s/%s\n' "${ROOT}" "${RUN_ID}"
}

env_path() {
  printf '%s/s3.env\n' "$(fixture_dir)"
}

log_path() {
  printf '%s/s3.log\n' "$(fixture_dir)"
}

container_name() {
  printf 'voidb-fixture-s3-%s-main\n' "${RUN_ID}"
}

network_name() {
  printf 'voidb-fixture-s3-%s\n' "${RUN_ID}"
}

s3_client_image() {
  printf '%s\n' "${VOIDB_S3_CLIENT_IMAGE:-quay.io/minio/mc:RELEASE.2025-04-16T18-13-26Z}"
}

cleanup_fixture() {
  "${SCRIPT_DIR}/local-fixture-smoke.sh" teardown \
    --fixture s3 \
    --run-id "${RUN_ID}" \
    --root "${ROOT}" >/dev/null 2>&1 || true
}

write_evidence() {
  local status="$1"
  local cleanup_status="$2"
  local smoke_log="$3"
  local commit
  commit="$(/usr/bin/git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || echo "unknown")"
  local docker_version
  docker_version="$(docker version --format 'client={{.Client.Version}} server={{.Server.Version}}' 2>/dev/null || echo "unknown")"

  mkdir -p "$(dirname "${REPORT_PATH}")"
  cat > "${REPORT_PATH}" <<EOF
# S3 Fixture Capability Smoke Evidence

- Requirements: 64 - Promote S3 fixture-backed readiness; Complete S3 and WebDAV Agent transfer workflows
- Fixture: s3
- Run id: ${RUN_ID}
- Commit: ${commit}
- Status: ${status}
- Platform: $(uname -s)-$(uname -m)
- Docker: ${docker_version}
- Image: ${IMAGE}
- Client image: $(s3_client_image)
- Container: $(container_name)
- Network: $(network_name)
- Health: MinIO API accepted local TCP connections and scratch bucket exists
- Capabilities exercised: s3.buckets, s3.list, s3.stat, s3.get, s3.put, s3.delete, s3.mkdir, s3.copy, s3.move, s3.presign, s3.transfer, s3.transfer_status, s3.sync_plan
- Destructive policy: dry-run put/delete/mkdir succeeded without a live target; acknowledged put/delete touched only the generated scratch bucket and prefix
- Object coverage: paged prefix list, full prefix list, bounded get, stat, delete, missing-object errors, and dry-run sync plan
- Transfer reliability: no-overwrite conflict, verified move, expired presign, SHA-256 mismatch, multipart cancellation/resume, retained-state cleanup on close, and exact byte round-trip
- Redaction: bad-auth and missing-object errors checked for withheld endpoint and credentials; fixture logs captured through redaction filter
- Variables present by name: VOIDB_FIXTURE_RUN_ID, VOIDB_FIXTURE_NAME, VOIDB_FIXTURE_CONTAINER, VOIDB_FIXTURE_NETWORK, VOIDB_FIXTURE_IMAGE, VOIDB_FIXTURE_HOST, VOIDB_FIXTURE_PORT, VOIDB_S3_SMOKE_PROFILE, VOIDB_S3_SMOKE_CONNECTION, VOIDB_S3_SMOKE_PROVIDER, VOIDB_S3_SMOKE_ENDPOINT, VOIDB_S3_SMOKE_BUCKET, VOIDB_S3_SMOKE_PREFIX, VOIDB_S3_SMOKE_REGION, VOIDB_S3_SMOKE_ACCESS_KEY, VOIDB_S3_SMOKE_SECRET_KEY, VOIDB_S3_SMOKE_URL, VOIDB_S3_SMOKE_CLIENT_IMAGE
- Log capture: $(log_path)
- Capability log: ${smoke_log}
- Cleanup: ${cleanup_status}
- Release decision: capability and Agent transfer reliability smoke passed
EOF

  echo "report: ${REPORT_PATH}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-id)
      [[ $# -ge 2 ]] || die "missing value for --run-id"
      RUN_ID="$2"
      shift 2
      ;;
    --root)
      [[ $# -ge 2 ]] || die "missing value for --root"
      ROOT="$2"
      shift 2
      ;;
    --report)
      [[ $# -ge 2 ]] || die "missing value for --report"
      REPORT_PATH="$2"
      shift 2
      ;;
    --image)
      [[ $# -ge 2 ]] || die "missing value for --image"
      IMAGE="$2"
      shift 2
      ;;
    --timeout)
      [[ $# -ge 2 ]] || die "missing value for --timeout"
      TIMEOUT="$2"
      [[ "${TIMEOUT}" =~ ^[0-9]+$ ]] || die "timeout must be numeric"
      shift 2
      ;;
    --tail)
      [[ $# -ge 2 ]] || die "missing value for --tail"
      TAIL_LINES="$2"
      [[ "${TAIL_LINES}" =~ ^[0-9]+$ ]] || die "tail must be numeric"
      shift 2
      ;;
    --pull)
      PULL=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

if [[ -z "${RUN_ID}" ]]; then
  RUN_ID="$(generate_run_id)"
fi
sanitize_id "${RUN_ID}"

START_ARGS=(
  --fixture s3
  --run-id "${RUN_ID}"
  --root "${ROOT}"
  --image "${IMAGE}"
  --timeout "${TIMEOUT}"
  --tail "${TAIL_LINES}"
)
if [[ "${PULL}" -eq 1 ]]; then
  START_ARGS+=(--pull)
fi

mkdir -p "$(fixture_dir)"
cleanup_status="not_run"
smoke_log="$(fixture_dir)/s3-capability-smoke.log"
trap 'cleanup_fixture' EXIT INT TERM ERR

"${SCRIPT_DIR}/local-fixture-smoke.sh" start "${START_ARGS[@]}"
"${SCRIPT_DIR}/local-fixture-smoke.sh" wait "${START_ARGS[@]}"

set -a
# shellcheck disable=SC1090
source "$(env_path)"
set +a

(
  cd "${REPO_ROOT}"
  cargo run -p voidb-plugin-s3 --example fixture_smoke --quiet
  cargo run -p voidb-plugin-s3 --example s3_transfer_reliability --quiet
) > "${smoke_log}" 2>&1

"${SCRIPT_DIR}/local-fixture-smoke.sh" logs "${START_ARGS[@]}"
cleanup_fixture
cleanup_status="removed container, network, and generated env file"
trap - EXIT INT TERM ERR

write_evidence "passed" "${cleanup_status}" "${smoke_log}"
