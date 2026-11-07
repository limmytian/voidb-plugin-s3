#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

COMMAND="run"
FIXTURE="probe"
RUN_ID=""
ROOT="${REPO_ROOT}/target/fixtures"
REPORT_PATH=""
IMAGE=""
S3_CLIENT_IMAGE="${VOIDB_S3_CLIENT_IMAGE:-quay.io/minio/mc:RELEASE.2025-04-16T18-13-26Z}"
TIMEOUT=30
TAIL_LINES=80
KEEP=0
PULL=0

usage() {
  cat <<'EOF'
Usage: scripts/local-fixture-smoke.sh [run|start|wait|logs|teardown|status] [options]

Manages disposable local Docker fixtures for release smoke runs.
The "probe" fixture validates generic lifecycle behavior. The "redis" fixture
provisions a disposable Redis target for Redis plugin readiness smoke. The
"email" fixture provisions disposable IMAP, POP3, and SMTP services for Email
plugin readiness smoke. The "ssh" fixture provisions a disposable OpenSSH
server for SSH plugin readiness smoke. The "s3" fixture provisions a
disposable MinIO bucket and scratch prefix for S3 plugin readiness smoke. The
"webdav" fixture provisions a disposable local WebDAV server and scratch
directory for WebDAV plugin readiness smoke. The "mysql" fixture provisions a
disposable MySQL database with seeded schema and rows for MySQL plugin
readiness smoke. The "mongodb" fixture provisions a disposable MongoDB
database with a seeded collection for MongoDB plugin readiness smoke. The
"elasticsearch" fixture provisions a disposable Elasticsearch index with seeded
documents for Elasticsearch plugin readiness smoke. The "kubernetes" fixture
provisions a disposable local kind cluster, scratch namespace, and kubeconfig
for Kubernetes plugin readiness smoke. The "jenkins" fixture provisions a
disposable local Jenkins server with a seeded job and build for Jenkins plugin
readiness smoke.

Commands:
  run          Start, wait, capture logs, write evidence, and teardown.
  start        Start the fixture and write its generated env file.
  wait         Wait for a started fixture to become healthy.
  logs         Capture redacted container logs.
  teardown     Remove fixture containers, networks, and generated env files.
  status       Show fixture state without printing env values.

Options:
  --fixture <name>      Fixture name. Currently supported: probe, redis, email, ssh, s3, webdav, mysql, mongodb, elasticsearch, kubernetes, jenkins.
  --run-id <id>         Stable run id. Generated for run/start when omitted.
  --root <dir>          Fixture state root. Default: target/fixtures.
  --report <path>       Evidence report path. Default: target/fixtures/<run-id>/<fixture>-evidence.md.
  --image <image>       Docker image for the fixture. Defaults by fixture.
  --timeout <seconds>   Health wait timeout. Default: 30.
  --tail <lines>        Log lines to keep. Default: 80.
  --pull                Pull the fixture image when it is missing.
  --keep                Keep the fixture running after run/start.
  -h, --help            Show this help.

The helper prints resource names and variable names only. It never prints
generated env values or raw unredacted logs.

Set VOIDB_S3_CLIENT_IMAGE to override the MinIO client image used for S3 bucket
setup.
EOF
}

die() {
  echo "error: $*" >&2
  exit 2
}

run_step() {
  printf '\n==> '
  printf '%q ' "$@"
  printf '\n'
  "$@"
}

run_redacted_step() {
  local label="$1"
  shift
  printf '\n==> %s\n' "${label}"
  "$@"
}

sanitize_id() {
  local value="$1"
  [[ "${value}" =~ ^[A-Za-z0-9_.-]+$ ]] || die "invalid id: ${value}"
}

generate_run_id() {
  printf '%s-%s\n' "$(date -u +%Y%m%dT%H%M%SZ)" "$$"
}

ensure_supported_fixture() {
  case "${FIXTURE}" in
    probe|redis|email|ssh|s3|webdav|mysql|mongodb|elasticsearch|kubernetes|jenkins) ;;
    *)
      die "unsupported fixture: ${FIXTURE}"
      ;;
  esac
}

ensure_ids() {
  ensure_supported_fixture
  if [[ -z "${IMAGE}" ]]; then
    IMAGE="$(default_image)"
  fi
  if [[ -z "${RUN_ID}" ]]; then
    case "${COMMAND}" in
      run|start)
        RUN_ID="$(generate_run_id)"
        ;;
      *)
        die "--run-id is required for ${COMMAND}"
        ;;
    esac
  fi
  sanitize_id "${FIXTURE}"
  sanitize_id "${RUN_ID}"
}

fixture_dir() {
  printf '%s/%s\n' "${ROOT}" "${RUN_ID}"
}

env_path() {
  printf '%s/%s.env\n' "$(fixture_dir)" "${FIXTURE}"
}

log_path() {
  printf '%s/%s.log\n' "$(fixture_dir)" "${FIXTURE}"
}

default_report_path() {
  printf '%s/%s-evidence.md\n' "$(fixture_dir)" "${FIXTURE}"
}

container_name() {
  if [[ "${FIXTURE}" == "kubernetes" ]]; then
    printf '%s-control-plane\n' "$(kubernetes_cluster_name)"
    return
  fi
  printf 'voidb-fixture-%s-%s-main\n' "${FIXTURE}" "${RUN_ID}"
}

network_name() {
  if [[ "${FIXTURE}" == "kubernetes" ]]; then
    echo "kind"
    return
  fi
  printf 'voidb-fixture-%s-%s\n' "${FIXTURE}" "${RUN_ID}"
}

default_image() {
  case "${FIXTURE}" in
    probe|redis) echo "redis:7-alpine" ;;
    email) echo "greenmail/standalone:2.1.9" ;;
    ssh) echo "lscr.io/linuxserver/openssh-server@sha256:67d4c3a1402179a6579aa217a38b52ced557eb8a0c17a8e32fe986a4549fdee4" ;;
    s3) echo "quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z" ;;
    webdav) echo "rclone/rclone:1.70.2" ;;
    mysql) echo "mysql:8.4" ;;
    mongodb) echo "mongo:7.0" ;;
    elasticsearch) echo "docker.elastic.co/elasticsearch/elasticsearch:8.15.3" ;;
    kubernetes) echo "kindest/node@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5" ;;
    jenkins) echo "jenkins/jenkins:lts-jdk17" ;;
  esac
}

probe_port() {
  case "${FIXTURE}" in
    probe|redis) echo "6379" ;;
    email) echo "3143" ;;
    ssh) echo "2222" ;;
    s3) echo "9000" ;;
    webdav) echo "8080" ;;
    mysql) echo "3306" ;;
    mongodb) echo "27017" ;;
    elasticsearch) echo "9200" ;;
    kubernetes) echo "6443" ;;
    jenkins) echo "8080" ;;
  esac
}

email_smtp_port() {
  echo "3025"
}

email_pop3_port() {
  echo "3110"
}

email_imap_port() {
  echo "3143"
}

email_api_port() {
  echo "8080"
}

email_address() {
  printf 'voidb-fixture-%s@example.test\n' "${RUN_ID}"
}

email_password() {
  printf 'voidb-fixture-password-%s\n' "${RUN_ID}"
}

email_connection_name() {
  printf 'email-fixture-%s\n' "${RUN_ID}"
}

ssh_user() {
  echo "voidb"
}

ssh_password() {
  printf 'voidb-fixture-password-%s\n' "${RUN_ID}"
}

ssh_profile_name() {
  printf 'ssh-fixture-%s\n' "${RUN_ID}"
}

ssh_scratch_dir() {
  printf '/config/voidb-smoke-%s\n' "${RUN_ID}"
}

ssh_remote_file() {
  printf '%s/fixture.txt\n' "$(ssh_scratch_dir)"
}

s3_safe_id() {
  local safe
  safe="$(printf '%s' "${RUN_ID}" \
    | tr '[:upper:]_' '[:lower:]-' \
    | sed -E 's/[^a-z0-9-]+/-/g; s/^-+//; s/-+$//' \
    | cut -c1-40 \
    | sed -E 's/-+$//')"
  if [[ -z "${safe}" ]]; then
    safe="run"
  fi
  printf '%s\n' "${safe}"
}

s3_access_key() {
  printf 'voidbminio%s\n' "$(s3_safe_id | tr -d '-')"
}

s3_secret_key() {
  printf 'voidbminiosecret%s\n' "$(s3_safe_id | tr -d '-')"
}

s3_profile_name() {
  printf 's3-fixture-%s\n' "$(s3_safe_id)"
}

s3_bucket() {
  printf 'voidb-fixture-%s\n' "$(s3_safe_id)"
}

s3_prefix() {
  printf 'voidb-smoke/%s/\n' "$(s3_safe_id)"
}

s3_region() {
  echo "us-east-1"
}

webdav_safe_id() {
  local safe
  safe="$(printf '%s' "${RUN_ID}" \
    | tr '[:upper:]_' '[:lower:]-' \
    | sed -E 's/[^a-z0-9-]+/-/g; s/^-+//; s/-+$//' \
    | cut -c1-40 \
    | sed -E 's/-+$//')"
  if [[ -z "${safe}" ]]; then
    safe="run"
  fi
  printf '%s\n' "${safe}"
}

webdav_user() {
  echo "voidb-webdav-user"
}

webdav_password() {
  printf 'voidbwebdavsecret%s\n' "$(webdav_safe_id | tr -d '-')"
}

webdav_profile_name() {
  printf 'webdav-fixture-%s\n' "$(webdav_safe_id)"
}

webdav_root_path() {
  printf '/voidb-smoke/%s\n' "$(webdav_safe_id)"
}

webdav_seed_path() {
  printf '%s/seed.txt\n' "$(webdav_root_path)"
}

webdav_root_dir() {
  printf '%s/webdav-root\n' "$(fixture_dir)"
}

webdav_scratch_host_dir() {
  printf '%s%s\n' "$(webdav_root_dir)" "$(webdav_root_path)"
}

webdav_seed_host_file() {
  printf '%s/seed.txt\n' "$(webdav_scratch_host_dir)"
}

mysql_safe_id() {
  local safe
  safe="$(printf '%s' "${RUN_ID}" \
    | tr '[:upper:]_' '[:lower:]-' \
    | sed -E 's/[^a-z0-9-]+/-/g; s/^-+//; s/-+$//' \
    | cut -c1-32 \
    | sed -E 's/-+$//')"
  if [[ -z "${safe}" ]]; then
    safe="run"
  fi
  printf '%s\n' "${safe}"
}

mysql_sql_id() {
  printf '%s\n' "$(mysql_safe_id | tr '-' '_')"
}

mysql_profile_name() {
  printf 'mysql-fixture-%s\n' "$(mysql_safe_id)"
}

mysql_database() {
  printf 'voidb_fixture_%s\n' "$(mysql_sql_id)"
}

mysql_user() {
  echo "voidb_mysql_user"
}

mysql_password() {
  printf 'voidbmysqlsecret%s\n' "$(mysql_safe_id | tr -d '-')"
}

mysql_root_password() {
  printf 'voidbmysqlrootsecret%s\n' "$(mysql_safe_id | tr -d '-')"
}

mysql_seed_table() {
  echo "fixture_accounts"
}

mysql_init_dir() {
  printf '%s/mysql-init\n' "$(fixture_dir)"
}

mysql_seed_file() {
  printf '%s/001-voidb-smoke.sql\n' "$(mysql_init_dir)"
}

mongodb_safe_id() {
  local safe
  safe="$(printf '%s' "${RUN_ID}" \
    | tr '[:upper:]_' '[:lower:]-' \
    | sed -E 's/[^a-z0-9-]+/-/g; s/^-+//; s/-+$//' \
    | cut -c1-32 \
    | sed -E 's/-+$//')"
  if [[ -z "${safe}" ]]; then
    safe="run"
  fi
  printf '%s\n' "${safe}"
}

mongodb_db_id() {
  printf '%s\n' "$(mongodb_safe_id | tr '-' '_')"
}

mongodb_profile_name() {
  printf 'mongodb-fixture-%s\n' "$(mongodb_safe_id)"
}

mongodb_database() {
  printf 'voidb_fixture_%s\n' "$(mongodb_db_id)"
}

mongodb_collection() {
  echo "fixture_accounts"
}

mongodb_user() {
  echo "voidb_mongodb_user"
}

mongodb_password() {
  printf 'voidbmongodbsecret%s\n' "$(mongodb_safe_id | tr -d '-')"
}

mongodb_root_user() {
  echo "voidb_mongodb_root"
}

mongodb_root_password() {
  printf 'voidbmongodbrootsecret%s\n' "$(mongodb_safe_id | tr -d '-')"
}

mongodb_replica_set() {
  echo "voidb-rs0"
}

mongodb_document_id() {
  printf 'fixture-%s-alpha\n' "$(mongodb_safe_id)"
}

mongodb_init_dir() {
  printf '%s/mongodb-init\n' "$(fixture_dir)"
}

mongodb_seed_file() {
  printf '%s/001-voidb-smoke.js\n' "$(mongodb_init_dir)"
}

elasticsearch_safe_id() {
  local safe
  safe="$(printf '%s' "${RUN_ID}" \
    | tr '[:upper:]_' '[:lower:]-' \
    | sed -E 's/[^a-z0-9-]+/-/g; s/^-+//; s/-+$//' \
    | cut -c1-32 \
    | sed -E 's/-+$//')"
  if [[ -z "${safe}" ]]; then
    safe="run"
  fi
  printf '%s\n' "${safe}"
}

elasticsearch_profile_name() {
  printf 'elasticsearch-fixture-%s\n' "$(elasticsearch_safe_id)"
}

elasticsearch_index() {
  printf 'voidb-fixture-%s\n' "$(elasticsearch_safe_id)"
}

elasticsearch_document_id() {
  printf 'fixture-%s-alpha\n' "$(elasticsearch_safe_id)"
}

jenkins_safe_id() {
  local safe
  safe="$(printf '%s' "${RUN_ID}" \
    | tr '[:upper:]_' '[:lower:]-' \
    | sed -E 's/[^a-z0-9-]+/-/g; s/^-+//; s/-+$//' \
    | cut -c1-32 \
    | sed -E 's/-+$//')"
  if [[ -z "${safe}" ]]; then
    safe="run"
  fi
  printf '%s\n' "${safe}"
}

jenkins_profile_name() {
  printf 'jenkins-fixture-%s\n' "$(jenkins_safe_id)"
}

jenkins_user() {
  echo "voidb_jenkins_user"
}

jenkins_password() {
  printf 'voidbjenkinssecret%s\n' "$(jenkins_safe_id | tr -d '-')"
}

jenkins_job_name() {
  printf 'voidb-fixture-%s\n' "$(jenkins_safe_id)"
}

jenkins_build_number() {
  echo "1"
}

jenkins_init_dir() {
  printf '%s/jenkins-init\n' "$(fixture_dir)"
}

jenkins_init_file() {
  printf '%s/001-voidb-security.groovy\n' "$(jenkins_init_dir)"
}

jenkins_build_marker() {
  printf '%s/jenkins-build-triggered\n' "$(fixture_dir)"
}

kubernetes_safe_id() {
  local safe
  safe="$(printf '%s' "${RUN_ID}" \
    | tr '[:upper:]_' '[:lower:]-' \
    | sed -E 's/[^a-z0-9-]+/-/g; s/^-+//; s/-+$//' \
    | cut -c1-28 \
    | sed -E 's/-+$//')"
  if [[ -z "${safe}" ]]; then
    safe="run"
  fi
  printf '%s\n' "${safe}"
}

kubernetes_cluster_name() {
  printf 'voidb-k8s-%s\n' "$(kubernetes_safe_id)"
}

kubernetes_context_name() {
  printf 'kind-%s\n' "$(kubernetes_cluster_name)"
}

kubernetes_namespace() {
  printf 'voidb-smoke-%s\n' "$(kubernetes_safe_id)"
}

kubernetes_profile_name() {
  printf 'kubernetes-fixture-%s\n' "$(kubernetes_safe_id)"
}

kubernetes_configmap_name() {
  echo "voidb-agent-probe"
}

kubernetes_kubeconfig_path() {
  printf '%s/kubeconfig\n' "$(fixture_dir)"
}

kubernetes_seed_manifest() {
  printf '%s/kubernetes-seed.yaml\n' "$(fixture_dir)"
}

ssh_config_dir() {
  printf '%s/ssh-config\n' "$(fixture_dir)"
}

ssh_private_key_path() {
  printf '%s/client_ed25519\n' "$(ssh_config_dir)"
}

ssh_public_key_path() {
  printf '%s.pub\n' "$(ssh_private_key_path)"
}

ssh_known_hosts_path() {
  printf '%s/ssh_known_hosts\n' "$(fixture_dir)"
}

redis_smoke_db() {
  echo "0"
}

redis_key_prefix() {
  printf 'voidb-fixture:%s:%s\n' "${RUN_ID}" "${FIXTURE}"
}

redis_profile_name() {
  printf 'redis-fixture-%s\n' "${RUN_ID}"
}

evidence_requirement() {
  case "${FIXTURE}" in
    email) echo "62 - Promote Email fixture-backed readiness" ;;
    ssh) echo "63 - Promote SSH fixture-backed readiness" ;;
    s3) echo "64 - Promote S3 fixture-backed readiness" ;;
    webdav) echo "65 - Promote WebDAV fixture-backed readiness" ;;
    mysql) echo "66 - Promote MySQL fixture-backed readiness" ;;
    kubernetes) echo "67 - Promote Kubernetes fixture-backed readiness" ;;
    mongodb) echo "68 - Promote MongoDB fixture-backed readiness" ;;
    elasticsearch) echo "69 - Promote Elasticsearch fixture-backed readiness" ;;
    jenkins) echo "70 - Promote Jenkins fixture-backed readiness" ;;
    redis) echo "61 - Promote Redis fixture-backed readiness" ;;
    probe) echo "59 - Build local Docker fixture harness for release smokes" ;;
  esac
}

release_decision() {
  case "${FIXTURE}" in
    email) echo "fixture ready; capability smoke pending" ;;
    ssh) echo "fixture ready; capability and TUI smoke pending" ;;
    s3) echo "fixture ready; capability smoke pending" ;;
    webdav) echo "fixture ready; capability smoke pending" ;;
    mysql) echo "fixture ready; SQL capability smoke pending" ;;
    kubernetes) echo "fixture ready; capability smoke pending" ;;
    mongodb) echo "fixture ready; capability smoke pending" ;;
    elasticsearch) echo "fixture ready; capability smoke pending" ;;
    jenkins) echo "fixture ready; capability smoke pending" ;;
    redis) echo "fixture ready; capability smoke pending" ;;
    probe) echo "no maturity change; lifecycle helper validation only" ;;
  esac
}

ensure_docker() {
  command -v docker >/dev/null 2>&1 || die "docker is not installed"
  docker info >/dev/null 2>&1 || die "docker daemon is unavailable"
}

ensure_docker_image() {
  local image="$1"
  if docker image inspect "${image}" >/dev/null 2>&1; then
    return
  fi

  if [[ "${PULL}" -eq 1 ]]; then
    run_step docker pull "${image}" >/dev/null
    return
  fi

  die "missing image ${image}; pull it first or pass --pull"
}

ensure_image() {
  ensure_docker_image "${IMAGE}"
  if [[ "${FIXTURE}" == "s3" ]]; then
    ensure_docker_image "${S3_CLIENT_IMAGE}"
  fi
}

host_port_for_container() {
  local container="$1"
  local port="$2"
  docker port "${container}" "${port}/tcp" | awk -F: '{ print $NF }'
}

port_open() {
  local host="$1"
  local port="$2"
  if command -v nc >/dev/null 2>&1; then
    nc -z "${host}" "${port}" >/dev/null 2>&1
  else
    bash -c ":</dev/tcp/${host}/${port}" >/dev/null 2>&1
  fi
}

health_summary() {
  case "${FIXTURE}" in
    email) echo "smtp and imap ports accepted local TCP connections" ;;
    ssh) echo "ssh port accepted local TCP connections; strict known_hosts public-key exec succeeded" ;;
    s3) echo "MinIO API accepted local TCP connections and scratch bucket exists" ;;
    webdav) echo "WebDAV endpoint accepted authenticated PROPFIND for the scratch directory" ;;
    mysql) echo "MySQL port accepted local TCP connections and seeded database query succeeded" ;;
    kubernetes) echo "kind cluster node is Ready and scratch namespace/configmap exist" ;;
    mongodb) echo "MongoDB single-node replica set elected a primary and the seeded collection query succeeded" ;;
    elasticsearch) echo "Elasticsearch HTTP port accepted local connections, cluster reached yellow/green health, and seeded index count query succeeded" ;;
    jenkins) echo "Jenkins HTTP port accepted local connections, authenticated API ping succeeded, seeded job exists, and build 1 completed successfully" ;;
    probe|redis) echo "redis-cli ping returned PONG" ;;
  esac
}

fixture_variables() {
  case "${FIXTURE}" in
    email)
      echo "VOIDB_FIXTURE_RUN_ID, VOIDB_FIXTURE_NAME, VOIDB_FIXTURE_CONTAINER, VOIDB_FIXTURE_NETWORK, VOIDB_FIXTURE_IMAGE, VOIDB_FIXTURE_HOST, VOIDB_FIXTURE_PORT, VOIDB_EMAIL_SMOKE_CONNECTION, VOIDB_EMAIL_SMOKE_FOLDER, VOIDB_EMAIL_SMOKE_PROTOCOL, VOIDB_EMAIL_SMOKE_SECURITY, VOIDB_EMAIL_SMOKE_EMAIL, VOIDB_EMAIL_SMOKE_PASSWORD, VOIDB_EMAIL_SMOKE_IMAP_HOST, VOIDB_EMAIL_SMOKE_IMAP_PORT, VOIDB_EMAIL_SMOKE_POP3_HOST, VOIDB_EMAIL_SMOKE_POP3_PORT, VOIDB_EMAIL_SMOKE_SMTP_CONNECTION, VOIDB_EMAIL_SMOKE_SMTP_HOST, VOIDB_EMAIL_SMOKE_SMTP_PORT, VOIDB_EMAIL_SMOKE_SEND_TO, VOIDB_EMAIL_SMOKE_API_HOST, VOIDB_EMAIL_SMOKE_API_PORT"
      ;;
    probe|redis)
      echo "VOIDB_FIXTURE_RUN_ID, VOIDB_FIXTURE_NAME, VOIDB_FIXTURE_CONTAINER, VOIDB_FIXTURE_NETWORK, VOIDB_FIXTURE_IMAGE, VOIDB_FIXTURE_HOST, VOIDB_FIXTURE_PORT, VOIDB_REDIS_SMOKE_HOST, VOIDB_REDIS_SMOKE_PORT, VOIDB_REDIS_SMOKE_DB, VOIDB_REDIS_SMOKE_PROFILE, VOIDB_REDIS_SMOKE_KEY_PREFIX, VOIDB_REDIS_SMOKE_URL"
      ;;
    ssh)
      echo "VOIDB_FIXTURE_RUN_ID, VOIDB_FIXTURE_NAME, VOIDB_FIXTURE_CONTAINER, VOIDB_FIXTURE_NETWORK, VOIDB_FIXTURE_IMAGE, VOIDB_FIXTURE_HOST, VOIDB_FIXTURE_PORT, VOIDB_SSH_SMOKE_PROFILE, VOIDB_SSH_SMOKE_HOST, VOIDB_SSH_SMOKE_PORT, VOIDB_SSH_SMOKE_USER, VOIDB_SSH_SMOKE_PASSWORD, VOIDB_SSH_SMOKE_PRIVATE_KEY_PATH, VOIDB_SSH_SMOKE_PUBLIC_KEY_PATH, VOIDB_SSH_SMOKE_KNOWN_HOSTS, VOIDB_SSH_SMOKE_SCRATCH, VOIDB_SSH_SMOKE_REMOTE_FILE"
      ;;
    s3)
      echo "VOIDB_FIXTURE_RUN_ID, VOIDB_FIXTURE_NAME, VOIDB_FIXTURE_CONTAINER, VOIDB_FIXTURE_NETWORK, VOIDB_FIXTURE_IMAGE, VOIDB_FIXTURE_HOST, VOIDB_FIXTURE_PORT, VOIDB_S3_SMOKE_PROFILE, VOIDB_S3_SMOKE_CONNECTION, VOIDB_S3_SMOKE_PROVIDER, VOIDB_S3_SMOKE_ENDPOINT, VOIDB_S3_SMOKE_BUCKET, VOIDB_S3_SMOKE_PREFIX, VOIDB_S3_SMOKE_REGION, VOIDB_S3_SMOKE_ACCESS_KEY, VOIDB_S3_SMOKE_SECRET_KEY, VOIDB_S3_SMOKE_URL, VOIDB_S3_SMOKE_CLIENT_IMAGE"
      ;;
    webdav)
      echo "VOIDB_FIXTURE_RUN_ID, VOIDB_FIXTURE_NAME, VOIDB_FIXTURE_CONTAINER, VOIDB_FIXTURE_NETWORK, VOIDB_FIXTURE_IMAGE, VOIDB_FIXTURE_HOST, VOIDB_FIXTURE_PORT, VOIDB_WEBDAV_SMOKE_PROFILE, VOIDB_WEBDAV_SMOKE_CONNECTION, VOIDB_WEBDAV_SMOKE_URL, VOIDB_WEBDAV_SMOKE_ROOT, VOIDB_WEBDAV_SMOKE_SEED_PATH, VOIDB_WEBDAV_SMOKE_USER, VOIDB_WEBDAV_SMOKE_PASSWORD, VOIDB_WEBDAV_SMOKE_AUTH, VOIDB_WEBDAV_SMOKE_VERIFY_SSL"
      ;;
    mysql)
      echo "VOIDB_FIXTURE_RUN_ID, VOIDB_FIXTURE_NAME, VOIDB_FIXTURE_CONTAINER, VOIDB_FIXTURE_NETWORK, VOIDB_FIXTURE_IMAGE, VOIDB_FIXTURE_HOST, VOIDB_FIXTURE_PORT, VOIDB_MYSQL_SMOKE_PROFILE, VOIDB_MYSQL_SMOKE_CONNECTION, VOIDB_MYSQL_SMOKE_HOST, VOIDB_MYSQL_SMOKE_PORT, VOIDB_MYSQL_SMOKE_USER, VOIDB_MYSQL_SMOKE_PASSWORD, VOIDB_MYSQL_SMOKE_DATABASE, VOIDB_MYSQL_SMOKE_TABLE, VOIDB_MYSQL_SMOKE_SSL_MODE, VOIDB_MYSQL_TEST_HOST, VOIDB_MYSQL_TEST_PORT, VOIDB_MYSQL_TEST_USER, VOIDB_MYSQL_TEST_PASSWORD, VOIDB_MYSQL_TEST_DATABASE"
      ;;
    mongodb)
      echo "VOIDB_FIXTURE_RUN_ID, VOIDB_FIXTURE_NAME, VOIDB_FIXTURE_CONTAINER, VOIDB_FIXTURE_NETWORK, VOIDB_FIXTURE_IMAGE, VOIDB_FIXTURE_HOST, VOIDB_FIXTURE_PORT, VOIDB_MONGODB_SMOKE_PROFILE, VOIDB_MONGODB_SMOKE_CONNECTION, VOIDB_MONGODB_SMOKE_URI, VOIDB_MONGODB_SMOKE_HOST, VOIDB_MONGODB_SMOKE_PORT, VOIDB_MONGODB_SMOKE_USER, VOIDB_MONGODB_SMOKE_PASSWORD, VOIDB_MONGODB_SMOKE_DATABASE, VOIDB_MONGODB_SMOKE_COLLECTION, VOIDB_MONGODB_SMOKE_AUTH_SOURCE, VOIDB_MONGODB_SMOKE_DOCUMENT_ID"
      ;;
    elasticsearch)
      echo "VOIDB_FIXTURE_RUN_ID, VOIDB_FIXTURE_NAME, VOIDB_FIXTURE_CONTAINER, VOIDB_FIXTURE_NETWORK, VOIDB_FIXTURE_IMAGE, VOIDB_FIXTURE_HOST, VOIDB_FIXTURE_PORT, VOIDB_ES_SMOKE_PROFILE, VOIDB_ES_SMOKE_CONNECTION, VOIDB_ES_SMOKE_URL, VOIDB_ES_SMOKE_ENDPOINT, VOIDB_ES_SMOKE_HOST, VOIDB_ES_SMOKE_PORT, VOIDB_ES_SMOKE_INDEX, VOIDB_ES_SMOKE_DOCUMENT_ID, VOIDB_ES_SMOKE_VERIFY_SSL"
      ;;
    jenkins)
      echo "VOIDB_FIXTURE_RUN_ID, VOIDB_FIXTURE_NAME, VOIDB_FIXTURE_CONTAINER, VOIDB_FIXTURE_NETWORK, VOIDB_FIXTURE_IMAGE, VOIDB_FIXTURE_HOST, VOIDB_FIXTURE_PORT, VOIDB_JENKINS_SMOKE_PROFILE, VOIDB_JENKINS_SMOKE_CONNECTION, VOIDB_JENKINS_SMOKE_URL, VOIDB_JENKINS_SMOKE_HOST, VOIDB_JENKINS_SMOKE_PORT, VOIDB_JENKINS_SMOKE_USER, VOIDB_JENKINS_SMOKE_TOKEN, VOIDB_JENKINS_SMOKE_JOB, VOIDB_JENKINS_SMOKE_BUILD, VOIDB_JENKINS_SMOKE_VERIFY_SSL"
      ;;
    kubernetes)
      echo "VOIDB_FIXTURE_RUN_ID, VOIDB_FIXTURE_NAME, VOIDB_FIXTURE_CONTAINER, VOIDB_FIXTURE_NETWORK, VOIDB_FIXTURE_IMAGE, VOIDB_FIXTURE_HOST, VOIDB_FIXTURE_PORT, VOIDB_K8S_SMOKE_PROFILE, VOIDB_K8S_SMOKE_CONNECTION, VOIDB_K8S_SMOKE_CLUSTER, VOIDB_K8S_SMOKE_CONTEXT, VOIDB_K8S_SMOKE_NAMESPACE, VOIDB_K8S_SMOKE_CONFIGMAP, VOIDB_K8S_SMOKE_KUBECONFIG, VOIDB_K8S_SMOKE_API_SERVER"
      ;;
  esac
}

ensure_ssh_tools() {
  command -v ssh >/dev/null 2>&1 || die "ssh client is not installed"
  command -v ssh-keygen >/dev/null 2>&1 || die "ssh-keygen is not installed"
  command -v ssh-keyscan >/dev/null 2>&1 || die "ssh-keyscan is not installed"
}

prepare_ssh_fixture_files() {
  ensure_ssh_tools
  local config_dir
  config_dir="$(ssh_config_dir)"
  local scratch_host_dir
  scratch_host_dir="${config_dir}$(ssh_scratch_dir | sed 's#^/config##')"
  local remote_file_host_path
  remote_file_host_path="${config_dir}$(ssh_remote_file | sed 's#^/config##')"
  mkdir -p "${config_dir}/ssh_host_keys" "${scratch_host_dir}"

  local client_key
  client_key="$(ssh_private_key_path)"
  if [[ ! -f "${client_key}" ]]; then
    ssh-keygen -q -t ed25519 -N "" -C "voidb-fixture-client-${RUN_ID}" -f "${client_key}"
  fi

  local host_key_dir
  host_key_dir="${config_dir}/ssh_host_keys"
  if [[ ! -f "${host_key_dir}/ssh_host_ed25519_key" ]]; then
    ssh-keygen -q -t ed25519 -N "" -C "voidb-fixture-host-${RUN_ID}" -f "${host_key_dir}/ssh_host_ed25519_key"
  fi
  if [[ ! -f "${host_key_dir}/ssh_host_ecdsa_key" ]]; then
    ssh-keygen -q -t ecdsa -b 256 -N "" -C "voidb-fixture-host-${RUN_ID}" -f "${host_key_dir}/ssh_host_ecdsa_key"
  fi
  if [[ ! -f "${host_key_dir}/ssh_host_rsa_key" ]]; then
    ssh-keygen -q -t rsa -b 3072 -N "" -C "voidb-fixture-host-${RUN_ID}" -f "${host_key_dir}/ssh_host_rsa_key"
  fi

  chmod 600 "${client_key}" "${host_key_dir}"/ssh_host_*_key
  chmod 644 "$(ssh_public_key_path)" "${host_key_dir}"/ssh_host_*_key.pub
  printf 'fixture file for %s\n' "${RUN_ID}" > "${remote_file_host_path}"
}

capture_ssh_known_hosts() {
  ensure_ssh_tools
  local known_hosts
  known_hosts="${VOIDB_SSH_SMOKE_KNOWN_HOSTS:-$(ssh_known_hosts_path)}"
  local tmp_path
  tmp_path="${known_hosts}.tmp"
  mkdir -p "$(dirname "${known_hosts}")"
  ssh-keyscan -T 5 -p "${VOIDB_SSH_SMOKE_PORT}" "${VOIDB_SSH_SMOKE_HOST}" \
    2>/dev/null | sort -u > "${tmp_path}"
  [[ -s "${tmp_path}" ]] || return 1
  mv "${tmp_path}" "${known_hosts}"
}

ssh_exec_health_check() {
  ensure_ssh_tools
  ssh \
    -i "${VOIDB_SSH_SMOKE_PRIVATE_KEY_PATH}" \
    -o BatchMode=yes \
    -o IdentitiesOnly=yes \
    -o StrictHostKeyChecking=yes \
    -o UserKnownHostsFile="${VOIDB_SSH_SMOKE_KNOWN_HOSTS}" \
    -o ConnectTimeout=5 \
    -p "${VOIDB_SSH_SMOKE_PORT}" \
    "${VOIDB_SSH_SMOKE_USER}@${VOIDB_SSH_SMOKE_HOST}" \
    "mkdir -p '${VOIDB_SSH_SMOKE_SCRATCH}' && test -f '${VOIDB_SSH_SMOKE_REMOTE_FILE}'"
}

ensure_webdav_tools() {
  command -v curl >/dev/null 2>&1 || die "curl is not installed"
}

prepare_webdav_fixture_files() {
  local root_dir
  root_dir="$(webdav_root_dir)"
  local scratch_dir
  scratch_dir="$(webdav_scratch_host_dir)"
  mkdir -p "${scratch_dir}"
  chmod 700 "${root_dir}" "${scratch_dir}"
  printf 'fixture file for %s\n' "${RUN_ID}" > "$(webdav_seed_host_file)"
}

webdav_health_check() {
  ensure_webdav_tools
  local base_url
  base_url="${VOIDB_WEBDAV_SMOKE_URL%/}"
  curl -fsS \
    --max-time 5 \
    --user "${VOIDB_WEBDAV_SMOKE_USER}:${VOIDB_WEBDAV_SMOKE_PASSWORD}" \
    --request PROPFIND \
    --header "Depth: 1" \
    "${base_url}${VOIDB_WEBDAV_SMOKE_ROOT}/" >/dev/null
}

prepare_mysql_fixture_files() {
  local init_dir
  init_dir="$(mysql_init_dir)"
  mkdir -p "${init_dir}"
  chmod 755 "${init_dir}"
  cat > "$(mysql_seed_file)" <<EOF
CREATE DATABASE IF NOT EXISTS \`$(mysql_database)\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE \`$(mysql_database)\`;

CREATE TABLE IF NOT EXISTS $(mysql_seed_table) (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(80) NOT NULL,
  balance_cents INT NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO $(mysql_seed_table) (name, balance_cents, active)
VALUES
  ('alpha fixture account', 1250, TRUE),
  ('beta fixture account', 2500, TRUE),
  ('inactive fixture account', 0, FALSE);

CREATE VIEW fixture_active_accounts AS
SELECT id, name, balance_cents
FROM $(mysql_seed_table)
WHERE active = TRUE;
EOF
  chmod 644 "$(mysql_seed_file)"
}

mysql_health_check() {
  docker exec "${VOIDB_FIXTURE_CONTAINER}" \
    mysqladmin ping \
    --protocol=TCP \
    -h 127.0.0.1 \
    -u"${VOIDB_MYSQL_SMOKE_USER}" \
    -p"${VOIDB_MYSQL_SMOKE_PASSWORD}" \
    --silent >/dev/null 2>&1 || return 1

  docker exec "${VOIDB_FIXTURE_CONTAINER}" \
    mysql \
    --protocol=TCP \
    -h 127.0.0.1 \
    -u"${VOIDB_MYSQL_SMOKE_USER}" \
    -p"${VOIDB_MYSQL_SMOKE_PASSWORD}" \
    --database "${VOIDB_MYSQL_SMOKE_DATABASE}" \
    --batch \
    --skip-column-names \
    -e "SELECT COUNT(*) FROM $(mysql_seed_table)" 2>/dev/null \
    | grep -Eq '^[[:space:]]*3[[:space:]]*$'
}

prepare_mongodb_fixture_files() {
  local init_dir
  init_dir="$(mongodb_init_dir)"
  mkdir -p "${init_dir}"
  chmod 755 "${init_dir}"
  cat > "$(mongodb_seed_file)" <<EOF
const dbName = "$(mongodb_database)";
const collectionName = "$(mongodb_collection)";
const appUser = "$(mongodb_user)";
const appPassword = "$(mongodb_password)";
const targetDb = db.getSiblingDB(dbName);

targetDb.createUser({
  user: appUser,
  pwd: appPassword,
  roles: [{ role: "readWrite", db: dbName }]
});

targetDb.getCollection(collectionName).insertMany([
  {
    _id: "$(mongodb_document_id)",
    name: "alpha fixture account",
    balance_cents: 1250,
    active: true,
    tags: ["fixture", "alpha"]
  },
  {
    _id: "fixture-$(mongodb_safe_id)-beta",
    name: "beta fixture account",
    balance_cents: 2500,
    active: true,
    tags: ["fixture", "beta"]
  },
  {
    _id: "fixture-$(mongodb_safe_id)-inactive",
    name: "inactive fixture account",
    balance_cents: 0,
    active: false,
    tags: ["fixture", "inactive"]
  }
]);

targetDb.getCollection(collectionName).createIndex({ active: 1, name: 1 });
targetDb.getCollection("fixture_meta").insertOne({
  _id: "run",
  run_id: "$(mongodb_safe_id)",
  fixture: "mongodb"
});
EOF
  chmod 644 "$(mongodb_seed_file)"
}

mongodb_health_check() {
  local replica_state
  replica_state="$(docker exec "${VOIDB_FIXTURE_CONTAINER}" \
    mongosh \
    --quiet \
    --username "$(mongodb_root_user)" \
    --password "$(mongodb_root_password)" \
    --authenticationDatabase admin \
    admin \
    --eval "try { print(rs.status().myState) } catch (error) { if (error.codeName === 'NotYetInitialized') { rs.initiate({ _id: '$(mongodb_replica_set)', members: [{ _id: 0, host: 'voidb-fixture-mongodb:27017' }] }); print(0); } else { quit(2); } }" \
    2>/dev/null | tail -n 1)" || return 1
  [[ "${replica_state}" == "1" ]] || return 1
  docker exec "${VOIDB_FIXTURE_CONTAINER}" \
    mongosh \
    --quiet \
    --username "${VOIDB_MONGODB_SMOKE_USER}" \
    --password "${VOIDB_MONGODB_SMOKE_PASSWORD}" \
    --authenticationDatabase "${VOIDB_MONGODB_SMOKE_AUTH_SOURCE}" \
    "${VOIDB_MONGODB_SMOKE_DATABASE}" \
    --eval "print(db.getCollection('${VOIDB_MONGODB_SMOKE_COLLECTION}').countDocuments({}))" 2>/dev/null \
    | grep -Eq '^[[:space:]]*3[[:space:]]*$'
}

ensure_elasticsearch_tools() {
  command -v curl >/dev/null 2>&1 || die "curl is not installed"
}

elasticsearch_seed_fixture() {
  ensure_elasticsearch_tools
  local base_url
  base_url="${VOIDB_ES_SMOKE_URL%/}"
  local index
  index="${VOIDB_ES_SMOKE_INDEX}"
  local status
  status="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 5 "${base_url}/${index}" 2>/dev/null || true)"
  if [[ "${status}" == "000" ]]; then
    return 1
  fi
  if [[ "${status}" == "404" ]]; then
    curl -fsS --max-time 10 \
      --request PUT \
      --header "Content-Type: application/json" \
      --data-binary @- \
      "${base_url}/${index}" >/dev/null <<EOF
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "properties": {
      "name": { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
      "balance_cents": { "type": "integer" },
      "active": { "type": "boolean" },
      "tags": { "type": "keyword" },
      "fixture": { "type": "boolean" },
      "run_id": { "type": "keyword" },
      "created_at": { "type": "date" }
    }
  }
}
EOF
  elif [[ "${status}" != "200" ]]; then
    return 1
  fi

  local bulk_response
  bulk_response="$(curl -fsS --max-time 10 \
    --request POST \
    --header "Content-Type: application/x-ndjson" \
    --data-binary @- \
    "${base_url}/_bulk?refresh=true" <<EOF
{"index":{"_index":"${index}","_id":"$(elasticsearch_document_id)"}}
{"name":"alpha fixture document","balance_cents":1250,"active":true,"tags":["fixture","alpha"],"fixture":true,"run_id":"$(elasticsearch_safe_id)","created_at":"2026-07-05T00:00:00Z"}
{"index":{"_index":"${index}","_id":"fixture-$(elasticsearch_safe_id)-beta"}}
{"name":"beta fixture document","balance_cents":2500,"active":true,"tags":["fixture","beta"],"fixture":true,"run_id":"$(elasticsearch_safe_id)","created_at":"2026-07-05T00:01:00Z"}
{"index":{"_index":"${index}","_id":"fixture-$(elasticsearch_safe_id)-inactive"}}
{"name":"inactive fixture document","balance_cents":0,"active":false,"tags":["fixture","inactive"],"fixture":true,"run_id":"$(elasticsearch_safe_id)","created_at":"2026-07-05T00:02:00Z"}
EOF
)"
  printf '%s\n' "${bulk_response}" | grep -Eq '"errors"[[:space:]]*:[[:space:]]*false'
}

elasticsearch_health_check() {
  ensure_elasticsearch_tools
  local base_url
  base_url="${VOIDB_ES_SMOKE_URL%/}"
  local health
  health="$(curl -fsS --max-time 8 "${base_url}/_cluster/health?wait_for_status=yellow&timeout=5s" 2>/dev/null)" || return 1
  printf '%s\n' "${health}" | grep -Eq '"status"[[:space:]]*:[[:space:]]*"(yellow|green)"' || return 1

  local count
  count="$(curl -fsS --max-time 5 "${base_url}/${VOIDB_ES_SMOKE_INDEX}/_count?q=fixture:true" 2>/dev/null)" || return 1
  printf '%s\n' "${count}" | grep -Eq '"count"[[:space:]]*:[[:space:]]*3'
}

ensure_jenkins_tools() {
  command -v curl >/dev/null 2>&1 || die "curl is not installed"
}

prepare_jenkins_fixture_files() {
  local init_dir
  init_dir="$(jenkins_init_dir)"
  mkdir -p "${init_dir}"
  chmod 755 "${init_dir}"
  cat > "$(jenkins_init_file)" <<'EOF'
import hudson.security.FullControlOnceLoggedInAuthorizationStrategy
import hudson.security.HudsonPrivateSecurityRealm
import jenkins.model.Jenkins

def instance = Jenkins.get()
def user = System.getenv("VOIDB_JENKINS_ADMIN_USER")
def password = System.getenv("VOIDB_JENKINS_ADMIN_PASSWORD")

def realm = new HudsonPrivateSecurityRealm(false)
if (realm.getUser(user) == null) {
  realm.createAccount(user, password)
}
instance.setSecurityRealm(realm)

def strategy = new FullControlOnceLoggedInAuthorizationStrategy()
strategy.setAllowAnonymousRead(false)
instance.setAuthorizationStrategy(strategy)
instance.setCrumbIssuer(null)
instance.save()
EOF
  chmod 644 "$(jenkins_init_file)"
}

jenkins_base_url() {
  printf '%s\n' "${VOIDB_JENKINS_SMOKE_URL%/}"
}

jenkins_auth() {
  printf '%s:%s\n' "${VOIDB_JENKINS_SMOKE_USER}" "${VOIDB_JENKINS_SMOKE_TOKEN}"
}

jenkins_crumb_args() {
  local base_url
  base_url="$(jenkins_base_url)"
  local crumb_json
  crumb_json="$(curl -fsS --max-time 10 --user "$(jenkins_auth)" \
    "${base_url}/crumbIssuer/api/json" 2>/dev/null || true)"
  if [[ -z "${crumb_json}" ]]; then
    return 0
  fi
  local field
  local crumb
  field="$(printf '%s\n' "${crumb_json}" \
    | sed -n 's/.*"crumbRequestField"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')"
  crumb="$(printf '%s\n' "${crumb_json}" \
    | sed -n 's/.*"crumb"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')"
  if [[ -n "${field}" && -n "${crumb}" ]]; then
    printf '%s\n%s\n' "${field}" "${crumb}"
  fi
}

jenkins_post() {
  local url="$1"
  shift
  local crumb_info
  crumb_info="$(jenkins_crumb_args)"
  local args=(--max-time 20 --user "$(jenkins_auth)")
  if [[ -n "${crumb_info}" ]]; then
    local field
    local crumb
    field="$(printf '%s\n' "${crumb_info}" | sed -n '1p')"
    crumb="$(printf '%s\n' "${crumb_info}" | sed -n '2p')"
    args+=(--header "${field}: ${crumb}")
  fi
  curl -fsS "${args[@]}" --request POST "$@" "${url}"
}

jenkins_seed_fixture() {
  ensure_jenkins_tools
  local base_url
  base_url="$(jenkins_base_url)"
  local job
  job="${VOIDB_JENKINS_SMOKE_JOB}"

  curl -fsS --max-time 10 --user "$(jenkins_auth)" \
    "${base_url}/api/json?tree=nodeName" >/dev/null || return 1

  local job_status
  job_status="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 10 \
    --user "$(jenkins_auth)" \
    "${base_url}/job/${job}/api/json" 2>/dev/null || true)"
  if [[ "${job_status}" == "404" ]]; then
    jenkins_post \
      "${base_url}/createItem?name=${job}" \
      --header "Content-Type: application/xml" \
      --data-binary @- >/dev/null <<EOF
<project>
  <actions/>
  <description>VoidB Jenkins fixture job for ${RUN_ID}</description>
  <keepDependencies>false</keepDependencies>
  <properties/>
  <scm class="hudson.scm.NullSCM"/>
  <canRoam>true</canRoam>
  <disabled>false</disabled>
  <blockBuildWhenDownstreamBuilding>false</blockBuildWhenDownstreamBuilding>
  <blockBuildWhenUpstreamBuilding>false</blockBuildWhenUpstreamBuilding>
  <triggers/>
  <concurrentBuild>false</concurrentBuild>
  <builders>
    <hudson.tasks.Shell>
      <command>echo voidb jenkins fixture $(jenkins_safe_id); echo fixture build complete</command>
    </hudson.tasks.Shell>
  </builders>
  <publishers/>
  <buildWrappers/>
</project>
EOF
  elif [[ "${job_status}" != "200" ]]; then
    return 1
  fi

  local build_url
  build_url="${base_url}/job/${job}/$(jenkins_build_number)/api/json?tree=building,result"
  local build_status
  build_status="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 10 \
    --user "$(jenkins_auth)" "${build_url}" 2>/dev/null || true)"
  if [[ "${build_status}" == "404" && ! -f "$(jenkins_build_marker)" ]]; then
    jenkins_post "${base_url}/job/${job}/build" >/dev/null
    touch "$(jenkins_build_marker)"
  fi
  jenkins_wait_seed_build
}

jenkins_wait_seed_build() {
  local base_url
  base_url="$(jenkins_base_url)"
  local job
  job="${VOIDB_JENKINS_SMOKE_JOB}"
  local build_url
  build_url="${base_url}/job/${job}/$(jenkins_build_number)/api/json?tree=building,result"
  local deadline
  deadline=$((SECONDS + 90))
  while [[ "${SECONDS}" -le "${deadline}" ]]; do
    local build
    build="$(curl -fsS --max-time 10 --user "$(jenkins_auth)" "${build_url}" 2>/dev/null || true)"
    if printf '%s\n' "${build}" | grep -Eq '"building"[[:space:]]*:[[:space:]]*false' \
      && printf '%s\n' "${build}" | grep -Eq '"result"[[:space:]]*:[[:space:]]*"SUCCESS"'; then
      return 0
    fi
    sleep 1
  done
  return 1
}

jenkins_health_check() {
  ensure_jenkins_tools
  local base_url
  base_url="$(jenkins_base_url)"
  local job
  job="${VOIDB_JENKINS_SMOKE_JOB}"
  curl -fsS --max-time 10 --user "$(jenkins_auth)" \
    "${base_url}/api/json?tree=nodeName" >/dev/null || return 1
  curl -fsS --max-time 10 --user "$(jenkins_auth)" \
    "${base_url}/job/${job}/api/json?tree=name,buildable" \
    | grep -Eq '"name"[[:space:]]*:[[:space:]]*"'"${job}"'"' || return 1
  curl -fsS --max-time 10 --user "$(jenkins_auth)" \
    "${base_url}/job/${job}/$(jenkins_build_number)/api/json?tree=building,result" \
    | grep -Eq '"result"[[:space:]]*:[[:space:]]*"SUCCESS"'
}

ensure_kubernetes_tools() {
  command -v kind >/dev/null 2>&1 || die "kind is not installed"
  command -v kubectl >/dev/null 2>&1 || die "kubectl is not installed"
}

kubernetes_cluster_exists() {
  ensure_kubernetes_tools
  kind get clusters 2>/dev/null | grep -Fxq "$(kubernetes_cluster_name)"
}

kubernetes_api_server() {
  kubectl config view \
    --kubeconfig "$(kubernetes_kubeconfig_path)" \
    -o jsonpath='{.clusters[0].cluster.server}'
}

kubernetes_api_port() {
  kubernetes_api_server | sed -E 's#^https?://[^:]+:([0-9]+).*$#\1#'
}

prepare_kubernetes_fixture_files() {
  ensure_kubernetes_tools
  mkdir -p "$(fixture_dir)"
  cat > "$(kubernetes_seed_manifest)" <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: $(kubernetes_namespace)
  labels:
    voidb.fixture: "true"
    voidb.fixture.run_id: "$(kubernetes_safe_id)"
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: $(kubernetes_configmap_name)
  namespace: $(kubernetes_namespace)
  labels:
    voidb.fixture: "true"
    voidb.fixture.run_id: "$(kubernetes_safe_id)"
data:
  seed.txt: "fixture file for ${RUN_ID}"
EOF
  chmod 600 "$(kubernetes_seed_manifest)"
}

kubernetes_apply_seed() {
  kubectl \
    --kubeconfig "${VOIDB_K8S_SMOKE_KUBECONFIG}" \
    --context "${VOIDB_K8S_SMOKE_CONTEXT}" \
    apply -f "$(kubernetes_seed_manifest)" >/dev/null
}

kubernetes_health_check() {
  kubectl \
    --kubeconfig "${VOIDB_K8S_SMOKE_KUBECONFIG}" \
    --context "${VOIDB_K8S_SMOKE_CONTEXT}" \
    wait --for=condition=Ready nodes --all --timeout=5s >/dev/null 2>&1 || return 1
  kubectl \
    --kubeconfig "${VOIDB_K8S_SMOKE_KUBECONFIG}" \
    --context "${VOIDB_K8S_SMOKE_CONTEXT}" \
    get namespace "${VOIDB_K8S_SMOKE_NAMESPACE}" >/dev/null 2>&1 || return 1
  kubectl \
    --kubeconfig "${VOIDB_K8S_SMOKE_KUBECONFIG}" \
    --context "${VOIDB_K8S_SMOKE_CONTEXT}" \
    get configmap "${VOIDB_K8S_SMOKE_CONFIGMAP}" \
    --namespace "${VOIDB_K8S_SMOKE_NAMESPACE}" >/dev/null 2>&1
}

s3_create_bucket() {
  local client_image
  client_image="${VOIDB_S3_SMOKE_CLIENT_IMAGE:-${S3_CLIENT_IMAGE}}"
  docker run --rm \
    --network "${VOIDB_FIXTURE_NETWORK}" \
    -e "MC_HOST_voidb=http://${VOIDB_S3_SMOKE_ACCESS_KEY}:${VOIDB_S3_SMOKE_SECRET_KEY}@${VOIDB_FIXTURE_CONTAINER}:$(probe_port)" \
    --entrypoint /bin/sh \
    "${client_image}" \
    -c "mc mb --ignore-existing \"voidb/${VOIDB_S3_SMOKE_BUCKET}\""
}

write_env_file() {
  local env_file
  env_file="$(env_path)"
  local container
  container="$(container_name)"
  local network
  network="$(network_name)"

  umask 077
  mkdir -p "$(fixture_dir)"
  case "${FIXTURE}" in
    probe|redis)
      local port
      port="$(probe_port)"
      local host_port
      host_port="$(host_port_for_container "${container}" "${port}")"
      local redis_db
      redis_db="$(redis_smoke_db)"
      local key_prefix
      key_prefix="$(redis_key_prefix)"
      local profile
      profile="$(redis_profile_name)"
      cat > "${env_file}" <<EOF
VOIDB_FIXTURE_RUN_ID=${RUN_ID}
VOIDB_FIXTURE_NAME=${FIXTURE}
VOIDB_FIXTURE_CONTAINER=${container}
VOIDB_FIXTURE_NETWORK=${network}
VOIDB_FIXTURE_IMAGE=${IMAGE}
VOIDB_FIXTURE_HOST=127.0.0.1
VOIDB_FIXTURE_PORT=${host_port}
VOIDB_REDIS_SMOKE_HOST=127.0.0.1
VOIDB_REDIS_SMOKE_PORT=${host_port}
VOIDB_REDIS_SMOKE_DB=${redis_db}
VOIDB_REDIS_SMOKE_PROFILE=${profile}
VOIDB_REDIS_SMOKE_KEY_PREFIX=${key_prefix}
VOIDB_REDIS_SMOKE_URL=redis://127.0.0.1:${host_port}/${redis_db}
EOF
      ;;
    email)
      local smtp_host_port
      smtp_host_port="$(host_port_for_container "${container}" "$(email_smtp_port)")"
      local pop3_host_port
      pop3_host_port="$(host_port_for_container "${container}" "$(email_pop3_port)")"
      local imap_host_port
      imap_host_port="$(host_port_for_container "${container}" "$(email_imap_port)")"
      local api_host_port
      api_host_port="$(host_port_for_container "${container}" "$(email_api_port)")"
      local email
      email="$(email_address)"
      local password
      password="$(email_password)"
      local connection
      connection="$(email_connection_name)"
      cat > "${env_file}" <<EOF
VOIDB_FIXTURE_RUN_ID=${RUN_ID}
VOIDB_FIXTURE_NAME=${FIXTURE}
VOIDB_FIXTURE_CONTAINER=${container}
VOIDB_FIXTURE_NETWORK=${network}
VOIDB_FIXTURE_IMAGE=${IMAGE}
VOIDB_FIXTURE_HOST=127.0.0.1
VOIDB_FIXTURE_PORT=${imap_host_port}
VOIDB_EMAIL_SMOKE_CONNECTION=${connection}
VOIDB_EMAIL_SMOKE_FOLDER=INBOX
VOIDB_EMAIL_SMOKE_PROTOCOL=imap
VOIDB_EMAIL_SMOKE_SECURITY=none
VOIDB_EMAIL_SMOKE_EMAIL=${email}
VOIDB_EMAIL_SMOKE_PASSWORD=${password}
VOIDB_EMAIL_SMOKE_IMAP_HOST=127.0.0.1
VOIDB_EMAIL_SMOKE_IMAP_PORT=${imap_host_port}
VOIDB_EMAIL_SMOKE_POP3_HOST=127.0.0.1
VOIDB_EMAIL_SMOKE_POP3_PORT=${pop3_host_port}
VOIDB_EMAIL_SMOKE_SMTP_CONNECTION=${connection}
VOIDB_EMAIL_SMOKE_SMTP_HOST=127.0.0.1
VOIDB_EMAIL_SMOKE_SMTP_PORT=${smtp_host_port}
VOIDB_EMAIL_SMOKE_SEND_TO=${email}
VOIDB_EMAIL_SMOKE_API_HOST=127.0.0.1
VOIDB_EMAIL_SMOKE_API_PORT=${api_host_port}
EOF
      ;;
    ssh)
      local host_port
      host_port="$(host_port_for_container "${container}" "$(probe_port)")"
      cat > "${env_file}" <<EOF
VOIDB_FIXTURE_RUN_ID=${RUN_ID}
VOIDB_FIXTURE_NAME=${FIXTURE}
VOIDB_FIXTURE_CONTAINER=${container}
VOIDB_FIXTURE_NETWORK=${network}
VOIDB_FIXTURE_IMAGE=${IMAGE}
VOIDB_FIXTURE_HOST=127.0.0.1
VOIDB_FIXTURE_PORT=${host_port}
VOIDB_SSH_SMOKE_PROFILE=$(ssh_profile_name)
VOIDB_SSH_SMOKE_HOST=127.0.0.1
VOIDB_SSH_SMOKE_PORT=${host_port}
VOIDB_SSH_SMOKE_USER=$(ssh_user)
VOIDB_SSH_SMOKE_PASSWORD=$(ssh_password)
VOIDB_SSH_SMOKE_PRIVATE_KEY_PATH=$(ssh_private_key_path)
VOIDB_SSH_SMOKE_PUBLIC_KEY_PATH=$(ssh_public_key_path)
VOIDB_SSH_SMOKE_KNOWN_HOSTS=$(ssh_known_hosts_path)
VOIDB_SSH_SMOKE_SCRATCH=$(ssh_scratch_dir)
VOIDB_SSH_SMOKE_REMOTE_FILE=$(ssh_remote_file)
EOF
      ;;
    s3)
      local host_port
      host_port="$(host_port_for_container "${container}" "$(probe_port)")"
      cat > "${env_file}" <<EOF
VOIDB_FIXTURE_RUN_ID=${RUN_ID}
VOIDB_FIXTURE_NAME=${FIXTURE}
VOIDB_FIXTURE_CONTAINER=${container}
VOIDB_FIXTURE_NETWORK=${network}
VOIDB_FIXTURE_IMAGE=${IMAGE}
VOIDB_FIXTURE_HOST=127.0.0.1
VOIDB_FIXTURE_PORT=${host_port}
VOIDB_S3_SMOKE_PROFILE=$(s3_profile_name)
VOIDB_S3_SMOKE_CONNECTION=$(s3_profile_name)
VOIDB_S3_SMOKE_PROVIDER=minio
VOIDB_S3_SMOKE_ENDPOINT=http://127.0.0.1:${host_port}
VOIDB_S3_SMOKE_BUCKET=$(s3_bucket)
VOIDB_S3_SMOKE_PREFIX=$(s3_prefix)
VOIDB_S3_SMOKE_REGION=$(s3_region)
VOIDB_S3_SMOKE_ACCESS_KEY=$(s3_access_key)
VOIDB_S3_SMOKE_SECRET_KEY=$(s3_secret_key)
VOIDB_S3_SMOKE_URL=http://127.0.0.1:${host_port}
VOIDB_S3_SMOKE_CLIENT_IMAGE=${S3_CLIENT_IMAGE}
EOF
      ;;
    webdav)
      local host_port
      host_port="$(host_port_for_container "${container}" "$(probe_port)")"
      cat > "${env_file}" <<EOF
VOIDB_FIXTURE_RUN_ID=${RUN_ID}
VOIDB_FIXTURE_NAME=${FIXTURE}
VOIDB_FIXTURE_CONTAINER=${container}
VOIDB_FIXTURE_NETWORK=${network}
VOIDB_FIXTURE_IMAGE=${IMAGE}
VOIDB_FIXTURE_HOST=127.0.0.1
VOIDB_FIXTURE_PORT=${host_port}
VOIDB_WEBDAV_SMOKE_PROFILE=$(webdav_profile_name)
VOIDB_WEBDAV_SMOKE_CONNECTION=$(webdav_profile_name)
VOIDB_WEBDAV_SMOKE_URL=http://127.0.0.1:${host_port}
VOIDB_WEBDAV_SMOKE_ROOT=$(webdav_root_path)
VOIDB_WEBDAV_SMOKE_SEED_PATH=$(webdav_seed_path)
VOIDB_WEBDAV_SMOKE_USER=$(webdav_user)
VOIDB_WEBDAV_SMOKE_PASSWORD=$(webdav_password)
VOIDB_WEBDAV_SMOKE_AUTH=basic
VOIDB_WEBDAV_SMOKE_VERIFY_SSL=false
EOF
      ;;
    mysql)
      local host_port
      host_port="$(host_port_for_container "${container}" "$(probe_port)")"
      cat > "${env_file}" <<EOF
VOIDB_FIXTURE_RUN_ID=${RUN_ID}
VOIDB_FIXTURE_NAME=${FIXTURE}
VOIDB_FIXTURE_CONTAINER=${container}
VOIDB_FIXTURE_NETWORK=${network}
VOIDB_FIXTURE_IMAGE=${IMAGE}
VOIDB_FIXTURE_HOST=127.0.0.1
VOIDB_FIXTURE_PORT=${host_port}
VOIDB_MYSQL_SMOKE_PROFILE=$(mysql_profile_name)
VOIDB_MYSQL_SMOKE_CONNECTION=$(mysql_profile_name)
VOIDB_MYSQL_SMOKE_HOST=127.0.0.1
VOIDB_MYSQL_SMOKE_PORT=${host_port}
VOIDB_MYSQL_SMOKE_USER=$(mysql_user)
VOIDB_MYSQL_SMOKE_PASSWORD=$(mysql_password)
VOIDB_MYSQL_SMOKE_DATABASE=$(mysql_database)
VOIDB_MYSQL_SMOKE_TABLE=$(mysql_seed_table)
VOIDB_MYSQL_SMOKE_SSL_MODE=disabled
VOIDB_MYSQL_TEST_HOST=127.0.0.1
VOIDB_MYSQL_TEST_PORT=${host_port}
VOIDB_MYSQL_TEST_USER=$(mysql_user)
VOIDB_MYSQL_TEST_PASSWORD=$(mysql_password)
VOIDB_MYSQL_TEST_DATABASE=$(mysql_database)
EOF
      ;;
    mongodb)
      local host_port
      host_port="$(host_port_for_container "${container}" "$(probe_port)")"
      local db_name
      db_name="$(mongodb_database)"
      cat > "${env_file}" <<EOF
VOIDB_FIXTURE_RUN_ID=${RUN_ID}
VOIDB_FIXTURE_NAME=${FIXTURE}
VOIDB_FIXTURE_CONTAINER=${container}
VOIDB_FIXTURE_NETWORK=${network}
VOIDB_FIXTURE_IMAGE=${IMAGE}
VOIDB_FIXTURE_HOST=127.0.0.1
VOIDB_FIXTURE_PORT=${host_port}
VOIDB_MONGODB_SMOKE_PROFILE=$(mongodb_profile_name)
VOIDB_MONGODB_SMOKE_CONNECTION=$(mongodb_profile_name)
VOIDB_MONGODB_SMOKE_URI='mongodb://$(mongodb_user):$(mongodb_password)@127.0.0.1:${host_port}/${db_name}?authSource=${db_name}&replicaSet=$(mongodb_replica_set)&directConnection=true'
VOIDB_MONGODB_SMOKE_HOST=127.0.0.1
VOIDB_MONGODB_SMOKE_PORT=${host_port}
VOIDB_MONGODB_SMOKE_USER=$(mongodb_user)
VOIDB_MONGODB_SMOKE_PASSWORD=$(mongodb_password)
VOIDB_MONGODB_SMOKE_DATABASE=${db_name}
VOIDB_MONGODB_SMOKE_COLLECTION=$(mongodb_collection)
VOIDB_MONGODB_SMOKE_AUTH_SOURCE=${db_name}
VOIDB_MONGODB_SMOKE_DOCUMENT_ID=$(mongodb_document_id)
EOF
      ;;
    elasticsearch)
      local host_port
      host_port="$(host_port_for_container "${container}" "$(probe_port)")"
      cat > "${env_file}" <<EOF
VOIDB_FIXTURE_RUN_ID=${RUN_ID}
VOIDB_FIXTURE_NAME=${FIXTURE}
VOIDB_FIXTURE_CONTAINER=${container}
VOIDB_FIXTURE_NETWORK=${network}
VOIDB_FIXTURE_IMAGE=${IMAGE}
VOIDB_FIXTURE_HOST=127.0.0.1
VOIDB_FIXTURE_PORT=${host_port}
VOIDB_ES_SMOKE_PROFILE=$(elasticsearch_profile_name)
VOIDB_ES_SMOKE_CONNECTION=$(elasticsearch_profile_name)
VOIDB_ES_SMOKE_URL=http://127.0.0.1:${host_port}
VOIDB_ES_SMOKE_ENDPOINT=http://127.0.0.1:${host_port}
VOIDB_ES_SMOKE_HOST=127.0.0.1
VOIDB_ES_SMOKE_PORT=${host_port}
VOIDB_ES_SMOKE_INDEX=$(elasticsearch_index)
VOIDB_ES_SMOKE_DOCUMENT_ID=$(elasticsearch_document_id)
VOIDB_ES_SMOKE_VERIFY_SSL=false
EOF
      ;;
    jenkins)
      local host_port
      host_port="$(host_port_for_container "${container}" "$(probe_port)")"
      cat > "${env_file}" <<EOF
VOIDB_FIXTURE_RUN_ID=${RUN_ID}
VOIDB_FIXTURE_NAME=${FIXTURE}
VOIDB_FIXTURE_CONTAINER=${container}
VOIDB_FIXTURE_NETWORK=${network}
VOIDB_FIXTURE_IMAGE=${IMAGE}
VOIDB_FIXTURE_HOST=127.0.0.1
VOIDB_FIXTURE_PORT=${host_port}
VOIDB_JENKINS_SMOKE_PROFILE=$(jenkins_profile_name)
VOIDB_JENKINS_SMOKE_CONNECTION=$(jenkins_profile_name)
VOIDB_JENKINS_SMOKE_URL=http://127.0.0.1:${host_port}
VOIDB_JENKINS_SMOKE_HOST=127.0.0.1
VOIDB_JENKINS_SMOKE_PORT=${host_port}
VOIDB_JENKINS_SMOKE_USER=$(jenkins_user)
VOIDB_JENKINS_SMOKE_TOKEN=$(jenkins_password)
VOIDB_JENKINS_SMOKE_JOB=$(jenkins_job_name)
VOIDB_JENKINS_SMOKE_BUILD=$(jenkins_build_number)
VOIDB_JENKINS_SMOKE_VERIFY_SSL=false
EOF
      ;;
    kubernetes)
      local api_server
      api_server="$(kubernetes_api_server)"
      local api_port
      api_port="$(kubernetes_api_port)"
      cat > "${env_file}" <<EOF
VOIDB_FIXTURE_RUN_ID=${RUN_ID}
VOIDB_FIXTURE_NAME=${FIXTURE}
VOIDB_FIXTURE_CONTAINER=${container}
VOIDB_FIXTURE_NETWORK=${network}
VOIDB_FIXTURE_IMAGE=${IMAGE}
VOIDB_FIXTURE_HOST=127.0.0.1
VOIDB_FIXTURE_PORT=${api_port}
VOIDB_K8S_SMOKE_PROFILE=$(kubernetes_profile_name)
VOIDB_K8S_SMOKE_CONNECTION=$(kubernetes_profile_name)
VOIDB_K8S_SMOKE_CLUSTER=$(kubernetes_cluster_name)
VOIDB_K8S_SMOKE_CONTEXT=$(kubernetes_context_name)
VOIDB_K8S_SMOKE_NAMESPACE=$(kubernetes_namespace)
VOIDB_K8S_SMOKE_CONFIGMAP=$(kubernetes_configmap_name)
VOIDB_K8S_SMOKE_KUBECONFIG=$(kubernetes_kubeconfig_path)
VOIDB_K8S_SMOKE_API_SERVER=${api_server}
EOF
      ;;
  esac
}

start_fixture() {
  ensure_ids
  ensure_docker
  ensure_image

  local dir
  dir="$(fixture_dir)"
  local container
  container="$(container_name)"
  local network
  network="$(network_name)"
  local port
  port="$(probe_port)"

  mkdir -p "${dir}"
  chmod 700 "${dir}"

  if docker ps -a --format '{{.Names}}' | grep -Fxq "${container}"; then
    die "container already exists: ${container}"
  fi
  if [[ "${FIXTURE}" == "kubernetes" ]] && kubernetes_cluster_exists; then
    die "kind cluster already exists: $(kubernetes_cluster_name)"
  fi
  if [[ "${FIXTURE}" != "kubernetes" ]] && docker network inspect "${network}" >/dev/null 2>&1; then
    die "network already exists: ${network}"
  fi

  if [[ "${FIXTURE}" != "kubernetes" ]]; then
    run_step docker network create \
      --label voidb.fixture=true \
      --label "voidb.fixture.run_id=${RUN_ID}" \
      "${network}" >/dev/null
  fi

  case "${FIXTURE}" in
    probe|redis)
      run_step docker run -d \
        --name "${container}" \
        --network "${network}" \
        --label voidb.fixture=true \
        --label "voidb.fixture.run_id=${RUN_ID}" \
        --label "voidb.fixture.name=${FIXTURE}" \
        -p "127.0.0.1::${port}" \
        "${IMAGE}" \
        redis-server --save "" --appendonly no >/dev/null
      ;;
    email)
      local greenmail_opts
      greenmail_opts="-Dgreenmail.setup.test.all -Dgreenmail.hostname=0.0.0.0 -Dgreenmail.users=$(email_address):$(email_password)"
      run_redacted_step "docker run -d <email fixture env redacted> ${IMAGE}" \
        docker run -d \
        --name "${container}" \
        --network "${network}" \
        --label voidb.fixture=true \
        --label "voidb.fixture.run_id=${RUN_ID}" \
        --label "voidb.fixture.name=${FIXTURE}" \
        -e "GREENMAIL_OPTS=${greenmail_opts}" \
        -p "127.0.0.1::$(email_smtp_port)" \
        -p "127.0.0.1::$(email_pop3_port)" \
        -p "127.0.0.1::$(email_imap_port)" \
        -p "127.0.0.1::$(email_api_port)" \
        "${IMAGE}" >/dev/null
      ;;
    ssh)
      prepare_ssh_fixture_files
      run_redacted_step "docker run -d <ssh fixture env redacted> ${IMAGE}" \
        docker run -d \
        --name "${container}" \
        --network "${network}" \
        --hostname "voidb-fixture-ssh" \
        --label voidb.fixture=true \
        --label "voidb.fixture.run_id=${RUN_ID}" \
        --label "voidb.fixture.name=${FIXTURE}" \
        -e "PUID=$(id -u)" \
        -e "PGID=$(id -g)" \
        -e "TZ=Etc/UTC" \
        -e "SUDO_ACCESS=false" \
        -e "PASSWORD_ACCESS=true" \
        -e "USER_PASSWORD=$(ssh_password)" \
        -e "USER_NAME=$(ssh_user)" \
        -e "PUBLIC_KEY_FILE=/config/client_ed25519.pub" \
        -e "LOG_STDOUT=true" \
        -p "127.0.0.1::$(probe_port)" \
        -v "$(cd "$(ssh_config_dir)" && pwd):/config" \
        "${IMAGE}" >/dev/null
      ;;
    s3)
      run_redacted_step "docker run -d <s3 fixture env redacted> ${IMAGE}" \
        docker run -d \
        --name "${container}" \
        --network "${network}" \
        --hostname "voidb-fixture-s3" \
        --label voidb.fixture=true \
        --label "voidb.fixture.run_id=${RUN_ID}" \
        --label "voidb.fixture.name=${FIXTURE}" \
        -e "MINIO_ROOT_USER=$(s3_access_key)" \
        -e "MINIO_ROOT_PASSWORD=$(s3_secret_key)" \
        -p "127.0.0.1::$(probe_port)" \
        "${IMAGE}" \
        server /data --address ":$(probe_port)" >/dev/null
      ;;
    webdav)
      prepare_webdav_fixture_files
      run_redacted_step "docker run -d <webdav fixture env redacted> ${IMAGE}" \
        docker run -d \
        --name "${container}" \
        --network "${network}" \
        --hostname "voidb-fixture-webdav" \
        --label voidb.fixture=true \
        --label "voidb.fixture.run_id=${RUN_ID}" \
        --label "voidb.fixture.name=${FIXTURE}" \
        -p "127.0.0.1::$(probe_port)" \
        -v "$(cd "$(webdav_root_dir)" && pwd):/data" \
        "${IMAGE}" \
        serve webdav /data \
        --addr ":$(probe_port)" \
        --user "$(webdav_user)" \
        --pass "$(webdav_password)" \
        --baseurl "/" >/dev/null
      ;;
    mysql)
      prepare_mysql_fixture_files
      run_redacted_step "docker run -d <mysql fixture env redacted> ${IMAGE}" \
        docker run -d \
        --name "${container}" \
        --network "${network}" \
        --hostname "voidb-fixture-mysql" \
        --label voidb.fixture=true \
        --label "voidb.fixture.run_id=${RUN_ID}" \
        --label "voidb.fixture.name=${FIXTURE}" \
        -e "MYSQL_ROOT_PASSWORD=$(mysql_root_password)" \
        -e "MYSQL_DATABASE=$(mysql_database)" \
        -e "MYSQL_USER=$(mysql_user)" \
        -e "MYSQL_PASSWORD=$(mysql_password)" \
        -p "127.0.0.1::$(probe_port)" \
        -v "$(cd "$(mysql_init_dir)" && pwd):/docker-entrypoint-initdb.d:ro" \
        "${IMAGE}" \
        --character-set-server=utf8mb4 \
        --collation-server=utf8mb4_unicode_ci \
        --skip-name-resolve >/dev/null
      ;;
    mongodb)
      prepare_mongodb_fixture_files
      run_redacted_step "docker run -d <mongodb fixture env redacted> ${IMAGE}" \
        docker run -d \
        --name "${container}" \
        --network "${network}" \
        --hostname "voidb-fixture-mongodb" \
        --label voidb.fixture=true \
        --label "voidb.fixture.run_id=${RUN_ID}" \
        --label "voidb.fixture.name=${FIXTURE}" \
        -e "MONGO_INITDB_ROOT_USERNAME=$(mongodb_root_user)" \
        -e "MONGO_INITDB_ROOT_PASSWORD=$(mongodb_root_password)" \
        -e "MONGO_INITDB_DATABASE=$(mongodb_database)" \
        -p "127.0.0.1::$(probe_port)" \
        -v "$(cd "$(mongodb_init_dir)" && pwd):/docker-entrypoint-initdb.d:ro" \
        --entrypoint bash \
        "${IMAGE}" \
        -c 'umask 077; head -c 756 /dev/urandom | base64 > /tmp/voidb-replica-keyfile; chown mongodb:mongodb /tmp/voidb-replica-keyfile; exec /usr/local/bin/docker-entrypoint.sh mongod --replSet "voidb-rs0" --bind_ip_all --keyFile /tmp/voidb-replica-keyfile' >/dev/null
      ;;
    elasticsearch)
      run_redacted_step "docker run -d <elasticsearch fixture env redacted> ${IMAGE}" \
        docker run -d \
        --name "${container}" \
        --network "${network}" \
        --hostname "voidb-fixture-elasticsearch" \
        --label voidb.fixture=true \
        --label "voidb.fixture.run_id=${RUN_ID}" \
        --label "voidb.fixture.name=${FIXTURE}" \
        -e "discovery.type=single-node" \
        -e "xpack.security.enabled=false" \
        -e "xpack.security.enrollment.enabled=false" \
        -e "xpack.security.http.ssl.enabled=false" \
        -e "action.destructive_requires_name=true" \
        -e "ES_JAVA_OPTS=-Xms512m -Xmx512m" \
        -p "127.0.0.1::$(probe_port)" \
        "${IMAGE}" >/dev/null
      ;;
    jenkins)
      prepare_jenkins_fixture_files
      run_redacted_step "docker run -d <jenkins fixture env redacted> ${IMAGE}" \
        docker run -d \
        --name "${container}" \
        --network "${network}" \
        --hostname "voidb-fixture-jenkins" \
        --label voidb.fixture=true \
        --label "voidb.fixture.run_id=${RUN_ID}" \
        --label "voidb.fixture.name=${FIXTURE}" \
        -e "JAVA_OPTS=-Djenkins.install.runSetupWizard=false -Djenkins.CLI.disabled=true -Xms256m -Xmx768m" \
        -e "VOIDB_JENKINS_ADMIN_USER=$(jenkins_user)" \
        -e "VOIDB_JENKINS_ADMIN_PASSWORD=$(jenkins_password)" \
        -p "127.0.0.1::$(probe_port)" \
        -v "$(cd "$(jenkins_init_dir)" && pwd):/usr/share/jenkins/ref/init.groovy.d:ro" \
        "${IMAGE}" >/dev/null
      ;;
    kubernetes)
      prepare_kubernetes_fixture_files
      run_redacted_step "kind create cluster <kubeconfig path redacted> ${IMAGE}" \
        kind create cluster \
        --quiet \
        --name "$(kubernetes_cluster_name)" \
        --image "${IMAGE}" \
        --kubeconfig "$(kubernetes_kubeconfig_path)" \
        --wait "${TIMEOUT}s" >/dev/null
      chmod 600 "$(kubernetes_kubeconfig_path)"
      write_env_file
      load_env_file
      kubernetes_apply_seed
      ;;
  esac

  if [[ "${FIXTURE}" != "kubernetes" ]]; then
    write_env_file
  fi
  echo "fixture: ${FIXTURE}"
  echo "run_id: ${RUN_ID}"
  echo "container: ${container}"
  echo "network: ${network}"
  echo "env_file: $(env_path)"
}

load_env_file() {
  ensure_ids
  local env_file
  env_file="$(env_path)"
  [[ -f "${env_file}" ]] || die "missing env file: ${env_file}"
  set -a
  # shellcheck disable=SC1090
  source "${env_file}"
  set +a
}

wait_fixture() {
  load_env_file
  local deadline
  deadline=$((SECONDS + TIMEOUT))
  local ok=0

  while [[ "${SECONDS}" -le "${deadline}" ]]; do
    case "${FIXTURE}" in
      probe|redis)
        if docker exec "${VOIDB_FIXTURE_CONTAINER}" \
          redis-cli -n "${VOIDB_REDIS_SMOKE_DB:-0}" ping 2>/dev/null \
          | grep -Fxq "PONG"; then
          ok=1
          break
        fi
        ;;
      email)
        if port_open "${VOIDB_FIXTURE_HOST}" "${VOIDB_EMAIL_SMOKE_SMTP_PORT}" \
          && port_open "${VOIDB_FIXTURE_HOST}" "${VOIDB_EMAIL_SMOKE_IMAP_PORT}"; then
          ok=1
          break
        fi
        ;;
      ssh)
        if port_open "${VOIDB_FIXTURE_HOST}" "${VOIDB_SSH_SMOKE_PORT}" \
          && capture_ssh_known_hosts \
          && ssh_exec_health_check >/dev/null 2>&1; then
          ok=1
          break
        fi
        ;;
      s3)
        if port_open "${VOIDB_FIXTURE_HOST}" "${VOIDB_FIXTURE_PORT}" \
          && s3_create_bucket >/dev/null 2>&1; then
          ok=1
          break
        fi
        ;;
      webdav)
        if port_open "${VOIDB_FIXTURE_HOST}" "${VOIDB_FIXTURE_PORT}" \
          && webdav_health_check >/dev/null 2>&1; then
          ok=1
          break
        fi
        ;;
      mysql)
        if port_open "${VOIDB_FIXTURE_HOST}" "${VOIDB_FIXTURE_PORT}" \
          && mysql_health_check >/dev/null 2>&1; then
          ok=1
          break
        fi
        ;;
      mongodb)
        if port_open "${VOIDB_FIXTURE_HOST}" "${VOIDB_FIXTURE_PORT}" \
          && mongodb_health_check >/dev/null 2>&1; then
          ok=1
          break
        fi
        ;;
      elasticsearch)
        if port_open "${VOIDB_FIXTURE_HOST}" "${VOIDB_FIXTURE_PORT}" \
          && elasticsearch_seed_fixture >/dev/null 2>&1 \
          && elasticsearch_health_check >/dev/null 2>&1; then
          ok=1
          break
        fi
        ;;
      jenkins)
        if port_open "${VOIDB_FIXTURE_HOST}" "${VOIDB_FIXTURE_PORT}" \
          && jenkins_seed_fixture >/dev/null 2>&1 \
          && jenkins_health_check >/dev/null 2>&1; then
          ok=1
          break
        fi
        ;;
      kubernetes)
        if kubernetes_health_check >/dev/null 2>&1; then
          ok=1
          break
        fi
        ;;
    esac
    sleep 1
  done

  if [[ "${ok}" -ne 1 ]]; then
    capture_logs || true
    die "fixture did not become healthy within ${TIMEOUT}s"
  fi

  echo "fixture: ${FIXTURE}"
  echo "run_id: ${RUN_ID}"
  echo "status: ready"
  echo "health: $(health_summary)"
}

redact_stream() {
  local sensitive_values=()
  if [[ -f "$(env_path)" ]]; then
    local name
    local value
    while IFS='=' read -r name value; do
      if [[ "${name}" =~ (PASSWORD|TOKEN|SECRET|KEY|URL|URI|CONNECTION_STRING|AUTH|COOKIE|USER|DATABASE|INDEX|DOCUMENT_ID|JOB|KUBECONFIG|API_SERVER) \
        && -n "${value}" ]]; then
        sensitive_values+=("${value}")
      fi
    done < "$(env_path)"
  fi

  local line
  while IFS= read -r line; do
    local value
    if ((${#sensitive_values[@]} > 0)); then
      for value in "${sensitive_values[@]}"; do
        line="${line//${value}/<redacted:fixture-secret>}"
      done
    fi
    line="$(printf '%s\n' "${line}" \
      | sed -E 's#(redis|mysql|postgres|mongodb|https?)://[^[:space:]]+#<redacted:url>#g')"
    printf '%s\n' "${line}"
  done
}

capture_logs() {
  load_env_file
  local log_file
  log_file="$(log_path)"
  local container
  container="${VOIDB_FIXTURE_CONTAINER}"

  mkdir -p "$(fixture_dir)"
  docker logs --tail "${TAIL_LINES}" --timestamps "${container}" 2>&1 \
    | redact_stream > "${log_file}"

  echo "fixture: ${FIXTURE}"
  echo "run_id: ${RUN_ID}"
  echo "log_file: ${log_file}"
  echo "redaction: applied"
}

write_evidence() {
  local status="$1"
  local cleanup_status="$2"
  load_env_file

  local report
  if [[ -n "${REPORT_PATH}" ]]; then
    report="${REPORT_PATH}"
  else
    report="$(default_report_path)"
  fi

  local commit
  commit="$(/usr/bin/git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || echo "unknown")"
  local docker_version
  docker_version="$(docker version --format 'client={{.Client.Version}} server={{.Server.Version}}' 2>/dev/null || echo "unknown")"

  mkdir -p "$(dirname "${report}")"
  cat > "${report}" <<EOF
# Local Fixture Smoke Evidence

- Requirement: $(evidence_requirement)
- Fixture: ${FIXTURE}
- Run id: ${RUN_ID}
- Commit: ${commit}
- Status: ${status}
- Platform: $(uname -s)-$(uname -m)
- Docker: ${docker_version}
- Image: ${VOIDB_FIXTURE_IMAGE}
- Container: ${VOIDB_FIXTURE_CONTAINER}
- Network: ${VOIDB_FIXTURE_NETWORK}
- Health: $(health_summary)
- Variables present by name: $(fixture_variables)
- Log capture: $(log_path)
- Redaction: applied
- Cleanup: ${cleanup_status}
- Release decision: $(release_decision)
EOF

  echo "report: ${report}"
}

teardown_fixture() {
  ensure_ids
  local env_file
  env_file="$(env_path)"
  local container
  container="$(container_name)"
  local network
  network="$(network_name)"

  if [[ -f "${env_file}" ]]; then
    # shellcheck disable=SC1090
    source "${env_file}"
    container="${VOIDB_FIXTURE_CONTAINER:-${container}}"
    network="${VOIDB_FIXTURE_NETWORK:-${network}}"
  fi

  if [[ "${FIXTURE}" == "kubernetes" ]]; then
    if kubernetes_cluster_exists; then
      run_step kind delete cluster --quiet --name "$(kubernetes_cluster_name)" >/dev/null
    fi
    rm -f "$(kubernetes_kubeconfig_path)" "$(kubernetes_seed_manifest)" "${env_file}"
    echo "fixture: ${FIXTURE}"
    echo "run_id: ${RUN_ID}"
    echo "cleanup: removed kind cluster and generated env file when present"
    return
  fi

  if docker ps -a --format '{{.Names}}' | grep -Fxq "${container}"; then
    run_step docker rm -f "${container}" >/dev/null
  fi

  if docker network inspect "${network}" >/dev/null 2>&1; then
    run_step docker network rm "${network}" >/dev/null
  fi

  if [[ "${FIXTURE}" == "ssh" ]]; then
    rm -rf "$(ssh_config_dir)"
    rm -f "$(ssh_known_hosts_path)"
  fi
  if [[ "${FIXTURE}" == "webdav" ]]; then
    rm -rf "$(webdav_root_dir)"
  fi
  if [[ "${FIXTURE}" == "mysql" ]]; then
    rm -rf "$(mysql_init_dir)"
  fi
  if [[ "${FIXTURE}" == "mongodb" ]]; then
    rm -rf "$(mongodb_init_dir)"
  fi
  if [[ "${FIXTURE}" == "jenkins" ]]; then
    rm -rf "$(jenkins_init_dir)"
    rm -f "$(jenkins_build_marker)"
  fi

  rm -f "${env_file}"
  echo "fixture: ${FIXTURE}"
  echo "run_id: ${RUN_ID}"
  echo "cleanup: removed container, network, and generated env file when present"
}

show_status() {
  ensure_ids
  local container
  container="$(container_name)"
  local network
  network="$(network_name)"
  local container_state="missing"
  local network_state="missing"
  local env_state="missing"

  if [[ "${FIXTURE}" == "kubernetes" ]]; then
    if docker ps -a --format '{{.Names}}' | grep -Fxq "${container}"; then
      container_state="$(docker inspect -f '{{.State.Status}}' "${container}")"
    fi
    if docker network inspect "$(network_name)" >/dev/null 2>&1; then
      network_state="shared-kind-present"
    fi
    if [[ -f "$(env_path)" ]]; then
      env_state="present"
    fi
    echo "fixture: ${FIXTURE}"
    echo "run_id: ${RUN_ID}"
    echo "container: ${container_state}"
    echo "network: ${network_state}"
    echo "env_file: ${env_state}"
    return
  fi

  if docker ps -a --format '{{.Names}}' | grep -Fxq "${container}"; then
    container_state="$(docker inspect -f '{{.State.Status}}' "${container}")"
  fi
  if docker network inspect "${network}" >/dev/null 2>&1; then
    network_state="present"
  fi
  if [[ -f "$(env_path)" ]]; then
    env_state="present"
  fi

  echo "fixture: ${FIXTURE}"
  echo "run_id: ${RUN_ID}"
  echo "container: ${container_state}"
  echo "network: ${network_state}"
  echo "env_file: ${env_state}"
}

run_fixture() {
  ensure_ids
  local cleanup_status="not_run"

  trap 'teardown_fixture >/dev/null 2>&1 || true' INT TERM ERR
  start_fixture
  wait_fixture
  capture_logs

  if [[ "${KEEP}" -eq 1 ]]; then
    cleanup_status="preserved by --keep"
  else
    teardown_fixture
    if [[ "${FIXTURE}" == "kubernetes" ]]; then
      cleanup_status="removed kind cluster and generated env file"
    else
      cleanup_status="removed container, network, and generated env file"
    fi
  fi

  trap - INT TERM ERR
  if [[ "${KEEP}" -eq 1 ]]; then
    write_evidence "passed" "${cleanup_status}"
  else
    # Recreate enough state for the evidence writer after teardown removes env.
    write_env_after_teardown "${cleanup_status}"
  fi
}

write_env_after_teardown() {
  local cleanup_status="$1"
  local env_file
  env_file="$(env_path)"
  umask 077
  mkdir -p "$(fixture_dir)"
  case "${FIXTURE}" in
    probe|redis)
      cat > "${env_file}" <<EOF
VOIDB_FIXTURE_RUN_ID=${RUN_ID}
VOIDB_FIXTURE_NAME=${FIXTURE}
VOIDB_FIXTURE_CONTAINER=$(container_name)
VOIDB_FIXTURE_NETWORK=$(network_name)
VOIDB_FIXTURE_IMAGE=${IMAGE}
VOIDB_FIXTURE_HOST=127.0.0.1
VOIDB_FIXTURE_PORT=removed
VOIDB_REDIS_SMOKE_HOST=127.0.0.1
VOIDB_REDIS_SMOKE_PORT=removed
VOIDB_REDIS_SMOKE_DB=$(redis_smoke_db)
VOIDB_REDIS_SMOKE_PROFILE=$(redis_profile_name)
VOIDB_REDIS_SMOKE_KEY_PREFIX=$(redis_key_prefix)
VOIDB_REDIS_SMOKE_URL=removed
EOF
      ;;
    email)
      cat > "${env_file}" <<EOF
VOIDB_FIXTURE_RUN_ID=${RUN_ID}
VOIDB_FIXTURE_NAME=${FIXTURE}
VOIDB_FIXTURE_CONTAINER=$(container_name)
VOIDB_FIXTURE_NETWORK=$(network_name)
VOIDB_FIXTURE_IMAGE=${IMAGE}
VOIDB_FIXTURE_HOST=127.0.0.1
VOIDB_FIXTURE_PORT=removed
VOIDB_EMAIL_SMOKE_CONNECTION=$(email_connection_name)
VOIDB_EMAIL_SMOKE_FOLDER=INBOX
VOIDB_EMAIL_SMOKE_PROTOCOL=imap
VOIDB_EMAIL_SMOKE_SECURITY=none
VOIDB_EMAIL_SMOKE_EMAIL=$(email_address)
VOIDB_EMAIL_SMOKE_PASSWORD=$(email_password)
VOIDB_EMAIL_SMOKE_IMAP_HOST=127.0.0.1
VOIDB_EMAIL_SMOKE_IMAP_PORT=removed
VOIDB_EMAIL_SMOKE_POP3_HOST=127.0.0.1
VOIDB_EMAIL_SMOKE_POP3_PORT=removed
VOIDB_EMAIL_SMOKE_SMTP_CONNECTION=$(email_connection_name)
VOIDB_EMAIL_SMOKE_SMTP_HOST=127.0.0.1
VOIDB_EMAIL_SMOKE_SMTP_PORT=removed
VOIDB_EMAIL_SMOKE_SEND_TO=$(email_address)
VOIDB_EMAIL_SMOKE_API_HOST=127.0.0.1
VOIDB_EMAIL_SMOKE_API_PORT=removed
EOF
      ;;
    ssh)
      cat > "${env_file}" <<EOF
VOIDB_FIXTURE_RUN_ID=${RUN_ID}
VOIDB_FIXTURE_NAME=${FIXTURE}
VOIDB_FIXTURE_CONTAINER=$(container_name)
VOIDB_FIXTURE_NETWORK=$(network_name)
VOIDB_FIXTURE_IMAGE=${IMAGE}
VOIDB_FIXTURE_HOST=127.0.0.1
VOIDB_FIXTURE_PORT=removed
VOIDB_SSH_SMOKE_PROFILE=$(ssh_profile_name)
VOIDB_SSH_SMOKE_HOST=127.0.0.1
VOIDB_SSH_SMOKE_PORT=removed
VOIDB_SSH_SMOKE_USER=$(ssh_user)
VOIDB_SSH_SMOKE_PASSWORD=$(ssh_password)
VOIDB_SSH_SMOKE_PRIVATE_KEY_PATH=removed
VOIDB_SSH_SMOKE_PUBLIC_KEY_PATH=removed
VOIDB_SSH_SMOKE_KNOWN_HOSTS=removed
VOIDB_SSH_SMOKE_SCRATCH=$(ssh_scratch_dir)
VOIDB_SSH_SMOKE_REMOTE_FILE=$(ssh_remote_file)
EOF
      ;;
    s3)
      cat > "${env_file}" <<EOF
VOIDB_FIXTURE_RUN_ID=${RUN_ID}
VOIDB_FIXTURE_NAME=${FIXTURE}
VOIDB_FIXTURE_CONTAINER=$(container_name)
VOIDB_FIXTURE_NETWORK=$(network_name)
VOIDB_FIXTURE_IMAGE=${IMAGE}
VOIDB_FIXTURE_HOST=127.0.0.1
VOIDB_FIXTURE_PORT=removed
VOIDB_S3_SMOKE_PROFILE=$(s3_profile_name)
VOIDB_S3_SMOKE_CONNECTION=$(s3_profile_name)
VOIDB_S3_SMOKE_PROVIDER=minio
VOIDB_S3_SMOKE_ENDPOINT=removed
VOIDB_S3_SMOKE_BUCKET=$(s3_bucket)
VOIDB_S3_SMOKE_PREFIX=$(s3_prefix)
VOIDB_S3_SMOKE_REGION=$(s3_region)
VOIDB_S3_SMOKE_ACCESS_KEY=$(s3_access_key)
VOIDB_S3_SMOKE_SECRET_KEY=$(s3_secret_key)
VOIDB_S3_SMOKE_URL=removed
VOIDB_S3_SMOKE_CLIENT_IMAGE=${S3_CLIENT_IMAGE}
EOF
      ;;
    webdav)
      cat > "${env_file}" <<EOF
VOIDB_FIXTURE_RUN_ID=${RUN_ID}
VOIDB_FIXTURE_NAME=${FIXTURE}
VOIDB_FIXTURE_CONTAINER=$(container_name)
VOIDB_FIXTURE_NETWORK=$(network_name)
VOIDB_FIXTURE_IMAGE=${IMAGE}
VOIDB_FIXTURE_HOST=127.0.0.1
VOIDB_FIXTURE_PORT=removed
VOIDB_WEBDAV_SMOKE_PROFILE=$(webdav_profile_name)
VOIDB_WEBDAV_SMOKE_CONNECTION=$(webdav_profile_name)
VOIDB_WEBDAV_SMOKE_URL=removed
VOIDB_WEBDAV_SMOKE_ROOT=$(webdav_root_path)
VOIDB_WEBDAV_SMOKE_SEED_PATH=$(webdav_seed_path)
VOIDB_WEBDAV_SMOKE_USER=$(webdav_user)
VOIDB_WEBDAV_SMOKE_PASSWORD=$(webdav_password)
VOIDB_WEBDAV_SMOKE_AUTH=basic
VOIDB_WEBDAV_SMOKE_VERIFY_SSL=false
EOF
      ;;
    mysql)
      cat > "${env_file}" <<EOF
VOIDB_FIXTURE_RUN_ID=${RUN_ID}
VOIDB_FIXTURE_NAME=${FIXTURE}
VOIDB_FIXTURE_CONTAINER=$(container_name)
VOIDB_FIXTURE_NETWORK=$(network_name)
VOIDB_FIXTURE_IMAGE=${IMAGE}
VOIDB_FIXTURE_HOST=127.0.0.1
VOIDB_FIXTURE_PORT=removed
VOIDB_MYSQL_SMOKE_PROFILE=$(mysql_profile_name)
VOIDB_MYSQL_SMOKE_CONNECTION=$(mysql_profile_name)
VOIDB_MYSQL_SMOKE_HOST=127.0.0.1
VOIDB_MYSQL_SMOKE_PORT=removed
VOIDB_MYSQL_SMOKE_USER=$(mysql_user)
VOIDB_MYSQL_SMOKE_PASSWORD=$(mysql_password)
VOIDB_MYSQL_SMOKE_DATABASE=$(mysql_database)
VOIDB_MYSQL_SMOKE_TABLE=$(mysql_seed_table)
VOIDB_MYSQL_SMOKE_SSL_MODE=disabled
VOIDB_MYSQL_TEST_HOST=127.0.0.1
VOIDB_MYSQL_TEST_PORT=removed
VOIDB_MYSQL_TEST_USER=$(mysql_user)
VOIDB_MYSQL_TEST_PASSWORD=$(mysql_password)
VOIDB_MYSQL_TEST_DATABASE=$(mysql_database)
EOF
      ;;
    mongodb)
      local db_name
      db_name="$(mongodb_database)"
      cat > "${env_file}" <<EOF
VOIDB_FIXTURE_RUN_ID=${RUN_ID}
VOIDB_FIXTURE_NAME=${FIXTURE}
VOIDB_FIXTURE_CONTAINER=$(container_name)
VOIDB_FIXTURE_NETWORK=$(network_name)
VOIDB_FIXTURE_IMAGE=${IMAGE}
VOIDB_FIXTURE_HOST=127.0.0.1
VOIDB_FIXTURE_PORT=removed
VOIDB_MONGODB_SMOKE_PROFILE=$(mongodb_profile_name)
VOIDB_MONGODB_SMOKE_CONNECTION=$(mongodb_profile_name)
VOIDB_MONGODB_SMOKE_URI=removed
VOIDB_MONGODB_SMOKE_HOST=127.0.0.1
VOIDB_MONGODB_SMOKE_PORT=removed
VOIDB_MONGODB_SMOKE_USER=$(mongodb_user)
VOIDB_MONGODB_SMOKE_PASSWORD=$(mongodb_password)
VOIDB_MONGODB_SMOKE_DATABASE=${db_name}
VOIDB_MONGODB_SMOKE_COLLECTION=$(mongodb_collection)
VOIDB_MONGODB_SMOKE_AUTH_SOURCE=${db_name}
VOIDB_MONGODB_SMOKE_DOCUMENT_ID=$(mongodb_document_id)
EOF
      ;;
    elasticsearch)
      cat > "${env_file}" <<EOF
VOIDB_FIXTURE_RUN_ID=${RUN_ID}
VOIDB_FIXTURE_NAME=${FIXTURE}
VOIDB_FIXTURE_CONTAINER=$(container_name)
VOIDB_FIXTURE_NETWORK=$(network_name)
VOIDB_FIXTURE_IMAGE=${IMAGE}
VOIDB_FIXTURE_HOST=127.0.0.1
VOIDB_FIXTURE_PORT=removed
VOIDB_ES_SMOKE_PROFILE=$(elasticsearch_profile_name)
VOIDB_ES_SMOKE_CONNECTION=$(elasticsearch_profile_name)
VOIDB_ES_SMOKE_URL=removed
VOIDB_ES_SMOKE_ENDPOINT=removed
VOIDB_ES_SMOKE_HOST=127.0.0.1
VOIDB_ES_SMOKE_PORT=removed
VOIDB_ES_SMOKE_INDEX=$(elasticsearch_index)
VOIDB_ES_SMOKE_DOCUMENT_ID=$(elasticsearch_document_id)
VOIDB_ES_SMOKE_VERIFY_SSL=false
EOF
      ;;
    jenkins)
      cat > "${env_file}" <<EOF
VOIDB_FIXTURE_RUN_ID=${RUN_ID}
VOIDB_FIXTURE_NAME=${FIXTURE}
VOIDB_FIXTURE_CONTAINER=$(container_name)
VOIDB_FIXTURE_NETWORK=$(network_name)
VOIDB_FIXTURE_IMAGE=${IMAGE}
VOIDB_FIXTURE_HOST=127.0.0.1
VOIDB_FIXTURE_PORT=removed
VOIDB_JENKINS_SMOKE_PROFILE=$(jenkins_profile_name)
VOIDB_JENKINS_SMOKE_CONNECTION=$(jenkins_profile_name)
VOIDB_JENKINS_SMOKE_URL=removed
VOIDB_JENKINS_SMOKE_HOST=127.0.0.1
VOIDB_JENKINS_SMOKE_PORT=removed
VOIDB_JENKINS_SMOKE_USER=$(jenkins_user)
VOIDB_JENKINS_SMOKE_TOKEN=$(jenkins_password)
VOIDB_JENKINS_SMOKE_JOB=$(jenkins_job_name)
VOIDB_JENKINS_SMOKE_BUILD=$(jenkins_build_number)
VOIDB_JENKINS_SMOKE_VERIFY_SSL=false
EOF
      ;;
    kubernetes)
      cat > "${env_file}" <<EOF
VOIDB_FIXTURE_RUN_ID=${RUN_ID}
VOIDB_FIXTURE_NAME=${FIXTURE}
VOIDB_FIXTURE_CONTAINER=$(container_name)
VOIDB_FIXTURE_NETWORK=$(network_name)
VOIDB_FIXTURE_IMAGE=${IMAGE}
VOIDB_FIXTURE_HOST=127.0.0.1
VOIDB_FIXTURE_PORT=removed
VOIDB_K8S_SMOKE_PROFILE=$(kubernetes_profile_name)
VOIDB_K8S_SMOKE_CONNECTION=$(kubernetes_profile_name)
VOIDB_K8S_SMOKE_CLUSTER=$(kubernetes_cluster_name)
VOIDB_K8S_SMOKE_CONTEXT=$(kubernetes_context_name)
VOIDB_K8S_SMOKE_NAMESPACE=$(kubernetes_namespace)
VOIDB_K8S_SMOKE_CONFIGMAP=$(kubernetes_configmap_name)
VOIDB_K8S_SMOKE_KUBECONFIG=removed
VOIDB_K8S_SMOKE_API_SERVER=removed
EOF
      ;;
  esac
  write_evidence "passed" "${cleanup_status}"
  rm -f "${env_file}"
}

if [[ $# -gt 0 ]]; then
  case "$1" in
    run|start|wait|logs|teardown|status)
      COMMAND="$1"
      shift
      ;;
  esac
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --fixture)
      [[ $# -ge 2 ]] || die "missing value for --fixture"
      FIXTURE="$2"
      shift 2
      ;;
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
    --keep)
      KEEP=1
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

case "${COMMAND}" in
  run)
    run_fixture
    ;;
  start)
    start_fixture
    ;;
  wait)
    wait_fixture
    ;;
  logs)
    capture_logs
    ;;
  teardown)
    ensure_docker
    teardown_fixture
    ;;
  status)
    ensure_docker
    show_status
    ;;
  *)
    die "unsupported command: ${COMMAND}"
    ;;
esac
