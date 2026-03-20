#!/usr/bin/env bash
# Copyright 2026 Yellowbrick Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

DUCKDB_IMAGE=${DUCKDB_IMAGE:-duckdb/duckdb:latest}
DOCKER_COMPOSE_MAIN=${DOCKER_COMPOSE_MAIN:-docker compose -f docker/docker-compose.yml}
COMPOSE_SMOKE_SAVE_LOG_DIR_DEFAULT=${COMPOSE_SMOKE_SAVE_LOG_DIR:-target/compose-smoke-logs}
COMPOSE_SMOKE_KEEP_ON_FAIL=${COMPOSE_SMOKE_KEEP_ON_FAIL:-false}
COMPOSE_SMOKE_KEEP_ON_EXIT=${COMPOSE_SMOKE_KEEP_ON_EXIT:-false}
COMPOSE_SMOKE_CLIENTS=${COMPOSE_SMOKE_CLIENTS:-duckdb,trino}
COMPOSE_SMOKE_UPSTREAM_ICEBERG_IMPORT=${COMPOSE_SMOKE_UPSTREAM_ICEBERG_IMPORT:-true}
COMPOSE_SMOKE_UPSTREAM_ICEBERG_URI=${COMPOSE_SMOKE_UPSTREAM_ICEBERG_URI:-http://polaris:8181/api/catalog}
COMPOSE_SMOKE_UPSTREAM_ICEBERG_SOURCE_NS=${COMPOSE_SMOKE_UPSTREAM_ICEBERG_SOURCE_NS:-sales}
COMPOSE_SMOKE_UPSTREAM_ICEBERG_DEST_CATALOG=${COMPOSE_SMOKE_UPSTREAM_ICEBERG_DEST_CATALOG:-polaris_import}
COMPOSE_SMOKE_UPSTREAM_ICEBERG_EXPECTED_TABLE=${COMPOSE_SMOKE_UPSTREAM_ICEBERG_EXPECTED_TABLE:-polaris_import.sales.trino_types}
COMPOSE_SMOKE_UPSTREAM_ICEBERG_WAREHOUSE=${COMPOSE_SMOKE_UPSTREAM_ICEBERG_WAREHOUSE:-quickstart_catalog}
COMPOSE_SMOKE_UPSTREAM_ICEBERG_TABLE=${COMPOSE_SMOKE_UPSTREAM_ICEBERG_TABLE:-trino_types}
COMPOSE_SMOKE_UPSTREAM_ICEBERG_METADATA_PREFIX=${COMPOSE_SMOKE_UPSTREAM_ICEBERG_METADATA_PREFIX:-s3://floecat/sales/us/trino_types/metadata/}
COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_IMPORT=${COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_IMPORT:-true}
COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_URI=${COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_URI:-http://unity:8080}
COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_SOURCE_NS=${COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_SOURCE_NS:-unity.default}
COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_SOURCE_TABLE=${COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_SOURCE_TABLE:-call_center}
COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_DEST_CATALOG=${COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_DEST_CATALOG:-unity_import}
COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_DEST_NS=${COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_DEST_NS:-unity_smoke}
COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_EXPECTED_TABLE=${COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_EXPECTED_TABLE:-unity_import.unity_smoke.call_center}
COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_CONNECTOR_ARGS=${COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_CONNECTOR_ARGS:---auth-scheme none}
COMPOSE_SMOKE_UNITY_HEALTH_PATH=${COMPOSE_SMOKE_UNITY_HEALTH_PATH:-/api/2.1/unity-catalog/catalogs}
COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_STORAGE_LOCATION=${COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_STORAGE_LOCATION:-s3://floecat-delta/call_center}
COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_S3_ENDPOINT=${COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_S3_ENDPOINT:-http://localstack:4566}
COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_S3_REGION=${COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_S3_REGION:-us-east-1}
COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_S3_ACCESS_KEY_ID=${COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_S3_ACCESS_KEY_ID:-test}
COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_S3_SECRET_ACCESS_KEY=${COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_S3_SECRET_ACCESS_KEY:-test}
COMPOSE_SMOKE_ICEBERG_FORMAT_MATRIX=${COMPOSE_SMOKE_ICEBERG_FORMAT_MATRIX:-true}
COMPOSE_SMOKE_ICEBERG_FORMAT_MATRIX_STATS_RETRIES=${COMPOSE_SMOKE_ICEBERG_FORMAT_MATRIX_STATS_RETRIES:-45}
COMPOSE_SMOKE_ICEBERG_FORMAT_MATRIX_STATS_SLEEP_SECONDS=${COMPOSE_SMOKE_ICEBERG_FORMAT_MATRIX_STATS_SLEEP_SECONDS:-2}

is_truthy() {
  local raw="${1:-}"
  local normalized="${raw,,}"
  case "${normalized}" in
    1|true|yes|on)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

should_keep_on_fail() {
  is_truthy "${COMPOSE_SMOKE_KEEP_ON_FAIL}"
}

should_keep_on_exit() {
  is_truthy "${COMPOSE_SMOKE_KEEP_ON_EXIT}"
}

should_run_client() {
  local client="$1"
  local normalized=",${COMPOSE_SMOKE_CLIENTS//[[:space:]]/},"
  [[ "$normalized" == *",$client,"* ]]
}

docker_run() {
  docker run "$@"
}

run_cli_script() {
  local compose_cmd="$1"
  local script="$2"
  printf "%s\n" "$script" | eval "$compose_cmd run --rm -T cli"
}

gateway_json_request() {
  local compose_project="$1"
  local method="$2"
  local path="$3"
  local body="${4:-}"
  local idempotency_key="${5:-}"
  local curl_args=(
    --rm
    --network
    "${compose_project}_floecat"
    curlimages/curl:8.12.1
    -sS
    -w
    "\n%{http_code}\n"
    -X
    "$method"
    "http://iceberg-rest:9200$path"
    -H
    "Content-Type: application/json"
  )
  if [ -n "$idempotency_key" ]; then
    curl_args+=(-H "Idempotency-Key: $idempotency_key")
  fi
  if [ -n "$body" ]; then
    curl_args+=(-d "$body")
  fi
  docker_run "${curl_args[@]}"
}

latest_s3_metadata_uri() {
  local compose_project="$1"
  local metadata_prefix="$2"
  local aws_cli
  aws_cli="docker_run --rm --network ${compose_project}_floecat -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test -e AWS_DEFAULT_REGION=us-east-1 amazon/aws-cli:2.17.50"
  local metadata_key
  metadata_key=$($aws_cli --endpoint-url http://localstack:4566 s3 ls "$metadata_prefix" --recursive \
    | awk '$4 ~ /\.metadata\.json$/ {print $4}' \
    | tail -n1)
  if [ -z "$metadata_key" ]; then
    return 1
  fi
  printf 's3://floecat/%s\n' "$metadata_key"
}

wait_for_connector_job() {
  local compose_cmd="$1"
  local label="$2"
  local job_id="$3"
  local max_attempts="${4:-90}"
  local sleep_seconds="${5:-2}"
  local attempt
  local out
  local state

  if [ -z "$job_id" ]; then
    echo "[FAIL] $label missing reconcile job id"
    return 1
  fi

  for attempt in $(seq 1 "$max_attempts"); do
    out=$(run_cli_script "$compose_cmd" "account t-0001
connector job $job_id
quit")
    state=$(printf "%s\n" "$out" | sed -n 's/.*state=\([A-Z_]*\).*/\1/p' | head -n1)
    if [ "$state" = "JS_SUCCEEDED" ]; then
      echo "[PASS] $label job succeeded ($job_id)"
      return 0
    fi
    if [ "$state" = "JS_FAILED" ] || [ "$state" = "JS_CANCELLED" ]; then
      echo "$out"
      echo "[FAIL] $label job terminal state=$state ($job_id)"
      return 1
    fi
    sleep "$sleep_seconds"
  done

  echo "$out"
  echo "[FAIL] $label job timed out waiting for terminal success ($job_id)"
  return 1
}

assert_contains() {
  local check_name="$1"
  local output="$2"
  local pattern="$3"

  if echo "$output" | grep -q "$pattern"; then
    echo "[PASS] $check_name"
  else
    echo "[FAIL] $check_name (missing: $pattern)"
    echo "---- output begin ----"
    echo "$output"
    echo "---- output end ----"
    return 1
  fi
}

assert_table_stats_available() {
  local compose_cmd="$1"
  local label="$2"
  local table_fqn="$3"
  local retries="$4"
  local sleep_seconds="$5"
  local attempt
  local out
  local snapshot_out
  local snapshot_id
  local snapshot_ids
  local last_out=""

  for attempt in $(seq 1 "$retries"); do
    out=$(run_cli_script "$compose_cmd" "account t-0001
stats files $table_fqn --current --limit 5
quit")
    last_out="$out"
    if echo "$out" | grep -q "PATH" && ! echo "$out" | grep -q "No file stats found."; then
      assert_contains "$label stats header" "$out" "PATH"
      echo "[PASS] $label stats available for $table_fqn"
      return 0
    fi

    # Current snapshot can briefly lag metrics writes; probe a few explicit snapshots as fallback.
    snapshot_out=$(run_cli_script "$compose_cmd" "account t-0001
snapshots $table_fqn
quit")
    snapshot_ids=$(echo "$snapshot_out" | awk '/^[[:space:]]*[0-9]+[[:space:]]/ {print $1}' | head -n 5)

    for snapshot_id in $snapshot_ids; do
      out=$(run_cli_script "$compose_cmd" "account t-0001
stats files $table_fqn --snapshot $snapshot_id --limit 5
quit")
      last_out="$out"
      if echo "$out" | grep -q "PATH" && ! echo "$out" | grep -q "No file stats found."; then
        assert_contains "$label stats header" "$out" "PATH"
        echo "[PASS] $label stats available for $table_fqn (snapshot=$snapshot_id)"
        return 0
      fi
    done

    sleep "$sleep_seconds"
  done

  echo "[FAIL] $label stats missing for $table_fqn"
  echo "---- output begin ----"
  echo "$last_out"
  echo "---- output end ----"
  return 1
}

maybe_assert_table_stats_available() {
  local compose_cmd="$1"
  local label="$2"
  local table_fqn="$3"
  local retries="$4"
  local sleep_seconds="$5"
  assert_table_stats_available "$compose_cmd" "$label" "$table_fqn" "$retries" "$sleep_seconds"
}

dump_dv_demo_delta_debug() {
  local compose_cmd="$1"
  local compose_project="$2"
  local label="$3"
  local duckdb_bootstrap="$4"
  local aws_cli="docker_run --rm --network ${compose_project}_floecat -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test -e AWS_DEFAULT_REGION=us-east-1 amazon/aws-cli:2.17.50"
  local out=""
  local snapshot_out=""
  local snapshot_ids=""
  local snapshot_id=""
  local gateway_resp=""
  local duckdb_debug_out=""

  echo "==> [SMOKE][DIAG] $label dv_demo_delta table get"
  out=$(run_cli_script "$compose_cmd" "account t-0001
table get examples.delta.dv_demo_delta
quit")
  echo "$out"

  echo "==> [SMOKE][DIAG] $label dv_demo_delta snapshots"
  snapshot_out=$(run_cli_script "$compose_cmd" "account t-0001
snapshots examples.delta.dv_demo_delta
quit")
  echo "$snapshot_out"

  echo "==> [SMOKE][DIAG] $label dv_demo_delta current file stats"
  out=$(run_cli_script "$compose_cmd" "account t-0001
stats files examples.delta.dv_demo_delta --current --limit 20
quit")
  echo "$out"

  echo "==> [SMOKE][DIAG] $label localstack dv_demo_delta S3 contents"
  $aws_cli --endpoint-url http://localstack:4566 s3 ls s3://floecat-delta/dv_demo_delta/ --recursive | sed -n '1,400p' || true

  echo "==> [SMOKE][DIAG] $label localstack dv_demo_delta compat metadata contents"
  $aws_cli --endpoint-url http://localstack:4566 s3 ls s3://floecat-delta/dv_demo_delta/metadata/ --recursive | sed -n '1,400p' || true

  snapshot_ids=$(printf "%s\n" "$snapshot_out" | awk '/^[[:space:]]*[0-9]+[[:space:]]/ {print $1}' | head -n 5)
  for snapshot_id in $snapshot_ids; do
    echo "==> [SMOKE][DIAG] $label dv_demo_delta file stats snapshot=$snapshot_id"
    out=$(run_cli_script "$compose_cmd" "account t-0001
stats files examples.delta.dv_demo_delta --snapshot $snapshot_id --limit 20
quit")
    echo "$out"
  done

  echo "==> [SMOKE][DIAG] $label gateway delta table response"
  gateway_resp=$(gateway_json_request "$compose_project" GET "/v1/examples/namespaces/delta/tables/dv_demo_delta")
  echo "$gateway_resp"
  echo "==> [SMOKE][DIAG] $label gateway delta compat snapshot summary"
  if command -v python3 >/dev/null 2>&1; then
    GATEWAY_RESP="$gateway_resp" python3 -c '
import json
import os

raw = os.environ.get("GATEWAY_RESP", "").strip()
if not raw:
    raise SystemExit(0)
lines = raw.splitlines()
payload = lines[0]
doc = json.loads(payload)
metadata = doc.get("metadata") or {}
current = metadata.get("current-snapshot-id")
refs = metadata.get("refs") or {}
main = ((refs.get("main") or {}).get("snapshot-id"))
snapshots = metadata.get("snapshots") or []
for snap in snapshots:
    sid = snap.get("snapshot-id")
    summary = snap.get("summary") or {}
    total_records = summary.get("total-records", "?")
    manifest = snap.get("manifest-list", "")
    flags = []
    if sid == current:
        flags.append("current")
    if sid == main:
        flags.append("main")
    flag_text = ",".join(flags) if flags else "-"
    print(f"compat_snapshot id={sid} flags={flag_text} total_records={total_records} manifest={manifest}")
' || true
  else
    echo "[WARN] python3 unavailable; skipping compact gateway snapshot summary"
  fi

  echo "==> [SMOKE][DIAG] $label duckdb dv_demo_delta focused query"
  if duckdb_debug_out=$(docker_run --rm --network "${compose_project}_floecat" "$DUCKDB_IMAGE" \
    duckdb -c "$duckdb_bootstrap SELECT 'dv_debug_count=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.delta.dv_demo_delta; SELECT 'dv_debug_content=' || COALESCE(CAST(MIN(id) AS VARCHAR), 'NULL') || ',' || COALESCE(CAST(MAX(id) AS VARCHAR), 'NULL') || ',' || COALESCE(MIN(v), 'NULL') || ',' || COALESCE(MAX(v), 'NULL') AS check FROM iceberg_floecat.delta.dv_demo_delta; SELECT id, v FROM iceberg_floecat.delta.dv_demo_delta ORDER BY id;" 2>&1); then
    echo "$duckdb_debug_out"
  else
    echo "$duckdb_debug_out"
    echo "[WARN] $label duckdb focused dv_demo_delta query failed"
  fi
}

cleanup_mode() {
  local compose_cmd="$1"
  eval "$compose_cmd down --remove-orphans -v" >/dev/null 2>&1 || true
}

save_mode_logs() {
  local compose_cmd="$1"
  local label="$2"
  local log_dir="$COMPOSE_SMOKE_SAVE_LOG_DIR_DEFAULT"

  mkdir -p "$log_dir" || true
  eval "$compose_cmd ps" > "$log_dir/${label}-ps.log" 2>&1 || true
  eval "$compose_cmd ps -a" > "$log_dir/${label}-ps-all.log" 2>&1 || true
  eval "$compose_cmd config" > "$log_dir/${label}-compose-config.log" 2>&1 || true
  eval "$compose_cmd logs --no-color" > "$log_dir/${label}-compose.log" 2>&1 || true
  local services
  services=$(eval "$compose_cmd config --services" 2>/dev/null || true)
  if [ -n "$services" ]; then
    local svc
    for svc in $services; do
      eval "$compose_cmd logs --no-color $svc" > "$log_dir/${label}-service-${svc}.log" 2>&1 || true
    done
  fi
}

save_mode_container_diagnostics() {
  local compose_cmd="$1"
  local label="$2"
  local log_dir="$COMPOSE_SMOKE_SAVE_LOG_DIR_DEFAULT"

  mkdir -p "$log_dir" || true

  local container_ids
  container_ids=$(eval "$compose_cmd ps -aq" 2>/dev/null || true)
  if [ -z "$container_ids" ]; then
    return 0
  fi

  local cid
  for cid in $container_ids; do
    local cname
    local safe_name
    cname=$(docker inspect --format '{{.Name}}' "$cid" 2>/dev/null | sed 's#^/##' || true)
    if [ -z "$cname" ]; then
      cname="$cid"
    fi
    safe_name=$(echo "$cname" | sed 's#[^A-Za-z0-9._-]#_#g')
    docker logs --timestamps "$cid" > "$log_dir/${label}-${safe_name}-docker.log" 2>&1 || true
    docker inspect "$cid" > "$log_dir/${label}-${safe_name}-inspect.json" 2>&1 || true
    docker exec "$cid" sh -lc 'env | sort' > "$log_dir/${label}-${safe_name}-env.log" 2>&1 || true
    case "$cname" in
      *service*|*iceberg-rest*)
        docker exec "$cid" sh -lc "find / -name 'floecat-protocol-gateway-common-test-*.jar' -print 2>/dev/null" \
          > "$log_dir/${label}-${safe_name}-common-test-jar-paths.log" 2>&1 || true
        ;;
    esac
  done
}

wait_for_url() {
  local url="$1"
  local timeout_seconds="$2"
  local label="$3"
  local i

  for i in $(seq 1 "$timeout_seconds"); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    if [ "$i" -eq "$timeout_seconds" ]; then
      echo "$label timed out" >&2
      return 1
    fi
    sleep 1
  done
}

run_mode() {
  local env_file="$1"
  local profile="$2"
  local label="$3"
  local pre_services="$4"
  local kc_host="${5:-}"
  local kc_port="${6:-}"

  local compose_project="floecat-smoke-$label"
  local compose_profiles="$profile"
  if [ "$profile" = "localstack" ] && is_truthy "$COMPOSE_SMOKE_UPSTREAM_ICEBERG_IMPORT"; then
    compose_profiles="${profile},polaris"
  fi
  if [ "$profile" = "localstack" ] && is_truthy "$COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_IMPORT"; then
    compose_profiles="${compose_profiles},unity"
  fi
  if { [ "$profile" = "localstack" ] || [ "$profile" = "localstack-oidc" ]; } && should_run_client trino; then
    compose_profiles="${compose_profiles},trino"
  fi
  local mode_env="FLOECAT_ENV_FILE=$env_file COMPOSE_PROFILES=$compose_profiles COMPOSE_PROJECT_NAME=$compose_project"

  if [ -n "$kc_host" ]; then
    mode_env="$mode_env KC_HOSTNAME=$kc_host"
  fi
  if [ -n "$kc_port" ]; then
    mode_env="$mode_env KC_HOSTNAME_PORT=$kc_port"
  fi

  local compose_cmd="$mode_env $DOCKER_COMPOSE_MAIN"

  on_mode_error() {
    echo "==> [SMOKE][FAIL] mode=$label"
    eval "$compose_cmd ps" || true
    echo "==> [SMOKE][DIAG] focused error excerpt"
    eval "$compose_cmd logs --no-color --tail=800" 2>&1 \
      | grep -aE "ERROR|WARN|Exception|FAIL|failed|timed out|currentSnapshot=<null>|snapshotCount=0" \
      | grep -aEv "localstack.request.aws[[:space:]]+: AWS s3\.[A-Za-z]+ => 200|io\.qua\.htt\.access-log.*\"(GET|HEAD) .*\" 20[04]" \
      || true
    echo "==> [SMOKE][DIAG] fallback tail (last 120 lines)"
    eval "$compose_cmd logs --no-color --tail=120" || true
    save_mode_logs "$compose_cmd" "${label}-fail"
    save_mode_container_diagnostics "$compose_cmd" "${label}-fail"
    if should_keep_on_fail || should_keep_on_exit; then
      echo "==> [SMOKE][KEEP] preserving compose stack for mode=$label (COMPOSE_SMOKE_KEEP_ON_FAIL=$COMPOSE_SMOKE_KEEP_ON_FAIL)"
    else
      cleanup_mode "$compose_cmd"
    fi
  }
  trap on_mode_error ERR

  on_mode_return() {
    local rc=$?
    if [ "$rc" -eq 0 ]; then
      save_mode_logs "$compose_cmd" "$label"
      save_mode_container_diagnostics "$compose_cmd" "$label"
      if should_keep_on_exit; then
        echo "==> [SMOKE][KEEP] preserving compose stack for mode=$label (COMPOSE_SMOKE_KEEP_ON_EXIT=$COMPOSE_SMOKE_KEEP_ON_EXIT)"
      else
        cleanup_mode "$compose_cmd"
      fi
    else
      save_mode_logs "$compose_cmd" "${label}-fail"
      save_mode_container_diagnostics "$compose_cmd" "${label}-fail"
      if should_keep_on_fail || should_keep_on_exit; then
        echo "==> [SMOKE][KEEP] preserving compose stack for mode=$label (COMPOSE_SMOKE_KEEP_ON_FAIL=$COMPOSE_SMOKE_KEEP_ON_FAIL)"
      else
        cleanup_mode "$compose_cmd"
      fi
    fi
    return "$rc"
  }
  trap on_mode_return RETURN

  echo "==> [SMOKE] mode=$label"
  cleanup_mode "$compose_cmd"

  if [ "$profile" = "localstack" ] && is_truthy "$COMPOSE_SMOKE_UPSTREAM_ICEBERG_IMPORT"; then
    if [ -z "$pre_services" ]; then
      pre_services="localstack polaris-db polaris-bootstrap polaris"
    elif [[ ",$pre_services," != *",polaris,"* ]]; then
      pre_services="$pre_services polaris-db polaris-bootstrap polaris"
    fi
  fi
  if [ "$profile" = "localstack" ] && is_truthy "$COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_IMPORT"; then
    if [ -z "$pre_services" ]; then
      pre_services="localstack unity"
    elif [[ ",$pre_services," != *",unity,"* ]]; then
      pre_services="$pre_services unity"
    fi
  fi

  if [ -n "$pre_services" ]; then
    if ! eval "$compose_cmd up -d $pre_services"; then
      if [ "$profile" = "localstack" ] && is_truthy "$COMPOSE_SMOKE_UPSTREAM_ICEBERG_IMPORT"; then
        local polaris_bootstrap_logs
        polaris_bootstrap_logs=$(eval "$compose_cmd logs --no-color polaris-bootstrap 2>&1" || true)
        if echo "$polaris_bootstrap_logs" | grep -aq "already been bootstrapped"; then
          echo "==> [SMOKE] polaris-bootstrap already initialized during pre-start; continuing"
          eval "$compose_cmd up -d --no-deps polaris"
        else
          return 1
        fi
      else
        return 1
      fi
    fi
  fi

  if [ "$profile" = "localstack" ] || [ "$profile" = "localstack-oidc" ]; then
    wait_for_url "http://localhost:4566/_localstack/health" 120 "LocalStack health"
  fi

  if [ "$profile" = "localstack-oidc" ]; then
    wait_for_url "http://localhost:8080/realms/floecat/.well-known/openid-configuration" 180 "Keycloak health"
  fi

  if [ "$profile" = "localstack" ] && is_truthy "$COMPOSE_SMOKE_UPSTREAM_ICEBERG_IMPORT"; then
    local polaris_mgmt_port="${FLOECAT_POLARIS_MGMT_HOST_PORT:-8282}"
    wait_for_url "http://localhost:${polaris_mgmt_port}/q/health" 180 "Polaris health"
  fi
  if [ "$profile" = "localstack" ] && is_truthy "$COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_IMPORT"; then
    local unity_host_port="${FLOECAT_UNITY_HOST_PORT:-8083}"
    wait_for_url "http://localhost:${unity_host_port}${COMPOSE_SMOKE_UNITY_HEALTH_PATH}" 180 "Unity Catalog health"
  fi

  if ! eval "$compose_cmd up -d"; then
    if [ "$profile" = "localstack" ] && is_truthy "$COMPOSE_SMOKE_UPSTREAM_ICEBERG_IMPORT"; then
      local polaris_bootstrap_logs
      polaris_bootstrap_logs=$(eval "$compose_cmd logs --no-color polaris-bootstrap 2>&1" || true)
        if echo "$polaris_bootstrap_logs" | grep -aq "already been bootstrapped"; then
          echo "==> [SMOKE] polaris-bootstrap already initialized; continuing"
          eval "$compose_cmd up -d --no-deps polaris"
        else
          return 1
        fi
    else
      return 1
    fi
  fi

  local i
  for i in $(seq 1 180); do
    if eval "$compose_cmd logs service 2>&1" | grep -q "Startup seeding completed successfully"; then
      break
    fi
    if eval "$compose_cmd logs service 2>&1" | grep -q "Startup seeding failed"; then
      eval "$compose_cmd logs --no-color"
      return 1
    fi
    if [ "$i" -eq 180 ]; then
      echo "Service seed completion timed out" >&2
      eval "$compose_cmd logs --no-color"
      return 1
    fi
    sleep 1
  done

  local out_iceberg
  out_iceberg=$(printf "account t-0001\nresolve table examples.iceberg.trino_types\nquit\n" | eval "$compose_cmd run --rm -T cli")
  echo "$out_iceberg"
  assert_contains "$label cli resolve iceberg account" "$out_iceberg" "account set:"
  assert_contains "$label cli resolve iceberg table" "$out_iceberg" "table id:"

  local out_delta
  out_delta=$(printf "account t-0001\nresolve table examples.delta.call_center\nquit\n" | eval "$compose_cmd run --rm -T cli")
  echo "$out_delta"
  assert_contains "$label cli resolve call_center account" "$out_delta" "account set:"
  assert_contains "$label cli resolve call_center table" "$out_delta" "table id:"

  local out_delta_local
  out_delta_local=$(printf "account t-0001\nresolve table examples.delta.my_local_delta_table\nquit\n" | eval "$compose_cmd run --rm -T cli")
  echo "$out_delta_local"
  assert_contains "$label cli resolve my_local_delta_table account" "$out_delta_local" "account set:"
  assert_contains "$label cli resolve my_local_delta_table table" "$out_delta_local" "table id:"

  local out_delta_dv
  out_delta_dv=$(printf "account t-0001\nresolve table examples.delta.dv_demo_delta\nquit\n" | eval "$compose_cmd run --rm -T cli")
  echo "$out_delta_dv"
  assert_contains "$label cli resolve dv_demo_delta account" "$out_delta_dv" "account set:"
  assert_contains "$label cli resolve dv_demo_delta table" "$out_delta_dv" "table id:"

  if [ "$profile" = "localstack" ] && is_truthy "$COMPOSE_SMOKE_UPSTREAM_ICEBERG_IMPORT"; then
    echo "==> [SMOKE] upstream iceberg rest import check"

    local aws_cli
    aws_cli="docker_run --rm --network ${compose_project}_floecat -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test -e AWS_DEFAULT_REGION=us-east-1 amazon/aws-cli:2.17.50"
    local localstack_container="${compose_project}-localstack-1"
    local warehouse="$COMPOSE_SMOKE_UPSTREAM_ICEBERG_WAREHOUSE"
    local source_namespace="$COMPOSE_SMOKE_UPSTREAM_ICEBERG_SOURCE_NS"
    local source_table="$COMPOSE_SMOKE_UPSTREAM_ICEBERG_TABLE"

    docker exec "$localstack_container" sh -lc "cat > /tmp/polaris-trust.json <<'JSON'
{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"*\"},\"Action\":\"sts:AssumeRole\"}]}
JSON
cat > /tmp/polaris-policy.json <<'JSON'
{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\",\"s3:PutObject\",\"s3:DeleteObject\",\"s3:ListBucket\"],\"Resource\":[\"arn:aws:s3:::floecat\",\"arn:aws:s3:::floecat/*\"]}]}
JSON
awslocal iam create-role --role-name polaris --assume-role-policy-document file:///tmp/polaris-trust.json >/dev/null 2>&1 || true
awslocal iam put-role-policy --role-name polaris --policy-name polaris-s3 --policy-document file:///tmp/polaris-policy.json >/dev/null 2>&1 || true" >/dev/null

    local metadata_key
    metadata_key=$($aws_cli --endpoint-url http://localstack:4566 s3 ls "$COMPOSE_SMOKE_UPSTREAM_ICEBERG_METADATA_PREFIX" --recursive \
      | awk '$4 ~ /\.metadata\.json$/ {print $4}' \
      | tail -n1)
    if [ -z "$metadata_key" ]; then
      echo "[FAIL] $label upstream iceberg metadata not found under $COMPOSE_SMOKE_UPSTREAM_ICEBERG_METADATA_PREFIX"
      return 1
    fi
    local metadata_uri
    metadata_uri="s3://floecat/$metadata_key"
    local bucket_root_uri
    bucket_root_uri="s3://floecat/"
    local metadata_dir_uri
    metadata_dir_uri="s3://floecat/$(dirname "$metadata_key")/"

    local polaris_token
    polaris_token=$(docker_run --rm --network "${compose_project}_floecat" curlimages/curl:8.12.1 -sS \
      -X POST "http://polaris:8181/api/catalog/v1/oauth/tokens" \
      -H "Content-Type: application/x-www-form-urlencoded" \
      --data "grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL" \
      | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')
    if [ -z "$polaris_token" ]; then
      echo "[FAIL] $label upstream iceberg failed to obtain Polaris OAuth token"
      return 1
    fi

    docker_run --rm --network "${compose_project}_floecat" curlimages/curl:8.12.1 -sS \
      -X POST "http://polaris:8181/api/management/v1/catalogs" \
      -H "Authorization: Bearer $polaris_token" \
      -H "Content-Type: application/json" \
      -d "{\"catalog\":{\"name\":\"$warehouse\",\"type\":\"INTERNAL\",\"readOnly\":false,\"properties\":{\"default-base-location\":\"$bucket_root_uri\"},\"storageConfigInfo\":{\"storageType\":\"S3\",\"allowedLocations\":[\"$bucket_root_uri\",\"$metadata_dir_uri\"],\"pathStyleAccess\":true,\"roleArn\":\"arn:aws:iam::000000000000:role/polaris\"}}}" \
      >/dev/null 2>&1 || true

    docker_run --rm --network "${compose_project}_floecat" curlimages/curl:8.12.1 -sS \
      -X POST "http://polaris:8181/api/catalog/v1/$warehouse/namespaces" \
      -H "Authorization: Bearer $polaris_token" \
      -H "Content-Type: application/json" \
      -d "{\"namespace\":[\"$source_namespace\"]}" \
      >/dev/null 2>&1 || true

    local register_resp
    register_resp=$(docker_run --rm --network "${compose_project}_floecat" curlimages/curl:8.12.1 -sS \
      -w "\n%{http_code}\n" \
      -X POST "http://polaris:8181/api/catalog/v1/$warehouse/namespaces/$source_namespace/register" \
      -H "Authorization: Bearer $polaris_token" \
      -H "Content-Type: application/json" \
      -d "{\"name\":\"$source_table\",\"metadata-location\":\"$metadata_uri\"}")
    local register_code
    register_code=$(printf "%s\n" "$register_resp" | tail -n1)
    if [ "$register_code" != "200" ] && [ "$register_code" != "201" ] && [ "$register_code" != "409" ]; then
      echo "[FAIL] $label upstream iceberg register failed (http=$register_code)"
      echo "$register_resp"
      return 1
    fi

    local rest_setup_out
    rest_setup_out=$(run_cli_script "$compose_cmd" "account t-0001
catalog create $COMPOSE_SMOKE_UPSTREAM_ICEBERG_DEST_CATALOG --desc compose-smoke-upstream-iceberg
connector create smoke-upstream-iceberg ICEBERG $COMPOSE_SMOKE_UPSTREAM_ICEBERG_URI $COMPOSE_SMOKE_UPSTREAM_ICEBERG_SOURCE_NS $COMPOSE_SMOKE_UPSTREAM_ICEBERG_DEST_CATALOG --auth-scheme oauth2 --auth token=$polaris_token --props iceberg.source=rest --props warehouse=$warehouse --props s3.endpoint=http://localstack:4566 --props s3.path-style-access=true --props s3.region=us-east-1 --props s3.access-key-id=test --props s3.secret-access-key=test
quit")
    echo "$rest_setup_out"
    assert_contains "$label upstream iceberg connector setup" "$rest_setup_out" "smoke-upstream-iceberg"

    local trigger_rest_out
    trigger_rest_out=$(run_cli_script "$compose_cmd" "account t-0001
connector trigger smoke-upstream-iceberg --full
quit")
    local rest_job_id
    rest_job_id=$(
      printf "%s\n" "$trigger_rest_out" \
        | tr -d '\r' \
        | sed -E 's/^floecat>[[:space:]]*//' \
        | awk 'match($0, /[0-9a-fA-F-]{36}/) {id=substr($0, RSTART, RLENGTH)} END {print id}'
    )
    wait_for_connector_job "$compose_cmd" "$label upstream iceberg connector" "$rest_job_id" 120 2

    local out_upstream_iceberg
    out_upstream_iceberg=$(run_cli_script "$compose_cmd" "account t-0001
resolve table $COMPOSE_SMOKE_UPSTREAM_ICEBERG_EXPECTED_TABLE
quit")
    echo "$out_upstream_iceberg"
    assert_contains "$label upstream iceberg imported account" "$out_upstream_iceberg" "account set:"
    assert_contains "$label upstream iceberg imported table" "$out_upstream_iceberg" "table id:"

    local out_upstream_iceberg_stats
    out_upstream_iceberg_stats=$(run_cli_script "$compose_cmd" "account t-0001
stats files $COMPOSE_SMOKE_UPSTREAM_ICEBERG_EXPECTED_TABLE --current --limit 5
quit")
    echo "$out_upstream_iceberg_stats"
    if echo "$out_upstream_iceberg_stats" | grep -q "No file stats found."; then
      echo "[FAIL] $label upstream iceberg file stats missing"
      return 1
    fi
    assert_contains "$label upstream iceberg file stats header" "$out_upstream_iceberg_stats" "PATH"
  elif [ "$profile" = "localstack" ]; then
    echo "==> [SMOKE] skipping upstream iceberg rest import (set COMPOSE_SMOKE_UPSTREAM_ICEBERG_IMPORT=false to disable)"
  fi

  if [ "$profile" = "localstack" ] && is_truthy "$COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_IMPORT"; then
    echo "==> [SMOKE] upstream delta unity import check"

    local unity_catalog="${COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_SOURCE_NS%%.*}"
    local unity_schema="${COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_SOURCE_NS#*.}"
    local unity_source_table="$COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_SOURCE_TABLE"
    local unity_storage_location="$COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_STORAGE_LOCATION"
    if [ -z "$unity_catalog" ] || [ -z "$unity_schema" ] || [ "$unity_schema" = "$COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_SOURCE_NS" ]; then
      echo "[FAIL] $label invalid COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_SOURCE_NS (expected catalog.schema): $COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_SOURCE_NS"
      return 1
    fi

    local unity_catalog_resp
    unity_catalog_resp=$(docker_run --rm --network "${compose_project}_floecat" curlimages/curl:8.12.1 -sS \
      -w "\n%{http_code}\n" \
      -X POST "http://unity:8080/api/2.1/unity-catalog/catalogs" \
      -H "Content-Type: application/json" \
      -d "{\"name\":\"$unity_catalog\",\"comment\":\"compose-smoke\"}")
    local unity_catalog_code
    unity_catalog_code=$(printf "%s\n" "$unity_catalog_resp" | tail -n1)
    if [ "$unity_catalog_code" != "200" ] && [ "$unity_catalog_code" != "201" ] && [ "$unity_catalog_code" != "409" ]; then
      echo "[FAIL] $label unity catalog create failed (http=$unity_catalog_code)"
      echo "$unity_catalog_resp"
      return 1
    fi

    local unity_schema_resp
    unity_schema_resp=$(docker_run --rm --network "${compose_project}_floecat" curlimages/curl:8.12.1 -sS \
      -w "\n%{http_code}\n" \
      -X POST "http://unity:8080/api/2.1/unity-catalog/schemas" \
      -H "Content-Type: application/json" \
      -d "{\"name\":\"$unity_schema\",\"catalog_name\":\"$unity_catalog\",\"comment\":\"compose-smoke\"}")
    local unity_schema_code
    unity_schema_code=$(printf "%s\n" "$unity_schema_resp" | tail -n1)
    if [ "$unity_schema_code" != "200" ] && [ "$unity_schema_code" != "201" ] && [ "$unity_schema_code" != "409" ]; then
      echo "[FAIL] $label unity schema create failed (http=$unity_schema_code)"
      echo "$unity_schema_resp"
      return 1
    fi

    local unity_table_resp
    unity_table_resp=$(docker_run --rm --network "${compose_project}_floecat" curlimages/curl:8.12.1 -sS \
      -w "\n%{http_code}\n" \
      -X POST "http://unity:8080/api/2.1/unity-catalog/tables" \
      -H "Content-Type: application/json" \
      -d "{\"name\":\"$unity_source_table\",\"catalog_name\":\"$unity_catalog\",\"schema_name\":\"$unity_schema\",\"table_type\":\"EXTERNAL\",\"data_source_format\":\"DELTA\",\"storage_location\":\"$unity_storage_location\"}")
    local unity_table_code
    unity_table_code=$(printf "%s\n" "$unity_table_resp" | tail -n1)
    if [ "$unity_table_code" != "200" ] && [ "$unity_table_code" != "201" ] && [ "$unity_table_code" != "409" ]; then
      echo "[FAIL] $label unity table register failed (http=$unity_table_code)"
      echo "$unity_table_resp"
      return 1
    fi

    local unity_setup_out
    unity_setup_out=$(run_cli_script "$compose_cmd" "account t-0001
catalog create $COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_DEST_CATALOG --desc compose-smoke-upstream-delta-unity
connector create smoke-upstream-delta-unity DELTA $COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_URI $COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_SOURCE_NS $COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_DEST_CATALOG --source-table $COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_SOURCE_TABLE --dest-ns $COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_DEST_NS --props delta.source=unity --props s3.endpoint=$COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_S3_ENDPOINT --props s3.path-style-access=true --props s3.region=$COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_S3_REGION --props s3.access-key-id=$COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_S3_ACCESS_KEY_ID --props s3.secret-access-key=$COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_S3_SECRET_ACCESS_KEY $COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_CONNECTOR_ARGS
quit")
    echo "$unity_setup_out"
    assert_contains "$label upstream delta unity connector setup" "$unity_setup_out" "smoke-upstream-delta-unity"

    local trigger_unity_out
    trigger_unity_out=$(run_cli_script "$compose_cmd" "account t-0001
connector trigger smoke-upstream-delta-unity --full
quit")
    local unity_job_id
    unity_job_id=$(
      printf "%s\n" "$trigger_unity_out" \
        | tr -d '\r' \
        | sed -E 's/^floecat>[[:space:]]*//' \
        | awk 'match($0, /[0-9a-fA-F-]{36}/) {id=substr($0, RSTART, RLENGTH)} END {print id}'
    )
    wait_for_connector_job "$compose_cmd" "$label upstream delta unity connector" "$unity_job_id" 120 2

    local out_upstream_delta_unity
    out_upstream_delta_unity=$(run_cli_script "$compose_cmd" "account t-0001
resolve table $COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_EXPECTED_TABLE
quit")
    echo "$out_upstream_delta_unity"
    assert_contains "$label upstream delta unity imported account" "$out_upstream_delta_unity" "account set:"
    assert_contains "$label upstream delta unity imported table" "$out_upstream_delta_unity" "table id:"

    local out_upstream_delta_unity_stats
    out_upstream_delta_unity_stats=$(run_cli_script "$compose_cmd" "account t-0001
stats files $COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_EXPECTED_TABLE --current --limit 5
quit")
    echo "$out_upstream_delta_unity_stats"
    if echo "$out_upstream_delta_unity_stats" | grep -q "No file stats found."; then
      echo "[FAIL] $label upstream delta unity file stats missing"
      return 1
    fi
    assert_contains "$label upstream delta unity file stats header" "$out_upstream_delta_unity_stats" "PATH"
  elif [ "$profile" = "localstack" ]; then
    echo "==> [SMOKE] skipping upstream delta unity import (set COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_IMPORT=true to enable)"
  fi

  if [ "$profile" = "localstack" ]; then
    echo "==> [SMOKE] gateway endpoint checks"

    local namespace_props_resp
    namespace_props_resp=$(gateway_json_request \
      "$compose_project" \
      POST \
      "/v1/examples/namespaces/iceberg/properties" \
      '{"removals":["compose-smoke.missing"],"updates":{"compose-smoke.owner":"gateway-smoke","compose-smoke.endpoint":"namespace-properties"}}' \
      "smoke-gateway-namespace-properties")
    local namespace_props_code
    namespace_props_code=$(printf "%s\n" "$namespace_props_resp" | tail -n1)
    if [ "$namespace_props_code" != "200" ]; then
      echo "[FAIL] $label gateway namespace property update failed (http=$namespace_props_code)"
      echo "$namespace_props_resp"
      return 1
    fi
    assert_contains "$label gateway namespace properties updated owner" "$namespace_props_resp" '"compose-smoke.owner"'
    assert_contains "$label gateway namespace properties updated endpoint" "$namespace_props_resp" '"compose-smoke.endpoint"'
    assert_contains "$label gateway namespace properties missing key" "$namespace_props_resp" '"compose-smoke.missing"'

    local namespace_cli_out
    namespace_cli_out=$(run_cli_script "$compose_cmd" "account t-0001
namespace get examples.iceberg
quit")
    echo "$namespace_cli_out"
    assert_contains "$label gateway namespace cli owner property" "$namespace_cli_out" "compose-smoke.owner = gateway-smoke"
    assert_contains "$label gateway namespace cli endpoint property" "$namespace_cli_out" "compose-smoke.endpoint = namespace-properties"

    local credentials_resp
    credentials_resp=$(gateway_json_request \
      "$compose_project" \
      GET \
      "/v1/examples/namespaces/iceberg/tables/trino_types/credentials")
    local credentials_code
    credentials_code=$(printf "%s\n" "$credentials_resp" | tail -n1)
    if [ "$credentials_code" != "200" ]; then
      echo "[FAIL] $label gateway credentials request failed (http=$credentials_code)"
      echo "$credentials_resp"
      return 1
    fi
    assert_contains "$label gateway credentials storage credentials key" "$credentials_resp" '"storage-credentials"'

    local plan_resp
    plan_resp=$(gateway_json_request \
      "$compose_project" \
      POST \
      "/v1/examples/namespaces/iceberg/tables/trino_types/plan" \
      '{}' \
      "smoke-gateway-plan-create")
    local plan_code
    plan_code=$(printf "%s\n" "$plan_resp" | tail -n1)
    if [ "$plan_code" != "200" ]; then
      echo "[FAIL] $label gateway plan create failed (http=$plan_code)"
      echo "$plan_resp"
      return 1
    fi
    assert_contains "$label gateway plan completed status" "$plan_resp" '"status":"completed"'
    assert_contains "$label gateway plan task list key" "$plan_resp" '"plan-tasks"'
    assert_contains "$label gateway plan scan tasks key" "$plan_resp" '"file-scan-tasks"'
    assert_contains "$label gateway plan data file key" "$plan_resp" '"data-file"'
    local plan_id
    plan_id=$(printf "%s\n" "$plan_resp" | sed -n 's/.*"plan-id":"\([^"]*\)".*/\1/p' | head -n1)
    if [ -z "$plan_id" ]; then
      echo "[FAIL] $label gateway plan id missing"
      echo "$plan_resp"
      return 1
    fi

    local plan_get_resp
    plan_get_resp=$(gateway_json_request \
      "$compose_project" \
      GET \
      "/v1/examples/namespaces/iceberg/tables/trino_types/plan/${plan_id}")
    local plan_get_code
    plan_get_code=$(printf "%s\n" "$plan_get_resp" | tail -n1)
    if [ "$plan_get_code" != "200" ]; then
      echo "[FAIL] $label gateway plan fetch failed (http=$plan_get_code)"
      echo "$plan_get_resp"
      return 1
    fi
    assert_contains "$label gateway plan fetch completed status" "$plan_get_resp" '"status":"completed"'
    assert_contains "$label gateway plan fetch scan tasks key" "$plan_get_resp" '"file-scan-tasks"'

    local plan_task_resp
    plan_task_resp=$(gateway_json_request \
      "$compose_project" \
      POST \
      "/v1/examples/namespaces/iceberg/tables/trino_types/tasks" \
      '{"plan-task":"missing-task"}')
    local plan_task_code
    plan_task_code=$(printf "%s\n" "$plan_task_resp" | tail -n1)
    if [ "$plan_task_code" != "404" ]; then
      echo "[FAIL] $label gateway plan task missing-task check failed (http=$plan_task_code)"
      echo "$plan_task_resp"
      return 1
    fi
    assert_contains "$label gateway plan task not found type" "$plan_task_resp" 'NoSuchPlanTask'

    local plan_delete_resp
    plan_delete_resp=$(gateway_json_request \
      "$compose_project" \
      DELETE \
      "/v1/examples/namespaces/iceberg/tables/trino_types/plan/${plan_id}" \
      '' \
      "smoke-gateway-plan-delete")
    local plan_delete_code
    plan_delete_code=$(printf "%s\n" "$plan_delete_resp" | tail -n1)
    if [ "$plan_delete_code" != "204" ]; then
      echo "[FAIL] $label gateway plan delete failed (http=$plan_delete_code)"
      echo "$plan_delete_resp"
      return 1
    fi

    local plan_missing_resp
    plan_missing_resp=$(gateway_json_request \
      "$compose_project" \
      GET \
      "/v1/examples/namespaces/iceberg/tables/trino_types/plan/${plan_id}")
    local plan_missing_code
    plan_missing_code=$(printf "%s\n" "$plan_missing_resp" | tail -n1)
    if [ "$plan_missing_code" != "200" ]; then
      echo "[FAIL] $label gateway deleted plan fetch check failed (http=$plan_missing_code)"
      echo "$plan_missing_resp"
      return 1
    fi
    assert_contains "$label gateway deleted plan cancelled status" "$plan_missing_resp" '"status":"cancelled"'

    local view_name="minimal_view_smoke"
    local view_renamed_name="minimal_view_smoke_renamed"
    gateway_json_request "$compose_project" DELETE "/v1/examples/namespaces/iceberg/views/${view_name}" '' "smoke-gateway-view-cleanup-src" >/dev/null 2>&1 || true
    gateway_json_request "$compose_project" DELETE "/v1/examples/namespaces/iceberg/views/${view_renamed_name}" '' "smoke-gateway-view-cleanup-dst" >/dev/null 2>&1 || true

    local view_create_resp
    view_create_resp=$(gateway_json_request \
      "$compose_project" \
      POST \
      "/v1/examples/namespaces/iceberg/views" \
      "{\"name\":\"${view_name}\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"c\",\"required\":false,\"type\":\"int\"}]},\"view-version\":{\"version-id\":0,\"timestamp-ms\":1,\"schema-id\":0,\"summary\":{},\"representations\":[{\"type\":\"sql\",\"sql\":\"select 1 as c\",\"dialect\":\"ansi\"}],\"default-namespace\":[\"iceberg\"]},\"properties\":{\"compose-smoke.view\":\"create\"}}" \
      "smoke-gateway-view-create")
    local view_create_code
    view_create_code=$(printf "%s\n" "$view_create_resp" | tail -n1)
    if [ "$view_create_code" != "200" ]; then
      echo "[FAIL] $label gateway view create failed (http=$view_create_code)"
      echo "$view_create_resp"
      return 1
    fi
    assert_contains "$label gateway view create metadata location" "$view_create_resp" '"metadata-location"'

    local view_list_resp
    view_list_resp=$(gateway_json_request \
      "$compose_project" \
      GET \
      "/v1/examples/namespaces/iceberg/views")
    local view_list_code
    view_list_code=$(printf "%s\n" "$view_list_resp" | tail -n1)
    if [ "$view_list_code" != "200" ]; then
      echo "[FAIL] $label gateway view list failed (http=$view_list_code)"
      echo "$view_list_resp"
      return 1
    fi
    assert_contains "$label gateway view listed" "$view_list_resp" "\"name\":\"${view_name}\""

    local view_get_resp
    view_get_resp=$(gateway_json_request \
      "$compose_project" \
      GET \
      "/v1/examples/namespaces/iceberg/views/${view_name}")
    local view_get_code
    view_get_code=$(printf "%s\n" "$view_get_resp" | tail -n1)
    if [ "$view_get_code" != "200" ]; then
      echo "[FAIL] $label gateway view get failed (http=$view_get_code)"
      echo "$view_get_resp"
      return 1
    fi
    assert_contains "$label gateway view get metadata key" "$view_get_resp" '"metadata"'

    local view_head_resp
    view_head_resp=$(gateway_json_request \
      "$compose_project" \
      HEAD \
      "/v1/examples/namespaces/iceberg/views/${view_name}")
    local view_head_code
    view_head_code=$(printf "%s\n" "$view_head_resp" | tail -n1)
    if [ "$view_head_code" != "204" ]; then
      echo "[FAIL] $label gateway view head failed (http=$view_head_code)"
      echo "$view_head_resp"
      return 1
    fi

    local view_commit_resp
    view_commit_resp=$(gateway_json_request \
      "$compose_project" \
      POST \
      "/v1/examples/namespaces/iceberg/views/${view_name}" \
      "{\"requirements\":[],\"updates\":[{\"action\":\"set-location\",\"location\":\"floecat://views/iceberg/${view_name}/v2.metadata.json\"}]}" \
      "smoke-gateway-view-commit")
    local view_commit_code
    view_commit_code=$(printf "%s\n" "$view_commit_resp" | tail -n1)
    if [ "$view_commit_code" != "200" ]; then
      echo "[FAIL] $label gateway view commit failed (http=$view_commit_code)"
      echo "$view_commit_resp"
      return 1
    fi
    assert_contains "$label gateway view commit new location" "$view_commit_resp" "floecat://views/iceberg/${view_name}/v2.metadata.json"

    local view_rename_resp
    view_rename_resp=$(gateway_json_request \
      "$compose_project" \
      POST \
      "/v1/examples/views/rename" \
      "{\"source\":{\"namespace\":[\"iceberg\"],\"name\":\"${view_name}\"},\"destination\":{\"namespace\":[\"iceberg\"],\"name\":\"${view_renamed_name}\"}}" \
      "smoke-gateway-view-rename")
    local view_rename_code
    view_rename_code=$(printf "%s\n" "$view_rename_resp" | tail -n1)
    if [ "$view_rename_code" != "204" ]; then
      echo "[FAIL] $label gateway view rename failed (http=$view_rename_code)"
      echo "$view_rename_resp"
      return 1
    fi

    local view_renamed_get_resp
    view_renamed_get_resp=$(gateway_json_request \
      "$compose_project" \
      GET \
      "/v1/examples/namespaces/iceberg/views/${view_renamed_name}")
    local view_renamed_get_code
    view_renamed_get_code=$(printf "%s\n" "$view_renamed_get_resp" | tail -n1)
    if [ "$view_renamed_get_code" != "200" ]; then
      echo "[FAIL] $label gateway renamed view get failed (http=$view_renamed_get_code)"
      echo "$view_renamed_get_resp"
      return 1
    fi
    assert_contains "$label gateway renamed view get metadata key" "$view_renamed_get_resp" '"metadata"'

    local view_delete_resp
    view_delete_resp=$(gateway_json_request \
      "$compose_project" \
      DELETE \
      "/v1/examples/namespaces/iceberg/views/${view_renamed_name}" \
      '' \
      "smoke-gateway-view-delete")
    local view_delete_code
    view_delete_code=$(printf "%s\n" "$view_delete_resp" | tail -n1)
    if [ "$view_delete_code" != "204" ]; then
      echo "[FAIL] $label gateway view delete failed (http=$view_delete_code)"
      echo "$view_delete_resp"
      return 1
    fi

    local view_deleted_head_resp
    view_deleted_head_resp=$(gateway_json_request \
      "$compose_project" \
      HEAD \
      "/v1/examples/namespaces/iceberg/views/${view_renamed_name}")
    local view_deleted_head_code
    view_deleted_head_code=$(printf "%s\n" "$view_deleted_head_resp" | tail -n1)
    if [ "$view_deleted_head_code" != "404" ]; then
      echo "[FAIL] $label gateway deleted view head check failed (http=$view_deleted_head_code)"
      echo "$view_deleted_head_resp"
      return 1
    fi

    local register_metadata_uri
    register_metadata_uri=$(latest_s3_metadata_uri "$compose_project" "$COMPOSE_SMOKE_UPSTREAM_ICEBERG_METADATA_PREFIX") || {
      echo "[FAIL] $label gateway register metadata not found under $COMPOSE_SMOKE_UPSTREAM_ICEBERG_METADATA_PREFIX"
      return 1
    }
    local register_table="minimal_register_smoke"
    local register_create_resp
    register_create_resp=$(gateway_json_request \
      "$compose_project" \
      POST \
      "/v1/examples/namespaces/iceberg/register" \
      "{\"name\":\"$register_table\",\"metadata-location\":\"$register_metadata_uri\",\"properties\":{\"compose-smoke.register\":\"create\"}}" \
      "smoke-gateway-register-create")
    local register_create_code
    register_create_code=$(printf "%s\n" "$register_create_resp" | tail -n1)
    if [ "$register_create_code" != "200" ] && [ "$register_create_code" != "409" ]; then
      echo "[FAIL] $label gateway register create failed (http=$register_create_code)"
      echo "$register_create_resp"
      return 1
    fi

    local register_overwrite_resp
    register_overwrite_resp=$(gateway_json_request \
      "$compose_project" \
      POST \
      "/v1/examples/namespaces/iceberg/register" \
      "{\"name\":\"$register_table\",\"metadata-location\":\"$register_metadata_uri\",\"overwrite\":true,\"properties\":{\"compose-smoke.register\":\"overwrite\",\"compose-smoke.source\":\"curl\"}}" \
      "smoke-gateway-register-overwrite")
    local register_overwrite_code
    register_overwrite_code=$(printf "%s\n" "$register_overwrite_resp" | tail -n1)
    if [ "$register_overwrite_code" != "200" ]; then
      echo "[FAIL] $label gateway register overwrite failed (http=$register_overwrite_code)"
      echo "$register_overwrite_resp"
      return 1
    fi
    assert_contains "$label gateway register overwrite response" "$register_overwrite_resp" '"metadata-location"'

    local register_cli_out
    register_cli_out=$(run_cli_script "$compose_cmd" "account t-0001
resolve table examples.iceberg.$register_table
table get examples.iceberg.$register_table
quit")
    echo "$register_cli_out"
    assert_contains "$label gateway register cli resolve" "$register_cli_out" "table id:"
    assert_contains "$label gateway register cli metadata property" "$register_cli_out" "metadata-location = $register_metadata_uri"
    assert_contains "$label gateway register cli current snapshot" "$register_cli_out" "current-snapshot-id = "

    maybe_assert_table_stats_available \
      "$compose_cmd" \
      "$label gateway register stats" \
      "examples.iceberg.$register_table" \
      "$COMPOSE_SMOKE_ICEBERG_FORMAT_MATRIX_STATS_RETRIES" \
      "$COMPOSE_SMOKE_ICEBERG_FORMAT_MATRIX_STATS_SLEEP_SECONDS"

    if should_run_client duckdb; then
      local duckdb_register_bootstrap
      duckdb_register_bootstrap="INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws; INSTALL iceberg; LOAD iceberg; CREATE OR REPLACE SECRET smoke_localstack_s3 (TYPE S3, PROVIDER config, KEY_ID 'test', SECRET 'test', REGION 'us-east-1', ENDPOINT 'localstack:4566', URL_STYLE 'path', USE_SSL false); SET s3_endpoint='localstack:4566'; SET s3_use_ssl=false; SET s3_url_style='path'; SET s3_region='us-east-1'; SET s3_access_key_id='test'; SET s3_secret_access_key='test'; ATTACH 'examples' AS iceberg_floecat (TYPE iceberg, ENDPOINT 'http://iceberg-rest:9200/', AUTHORIZATION_TYPE none, ACCESS_DELEGATION_MODE 'none'); SET s3_endpoint='localstack:4566'; SET s3_use_ssl=false; SET s3_url_style='path'; SET s3_region='us-east-1'; SET s3_access_key_id='test'; SET s3_secret_access_key='test';"
      local duckdb_register_out
      if ! duckdb_register_out=$(docker_run --rm --network "${compose_project}_floecat" "$DUCKDB_IMAGE" \
        duckdb -c "$duckdb_register_bootstrap SELECT 'registered_count=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.iceberg.${register_table};" 2>&1); then
        echo "$duckdb_register_out"
        echo "[FAIL] $label gateway register duckdb query failed"
        return 1
      fi
      echo "$duckdb_register_out"
      assert_contains "$label gateway register duckdb query" "$duckdb_register_out" "registered_count=1"
    fi

    if should_run_client trino; then
      local trino_register_out
      if ! trino_register_out=$(docker_run --rm --network "${compose_project}_floecat" -i python:3.12-alpine python - <<PY 2>&1
import json
import urllib.request

TRINO_URL = "http://trino:8080/v1/statement"
HEADERS = {
    "X-Trino-User": "smoke",
    "X-Trino-Source": "compose-smoke",
    "X-Trino-Catalog": "floecat",
    "X-Trino-Schema": "iceberg",
}

sql = "SELECT 'registered_count=' || CAST(COUNT(*) AS VARCHAR) FROM iceberg.${register_table}"
req = urllib.request.Request(
    TRINO_URL,
    data=sql.encode("utf-8"),
    headers={**HEADERS, "Content-Type": "text/plain; charset=utf-8"},
    method="POST",
)
rows = []
with urllib.request.urlopen(req, timeout=60) as resp:
    payload = json.loads(resp.read().decode("utf-8"))
while True:
    if payload.get("error"):
        err = payload["error"]
        raise RuntimeError(f"{err.get('errorName')}: {err.get('message')}")
    if payload.get("data"):
        rows.extend(payload["data"])
    next_uri = payload.get("nextUri")
    if not next_uri:
        break
    next_req = urllib.request.Request(next_uri, headers=HEADERS, method="GET")
    with urllib.request.urlopen(next_req, timeout=60) as next_resp:
        payload = json.loads(next_resp.read().decode("utf-8"))
print(rows[0][0])
PY
); then
        echo "$trino_register_out"
        echo "[FAIL] $label gateway register trino query failed"
        return 1
      fi
      echo "$trino_register_out"
      assert_contains "$label gateway register trino query" "$trino_register_out" "registered_count=1"
    fi
  fi

  if [ "$profile" = "localstack" ] && should_run_client trino; then
    local trino_host_port="${FLOECAT_TRINO_HOST_PORT:-8081}"
    wait_for_url "http://localhost:${trino_host_port}/v1/info" 180 "Trino health"
  fi

  if [ "$profile" = "localstack" ] && should_run_client duckdb; then
    local aws_cli="docker_run --rm --network ${compose_project}_floecat -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test -e AWS_DEFAULT_REGION=us-east-1 amazon/aws-cli:2.17.50"
    echo "==> [SMOKE] localstack pre-duckdb S3 listing (floecat-delta)"
    $aws_cli --endpoint-url http://localstack:4566 s3 ls s3://floecat-delta/ --recursive | sed -n '1,400p' || true
    echo "==> [SMOKE] localstack pre-duckdb S3 listing (floecat-delta/call_center)"
    $aws_cli --endpoint-url http://localstack:4566 s3 ls s3://floecat-delta/call_center/ --recursive | sed -n '1,200p' || true

    echo "==> [SMOKE] duckdb federation check (localstack)"
    local duckdb_bootstrap="INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws; INSTALL iceberg; LOAD iceberg; CREATE OR REPLACE SECRET smoke_localstack_s3 (TYPE S3, PROVIDER config, KEY_ID 'test', SECRET 'test', REGION 'us-east-1', ENDPOINT 'localstack:4566', URL_STYLE 'path', USE_SSL false); SET s3_endpoint='localstack:4566'; SET s3_use_ssl=false; SET s3_url_style='path'; SET s3_region='us-east-1'; SET s3_access_key_id='test'; SET s3_secret_access_key='test'; ATTACH 'examples' AS iceberg_floecat (TYPE iceberg, ENDPOINT 'http://iceberg-rest:9200/', AUTHORIZATION_TYPE none, ACCESS_DELEGATION_MODE 'none'); SET s3_endpoint='localstack:4566'; SET s3_use_ssl=false; SET s3_url_style='path'; SET s3_region='us-east-1'; SET s3_access_key_id='test'; SET s3_secret_access_key='test';"

    local duckdb_query
    duckdb_query="SELECT 'duckdb_smoke_ok' AS status; SELECT 'call_center=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.delta.call_center; SELECT 'my_local_delta_table=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.delta.my_local_delta_table; SELECT 'my_local_nonnull_name=' || CAST(COUNT(name) AS VARCHAR) AS check FROM iceberg_floecat.delta.my_local_delta_table; SELECT 'dv_demo_delta=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.delta.dv_demo_delta; SELECT 'dv_content=' || CAST(MIN(id) AS VARCHAR) || ',' || CAST(MAX(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) AS check FROM iceberg_floecat.delta.dv_demo_delta; SELECT 'empty_join=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.iceberg.trino_types i JOIN iceberg_floecat.delta.call_center d ON 1=0; DROP TABLE IF EXISTS iceberg_floecat.iceberg.duckdb_ctas_smoke; CREATE TABLE iceberg_floecat.iceberg.duckdb_ctas_smoke AS SELECT * FROM iceberg_floecat.delta.call_center LIMIT 5; SELECT 'ctas_count=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.iceberg.duckdb_ctas_smoke; DROP TABLE iceberg_floecat.iceberg.duckdb_ctas_smoke; DROP TABLE IF EXISTS iceberg_floecat.iceberg.duckdb_mutation_smoke; CREATE TABLE iceberg_floecat.iceberg.duckdb_mutation_smoke (id INTEGER, v VARCHAR); SELECT 'mut_after_create=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.iceberg.duckdb_mutation_smoke; INSERT INTO iceberg_floecat.iceberg.duckdb_mutation_smoke VALUES (1, 'a'), (2, 'b'), (3, 'c'); SELECT 'mut_after_insert=' || CAST(COUNT(*) AS VARCHAR) || ',' || CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) AS check FROM iceberg_floecat.iceberg.duckdb_mutation_smoke; DELETE FROM iceberg_floecat.iceberg.duckdb_mutation_smoke WHERE id = 2; SELECT 'mut_after_delete=' || CAST(COUNT(*) AS VARCHAR) || ',' || CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) AS check FROM iceberg_floecat.iceberg.duckdb_mutation_smoke; UPDATE iceberg_floecat.iceberg.duckdb_mutation_smoke SET v = 'c2' WHERE id = 3; SELECT 'mut_after_update=' || CAST(COUNT(*) AS VARCHAR) || ',' || CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) AS check FROM iceberg_floecat.iceberg.duckdb_mutation_smoke;"

    local duckdb_out
    if ! duckdb_out=$(docker_run --rm --network "${compose_project}_floecat" "$DUCKDB_IMAGE" \
      duckdb -c "$duckdb_bootstrap $duckdb_query" 2>&1); then
      echo "$duckdb_out"
      echo "[FAIL] $label duckdb command failed"
      return 1
    fi

    echo "$duckdb_out"
    dump_dv_demo_delta_debug "$compose_cmd" "$compose_project" "$label" "$duckdb_bootstrap"
    assert_contains "$label duckdb smoke marker" "$duckdb_out" "duckdb_smoke_ok"
    assert_contains "$label duckdb call_center count" "$duckdb_out" "call_center=42"
    assert_contains "$label duckdb my_local_delta_table count" "$duckdb_out" "my_local_delta_table=4"
    assert_contains "$label duckdb my_local_delta_table nonnull" "$duckdb_out" "my_local_nonnull_name=4"
    assert_contains "$label duckdb dv_demo_delta count" "$duckdb_out" "dv_demo_delta=2"
    assert_contains "$label duckdb dv_demo_delta content" "$duckdb_out" "dv_content=1,3,a,c"
    assert_contains "$label duckdb ctas count" "$duckdb_out" "ctas_count=5"
    assert_contains "$label duckdb mutation create" "$duckdb_out" "mut_after_create=0"
    assert_contains "$label duckdb mutation insert" "$duckdb_out" "mut_after_insert=3,6,a,c"
    assert_contains "$label duckdb mutation delete" "$duckdb_out" "mut_after_delete=2,4,a,c"
    assert_contains "$label duckdb mutation update" "$duckdb_out" "mut_after_update=2,4,a,c2"

    local duckdb_rename_out
    if duckdb_rename_out=$(docker_run --rm --network "${compose_project}_floecat" "$DUCKDB_IMAGE" \
      duckdb -c "$duckdb_bootstrap DROP TABLE IF EXISTS iceberg_floecat.iceberg.duckdb_rename_dst_smoke; DROP TABLE IF EXISTS iceberg_floecat.iceberg.duckdb_rename_src_smoke; CREATE TABLE iceberg_floecat.iceberg.duckdb_rename_src_smoke (id INTEGER, v VARCHAR); INSERT INTO iceberg_floecat.iceberg.duckdb_rename_src_smoke VALUES (1, 'x'); ALTER TABLE iceberg_floecat.iceberg.duckdb_rename_src_smoke RENAME TO duckdb_rename_dst_smoke; SELECT 'rename_row_count=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.iceberg.duckdb_rename_dst_smoke; DROP TABLE iceberg_floecat.iceberg.duckdb_rename_dst_smoke;" 2>&1); then
      echo "$duckdb_rename_out"
      assert_contains "$label duckdb rename row count" "$duckdb_rename_out" "rename_row_count=1"
    else
      echo "$duckdb_rename_out"
      assert_contains "$label duckdb rename unsupported" "$duckdb_rename_out" "Not implemented Error: Alter Schema Entry"
      docker_run --rm --network "${compose_project}_floecat" "$DUCKDB_IMAGE" \
        duckdb -c "$duckdb_bootstrap DROP TABLE IF EXISTS iceberg_floecat.iceberg.duckdb_rename_dst_smoke; DROP TABLE IF EXISTS iceberg_floecat.iceberg.duckdb_rename_src_smoke;" >/dev/null 2>&1 || true
    fi

    local duckdb_namespace_out
    if duckdb_namespace_out=$(docker_run --rm --network "${compose_project}_floecat" "$DUCKDB_IMAGE" \
      duckdb -c "$duckdb_bootstrap CREATE SCHEMA IF NOT EXISTS iceberg_floecat.duckdb_ns_smoke; SELECT 'ns_create_ok=1' AS check; DROP SCHEMA iceberg_floecat.duckdb_ns_smoke; SELECT 'ns_drop_ok=1' AS check;" 2>&1); then
      echo "$duckdb_namespace_out"
      assert_contains "$label duckdb namespace create" "$duckdb_namespace_out" "ns_create_ok=1"
      assert_contains "$label duckdb namespace drop" "$duckdb_namespace_out" "ns_drop_ok=1"
    else
      echo "$duckdb_namespace_out"
      echo "[WARN] $label duckdb namespace lifecycle unsupported; skipping strict assertion"
    fi

    local duckdb_snapshots_out
    local duckdb_snapshots_query_fn="COPY (SELECT snapshot_id FROM iceberg_snapshots('iceberg_floecat.iceberg.duckdb_mutation_smoke') ORDER BY timestamp_ms DESC) TO '/dev/stdout' (FORMAT CSV, HEADER FALSE);"
    if ! duckdb_snapshots_out=$(docker_run --rm --network "${compose_project}_floecat" "$DUCKDB_IMAGE" \
      duckdb -csv -c "$duckdb_bootstrap $duckdb_snapshots_query_fn" 2>&1); then
      echo "$duckdb_snapshots_out"
      echo "[FAIL] $label duckdb time travel snapshot discovery failed"
      return 1
    fi
    local duckdb_snapshot_ids
    mapfile -t duckdb_snapshot_ids < <(printf '%s\n' "$duckdb_snapshots_out" | tr -d '\r' | awk '/^[0-9]+$/ {print $1}')
    if [ "${#duckdb_snapshot_ids[@]}" -lt 3 ]; then
      echo "$duckdb_snapshots_out"
      echo "[FAIL] $label duckdb time travel requires at least 3 snapshots, found ${#duckdb_snapshot_ids[@]}"
      return 1
    fi
    local duckdb_snapshot_update_id="${duckdb_snapshot_ids[0]}"
    local duckdb_snapshot_delete_id="${duckdb_snapshot_ids[1]}"
    local duckdb_snapshot_insert_id="${duckdb_snapshot_ids[2]}"

    local duckdb_tt_query="SELECT 'mut_tt_after_insert=' || CAST(COUNT(*) AS VARCHAR) || ',' || CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) AS check FROM iceberg_floecat.iceberg.duckdb_mutation_smoke AT (VERSION => ${duckdb_snapshot_insert_id}); SELECT 'mut_tt_after_delete=' || CAST(COUNT(*) AS VARCHAR) || ',' || CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) AS check FROM iceberg_floecat.iceberg.duckdb_mutation_smoke AT (VERSION => ${duckdb_snapshot_delete_id}); SELECT 'mut_tt_after_update=' || CAST(COUNT(*) AS VARCHAR) || ',' || CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) AS check FROM iceberg_floecat.iceberg.duckdb_mutation_smoke AT (VERSION => ${duckdb_snapshot_update_id});"
    local duckdb_tt_out
    if ! duckdb_tt_out=$(docker_run --rm --network "${compose_project}_floecat" "$DUCKDB_IMAGE" \
      duckdb -c "$duckdb_bootstrap $duckdb_tt_query" 2>&1); then
      echo "$duckdb_tt_out"
      echo "[FAIL] $label duckdb time travel query failed"
      return 1
    fi
    echo "$duckdb_tt_out"
    assert_contains "$label duckdb time travel insert snapshot" "$duckdb_tt_out" "mut_tt_after_insert=3,6,a,c"
    assert_contains "$label duckdb time travel delete snapshot" "$duckdb_tt_out" "mut_tt_after_delete=2,4,a,c"
    assert_contains "$label duckdb time travel update snapshot" "$duckdb_tt_out" "mut_tt_after_update=2,4,a,c2"
    maybe_assert_table_stats_available \
      "$compose_cmd" \
      "$label duckdb mutation baseline" \
      "examples.iceberg.duckdb_mutation_smoke" \
      "$COMPOSE_SMOKE_ICEBERG_FORMAT_MATRIX_STATS_RETRIES" \
      "$COMPOSE_SMOKE_ICEBERG_FORMAT_MATRIX_STATS_SLEEP_SECONDS"

    local alter_out
    if alter_out=$(docker_run --rm --network "${compose_project}_floecat" "$DUCKDB_IMAGE" \
      duckdb -c "$duckdb_bootstrap ALTER TABLE iceberg_floecat.iceberg.duckdb_mutation_smoke ADD COLUMN note VARCHAR; SELECT 'mut_after_alter=' || CAST(COUNT(*) AS VARCHAR) || ',' || CAST(COUNT(note) AS VARCHAR) AS check FROM iceberg_floecat.iceberg.duckdb_mutation_smoke; DROP TABLE iceberg_floecat.iceberg.duckdb_mutation_smoke;" 2>&1); then
      echo "$alter_out"
      assert_contains "$label duckdb mutation alter" "$alter_out" "mut_after_alter=2,0"
    else
      echo "$alter_out"
      assert_contains "$label duckdb mutation alter unsupported" "$alter_out" "Not implemented Error: Alter Schema Entry"
      docker_run --rm --network "${compose_project}_floecat" "$DUCKDB_IMAGE" \
        duckdb -c "$duckdb_bootstrap DROP TABLE IF EXISTS iceberg_floecat.iceberg.duckdb_mutation_smoke;" >/dev/null 2>&1 || true
    fi

    local drop_check_out
    if drop_check_out=$(docker_run --rm --network "${compose_project}_floecat" "$DUCKDB_IMAGE" \
      duckdb -c "$duckdb_bootstrap SELECT COUNT(*) FROM iceberg_floecat.iceberg.duckdb_mutation_smoke;" 2>&1); then
      echo "$drop_check_out"
      echo "[FAIL] $label duckdb drop table verification failed (table still queryable)"
      return 1
    else
      assert_contains "$label duckdb drop table verification" "$drop_check_out" "Table with name duckdb_mutation_smoke does not exist"
      echo "[PASS] $label duckdb drop table verification (expected missing-table error after DROP TABLE)"
    fi

    if is_truthy "$COMPOSE_SMOKE_ICEBERG_FORMAT_MATRIX"; then
      echo "==> [SMOKE] duckdb iceberg format-version matrix (v1,v2)"
      local duckdb_fmt_out
      if ! duckdb_fmt_out=$(docker_run --rm --network "${compose_project}_floecat" "$DUCKDB_IMAGE" \
        duckdb -c "$duckdb_bootstrap DROP TABLE IF EXISTS iceberg_floecat.iceberg.duckdb_fmt_v1_smoke; DROP TABLE IF EXISTS iceberg_floecat.iceberg.duckdb_fmt_v2_smoke; CREATE TABLE iceberg_floecat.iceberg.duckdb_fmt_v1_smoke (id INTEGER, v VARCHAR) WITH (format_version=1); INSERT INTO iceberg_floecat.iceberg.duckdb_fmt_v1_smoke VALUES (101, 'duckdb_v1'); SELECT 'duckdb_fmt_v1_count=' || CAST(COUNT(*) AS VARCHAR) || ',' || CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) AS check FROM iceberg_floecat.iceberg.duckdb_fmt_v1_smoke; CREATE TABLE iceberg_floecat.iceberg.duckdb_fmt_v2_smoke (id INTEGER, v VARCHAR) WITH (format_version=2); INSERT INTO iceberg_floecat.iceberg.duckdb_fmt_v2_smoke VALUES (201, 'duckdb_v2'); SELECT 'duckdb_fmt_v2_count=' || CAST(COUNT(*) AS VARCHAR) || ',' || CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) AS check FROM iceberg_floecat.iceberg.duckdb_fmt_v2_smoke;" 2>&1); then
        echo "$duckdb_fmt_out"
        echo "[FAIL] $label duckdb format-version matrix command failed"
        return 1
      fi
      echo "$duckdb_fmt_out"
      assert_contains "$label duckdb format v1 queryability" "$duckdb_fmt_out" "duckdb_fmt_v1_count=1,101,duckdb_v1,duckdb_v1"
      assert_contains "$label duckdb format v2 queryability" "$duckdb_fmt_out" "duckdb_fmt_v2_count=1,201,duckdb_v2,duckdb_v2"
      maybe_assert_table_stats_available \
        "$compose_cmd" \
        "$label duckdb format v1" \
        "examples.iceberg.duckdb_fmt_v1_smoke" \
        "$COMPOSE_SMOKE_ICEBERG_FORMAT_MATRIX_STATS_RETRIES" \
        "$COMPOSE_SMOKE_ICEBERG_FORMAT_MATRIX_STATS_SLEEP_SECONDS"
      maybe_assert_table_stats_available \
        "$compose_cmd" \
        "$label duckdb format v2" \
        "examples.iceberg.duckdb_fmt_v2_smoke" \
        "$COMPOSE_SMOKE_ICEBERG_FORMAT_MATRIX_STATS_RETRIES" \
        "$COMPOSE_SMOKE_ICEBERG_FORMAT_MATRIX_STATS_SLEEP_SECONDS"
    fi
  fi

  if [ "$profile" = "localstack" ] && should_run_client trino; then
    echo "==> [SMOKE] trino federation check (localstack)"
    local trino_out
    if ! trino_out=$(docker_run --rm --network "${compose_project}_floecat" -i python:3.12-alpine python - <<'PY' 2>&1
import json
import os
import urllib.request

TRINO_URL = "http://trino:8080/v1/statement"
HEADERS = {
    "X-Trino-User": "smoke",
    "X-Trino-Source": "compose-smoke",
    "X-Trino-Catalog": "floecat",
    "X-Trino-Schema": "iceberg",
}


def run_sql(sql: str):
    req = urllib.request.Request(
        TRINO_URL,
        data=sql.encode("utf-8"),
        headers={**HEADERS, "Content-Type": "text/plain; charset=utf-8"},
        method="POST",
    )
    rows = []
    with urllib.request.urlopen(req, timeout=60) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    while True:
        if payload.get("error"):
            err = payload["error"]
            raise RuntimeError(f"{err.get('errorName')}: {err.get('message')}")
        if payload.get("data"):
            rows.extend(payload["data"])
        next_uri = payload.get("nextUri")
        if not next_uri:
            return rows
        next_req = urllib.request.Request(next_uri, headers=HEADERS, method="GET")
        with urllib.request.urlopen(next_req, timeout=60) as next_resp:
            payload = json.loads(next_resp.read().decode("utf-8"))


def scalar(sql: str) -> str:
    rows = run_sql(sql)
    if not rows or not rows[0]:
        raise RuntimeError(f"No rows returned for: {sql}")
    return str(rows[0][0])


print("trino_smoke_ok")
suffix = os.getenv("SMOKE_TRINO_SUFFIX", "").strip()


def smoke_name(base: str) -> str:
    return f"{base}_{suffix}" if suffix else base


ctas_table = smoke_name("trino_ctas_smoke")
rename_src_table = smoke_name("trino_rename_src_smoke")
rename_dst_table = smoke_name("trino_rename_dst_smoke")
mutation_table = smoke_name("trino_mutation_smoke")
namespace_name = smoke_name("trino_ns_smoke")
meta_table = smoke_name("trino_meta_smoke")
merge_table = smoke_name("trino_merge_smoke")
print(scalar("SELECT 'call_center=' || CAST(COUNT(*) AS VARCHAR) FROM delta.call_center"))
print(
    scalar(
        "SELECT 'my_local_delta_table=' || CAST(COUNT(*) AS VARCHAR) "
        "FROM delta.my_local_delta_table"
    )
)
print(
    scalar(
        "SELECT 'my_local_nonnull_name=' || CAST(COUNT(name) AS VARCHAR) "
        "FROM delta.my_local_delta_table"
    )
)
print(scalar("SELECT 'dv_demo_delta=' || CAST(COUNT(*) AS VARCHAR) FROM delta.dv_demo_delta"))
print(
    scalar(
        "SELECT 'dv_content=' || CAST(MIN(id) AS VARCHAR) || ',' || CAST(MAX(id) AS VARCHAR) "
        "|| ',' || MIN(v) || ',' || MAX(v) FROM delta.dv_demo_delta"
    )
)
print(
    scalar(
        "SELECT 'empty_join=' || CAST(COUNT(*) AS VARCHAR) "
        "FROM iceberg.trino_types i JOIN delta.call_center d ON 1=0"
    )
)
run_sql(f"DROP TABLE IF EXISTS iceberg.{ctas_table}")
run_sql(f"CREATE TABLE iceberg.{ctas_table} AS SELECT * FROM delta.call_center LIMIT 5")
print(scalar(f"SELECT 'ctas_count=' || CAST(COUNT(*) AS VARCHAR) FROM iceberg.{ctas_table}"))
run_sql(f"DROP TABLE iceberg.{ctas_table}")
drop_ok = False
try:
    run_sql(f"SELECT COUNT(*) FROM iceberg.{ctas_table}")
except Exception:
    drop_ok = True
print("drop_table_ok=" + ("1" if drop_ok else "0"))
run_sql(f"DROP SCHEMA IF EXISTS floecat.{namespace_name}")
run_sql(f"CREATE SCHEMA IF NOT EXISTS floecat.{namespace_name}")
print("ns_create_ok=1")
run_sql(f"DROP SCHEMA floecat.{namespace_name}")
print("ns_drop_ok=1")
run_sql(f"DROP TABLE IF EXISTS iceberg.{rename_dst_table}")
run_sql(f"DROP TABLE IF EXISTS iceberg.{rename_src_table}")
run_sql(f"CREATE TABLE iceberg.{rename_src_table} (id INTEGER, v VARCHAR)")
run_sql(f"INSERT INTO iceberg.{rename_src_table} VALUES (1, 'x')")
run_sql(f"ALTER TABLE iceberg.{rename_src_table} RENAME TO iceberg.{rename_dst_table}")
print(
    scalar(
        "SELECT 'rename_row_count=' || CAST(COUNT(*) AS VARCHAR) "
        f"FROM iceberg.{rename_dst_table}"
    )
)
run_sql(f"DROP TABLE iceberg.{rename_dst_table}")
run_sql(f"DROP TABLE IF EXISTS iceberg.{meta_table}")
run_sql(f"CREATE TABLE iceberg.{meta_table} (id INTEGER, v VARCHAR)")
run_sql(f"ALTER TABLE iceberg.{meta_table} ADD COLUMN note VARCHAR")
run_sql(f"ALTER TABLE iceberg.{meta_table} RENAME COLUMN v TO v2")
print(
    scalar(
        "SELECT 'meta_columns=' || CAST(COUNT(*) AS VARCHAR) "
        "FROM information_schema.columns "
        "WHERE table_catalog = 'floecat' AND table_schema = 'iceberg' "
        f"AND table_name = '{meta_table}' AND column_name IN ('id', 'v2', 'note')"
    )
)
run_sql(f"DROP TABLE iceberg.{meta_table}")
run_sql(f"DROP TABLE IF EXISTS iceberg.{merge_table}")
run_sql(f"CREATE TABLE iceberg.{merge_table} (id INTEGER, v VARCHAR)")
run_sql(f"INSERT INTO iceberg.{merge_table} VALUES (1, 'a'), (2, 'b')")
run_sql(
    f"MERGE INTO iceberg.{merge_table} t "
    "USING (VALUES (2, 'b2'), (3, 'c')) s(id, v) "
    "ON t.id = s.id "
    "WHEN MATCHED THEN UPDATE SET v = s.v "
    "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
)
print(
    scalar(
        "SELECT 'merge_after=' || CAST(COUNT(*) AS VARCHAR) || ',' || "
        "CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) "
        f"FROM iceberg.{merge_table}"
    )
)
run_sql(f"DROP TABLE iceberg.{merge_table}")
run_sql(f"DROP TABLE IF EXISTS iceberg.{mutation_table}")
run_sql(f"CREATE TABLE iceberg.{mutation_table} (id INTEGER, v VARCHAR)")
print(
    scalar(
        "SELECT 'mut_after_create=' || CAST(COUNT(*) AS VARCHAR) "
        f"FROM iceberg.{mutation_table}"
    )
)
run_sql(f"INSERT INTO iceberg.{mutation_table} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
insert_snapshot_id = scalar(
    "SELECT CAST(snapshot_id AS VARCHAR) "
    f"FROM iceberg.\"{mutation_table}$snapshots\" ORDER BY committed_at DESC LIMIT 1"
)
print(
    scalar(
        "SELECT 'mut_after_insert=' || CAST(COUNT(*) AS VARCHAR) || ',' || "
        "CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) "
        f"FROM iceberg.{mutation_table}"
    )
)
run_sql(f"DELETE FROM iceberg.{mutation_table} WHERE id = 2")
delete_snapshot_id = scalar(
    "SELECT CAST(snapshot_id AS VARCHAR) "
    f"FROM iceberg.\"{mutation_table}$snapshots\" ORDER BY committed_at DESC LIMIT 1"
)
print(
    scalar(
        "SELECT 'mut_after_delete=' || CAST(COUNT(*) AS VARCHAR) || ',' || "
        "CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) "
        f"FROM iceberg.{mutation_table}"
    )
)
run_sql(f"UPDATE iceberg.{mutation_table} SET v = 'c2' WHERE id = 3")
update_snapshot_id = scalar(
    "SELECT CAST(snapshot_id AS VARCHAR) "
    f"FROM iceberg.\"{mutation_table}$snapshots\" ORDER BY committed_at DESC LIMIT 1"
)
print(
    scalar(
        "SELECT 'mut_after_update=' || CAST(COUNT(*) AS VARCHAR) || ',' || "
        "CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) "
        f"FROM iceberg.{mutation_table}"
    )
)
print(
    scalar(
        "SELECT 'mut_tt_after_insert=' || CAST(COUNT(*) AS VARCHAR) || ',' || "
        "CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) "
        f"FROM iceberg.{mutation_table} FOR VERSION AS OF {insert_snapshot_id}"
    )
)
print(
    scalar(
        "SELECT 'mut_tt_after_delete=' || CAST(COUNT(*) AS VARCHAR) || ',' || "
        "CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) "
        f"FROM iceberg.{mutation_table} FOR VERSION AS OF {delete_snapshot_id}"
    )
)
print(
    scalar(
        "SELECT 'mut_tt_after_update=' || CAST(COUNT(*) AS VARCHAR) || ',' || "
        "CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) "
        f"FROM iceberg.{mutation_table} FOR VERSION AS OF {update_snapshot_id}"
    )
)
run_sql(f"ALTER TABLE iceberg.{mutation_table} ADD COLUMN note VARCHAR")
print(
    scalar(
        "SELECT 'mut_after_alter=' || CAST(COUNT(*) AS VARCHAR) || ',' || "
        f"CAST(COUNT(note) AS VARCHAR) FROM iceberg.{mutation_table}"
    )
)
run_sql(f"DROP TABLE iceberg.{mutation_table}")
drop_mutation_ok = False
try:
    run_sql(f"SELECT COUNT(*) FROM iceberg.{mutation_table}")
except Exception:
    drop_mutation_ok = True
print("drop_mutation_ok=" + ("1" if drop_mutation_ok else "0"))
PY
); then
      echo "$trino_out"
      echo "[FAIL] $label trino command failed"
      return 1
    fi

    echo "$trino_out"
    assert_contains "$label trino smoke marker" "$trino_out" "trino_smoke_ok"
    assert_contains "$label trino call_center count" "$trino_out" "call_center=42"
    assert_contains "$label trino my_local_delta_table count" "$trino_out" "my_local_delta_table=4"
    assert_contains "$label trino my_local_delta_table nonnull" "$trino_out" "my_local_nonnull_name=4"
    assert_contains "$label trino dv_demo_delta count" "$trino_out" "dv_demo_delta=2"
    assert_contains "$label trino dv_demo_delta content" "$trino_out" "dv_content=1,3,a,c"
    assert_contains "$label trino ctas count" "$trino_out" "ctas_count=5"
    assert_contains "$label trino drop table verification" "$trino_out" "drop_table_ok=1"
    assert_contains "$label trino namespace create" "$trino_out" "ns_create_ok=1"
    assert_contains "$label trino namespace drop" "$trino_out" "ns_drop_ok=1"
    assert_contains "$label trino rename row count" "$trino_out" "rename_row_count=1"
    assert_contains "$label trino metadata column update" "$trino_out" "meta_columns=3"
    assert_contains "$label trino merge mutation" "$trino_out" "merge_after=3,6,a,c"
    assert_contains "$label trino mutation create" "$trino_out" "mut_after_create=0"
    assert_contains "$label trino mutation insert" "$trino_out" "mut_after_insert=3,6,a,c"
    assert_contains "$label trino mutation delete" "$trino_out" "mut_after_delete=2,4,a,c"
    assert_contains "$label trino mutation update" "$trino_out" "mut_after_update=2,4,a,c2"
    assert_contains "$label trino time travel insert snapshot" "$trino_out" "mut_tt_after_insert=3,6,a,c"
    assert_contains "$label trino time travel delete snapshot" "$trino_out" "mut_tt_after_delete=2,4,a,c"
    assert_contains "$label trino time travel update snapshot" "$trino_out" "mut_tt_after_update=2,4,a,c2"
    assert_contains "$label trino mutation alter" "$trino_out" "mut_after_alter=2,0"
    assert_contains "$label trino mutation drop verification" "$trino_out" "drop_mutation_ok=1"

    if is_truthy "$COMPOSE_SMOKE_ICEBERG_FORMAT_MATRIX"; then
      echo "==> [SMOKE] trino iceberg format-version matrix (v1,v2)"
      local trino_fmt_out
      if ! trino_fmt_out=$(docker_run --rm --network "${compose_project}_floecat" -e CHECK_DUCKDB_FORMAT_TABLES="$(should_run_client duckdb && echo true || echo false)" -i python:3.12-alpine python - <<'PY' 2>&1
import json
import os
import urllib.request

TRINO_URL = "http://trino:8080/v1/statement"
ICEBERG_REST_TABLE_URL = "http://iceberg-rest:9200/v1/examples/namespaces/iceberg/tables/{}"
HEADERS = {
    "X-Trino-User": "smoke",
    "X-Trino-Source": "compose-smoke",
    "X-Trino-Catalog": "floecat",
    "X-Trino-Schema": "iceberg",
}


def run_sql(sql: str):
    req = urllib.request.Request(
        TRINO_URL,
        data=sql.encode("utf-8"),
        headers={**HEADERS, "Content-Type": "text/plain; charset=utf-8"},
        method="POST",
    )
    rows = []
    with urllib.request.urlopen(req, timeout=60) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    while True:
        if payload.get("error"):
            err = payload["error"]
            raise RuntimeError(f"{err.get('errorName')}: {err.get('message')}")
        if payload.get("data"):
            rows.extend(payload["data"])
        next_uri = payload.get("nextUri")
        if not next_uri:
            return rows
        next_req = urllib.request.Request(next_uri, headers=HEADERS, method="GET")
        with urllib.request.urlopen(next_req, timeout=60) as next_resp:
            payload = json.loads(next_resp.read().decode("utf-8"))


def scalar(sql: str) -> str:
    rows = run_sql(sql)
    if not rows or not rows[0]:
        raise RuntimeError(f"No rows returned for: {sql}")
    return str(rows[0][0])


def rest_format_version(table_name: str) -> int:
    req = urllib.request.Request(ICEBERG_REST_TABLE_URL.format(table_name), method="GET")
    with urllib.request.urlopen(req, timeout=60) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    return int(payload["metadata"]["format-version"])

run_sql("DROP TABLE IF EXISTS iceberg.trino_fmt_v2_smoke")
run_sql("DROP TABLE IF EXISTS iceberg.trino_fmt_v1_smoke")
run_sql("CREATE TABLE iceberg.trino_fmt_v1_smoke (id INTEGER, v VARCHAR) WITH (format_version = 1)")
run_sql("INSERT INTO iceberg.trino_fmt_v1_smoke VALUES (301, 'trino_v1')")
run_sql("CREATE TABLE iceberg.trino_fmt_v2_smoke (id INTEGER, v VARCHAR) WITH (format_version = 2)")
run_sql("INSERT INTO iceberg.trino_fmt_v2_smoke VALUES (401, 'trino_v2')")

print(
    scalar(
        "SELECT 'trino_fmt_v1_count=' || CAST(COUNT(*) AS VARCHAR) || ',' || "
        "CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) FROM iceberg.trino_fmt_v1_smoke"
    )
)
print(
    scalar(
        "SELECT 'trino_fmt_v2_count=' || CAST(COUNT(*) AS VARCHAR) || ',' || "
        "CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) FROM iceberg.trino_fmt_v2_smoke"
    )
)
print(f"trino_fmt_v1_format={rest_format_version('trino_fmt_v1_smoke')}")
print(f"trino_fmt_v2_format={rest_format_version('trino_fmt_v2_smoke')}")
run_sql("ALTER TABLE iceberg.trino_fmt_v1_smoke SET PROPERTIES format_version = 2")
print(f"trino_fmt_v1_format_upgraded={rest_format_version('trino_fmt_v1_smoke')}")

if os.getenv("CHECK_DUCKDB_FORMAT_TABLES", "").strip().lower() in {"1", "true", "yes", "on"}:
    print(f"duckdb_fmt_v1_format={rest_format_version('duckdb_fmt_v1_smoke')}")
    print(f"duckdb_fmt_v2_format={rest_format_version('duckdb_fmt_v2_smoke')}")
PY
); then
        echo "$trino_fmt_out"
        echo "[FAIL] $label trino format-version matrix command failed"
        return 1
      fi
      echo "$trino_fmt_out"
      assert_contains "$label trino format v1 queryability" "$trino_fmt_out" "trino_fmt_v1_count=1,301,trino_v1,trino_v1"
      assert_contains "$label trino format v2 queryability" "$trino_fmt_out" "trino_fmt_v2_count=1,401,trino_v2,trino_v2"
      assert_contains "$label trino format v1 metadata" "$trino_fmt_out" "trino_fmt_v1_format=1"
      assert_contains "$label trino format v2 metadata" "$trino_fmt_out" "trino_fmt_v2_format=2"
      assert_contains "$label trino format v1 alter upgrade metadata" "$trino_fmt_out" "trino_fmt_v1_format_upgraded=2"
      if should_run_client duckdb; then
        # Recent DuckDB+Iceberg paths materialize v2 metadata even when format_version=1 is requested.
        assert_contains "$label duckdb format v1 metadata" "$trino_fmt_out" "duckdb_fmt_v1_format=2"
        assert_contains "$label duckdb format v2 metadata" "$trino_fmt_out" "duckdb_fmt_v2_format=2"
      fi
      maybe_assert_table_stats_available \
        "$compose_cmd" \
        "$label trino format v1" \
        "examples.iceberg.trino_fmt_v1_smoke" \
        "$COMPOSE_SMOKE_ICEBERG_FORMAT_MATRIX_STATS_RETRIES" \
        "$COMPOSE_SMOKE_ICEBERG_FORMAT_MATRIX_STATS_SLEEP_SECONDS"
      maybe_assert_table_stats_available \
        "$compose_cmd" \
        "$label trino format v2" \
        "examples.iceberg.trino_fmt_v2_smoke" \
        "$COMPOSE_SMOKE_ICEBERG_FORMAT_MATRIX_STATS_RETRIES" \
        "$COMPOSE_SMOKE_ICEBERG_FORMAT_MATRIX_STATS_SLEEP_SECONDS"
    fi
  fi

  trap - ERR
  echo "==> [SMOKE][PASS] mode=$label"
}

SMOKE_MODES=${COMPOSE_SMOKE_MODES:-inmem,localstack,localstack-oidc}
echo "==> [COMPOSE] smoke modes=$SMOKE_MODES"

IFS=',' read -r -a mode_list <<< "$SMOKE_MODES"
for raw_mode in "${mode_list[@]}"; do
  mode="${raw_mode//[[:space:]]/}"
  case "$mode" in
    inmem)
      run_mode ./env.inmem "" inmem ""
      ;;
    localstack)
      run_mode ./env.localstack localstack localstack "localstack"
      ;;
    localstack-oidc)
      run_mode ./env.localstack-oidc localstack-oidc localstack-oidc "localstack keycloak" "keycloak" "8080"
      ;;
    "")
      ;;
    *)
      echo "Unknown compose smoke mode: '$mode'" >&2
      exit 1
      ;;
  esac
done

echo "==> [COMPOSE][PASS] smoke"
