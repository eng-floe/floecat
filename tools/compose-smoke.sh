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

set -Eeuo pipefail

DOCKER_COMPOSE_MAIN=${DOCKER_COMPOSE_MAIN:-docker compose -f docker/docker-compose.yml}
COMPOSE_SMOKE_RUN_ID=${COMPOSE_SMOKE_RUN_ID:-$(date +%Y%m%d-%H%M%S)-$$}
COMPOSE_SMOKE_SAVE_LOG_ROOT=${COMPOSE_SMOKE_SAVE_LOG_DIR:-target/compose-smoke-logs}
COMPOSE_SMOKE_SAVE_LOG_DIR_DEFAULT=${COMPOSE_SMOKE_SAVE_LOG_ROOT%/}/${COMPOSE_SMOKE_RUN_ID}
COMPOSE_SMOKE_PERSIST_CONSOLE=${COMPOSE_SMOKE_PERSIST_CONSOLE:-true}
COMPOSE_SMOKE_KEEP_ON_FAIL=${COMPOSE_SMOKE_KEEP_ON_FAIL:-false}
COMPOSE_SMOKE_KEEP_ON_EXIT=${COMPOSE_SMOKE_KEEP_ON_EXIT:-false}
COMPOSE_SMOKE_CLIENTS=${COMPOSE_SMOKE_CLIENTS:-duckdb,trino}
COMPOSE_SMOKE_DUCKDB_IMAGE=${COMPOSE_SMOKE_DUCKDB_IMAGE:-duckdb/duckdb:1.4.4}
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
COMPOSE_SMOKE_STATS_RETRIES=${COMPOSE_SMOKE_STATS_RETRIES:-45}
COMPOSE_SMOKE_STATS_SLEEP_SECONDS=${COMPOSE_SMOKE_STATS_SLEEP_SECONDS:-2}

is_truthy() {
  local raw="${1:-}"
  local normalized
  normalized=$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')
  case "${normalized}" in
    1|true|yes|on)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

mkdir -p "$COMPOSE_SMOKE_SAVE_LOG_DIR_DEFAULT" || true
ln -sfn "$COMPOSE_SMOKE_RUN_ID" "${COMPOSE_SMOKE_SAVE_LOG_ROOT%/}/latest" 2>/dev/null || true

if [ "${COMPOSE_SMOKE_CONSOLE_TEE_INITIALIZED:-0}" != "1" ] && is_truthy "$COMPOSE_SMOKE_PERSIST_CONSOLE"; then
  export COMPOSE_SMOKE_CONSOLE_TEE_INITIALIZED=1
  exec > >(tee -a "$COMPOSE_SMOKE_SAVE_LOG_DIR_DEFAULT/compose-smoke.console.log") 2>&1
fi

cat >"$COMPOSE_SMOKE_SAVE_LOG_DIR_DEFAULT/run-info.env" <<EOF
COMPOSE_SMOKE_RUN_ID=$COMPOSE_SMOKE_RUN_ID
COMPOSE_SMOKE_SAVE_LOG_ROOT=$COMPOSE_SMOKE_SAVE_LOG_ROOT
COMPOSE_SMOKE_SAVE_LOG_DIR_DEFAULT=$COMPOSE_SMOKE_SAVE_LOG_DIR_DEFAULT
COMPOSE_SMOKE_KEEP_ON_FAIL=$COMPOSE_SMOKE_KEEP_ON_FAIL
COMPOSE_SMOKE_KEEP_ON_EXIT=$COMPOSE_SMOKE_KEEP_ON_EXIT
COMPOSE_SMOKE_CLIENTS=$COMPOSE_SMOKE_CLIENTS
COMPOSE_SMOKE_DUCKDB_IMAGE=$COMPOSE_SMOKE_DUCKDB_IMAGE
COMPOSE_SMOKE_PERSIST_CONSOLE=$COMPOSE_SMOKE_PERSIST_CONSOLE
EOF

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

run_cli_script() {
  local compose_cmd="$1"
  local script="$2"
  local out
  local out_file
  out_file=$(mktemp)
  if ! printf "%s\n" "$script" | eval "$compose_cmd run --rm -T cli" >"$out_file" 2>&1; then
    out=$(tr -d '\000' <"$out_file")
    rm -f "$out_file"
    printf "%s\n" "$out" \
      | sed \
          -e '/^\[\+\] Creating /d' \
          -e '/^[[:space:]]*✔ Container .* Running[0-9.]*s$/d' \
          -e '/^[[:space:]]*Container .* Running[0-9.]*s$/d'
    return 1
  fi
  out=$(tr -d '\000' <"$out_file")
  rm -f "$out_file"
  printf "%s\n" "$out" \
    | sed \
        -e '/^\[\+\] Creating /d' \
        -e '/^[[:space:]]*✔ Container .* Running[0-9.]*s$/d' \
        -e '/^[[:space:]]*Container .* Running[0-9.]*s$/d'
}

wait_for_connector_job() {
  local compose_cmd="$1"
  local label="$2"
  local job_id="$3"
  local max_attempts="${4:-90}"
  local sleep_seconds="${5:-2}"
  local attempt
  local out
  local cleaned_out
  local state
  local message

  if [ -z "$job_id" ]; then
    echo "[FAIL] $label missing reconcile job id"
    return 1
  fi

  for attempt in $(seq 1 "$max_attempts"); do
    out=$(run_cli_script "$compose_cmd" "account t-0001
connector job $job_id
quit")
    cleaned_out=$(printf "%s\n" "$out" | tr -d '\r' | sed -E 's/^floecat>[[:space:]]*//')
    state=$(
      printf "%s\n" "$cleaned_out" \
        | sed -n \
            -e '/job_id=/s/.* state=\([A-Za-z_]*\).*/\1/p' \
            -e 's/^[[:space:]]*state:[[:space:]]*//p' \
        | head -n1 \
        | tr '[:upper:]' '[:lower:]'
    )
    message=$(
      printf "%s\n" "$cleaned_out" \
        | sed -n 's/^[[:space:]]*message:[[:space:]]*//p' \
        | head -n1 \
        | tr '[:upper:]' '[:lower:]'
    )
    if [ "$state" = "js_succeeded" ] || [ "$state" = "succeeded" ] || [ "$state" = "done" ] || [ "$message" = "succeeded" ]; then
      echo "[PASS] $label job succeeded ($job_id)"
      return 0
    fi
    if [ "$state" = "js_failed" ] || [ "$state" = "js_cancelled" ] || [ "$state" = "failed" ] || [ "$state" = "cancelled" ] || [ "$message" = "failed" ] || [ "$message" = "cancelled" ]; then
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

run_connector_trigger_and_wait() {
  local compose_cmd="$1"
  local label="$2"
  local trigger_script="$3"
  local max_attempts="${4:-120}"
  local sleep_seconds="${5:-2}"
  local trigger_out
  local job_id

  trigger_out=$(run_cli_script "$compose_cmd" "$trigger_script")
  job_id=$(extract_cli_job_id "$trigger_out")
  wait_for_connector_job "$compose_cmd" "$label" "$job_id" "$max_attempts" "$sleep_seconds"
}

list_connector_job_states_for_table() {
  local compose_cmd="$1"
  local table_ref="$2"
  local out

  out=$(run_cli_script "$compose_cmd" "account t-0001
connector jobs --page-size 200
quit")
  printf "%s\n" "$out" | awk -v table_ref="$table_ref" '
    /^job_id=/ {
      job_id = ""
      state = ""
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^job_id=/) {
          job_id = $i
          sub(/^job_id=/, "", job_id)
        } else if ($i ~ /^state=/) {
          state = $i
          sub(/^state=/, "", state)
        }
      }
      next
    }
    /^routing:[[:space:]]/ {
      if (job_id != "" && index($0, "table=" table_ref) > 0) {
        print job_id " " state
      }
      job_id = ""
      state = ""
    }
  '
}

wait_for_table_reconcile_jobs() {
  local compose_cmd="$1"
  local label="$2"
  local table_ref="$3"
  local baseline_job_ids="${4:-}"
  local expected_min_jobs="${5:-1}"
  local max_attempts="${6:-90}"
  local sleep_seconds="${7:-2}"
  local attempt
  local states
  local line
  local job_id
  local state
  local new_job_count
  local terminal_success_count
  local nonterminal_count

  echo "==> [SMOKE] waiting for reconcile jobs for $table_ref (retries=$max_attempts sleep=${sleep_seconds}s)"
  for attempt in $(seq 1 "$max_attempts"); do
    states=$(list_connector_job_states_for_table "$compose_cmd" "$table_ref")
    new_job_count=0
    terminal_success_count=0
    nonterminal_count=0

    while IFS= read -r line; do
      [ -n "$line" ] || continue
      job_id=${line%% *}
      state=${line#* }
      if printf "%s\n" "$baseline_job_ids" | grep -Fqx "$job_id"; then
        continue
      fi
      new_job_count=$((new_job_count + 1))
      case "$state" in
        JS_SUCCEEDED|SUCCEEDED)
          terminal_success_count=$((terminal_success_count + 1))
          ;;
        JS_FAILED|FAILED|JS_CANCELLED|CANCELLED)
          echo "$states"
          echo "[FAIL] $label reconcile job terminal state=$state for $table_ref ($job_id)"
          return 1
          ;;
        *)
          nonterminal_count=$((nonterminal_count + 1))
          ;;
      esac
    done <<< "$states"

    if [ "$new_job_count" -ge "$expected_min_jobs" ] && [ "$nonterminal_count" -eq 0 ] && [ "$terminal_success_count" -eq "$new_job_count" ]; then
      echo "[PASS] $label reconcile jobs settled for $table_ref ($new_job_count jobs)"
      return 0
    fi

    sleep "$sleep_seconds"
  done

  echo "$states"
  echo "[FAIL] $label reconcile jobs did not settle for $table_ref"
  return 1
}

extract_cli_job_id() {
  local cli_output="$1"
  printf "%s\n" "$cli_output" \
    | tr -d '\r' \
    | sed -E 's/^floecat>[[:space:]]*//' \
    | sed -n '/^[0-9a-fA-F-]\{36\}[[:space:]]*$/p' \
    | tail -n1
}

assert_contains() {
  local check_name="$1"
  local output="$2"
  local pattern="$3"

  if [[ "$output" == *"$pattern"* ]]; then
    echo "[PASS] $check_name"
  else
    echo "[FAIL] $check_name (missing: $pattern)"
    echo "---- output begin ----"
    echo "$output"
    echo "---- output end ----"
    return 1
  fi
}

assert_contains_any() {
  local check_name="$1"
  local output="$2"
  shift 2

  local pattern
  for pattern in "$@"; do
    if [[ "$output" == *"$pattern"* ]]; then
      echo "[PASS] $check_name"
      return 0
    fi
  done

  echo "[FAIL] $check_name (missing all expected patterns)"
  echo "---- expected begin ----"
  for pattern in "$@"; do
    echo "$pattern"
  done
  echo "---- expected end ----"
  echo "---- output begin ----"
  echo "$output"
  echo "---- output end ----"
  return 1
}

assert_remote_file_group_worker_activity() {
  local compose_cmd="$1"
  local label="$2"
  local out

  out=$(eval "$compose_cmd logs executor 2>&1" || true)
  assert_contains_any \
    "$label standalone worker activity" \
    "$out" \
    "remote_file_group_worker" \
    "Executed file group" \
    "submitLeasedFileGroupExecutionResult"
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

  echo "==> [SMOKE] waiting for stats for $table_fqn (retries=$retries sleep=${sleep_seconds}s)"
  for attempt in $(seq 1 "$retries"); do
    out=$(run_cli_script "$compose_cmd" "account t-0001
stats files $table_fqn --snapshot current --limit 5
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

    if [ "$attempt" -eq 1 ] || [ $((attempt % 5)) -eq 0 ] || [ "$attempt" -eq "$retries" ]; then
      echo "==> [SMOKE] stats not ready for $table_fqn yet (attempt $attempt/$retries)"
    fi
    sleep "$sleep_seconds"
  done

  echo "[FAIL] $label stats missing for $table_fqn"
  echo "---- output begin ----"
  echo "$last_out"
  echo "---- output end ----"
  return 1
}

assert_table_indexes_available() {
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

  echo "==> [SMOKE] waiting for index artifacts for $table_fqn (retries=$retries sleep=${sleep_seconds}s)"
  for attempt in $(seq 1 "$retries"); do
    out=$(run_cli_script "$compose_cmd" "account t-0001
stats index $table_fqn --snapshot current --limit 5
quit")
    last_out="$out"
    if echo "$out" | grep -q "IDX" \
      && echo "$out" | grep -q "PATH" \
      && ! echo "$out" | grep -q "No index artifacts found."; then
      assert_contains "$label index header" "$out" "IDX"
      assert_contains "$label index path header" "$out" "PATH"
      assert_contains "$label index artifact uri" "$out" "uri="
      echo "[PASS] $label index artifacts available for $table_fqn"
      return 0
    fi

    # Current snapshot can briefly lag artifact registration; probe a few explicit snapshots as fallback.
    snapshot_out=$(run_cli_script "$compose_cmd" "account t-0001
snapshots $table_fqn
quit")
    snapshot_ids=$(echo "$snapshot_out" | awk '/^[[:space:]]*[0-9]+[[:space:]]/ {print $1}' | head -n 5)

    for snapshot_id in $snapshot_ids; do
      out=$(run_cli_script "$compose_cmd" "account t-0001
stats index $table_fqn --snapshot $snapshot_id --limit 5
quit")
      last_out="$out"
      if echo "$out" | grep -q "IDX" \
        && echo "$out" | grep -q "PATH" \
        && ! echo "$out" | grep -q "No index artifacts found."; then
        assert_contains "$label index header" "$out" "IDX"
        assert_contains "$label index path header" "$out" "PATH"
        assert_contains "$label index artifact uri" "$out" "uri="
        echo "[PASS] $label index artifacts available for $table_fqn (snapshot=$snapshot_id)"
        return 0
      fi
    done

    if [ "$attempt" -eq 1 ] || [ $((attempt % 5)) -eq 0 ] || [ "$attempt" -eq "$retries" ]; then
      echo "==> [SMOKE] index artifacts not ready for $table_fqn yet (attempt $attempt/$retries)"
    fi
    sleep "$sleep_seconds"
  done

  echo "[FAIL] $label index artifacts missing for $table_fqn"
  echo "---- output begin ----"
  echo "$last_out"
  echo "---- output end ----"
  return 1
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

wait_for_url_auth() {
  local url="$1"
  local timeout_seconds="$2"
  local label="$3"
  local auth_header="$4"
  local i

  for i in $(seq 1 "$timeout_seconds"); do
    if curl -fsS -H "Authorization: $auth_header" "$url" >/dev/null 2>&1; then
      return 0
    fi
    if [ "$i" -eq "$timeout_seconds" ]; then
      echo "$label timed out" >&2
      return 1
    fi
    sleep 1
  done
}

fetch_keycloak_access_token() {
  local token_url="$1"
  local client_id="$2"
  local client_secret="$3"

  curl -fsS \
    -X POST "$token_url" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    --data "grant_type=client_credentials&client_id=$client_id&client_secret=$client_secret" \
    | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p'
}

run_mode() {
  local env_file="$1"
  local profile="$2"
  local label="$3"
  local pre_services="$4"
  local kc_host="${5:-}"
  local kc_port="${6:-}"
  local compose_profiles_override="${7:-}"
  local mode_env_extra="${8:-}"
  local executor_scale="${9:-}"
  local smoke_scope="${10:-full}"

  local compose_project="floecat-smoke-$label"
  local compose_profiles="${compose_profiles_override:-$profile}"
  if [ "$profile" = "localstack" ] && is_truthy "$COMPOSE_SMOKE_UPSTREAM_ICEBERG_IMPORT"; then
    compose_profiles="${compose_profiles},polaris"
  fi
  if [ "$smoke_scope" = "full" ] && [ "$profile" = "localstack" ] && is_truthy "$COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_IMPORT"; then
    compose_profiles="${compose_profiles},unity"
  fi
  if [ "$smoke_scope" = "full" ] && { [ "$profile" = "localstack" ] || [ "$profile" = "localstack-oidc" ]; } && should_run_client trino; then
    compose_profiles="${compose_profiles},trino"
  fi
  local -a mode_env_args=(
    "FLOECAT_ENV_FILE=$env_file"
    "COMPOSE_PROFILES=$compose_profiles"
    "COMPOSE_PROJECT_NAME=$compose_project"
  )

  if [ -n "$kc_host" ]; then
    mode_env_args+=("KC_HOSTNAME=$kc_host")
  fi
  if [ -n "$kc_port" ]; then
    mode_env_args+=("KC_HOSTNAME_PORT=$kc_port")
  fi
  if [ -n "$mode_env_extra" ]; then
    local extra_kv
    for extra_kv in $mode_env_extra; do
      mode_env_args+=("$extra_kv")
    done
  fi

  local mode_env=""
  local mode_env_kv
  for mode_env_kv in "${mode_env_args[@]}"; do
    printf -v mode_env '%s%q ' "$mode_env" "$mode_env_kv"
  done

  local compose_cmd="${mode_env}${DOCKER_COMPOSE_MAIN}"
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
  if [ "$smoke_scope" = "full" ] && [ "$profile" = "localstack" ] && is_truthy "$COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_IMPORT"; then
    if [ -z "$pre_services" ]; then
      pre_services="localstack unity"
    elif [[ ",$pre_services," != *",unity,"* ]]; then
      pre_services="$pre_services unity"
    fi
  fi

  if [ -n "$pre_services" ]; then
    if ! eval "$compose_cmd up -d $pre_services"; then
      return 1
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
  if [ "$smoke_scope" = "full" ] && [ "$profile" = "localstack" ] && is_truthy "$COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_IMPORT"; then
    local unity_host_port="${FLOECAT_UNITY_HOST_PORT:-8083}"
    wait_for_url "http://localhost:${unity_host_port}${COMPOSE_SMOKE_UNITY_HEALTH_PATH}" 180 "Unity Catalog health"
  fi

  local compose_up_cmd="$compose_cmd up -d"
  if [ -n "$executor_scale" ]; then
    compose_up_cmd="$compose_up_cmd --scale executor=$executor_scale"
  fi

  if ! eval "$compose_up_cmd"; then
    return 1
  fi

  local iceberg_rest_host_port="${FLOECAT_REST_HOST_PORT:-9200}"
  if [ "$profile" = "localstack-oidc" ]; then
    local oidc_client_id="${FLOECAT_OIDC_CLIENT_ID:-floecat-client}"
    local oidc_client_secret="${FLOECAT_OIDC_CLIENT_SECRET:-floecat-secret}"
    local oidc_token
    oidc_token=$(fetch_keycloak_access_token \
      "http://localhost:8080/realms/floecat/protocol/openid-connect/token" \
      "$oidc_client_id" \
      "$oidc_client_secret")
    if [ -z "$oidc_token" ]; then
      echo "Failed to obtain OIDC access token for Iceberg REST health check" >&2
      return 1
    fi
    wait_for_url_auth \
      "http://localhost:${iceberg_rest_host_port}/v1/config" \
      180 \
      "Iceberg REST health" \
      "Bearer $oidc_token"
  else
    wait_for_url "http://localhost:${iceberg_rest_host_port}/v1/config" 180 "Iceberg REST health"
  fi

  local i
  local service_logs
  for i in $(seq 1 180); do
    service_logs=$(eval "$compose_cmd logs service 2>&1" || true)
    if [[ "$service_logs" == *"Startup seeding completed successfully"* ]]; then
      break
    fi
    if [[ "$service_logs" == *"Startup seeding failed"* ]]; then
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

  if [ "$smoke_scope" = "full" ]; then
    local out_iceberg
    out_iceberg=$(run_cli_script "$compose_cmd" "account t-0001
resolve table examples.iceberg.trino_types
quit")
    echo "$out_iceberg"
    assert_contains "$label cli resolve iceberg account" "$out_iceberg" "account set:"
    assert_contains "$label cli resolve iceberg table" "$out_iceberg" "table id:"

    local out_delta
    out_delta=$(run_cli_script "$compose_cmd" "account t-0001
resolve table examples.delta.call_center
quit")
    echo "$out_delta"
    assert_contains "$label cli resolve call_center account" "$out_delta" "account set:"
    assert_contains "$label cli resolve call_center table" "$out_delta" "table id:"

    local out_delta_local
    out_delta_local=$(run_cli_script "$compose_cmd" "account t-0001
resolve table examples.delta.my_local_delta_table
quit")
    echo "$out_delta_local"
    assert_contains "$label cli resolve my_local_delta_table account" "$out_delta_local" "account set:"
    assert_contains "$label cli resolve my_local_delta_table table" "$out_delta_local" "table id:"

    local out_delta_dv
    out_delta_dv=$(run_cli_script "$compose_cmd" "account t-0001
resolve table examples.delta.dv_demo_delta
quit")
    echo "$out_delta_dv"
    assert_contains "$label cli resolve dv_demo_delta account" "$out_delta_dv" "account set:"
    assert_contains "$label cli resolve dv_demo_delta table" "$out_delta_dv" "table id:"
  fi

  if [ "$profile" = "localstack" ] && is_truthy "$COMPOSE_SMOKE_UPSTREAM_ICEBERG_IMPORT"; then
    echo "==> [SMOKE] upstream iceberg rest import check"

    local aws_cli
    aws_cli="docker run --rm --network ${compose_project}_floecat -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test -e AWS_DEFAULT_REGION=us-east-1 amazon/aws-cli:2.17.50"
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
    polaris_token=$(docker run --rm --network "${compose_project}_floecat" curlimages/curl:8.12.1 -sS \
      -X POST "http://polaris:8181/api/catalog/v1/oauth/tokens" \
      -H "Content-Type: application/x-www-form-urlencoded" \
      --data "grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL" \
      | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')
    if [ -z "$polaris_token" ]; then
      echo "[FAIL] $label upstream iceberg failed to obtain Polaris OAuth token"
      return 1
    fi

    docker run --rm --network "${compose_project}_floecat" curlimages/curl:8.12.1 -sS \
      -X POST "http://polaris:8181/api/management/v1/catalogs" \
      -H "Authorization: Bearer $polaris_token" \
      -H "Content-Type: application/json" \
      -d "{\"catalog\":{\"name\":\"$warehouse\",\"type\":\"INTERNAL\",\"readOnly\":false,\"properties\":{\"default-base-location\":\"$bucket_root_uri\"},\"storageConfigInfo\":{\"storageType\":\"S3\",\"allowedLocations\":[\"$bucket_root_uri\",\"$metadata_dir_uri\"],\"pathStyleAccess\":true,\"roleArn\":\"arn:aws:iam::000000000000:role/polaris\"}}}" \
      >/dev/null 2>&1 || true

    local polaris_principal_role
    polaris_principal_role="smoke-upstream-iceberg-role"
    local polaris_catalog_role
    polaris_catalog_role="smoke-upstream-iceberg-catalog-role"
    local polaris_grant_specs
    polaris_grant_specs=$(
      cat <<EOF
POST|http://polaris:8181/api/management/v1/principal-roles|{"principalRole":{"name":"$polaris_principal_role"}}
POST|http://polaris:8181/api/management/v1/catalogs/$warehouse/catalog-roles|{"catalogRole":{"name":"$polaris_catalog_role"}}
PUT|http://polaris:8181/api/management/v1/catalogs/$warehouse/catalog-roles/$polaris_catalog_role/grants|{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}
PUT|http://polaris:8181/api/management/v1/principal-roles/$polaris_principal_role/catalog-roles/$warehouse|{"catalogRole":{"name":"$polaris_catalog_role"}}
PUT|http://polaris:8181/api/management/v1/principals/root/principal-roles|{"principalRole":{"name":"$polaris_principal_role"}}
EOF
    )
    while IFS='|' read -r method url body; do
      [ -n "$method" ] || continue
      local polaris_resp
      polaris_resp=$(docker run --rm --network "${compose_project}_floecat" curlimages/curl:8.12.1 -sS \
        -w "\n%{http_code}\n" \
        -X "$method" "$url" \
        -H "Authorization: Bearer $polaris_token" \
        -H "Content-Type: application/json" \
        -d "$body")
      local polaris_code
      polaris_code=$(printf "%s\n" "$polaris_resp" | tail -n1)
      if [ "$polaris_code" != "200" ] && [ "$polaris_code" != "201" ] && [ "$polaris_code" != "409" ]; then
        echo "[FAIL] $label upstream iceberg Polaris RBAC setup failed (http=$polaris_code method=$method url=$url)"
        echo "$polaris_resp"
        return 1
      fi
    done <<EOF
$polaris_grant_specs
EOF

    docker run --rm --network "${compose_project}_floecat" curlimages/curl:8.12.1 -sS \
      -X POST "http://polaris:8181/api/catalog/v1/$warehouse/namespaces" \
      -H "Authorization: Bearer $polaris_token" \
      -H "Content-Type: application/json" \
      -d "{\"namespace\":[\"$source_namespace\"]}" \
      >/dev/null 2>&1 || true

    local register_resp
    register_resp=$(docker run --rm --network "${compose_project}_floecat" curlimages/curl:8.12.1 -sS \
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

    if [ "$smoke_scope" = "polaris-vended-creds" ]; then
      local load_table_resp
      load_table_resp=$(docker run --rm --network "${compose_project}_floecat" curlimages/curl:8.12.1 -sS \
        -X GET "http://polaris:8181/api/catalog/v1/$warehouse/namespaces/$source_namespace/tables/$source_table?snapshots=all" \
        -H "Authorization: Bearer $polaris_token" \
        -H "X-Iceberg-Access-Delegation: vended-credentials")
      if ! python3 - "$load_table_resp" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
creds = payload.get("storage-credentials")
if not isinstance(creds, list) or not creds:
    sys.exit(1)
PY
      then
        echo "[FAIL] $label upstream iceberg Polaris loadTable did not return storage-credentials"
        echo "$load_table_resp"
        return 1
      fi
    fi

    local storage_authority_name
    storage_authority_name="smoke-upstream-iceberg-storage"
    local rest_setup_out
    rest_setup_out=$(run_cli_script "$compose_cmd" "account t-0001
catalog create $COMPOSE_SMOKE_UPSTREAM_ICEBERG_DEST_CATALOG --desc compose-smoke-upstream-iceberg
storage-authority create $storage_authority_name --location-prefix s3://floecat/ --type s3 --region us-east-1 --endpoint http://localstack:4566 --path-style-access true --assume-role-arn arn:aws:iam::000000000000:role/polaris --duration-seconds 900 --cred-type aws --cred access_key_id=test --cred secret_access_key=test
connector create smoke-upstream-iceberg ICEBERG $COMPOSE_SMOKE_UPSTREAM_ICEBERG_URI $COMPOSE_SMOKE_UPSTREAM_ICEBERG_SOURCE_NS $COMPOSE_SMOKE_UPSTREAM_ICEBERG_DEST_CATALOG --auth-scheme oauth2 --cred-type client --cred endpoint=http://polaris:8181/api/catalog/v1/oauth/tokens --cred client_id=root --cred client_secret=s3cr3t --cred scope=PRINCIPAL_ROLE:ALL --props iceberg.source=rest --props rest.flavor=polaris --props warehouse=$warehouse --props s3.endpoint=http://localstack:4566 --props s3.path-style-access=true --props s3.region=us-east-1
quit")
    echo "$rest_setup_out"
    assert_contains "$label upstream iceberg storage authority setup" "$rest_setup_out" "$storage_authority_name"
    assert_contains "$label upstream iceberg connector setup" "$rest_setup_out" "smoke-upstream-iceberg"

    run_connector_trigger_and_wait \
      "$compose_cmd" \
      "$label upstream iceberg connector" \
      "account t-0001
connector trigger smoke-upstream-iceberg --full --mode metadata-and-capture --capture stats,index
quit" \
      120 \
      2

    local out_upstream_iceberg
    out_upstream_iceberg=$(run_cli_script "$compose_cmd" "account t-0001
resolve table $COMPOSE_SMOKE_UPSTREAM_ICEBERG_EXPECTED_TABLE
quit")
    echo "$out_upstream_iceberg"
    assert_contains "$label upstream iceberg imported account" "$out_upstream_iceberg" "account set:"
    assert_contains "$label upstream iceberg imported table" "$out_upstream_iceberg" "table id:"

    assert_table_stats_available \
      "$compose_cmd" \
      "$label upstream iceberg file stats" \
      "$COMPOSE_SMOKE_UPSTREAM_ICEBERG_EXPECTED_TABLE" \
      "${COMPOSE_SMOKE_STATS_RETRIES:-45}" \
      "${COMPOSE_SMOKE_STATS_SLEEP_SECONDS:-2}"
    assert_table_indexes_available \
      "$compose_cmd" \
      "$label upstream iceberg page indexes" \
      "$COMPOSE_SMOKE_UPSTREAM_ICEBERG_EXPECTED_TABLE" \
      "${COMPOSE_SMOKE_STATS_RETRIES:-45}" \
      "${COMPOSE_SMOKE_STATS_SLEEP_SECONDS:-2}"
  elif [ "$profile" = "localstack" ]; then
    echo "==> [SMOKE] skipping upstream iceberg rest import (set COMPOSE_SMOKE_UPSTREAM_ICEBERG_IMPORT=false to disable)"
  fi

  if [ "$smoke_scope" = "full" ] && [ "$profile" = "localstack" ] && is_truthy "$COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_IMPORT"; then
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
    unity_catalog_resp=$(docker run --rm --network "${compose_project}_floecat" curlimages/curl:8.12.1 -sS \
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
    unity_schema_resp=$(docker run --rm --network "${compose_project}_floecat" curlimages/curl:8.12.1 -sS \
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
    unity_table_resp=$(docker run --rm --network "${compose_project}_floecat" curlimages/curl:8.12.1 -sS \
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
connector create smoke-upstream-delta-unity DELTA $COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_URI $COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_SOURCE_NS $COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_DEST_CATALOG --source-table $COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_SOURCE_TABLE --dest-ns $COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_DEST_NS --props delta.source=unity --props s3.endpoint=$COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_S3_ENDPOINT --props s3.path-style-access=true --props s3.region=$COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_S3_REGION --cred-type aws --cred access_key_id=$COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_S3_ACCESS_KEY_ID --cred secret_access_key=$COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_S3_SECRET_ACCESS_KEY $COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_CONNECTOR_ARGS
quit")
    echo "$unity_setup_out"
    assert_contains "$label upstream delta unity connector setup" "$unity_setup_out" "smoke-upstream-delta-unity"

    run_connector_trigger_and_wait \
      "$compose_cmd" \
      "$label upstream delta unity connector" \
      "account t-0001
connector trigger smoke-upstream-delta-unity --full --mode metadata-and-capture --capture stats,index
quit" \
      120 \
      2

    local out_upstream_delta_unity
    out_upstream_delta_unity=$(run_cli_script "$compose_cmd" "account t-0001
resolve table $COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_EXPECTED_TABLE
quit")
    echo "$out_upstream_delta_unity"
    assert_contains "$label upstream delta unity imported account" "$out_upstream_delta_unity" "account set:"
    assert_contains "$label upstream delta unity imported table" "$out_upstream_delta_unity" "table id:"

    assert_table_stats_available \
      "$compose_cmd" \
      "$label upstream delta unity file stats" \
      "$COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_EXPECTED_TABLE" \
      "${COMPOSE_SMOKE_STATS_RETRIES:-45}" \
      "${COMPOSE_SMOKE_STATS_SLEEP_SECONDS:-2}"
    assert_table_indexes_available \
      "$compose_cmd" \
      "$label upstream delta unity page indexes" \
      "$COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_EXPECTED_TABLE" \
      "${COMPOSE_SMOKE_STATS_RETRIES:-45}" \
      "${COMPOSE_SMOKE_STATS_SLEEP_SECONDS:-2}"
  elif [ "$smoke_scope" = "full" ] && [ "$profile" = "localstack" ]; then
    echo "==> [SMOKE] skipping upstream delta unity import (set COMPOSE_SMOKE_UPSTREAM_DELTA_UNITY_IMPORT=true to enable)"
  fi

  if [ "$smoke_scope" = "full" ] && [ "$profile" = "localstack" ] && should_run_client trino; then
    local trino_host_port="${FLOECAT_TRINO_HOST_PORT:-8081}"
    wait_for_url "http://localhost:${trino_host_port}/v1/info" 180 "Trino health"
  fi

  if [ "$smoke_scope" = "full" ] && [ "$profile" = "localstack" ] && should_run_client duckdb; then
    local aws_cli="docker run --rm --network ${compose_project}_floecat -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test -e AWS_DEFAULT_REGION=us-east-1 amazon/aws-cli:2.17.50"
    echo "==> [SMOKE] localstack pre-duckdb S3 listing (floecat-delta)"
    $aws_cli --endpoint-url http://localstack:4566 s3 ls s3://floecat-delta/ --recursive | sed -n '1,400p' || true
    echo "==> [SMOKE] localstack pre-duckdb S3 listing (floecat-delta/call_center)"
    $aws_cli --endpoint-url http://localstack:4566 s3 ls s3://floecat-delta/call_center/ --recursive | sed -n '1,200p' || true

    echo "==> [SMOKE] duckdb federation check (localstack)"
    local duckdb_bootstrap="INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws; INSTALL iceberg; LOAD iceberg; CREATE OR REPLACE SECRET smoke_localstack_s3 (TYPE S3, PROVIDER config, KEY_ID 'test', SECRET 'test', REGION 'us-east-1', ENDPOINT 'localstack:4566', URL_STYLE 'path', USE_SSL false); SET s3_endpoint='localstack:4566'; SET s3_use_ssl=false; SET s3_url_style='path'; SET s3_region='us-east-1'; SET s3_access_key_id='test'; SET s3_secret_access_key='test'; ATTACH 'examples' AS iceberg_floecat (TYPE iceberg, ENDPOINT 'http://iceberg-rest:9200/', AUTHORIZATION_TYPE none, ACCESS_DELEGATION_MODE 'none', PURGE_REQUESTED true); SET s3_endpoint='localstack:4566'; SET s3_use_ssl=false; SET s3_url_style='path'; SET s3_region='us-east-1'; SET s3_access_key_id='test'; SET s3_secret_access_key='test';"
    local duckdb_query="SELECT 'duckdb_smoke_ok' AS status; SELECT 'call_center=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.delta.call_center; SELECT 'my_local_delta_table=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.delta.my_local_delta_table; SELECT 'my_local_nonnull_name=' || CAST(COUNT(name) AS VARCHAR) AS check FROM iceberg_floecat.delta.my_local_delta_table; SELECT 'dv_demo_delta=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.delta.dv_demo_delta; SELECT 'dv_content=' || CAST(MIN(id) AS VARCHAR) || ',' || CAST(MAX(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) AS check FROM iceberg_floecat.delta.dv_demo_delta; SELECT 'empty_join=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.iceberg.trino_types i JOIN iceberg_floecat.delta.call_center d ON 1=0; DROP TABLE IF EXISTS iceberg_floecat.iceberg.duckdb_ctas_smoke; CREATE TABLE iceberg_floecat.iceberg.duckdb_ctas_smoke AS SELECT * FROM iceberg_floecat.delta.call_center LIMIT 5; SELECT 'ctas_count=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.iceberg.duckdb_ctas_smoke; DROP TABLE iceberg_floecat.iceberg.duckdb_ctas_smoke; DROP TABLE IF EXISTS iceberg_floecat.iceberg.duckdb_recreate_smoke; CREATE TABLE iceberg_floecat.iceberg.duckdb_recreate_smoke (id INTEGER, v VARCHAR); INSERT INTO iceberg_floecat.iceberg.duckdb_recreate_smoke VALUES (11, 'first'); SELECT 'recreate_first_insert=' || CAST(COUNT(*) AS VARCHAR) || ',' || CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) AS check FROM iceberg_floecat.iceberg.duckdb_recreate_smoke; DELETE FROM iceberg_floecat.iceberg.duckdb_recreate_smoke WHERE id = 11; SELECT 'recreate_after_delete=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.iceberg.duckdb_recreate_smoke; DROP TABLE iceberg_floecat.iceberg.duckdb_recreate_smoke; CREATE TABLE iceberg_floecat.iceberg.duckdb_recreate_smoke (id INTEGER, v VARCHAR); INSERT INTO iceberg_floecat.iceberg.duckdb_recreate_smoke VALUES (22, 'second'); SELECT 'recreate_second_insert=' || CAST(COUNT(*) AS VARCHAR) || ',' || CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) AS check FROM iceberg_floecat.iceberg.duckdb_recreate_smoke; DROP TABLE iceberg_floecat.iceberg.duckdb_recreate_smoke; DROP TABLE IF EXISTS iceberg_floecat.iceberg.duckdb_mutation_smoke; CREATE TABLE iceberg_floecat.iceberg.duckdb_mutation_smoke (id INTEGER, v VARCHAR); SELECT 'mut_after_create=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.iceberg.duckdb_mutation_smoke; INSERT INTO iceberg_floecat.iceberg.duckdb_mutation_smoke VALUES (1, 'a'), (2, 'b'), (3, 'c'); SELECT 'mut_after_insert=' || CAST(COUNT(*) AS VARCHAR) || ',' || CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) AS check FROM iceberg_floecat.iceberg.duckdb_mutation_smoke; DELETE FROM iceberg_floecat.iceberg.duckdb_mutation_smoke WHERE id = 2; SELECT 'mut_after_delete=' || CAST(COUNT(*) AS VARCHAR) || ',' || CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) AS check FROM iceberg_floecat.iceberg.duckdb_mutation_smoke; UPDATE iceberg_floecat.iceberg.duckdb_mutation_smoke SET v = 'c2' WHERE id = 3; SELECT 'mut_after_update=' || CAST(COUNT(*) AS VARCHAR) || ',' || CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) AS check FROM iceberg_floecat.iceberg.duckdb_mutation_smoke;"

    local duckdb_out
    if ! duckdb_out=$(docker run --rm --network "${compose_project}_floecat" "$COMPOSE_SMOKE_DUCKDB_IMAGE" \
      duckdb -c "$duckdb_bootstrap $duckdb_query" 2>&1); then
      echo "$duckdb_out"
      echo "[FAIL] $label duckdb command failed"
      return 1
    fi

    echo "$duckdb_out"
    assert_contains "$label duckdb smoke marker" "$duckdb_out" "duckdb_smoke_ok"
    assert_contains "$label duckdb call_center count" "$duckdb_out" "call_center=42"
    assert_contains "$label duckdb my_local_delta_table count" "$duckdb_out" "my_local_delta_table=4"
    assert_contains "$label duckdb my_local_delta_table nonnull" "$duckdb_out" "my_local_nonnull_name=4"
    assert_contains "$label duckdb dv_demo_delta count" "$duckdb_out" "dv_demo_delta=2"
    assert_contains "$label duckdb dv_demo_delta content" "$duckdb_out" "dv_content=1,3,a,c"
    assert_contains "$label duckdb ctas count" "$duckdb_out" "ctas_count=5"
    assert_contains "$label duckdb recreate first insert" "$duckdb_out" "recreate_first_insert=1,11,first,first"
    assert_contains "$label duckdb recreate after delete" "$duckdb_out" "recreate_after_delete=0"
    assert_contains "$label duckdb recreate second insert" "$duckdb_out" "recreate_second_insert=1,22,second,second"
    assert_contains "$label duckdb mutation create" "$duckdb_out" "mut_after_create=0"
    assert_contains "$label duckdb mutation insert" "$duckdb_out" "mut_after_insert=3,6,a,c"
    assert_contains "$label duckdb mutation delete" "$duckdb_out" "mut_after_delete=2,4,a,c"
    assert_contains "$label duckdb mutation update" "$duckdb_out" "mut_after_update=2,4,a,c2"

    local duckdb_rename_out
    if duckdb_rename_out=$(docker run --rm --network "${compose_project}_floecat" "$COMPOSE_SMOKE_DUCKDB_IMAGE" \
      duckdb -c "$duckdb_bootstrap DROP TABLE IF EXISTS iceberg_floecat.iceberg.duckdb_rename_dst_smoke; DROP TABLE IF EXISTS iceberg_floecat.iceberg.duckdb_rename_src_smoke; CREATE TABLE iceberg_floecat.iceberg.duckdb_rename_src_smoke (id INTEGER, v VARCHAR); INSERT INTO iceberg_floecat.iceberg.duckdb_rename_src_smoke VALUES (1, 'x'); ALTER TABLE iceberg_floecat.iceberg.duckdb_rename_src_smoke RENAME TO duckdb_rename_dst_smoke; SELECT 'rename_row_count=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.iceberg.duckdb_rename_dst_smoke; DROP TABLE iceberg_floecat.iceberg.duckdb_rename_dst_smoke;" 2>&1); then
      echo "$duckdb_rename_out"
      assert_contains "$label duckdb rename row count" "$duckdb_rename_out" "rename_row_count=1"
    else
      echo "$duckdb_rename_out"
      assert_contains_any "$label duckdb rename unsupported" "$duckdb_rename_out" \
        "Not implemented Error: Alter Schema Entry" \
        "Not implemented Error: Alter table type not supported: RENAME_TABLE"
      docker run --rm --network "${compose_project}_floecat" "$COMPOSE_SMOKE_DUCKDB_IMAGE" \
        duckdb -c "$duckdb_bootstrap DROP TABLE IF EXISTS iceberg_floecat.iceberg.duckdb_rename_dst_smoke; DROP TABLE IF EXISTS iceberg_floecat.iceberg.duckdb_rename_src_smoke;" >/dev/null 2>&1 || true
    fi

    local duckdb_namespace_out
    if duckdb_namespace_out=$(docker run --rm --network "${compose_project}_floecat" "$COMPOSE_SMOKE_DUCKDB_IMAGE" \
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
    if ! duckdb_snapshots_out=$(docker run --rm --network "${compose_project}_floecat" "$COMPOSE_SMOKE_DUCKDB_IMAGE" \
      duckdb -csv -c "$duckdb_bootstrap $duckdb_snapshots_query_fn" 2>&1); then
      echo "$duckdb_snapshots_out"
      echo "[FAIL] $label duckdb time travel snapshot discovery failed"
      return 1
    fi
    local duckdb_snapshot_ids
    duckdb_snapshot_ids=()
    while IFS= read -r snapshot_id; do
      duckdb_snapshot_ids+=("$snapshot_id")
    done <<EOF
$(printf '%s\n' "$duckdb_snapshots_out" | tr -d '\r' | awk '/^[0-9]+$/ {print $1}')
EOF
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
    if ! duckdb_tt_out=$(docker run --rm --network "${compose_project}_floecat" "$COMPOSE_SMOKE_DUCKDB_IMAGE" \
      duckdb -c "$duckdb_bootstrap $duckdb_tt_query" 2>&1); then
      echo "$duckdb_tt_out"
      echo "[FAIL] $label duckdb time travel query failed"
      return 1
    fi
    echo "$duckdb_tt_out"
    assert_contains "$label duckdb time travel insert snapshot" "$duckdb_tt_out" "mut_tt_after_insert=3,6,a,c"
    assert_contains "$label duckdb time travel delete snapshot" "$duckdb_tt_out" "mut_tt_after_delete=2,4,a,c"
    assert_contains "$label duckdb time travel update snapshot" "$duckdb_tt_out" "mut_tt_after_update=2,4,a,c2"
    assert_table_stats_available \
      "$compose_cmd" \
      "$label duckdb mutation baseline" \
      "examples.iceberg.duckdb_mutation_smoke" \
      "$COMPOSE_SMOKE_STATS_RETRIES" \
      "$COMPOSE_SMOKE_STATS_SLEEP_SECONDS"

    local alter_out
    local alter_status=0
    alter_out=$(docker run --rm --network "${compose_project}_floecat" "$COMPOSE_SMOKE_DUCKDB_IMAGE" \
      duckdb -c "$duckdb_bootstrap ALTER TABLE iceberg_floecat.iceberg.duckdb_mutation_smoke ADD COLUMN note VARCHAR; SELECT 'mut_after_alter=' || CAST(COUNT(*) AS VARCHAR) || ',' || CAST(COUNT(note) AS VARCHAR) AS check FROM iceberg_floecat.iceberg.duckdb_mutation_smoke; DROP TABLE iceberg_floecat.iceberg.duckdb_mutation_smoke;" 2>&1) || alter_status=$?
    echo "$alter_out"
    if [ "$alter_status" -eq 0 ]; then
      assert_contains "$label duckdb mutation alter" "$alter_out" "mut_after_alter=2,0"
    else
      assert_contains_any "$label duckdb mutation alter unsupported" "$alter_out" \
        "Not implemented Error: Alter Schema Entry" \
        "Not implemented Error: Alter table type not supported:"
      docker run --rm --network "${compose_project}_floecat" "$COMPOSE_SMOKE_DUCKDB_IMAGE" \
        duckdb -c "$duckdb_bootstrap DROP TABLE IF EXISTS iceberg_floecat.iceberg.duckdb_mutation_smoke;" >/dev/null 2>&1 || true
    fi

    local drop_check_out
    if drop_check_out=$(docker run --rm --network "${compose_project}_floecat" "$COMPOSE_SMOKE_DUCKDB_IMAGE" \
      duckdb -c "$duckdb_bootstrap SELECT COUNT(*) FROM iceberg_floecat.iceberg.duckdb_ctas_smoke;" 2>&1); then
      echo "$drop_check_out"
      echo "[FAIL] $label duckdb drop table verification failed (table still queryable)"
      return 1
    else
      assert_contains "$label duckdb drop table verification" "$drop_check_out" "Table with name duckdb_ctas_smoke does not exist"
      echo "[PASS] $label duckdb drop table verification (expected missing-table error after DROP TABLE)"
    fi

    if is_truthy "$COMPOSE_SMOKE_ICEBERG_FORMAT_MATRIX"; then
      echo "==> [SMOKE] duckdb iceberg format-version matrix (v1,v2)"
      local duckdb_fmt_out
      if ! duckdb_fmt_out=$(docker run --rm --network "${compose_project}_floecat" "$COMPOSE_SMOKE_DUCKDB_IMAGE" \
        duckdb -c "$duckdb_bootstrap DROP TABLE IF EXISTS iceberg_floecat.iceberg.duckdb_fmt_v1_smoke; DROP TABLE IF EXISTS iceberg_floecat.iceberg.duckdb_fmt_v2_smoke; CREATE TABLE iceberg_floecat.iceberg.duckdb_fmt_v1_smoke (id INTEGER, v VARCHAR) WITH (format_version=1); INSERT INTO iceberg_floecat.iceberg.duckdb_fmt_v1_smoke VALUES (101, 'duckdb_v1'); SELECT 'duckdb_fmt_v1_count=' || CAST(COUNT(*) AS VARCHAR) || ',' || CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) AS check FROM iceberg_floecat.iceberg.duckdb_fmt_v1_smoke; CREATE TABLE iceberg_floecat.iceberg.duckdb_fmt_v2_smoke (id INTEGER, v VARCHAR) WITH (format_version=2); INSERT INTO iceberg_floecat.iceberg.duckdb_fmt_v2_smoke VALUES (201, 'duckdb_v2'); SELECT 'duckdb_fmt_v2_count=' || CAST(COUNT(*) AS VARCHAR) || ',' || CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) AS check FROM iceberg_floecat.iceberg.duckdb_fmt_v2_smoke;" 2>&1); then
        echo "$duckdb_fmt_out"
        echo "[FAIL] $label duckdb format-version matrix command failed"
        return 1
      fi
      echo "$duckdb_fmt_out"
      assert_contains "$label duckdb format v1 queryability" "$duckdb_fmt_out" "duckdb_fmt_v1_count=1,101,duckdb_v1,duckdb_v1"
      assert_contains "$label duckdb format v2 queryability" "$duckdb_fmt_out" "duckdb_fmt_v2_count=1,201,duckdb_v2,duckdb_v2"
      assert_table_stats_available \
        "$compose_cmd" \
        "$label duckdb format v1" \
        "examples.iceberg.duckdb_fmt_v1_smoke" \
        "$COMPOSE_SMOKE_STATS_RETRIES" \
        "$COMPOSE_SMOKE_STATS_SLEEP_SECONDS"
      assert_table_stats_available \
        "$compose_cmd" \
        "$label duckdb format v2" \
        "examples.iceberg.duckdb_fmt_v2_smoke" \
        "$COMPOSE_SMOKE_STATS_RETRIES" \
        "$COMPOSE_SMOKE_STATS_SLEEP_SECONDS"
    fi
  fi

  if [ "$smoke_scope" = "full" ] && [ "$profile" = "localstack" ] && should_run_client trino; then
    echo "==> [SMOKE] trino federation check (localstack)"
    local trino_out
    local trino_mutation_baseline_jobs=""
    trino_mutation_baseline_jobs=$(list_connector_job_states_for_table "$compose_cmd" "iceberg.trino_mutation_smoke" | awk '{print $1}')
    if ! trino_out=$(docker run --rm --network "${compose_project}_floecat" -i python:3.12-alpine python - <<'PY' 2>&1
import json
import sys
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
run_sql("DROP TABLE IF EXISTS iceberg.trino_ctas_smoke")
run_sql("CREATE TABLE iceberg.trino_ctas_smoke AS SELECT * FROM delta.call_center LIMIT 5")
print(scalar("SELECT 'ctas_count=' || CAST(COUNT(*) AS VARCHAR) FROM iceberg.trino_ctas_smoke"))
run_sql("DROP TABLE iceberg.trino_ctas_smoke")
run_sql("DROP VIEW IF EXISTS iceberg.trino_view_smoke")
run_sql("CREATE VIEW iceberg.trino_view_smoke AS SELECT id, v FROM delta.dv_demo_delta")
print(
    scalar(
        "SELECT 'view_initial=' || CAST(COUNT(*) AS VARCHAR) || ',' || "
        "CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) "
        "FROM iceberg.trino_view_smoke"
    )
)
run_sql("CREATE OR REPLACE VIEW iceberg.trino_view_smoke AS SELECT id, v FROM delta.dv_demo_delta WHERE id = 3")
print(
    scalar(
        "SELECT 'view_updated=' || CAST(COUNT(*) AS VARCHAR) || ',' || "
        "CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) "
        "FROM iceberg.trino_view_smoke"
    )
)
run_sql("DROP VIEW iceberg.trino_view_smoke")
drop_ok = False
try:
    run_sql("SELECT COUNT(*) FROM iceberg.trino_ctas_smoke")
except Exception:
    drop_ok = True
print("drop_table_ok=" + ("1" if drop_ok else "0"))
run_sql("DROP SCHEMA IF EXISTS floecat.trino_ns_smoke")
run_sql("CREATE SCHEMA IF NOT EXISTS floecat.trino_ns_smoke")
print("ns_create_ok=1")
run_sql("DROP SCHEMA floecat.trino_ns_smoke")
print("ns_drop_ok=1")
run_sql("DROP TABLE IF EXISTS iceberg.trino_rename_dst_smoke")
run_sql("DROP TABLE IF EXISTS iceberg.trino_rename_src_smoke")
run_sql("CREATE TABLE iceberg.trino_rename_src_smoke (id INTEGER, v VARCHAR)")
run_sql("INSERT INTO iceberg.trino_rename_src_smoke VALUES (1, 'x')")
run_sql("ALTER TABLE iceberg.trino_rename_src_smoke RENAME TO iceberg.trino_rename_dst_smoke")
print(
    scalar(
        "SELECT 'rename_row_count=' || CAST(COUNT(*) AS VARCHAR) "
        "FROM iceberg.trino_rename_dst_smoke"
    )
)
run_sql("DROP TABLE iceberg.trino_rename_dst_smoke")
run_sql("DROP TABLE IF EXISTS iceberg.trino_meta_smoke")
run_sql("CREATE TABLE iceberg.trino_meta_smoke (id INTEGER, v VARCHAR)")
run_sql("ALTER TABLE iceberg.trino_meta_smoke ADD COLUMN note VARCHAR")
run_sql("ALTER TABLE iceberg.trino_meta_smoke RENAME COLUMN v TO v2")
print(
    scalar(
        "SELECT 'meta_columns=' || CAST(COUNT(*) AS VARCHAR) "
        "FROM information_schema.columns "
        "WHERE table_catalog = 'floecat' AND table_schema = 'iceberg' "
        "AND table_name = 'trino_meta_smoke' AND column_name IN ('id', 'v2', 'note')"
    )
)
run_sql("DROP TABLE iceberg.trino_meta_smoke")
run_sql("DROP TABLE IF EXISTS iceberg.trino_merge_smoke")
run_sql("CREATE TABLE iceberg.trino_merge_smoke (id INTEGER, v VARCHAR)")
run_sql("INSERT INTO iceberg.trino_merge_smoke VALUES (1, 'a'), (2, 'b')")
run_sql(
    "MERGE INTO iceberg.trino_merge_smoke t "
    "USING (VALUES (2, 'b2'), (3, 'c')) s(id, v) "
    "ON t.id = s.id "
    "WHEN MATCHED THEN UPDATE SET v = s.v "
    "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
)
print(
    scalar(
        "SELECT 'merge_after=' || CAST(COUNT(*) AS VARCHAR) || ',' || "
        "CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) "
        "FROM iceberg.trino_merge_smoke"
    )
)
run_sql("DROP TABLE iceberg.trino_merge_smoke")
run_sql("DROP TABLE IF EXISTS iceberg.trino_mutation_smoke")
run_sql("CREATE TABLE iceberg.trino_mutation_smoke (id INTEGER, v VARCHAR)")


def latest_mutation_snapshot(*exclude_snapshot_ids):
    filters = ["snapshot_id IS NOT NULL", "parent_id IS NOT NULL"]
    for snapshot_id in exclude_snapshot_ids:
        filters.append(f"snapshot_id <> {int(snapshot_id)}")
    return scalar(
        "SELECT CAST(snapshot_id AS VARCHAR) "
        "FROM iceberg.\"trino_mutation_smoke$snapshots\" "
        f"WHERE {' AND '.join(filters)} "
        "ORDER BY committed_at DESC NULLS LAST, snapshot_id DESC "
        "LIMIT 1"
    )


print(
    scalar(
        "SELECT 'mut_after_create=' || CAST(COUNT(*) AS VARCHAR) "
        "FROM iceberg.trino_mutation_smoke"
    )
)
run_sql("INSERT INTO iceberg.trino_mutation_smoke VALUES (1, 'a'), (2, 'b'), (3, 'c')")
insert_snapshot_id = latest_mutation_snapshot()
print(
    scalar(
        "SELECT 'mut_after_insert=' || CAST(COUNT(*) AS VARCHAR) || ',' || "
        "CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) "
        "FROM iceberg.trino_mutation_smoke"
    )
)
run_sql("DELETE FROM iceberg.trino_mutation_smoke WHERE id = 2")
delete_snapshot_id = latest_mutation_snapshot(insert_snapshot_id)
print(
    scalar(
        "SELECT 'mut_after_delete=' || CAST(COUNT(*) AS VARCHAR) || ',' || "
        "CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) "
        "FROM iceberg.trino_mutation_smoke"
    )
)
run_sql("UPDATE iceberg.trino_mutation_smoke SET v = 'c2' WHERE id = 3")
update_snapshot_id = latest_mutation_snapshot(insert_snapshot_id, delete_snapshot_id)
print(
    scalar(
        "SELECT 'mut_after_update=' || CAST(COUNT(*) AS VARCHAR) || ',' || "
        "CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) "
        "FROM iceberg.trino_mutation_smoke"
    )
)
print(
    scalar(
        "SELECT 'mut_tt_after_insert=' || CAST(COUNT(*) AS VARCHAR) || ',' || "
        "CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) "
        f"FROM iceberg.trino_mutation_smoke FOR VERSION AS OF {insert_snapshot_id}"
    )
)
print(
    scalar(
        "SELECT 'mut_tt_after_delete=' || CAST(COUNT(*) AS VARCHAR) || ',' || "
        "CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) "
        f"FROM iceberg.trino_mutation_smoke FOR VERSION AS OF {delete_snapshot_id}"
    )
)
print(
    scalar(
        "SELECT 'mut_tt_after_update=' || CAST(COUNT(*) AS VARCHAR) || ',' || "
        "CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) "
        f"FROM iceberg.trino_mutation_smoke FOR VERSION AS OF {update_snapshot_id}"
    )
)
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
    assert_contains "$label trino view initial query" "$trino_out" "view_initial=2,4,a,c"
    assert_contains "$label trino view updated query" "$trino_out" "view_updated=1,3,c,c"
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
    wait_for_table_reconcile_jobs \
      "$compose_cmd" \
      "$label trino mutation" \
      "iceberg.trino_mutation_smoke" \
      "$trino_mutation_baseline_jobs" \
      4 \
      120 \
      2
    local trino_drop_out
    trino_drop_out=$(docker run --rm --network "${compose_project}_floecat" -i python:3.12-alpine python - <<'PY' 2>&1
import json
import urllib.request

TRINO_URL = "http://trino:8080/v1/statement"
HEADERS = {
    "X-Trino-User": "smoke",
    "X-Trino-Source": "compose-smoke",
    "X-Trino-Catalog": "floecat",
    "X-Trino-Schema": "iceberg",
}

req = urllib.request.Request(
    TRINO_URL,
    data=b"DROP TABLE iceberg.trino_mutation_smoke",
    headers={**HEADERS, "Content-Type": "text/plain; charset=utf-8"},
    method="POST",
)
with urllib.request.urlopen(req, timeout=60) as resp:
    payload = json.loads(resp.read().decode("utf-8"))
while payload.get("nextUri"):
    next_req = urllib.request.Request(payload["nextUri"], headers=HEADERS, method="GET")
    with urllib.request.urlopen(next_req, timeout=60) as next_resp:
        payload = json.loads(next_resp.read().decode("utf-8"))
    if payload.get("error"):
        err = payload["error"]
        raise RuntimeError(f"{err.get('errorName')}: {err.get('message')}")
print("trino_mutation_drop_ok=1")
PY
)
    echo "$trino_drop_out"
    assert_contains "$label trino mutation drop" "$trino_drop_out" "trino_mutation_drop_ok=1"

    if is_truthy "$COMPOSE_SMOKE_ICEBERG_FORMAT_MATRIX"; then
      echo "==> [SMOKE] trino iceberg format-version matrix (v1,v2)"
      local trino_fmt_out
      local check_duckdb_format_tables=false
      if should_run_client duckdb; then
        check_duckdb_format_tables=true
      fi
      if ! trino_fmt_out=$(docker run --rm --network "${compose_project}_floecat" -e CHECK_DUCKDB_FORMAT_TABLES="$check_duckdb_format_tables" -i python:3.12-alpine python - <<'PY' 2>&1
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
      assert_table_stats_available \
        "$compose_cmd" \
        "$label trino format v1" \
        "examples.iceberg.trino_fmt_v1_smoke" \
        "$COMPOSE_SMOKE_STATS_RETRIES" \
        "$COMPOSE_SMOKE_STATS_SLEEP_SECONDS"
      assert_table_stats_available \
        "$compose_cmd" \
        "$label trino format v2" \
        "examples.iceberg.trino_fmt_v2_smoke" \
        "$COMPOSE_SMOKE_STATS_RETRIES" \
        "$COMPOSE_SMOKE_STATS_SLEEP_SECONDS"
    fi
  fi

  if [ "$smoke_scope" = "full" ] && [ "$label" = "localstack-remote" ]; then
    assert_remote_file_group_worker_activity "$compose_cmd" "$label"
  fi

  trap - ERR
  echo "==> [SMOKE][PASS] mode=$label"
}

SMOKE_MODES=${COMPOSE_SMOKE_MODES:-localstack,localstack-oidc}
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
    localstack-remote)
      run_mode \
        ./env.localstack \
        localstack \
        localstack-remote \
        "localstack" \
        "" \
        "" \
        "localstack,reconciler-executor" \
        "QUARKUS_PROFILE_SERVICE=reconciler-control" \
        "${COMPOSE_SMOKE_REMOTE_EXECUTOR_SCALE:-1}"
      ;;
    localstack-polaris-vended-creds)
      run_mode \
        ./env.localstack \
        localstack \
        localstack-polaris-vended-creds \
        "localstack" \
        "" \
        "" \
        "localstack,reconciler-executor" \
        "FLOECAT_SECRETS=kv FLOECAT_RECONCILER_WORKER_MODE=remote FLOECAT_RECONCILER_MAX_PARALLELISM=0 FLOECAT_RECONCILER_AUTO_ENABLED=false FLOECAT_SEED_MODE=iceberg FLOECAT_SEED_SYNC_ENABLED=false FLOECAT_EXECUTOR_AWS_ACCESS_KEY_ID= FLOECAT_EXECUTOR_AWS_SECRET_ACCESS_KEY= FLOECAT_EXECUTOR_AWS_SESSION_TOKEN= FLOECAT_EXECUTOR_AWS_REGION=us-east-1 FLOECAT_EXECUTOR_AWS_DEFAULT_REGION=us-east-1 FLOECAT_EXECUTOR_AWS_ENDPOINT_URL=http://localstack:4566 FLOECAT_EXECUTOR_AWS_ENDPOINT_URL_S3=http://localstack:4566 FLOECAT_EXECUTOR_AWS_PROFILE= FLOECAT_EXECUTOR_AWS_SDK_LOAD_CONFIG=0" \
        "${COMPOSE_SMOKE_REMOTE_EXECUTOR_SCALE:-1}" \
        "polaris-vended-creds"
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
