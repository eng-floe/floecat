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

DOCKER_COMPOSE_MAIN=${DOCKER_COMPOSE_MAIN:-docker compose -f docker/docker-compose.yml}
COMPOSE_SMOKE_SAVE_LOG_DIR_DEFAULT=${COMPOSE_SMOKE_SAVE_LOG_DIR:-target/compose-smoke-logs}
COMPOSE_SMOKE_KEEP_ON_FAIL=${COMPOSE_SMOKE_KEEP_ON_FAIL:-false}
COMPOSE_SMOKE_KEEP_ON_EXIT=${COMPOSE_SMOKE_KEEP_ON_EXIT:-false}
COMPOSE_SMOKE_CLIENTS=${COMPOSE_SMOKE_CLIENTS:-duckdb,trino}

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
  local mode_env="FLOECAT_ENV_FILE=$env_file COMPOSE_PROFILES=$profile COMPOSE_PROJECT_NAME=$compose_project"

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
      | grep -E "ERROR|WARN|Exception|FAIL|failed|timed out|currentSnapshot=<null>|snapshotCount=0" \
      | grep -Ev "localstack.request.aws[[:space:]]+: AWS s3\.[A-Za-z]+ => 200|io\.qua\.htt\.access-log.*\"(GET|HEAD) .*\" 20[04]" \
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

  if [ -n "$pre_services" ]; then
    eval "$compose_cmd up -d $pre_services"
  fi

  if [ "$profile" = "localstack" ] || [ "$profile" = "localstack-oidc" ]; then
    wait_for_url "http://localhost:4566/_localstack/health" 120 "LocalStack health"
  fi

  if [ "$profile" = "localstack-oidc" ]; then
    wait_for_url "http://localhost:8080/realms/floecat/.well-known/openid-configuration" 180 "Keycloak health"
  fi

  eval "$compose_cmd up -d"

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

  if [ "$profile" = "localstack" ] && should_run_client trino; then
    local trino_host_port="${FLOECAT_TRINO_HOST_PORT:-8081}"
    wait_for_url "http://localhost:${trino_host_port}/v1/info" 180 "Trino health"
  fi

  if [ "$profile" = "localstack" ] && should_run_client duckdb; then
    local aws_cli="docker run --rm --network ${compose_project}_floecat -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test -e AWS_DEFAULT_REGION=us-east-1 amazon/aws-cli:2.17.50"
    echo "==> [SMOKE] localstack pre-duckdb S3 listing (floecat-delta)"
    $aws_cli --endpoint-url http://localstack:4566 s3 ls s3://floecat-delta/ --recursive | sed -n '1,400p' || true
    echo "==> [SMOKE] localstack pre-duckdb S3 listing (floecat-delta/call_center)"
    $aws_cli --endpoint-url http://localstack:4566 s3 ls s3://floecat-delta/call_center/ --recursive | sed -n '1,200p' || true

    echo "==> [SMOKE] duckdb federation check (localstack)"
    local duckdb_bootstrap="INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws; INSTALL iceberg; LOAD iceberg; CREATE OR REPLACE SECRET smoke_localstack_s3 (TYPE S3, PROVIDER config, KEY_ID 'test', SECRET 'test', REGION 'us-east-1', ENDPOINT 'localstack:4566', URL_STYLE 'path', USE_SSL false); SET s3_endpoint='localstack:4566'; SET s3_use_ssl=false; SET s3_url_style='path'; SET s3_region='us-east-1'; SET s3_access_key_id='test'; SET s3_secret_access_key='test'; ATTACH 'examples' AS iceberg_floecat (TYPE iceberg, ENDPOINT 'http://iceberg-rest:9200/', AUTHORIZATION_TYPE none, ACCESS_DELEGATION_MODE 'none'); SET s3_endpoint='localstack:4566'; SET s3_use_ssl=false; SET s3_url_style='path'; SET s3_region='us-east-1'; SET s3_access_key_id='test'; SET s3_secret_access_key='test';"
    local duckdb_query="SELECT 'duckdb_smoke_ok' AS status; SELECT 'call_center=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.delta.call_center; SELECT 'my_local_delta_table=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.delta.my_local_delta_table; SELECT 'my_local_nonnull_name=' || CAST(COUNT(name) AS VARCHAR) AS check FROM iceberg_floecat.delta.my_local_delta_table; SELECT 'dv_demo_delta=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.delta.dv_demo_delta; SELECT 'dv_content=' || CAST(MIN(id) AS VARCHAR) || ',' || CAST(MAX(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) AS check FROM iceberg_floecat.delta.dv_demo_delta; SELECT 'empty_join=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.iceberg.trino_types i JOIN iceberg_floecat.delta.call_center d ON 1=0; DROP TABLE IF EXISTS iceberg_floecat.iceberg.duckdb_ctas_smoke; CREATE TABLE iceberg_floecat.iceberg.duckdb_ctas_smoke AS SELECT * FROM iceberg_floecat.delta.call_center LIMIT 5; SELECT 'ctas_count=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.iceberg.duckdb_ctas_smoke; DROP TABLE iceberg_floecat.iceberg.duckdb_ctas_smoke; DROP TABLE IF EXISTS iceberg_floecat.iceberg.duckdb_mutation_smoke; CREATE TABLE iceberg_floecat.iceberg.duckdb_mutation_smoke (id INTEGER, v VARCHAR); SELECT 'mut_after_create=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.iceberg.duckdb_mutation_smoke; INSERT INTO iceberg_floecat.iceberg.duckdb_mutation_smoke VALUES (1, 'a'), (2, 'b'), (3, 'c'); SELECT 'mut_after_insert=' || CAST(COUNT(*) AS VARCHAR) || ',' || CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) AS check FROM iceberg_floecat.iceberg.duckdb_mutation_smoke; DELETE FROM iceberg_floecat.iceberg.duckdb_mutation_smoke WHERE id = 2; SELECT 'mut_after_delete=' || CAST(COUNT(*) AS VARCHAR) || ',' || CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) AS check FROM iceberg_floecat.iceberg.duckdb_mutation_smoke; UPDATE iceberg_floecat.iceberg.duckdb_mutation_smoke SET v = 'c2' WHERE id = 3; SELECT 'mut_after_update=' || CAST(COUNT(*) AS VARCHAR) || ',' || CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) AS check FROM iceberg_floecat.iceberg.duckdb_mutation_smoke;"

    local duckdb_out
    if ! duckdb_out=$(docker run --rm --network "${compose_project}_floecat" duckdb/duckdb:latest \
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
    assert_contains "$label duckdb mutation create" "$duckdb_out" "mut_after_create=0"
    assert_contains "$label duckdb mutation insert" "$duckdb_out" "mut_after_insert=3,6,a,c"
    assert_contains "$label duckdb mutation delete" "$duckdb_out" "mut_after_delete=2,4,a,c"
    assert_contains "$label duckdb mutation update" "$duckdb_out" "mut_after_update=2,4,a,c2"

    local duckdb_snapshots_out
    local duckdb_snapshots_query_fn="COPY (SELECT snapshot_id FROM iceberg_snapshots('iceberg_floecat.iceberg.duckdb_mutation_smoke') ORDER BY timestamp_ms DESC) TO '/dev/stdout' (FORMAT CSV, HEADER FALSE);"
    if ! duckdb_snapshots_out=$(docker run --rm --network "${compose_project}_floecat" duckdb/duckdb:latest \
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
    if ! duckdb_tt_out=$(docker run --rm --network "${compose_project}_floecat" duckdb/duckdb:latest \
      duckdb -c "$duckdb_bootstrap $duckdb_tt_query" 2>&1); then
      echo "$duckdb_tt_out"
      echo "[FAIL] $label duckdb time travel query failed"
      return 1
    fi
    echo "$duckdb_tt_out"
    assert_contains "$label duckdb time travel insert snapshot" "$duckdb_tt_out" "mut_tt_after_insert=3,6,a,c"
    assert_contains "$label duckdb time travel delete snapshot" "$duckdb_tt_out" "mut_tt_after_delete=2,4,a,c"
    assert_contains "$label duckdb time travel update snapshot" "$duckdb_tt_out" "mut_tt_after_update=2,4,a,c2"

    local alter_out
    if alter_out=$(docker run --rm --network "${compose_project}_floecat" duckdb/duckdb:latest \
      duckdb -c "$duckdb_bootstrap ALTER TABLE iceberg_floecat.iceberg.duckdb_mutation_smoke ADD COLUMN note VARCHAR; SELECT 'mut_after_alter=' || CAST(COUNT(*) AS VARCHAR) || ',' || CAST(COUNT(note) AS VARCHAR) AS check FROM iceberg_floecat.iceberg.duckdb_mutation_smoke; DROP TABLE iceberg_floecat.iceberg.duckdb_mutation_smoke;" 2>&1); then
      echo "$alter_out"
      assert_contains "$label duckdb mutation alter" "$alter_out" "mut_after_alter=2,0"
    else
      echo "$alter_out"
      assert_contains "$label duckdb mutation alter unsupported" "$alter_out" "Not implemented Error: Alter Schema Entry"
      docker run --rm --network "${compose_project}_floecat" duckdb/duckdb:latest \
        duckdb -c "$duckdb_bootstrap DROP TABLE IF EXISTS iceberg_floecat.iceberg.duckdb_mutation_smoke;" >/dev/null 2>&1 || true
    fi

    local drop_check_out
    if drop_check_out=$(docker run --rm --network "${compose_project}_floecat" duckdb/duckdb:latest \
      duckdb -c "$duckdb_bootstrap SELECT COUNT(*) FROM iceberg_floecat.iceberg.duckdb_ctas_smoke;" 2>&1); then
      echo "$drop_check_out"
      echo "[FAIL] $label duckdb drop table verification failed (table still queryable)"
      return 1
    else
      assert_contains "$label duckdb drop table verification" "$drop_check_out" "Table with name duckdb_ctas_smoke does not exist"
      echo "[PASS] $label duckdb drop table verification (expected missing-table error after DROP TABLE)"
    fi
  fi

  if [ "$profile" = "localstack" ] && should_run_client trino; then
    echo "==> [SMOKE] trino federation check (localstack)"
    local trino_out
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
drop_ok = False
try:
    run_sql("SELECT COUNT(*) FROM iceberg.trino_ctas_smoke")
except Exception:
    drop_ok = True
print("drop_table_ok=" + ("1" if drop_ok else "0"))
run_sql("DROP TABLE IF EXISTS iceberg.trino_mutation_smoke")
run_sql("CREATE TABLE iceberg.trino_mutation_smoke (id INTEGER, v VARCHAR)")
print(
    scalar(
        "SELECT 'mut_after_create=' || CAST(COUNT(*) AS VARCHAR) "
        "FROM iceberg.trino_mutation_smoke"
    )
)
run_sql("INSERT INTO iceberg.trino_mutation_smoke VALUES (1, 'a'), (2, 'b'), (3, 'c')")
insert_snapshot_id = scalar(
    "SELECT CAST(snapshot_id AS VARCHAR) "
    "FROM iceberg.\"trino_mutation_smoke$snapshots\" ORDER BY committed_at DESC LIMIT 1"
)
print(
    scalar(
        "SELECT 'mut_after_insert=' || CAST(COUNT(*) AS VARCHAR) || ',' || "
        "CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) "
        "FROM iceberg.trino_mutation_smoke"
    )
)
run_sql("DELETE FROM iceberg.trino_mutation_smoke WHERE id = 2")
delete_snapshot_id = scalar(
    "SELECT CAST(snapshot_id AS VARCHAR) "
    "FROM iceberg.\"trino_mutation_smoke$snapshots\" ORDER BY committed_at DESC LIMIT 1"
)
print(
    scalar(
        "SELECT 'mut_after_delete=' || CAST(COUNT(*) AS VARCHAR) || ',' || "
        "CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) "
        "FROM iceberg.trino_mutation_smoke"
    )
)
run_sql("UPDATE iceberg.trino_mutation_smoke SET v = 'c2' WHERE id = 3")
update_snapshot_id = scalar(
    "SELECT CAST(snapshot_id AS VARCHAR) "
    "FROM iceberg.\"trino_mutation_smoke$snapshots\" ORDER BY committed_at DESC LIMIT 1"
)
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
run_sql("DROP TABLE iceberg.trino_mutation_smoke")
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
    assert_contains "$label trino mutation create" "$trino_out" "mut_after_create=0"
    assert_contains "$label trino mutation insert" "$trino_out" "mut_after_insert=3,6,a,c"
    assert_contains "$label trino mutation delete" "$trino_out" "mut_after_delete=2,4,a,c"
    assert_contains "$label trino mutation update" "$trino_out" "mut_after_update=2,4,a,c2"
    assert_contains "$label trino time travel insert snapshot" "$trino_out" "mut_tt_after_insert=3,6,a,c"
    assert_contains "$label trino time travel delete snapshot" "$trino_out" "mut_tt_after_delete=2,4,a,c"
    assert_contains "$label trino time travel update snapshot" "$trino_out" "mut_tt_after_update=2,4,a,c2"
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
