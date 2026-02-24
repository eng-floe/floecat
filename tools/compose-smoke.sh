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
  local log_dir="${COMPOSE_SMOKE_SAVE_LOG_DIR:-}"

  if [ -z "$log_dir" ]; then
    return 0
  fi

  mkdir -p "$log_dir" || true
  eval "$compose_cmd ps" > "$log_dir/${label}-ps.log" 2>&1 || true
  eval "$compose_cmd logs --no-color" > "$log_dir/${label}-compose.log" 2>&1 || true
  eval "$compose_cmd logs --no-color service" > "$log_dir/${label}-service.log" 2>&1 || true
  eval "$compose_cmd logs --no-color iceberg-rest" > "$log_dir/${label}-iceberg-rest.log" 2>&1 || true
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
    cleanup_mode "$compose_cmd"
  }
  trap on_mode_error ERR

  on_mode_return() {
    cleanup_mode "$compose_cmd"
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
      exit 1
    fi
    if [ "$i" -eq 180 ]; then
      echo "Service seed completion timed out" >&2
      eval "$compose_cmd logs --no-color"
      exit 1
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

  if [ "$profile" = "localstack" ]; then
    local call_center_key="call_center/20250825_183517_00001_s25in_55937b16-9009-4a18-81ea-5a83e97eca53"
    local aws_cli="docker run --rm --network ${compose_project}_floecat -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test -e AWS_DEFAULT_REGION=us-east-1 amazon/aws-cli:2.17.50"
    echo "==> [SMOKE] verify seeded S3 object (localstack)"
    if ! $aws_cli --endpoint-url http://localstack:4566 s3api head-object --bucket floecat-delta --key "$call_center_key" >/dev/null 2>&1; then
      echo "[FAIL] $label missing seeded object s3://floecat-delta/$call_center_key"
      echo "==> [SMOKE][DIAG] localstack s3 ls floecat-delta/call_center/"
      $aws_cli --endpoint-url http://localstack:4566 s3 ls s3://floecat-delta/call_center/ --recursive || true
      echo "==> [SMOKE][DIAG] localstack s3 ls floecat-delta/_delta_log/"
      $aws_cli --endpoint-url http://localstack:4566 s3 ls s3://floecat-delta/call_center/_delta_log/ --recursive || true
      echo "==> [SMOKE][DIAG] localstack s3 ls floecat-delta/ (recursive, first 200)"
      $aws_cli --endpoint-url http://localstack:4566 s3 ls s3://floecat-delta/ --recursive | sed -n '1,200p' || true
      echo "==> [SMOKE][DIAG] service seeding log lines"
      eval "$compose_cmd logs --no-color --tail=1200 service" \
        | grep -E "SeedRunner|TestS3Fixtures|Startup seeding|Seeding|fixture|floecat\\.fixture|floecat\\.seed" || true
      echo "==> [SMOKE][DIAG] service log tail"
      eval "$compose_cmd logs --no-color --tail=300 service" || true
      echo "==> [SMOKE][DIAG] iceberg-rest log tail"
      eval "$compose_cmd logs --no-color --tail=200 iceberg-rest" || true
      save_mode_logs "$compose_cmd" "${label}-fail"
      return 1
    fi

    echo "==> [SMOKE] duckdb federation check (localstack)"
    local duckdb_bootstrap="INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws; INSTALL iceberg; LOAD iceberg; CREATE OR REPLACE SECRET smoke_localstack_s3 (TYPE S3, PROVIDER config, KEY_ID 'test', SECRET 'test', REGION 'us-east-1', ENDPOINT 'localstack:4566', URL_STYLE 'path', USE_SSL false); SET s3_endpoint='localstack:4566'; SET s3_use_ssl=false; SET s3_url_style='path'; SET s3_region='us-east-1'; SET s3_access_key_id='test'; SET s3_secret_access_key='test'; ATTACH 'examples' AS iceberg_floecat (TYPE iceberg, ENDPOINT 'http://iceberg-rest:9200/', AUTHORIZATION_TYPE none, ACCESS_DELEGATION_MODE 'none'); SET s3_endpoint='localstack:4566'; SET s3_use_ssl=false; SET s3_url_style='path'; SET s3_region='us-east-1'; SET s3_access_key_id='test'; SET s3_secret_access_key='test';"
    local duckdb_query="SELECT 'duckdb_smoke_ok' AS status; SELECT 'call_center=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.delta.call_center; SELECT 'my_local_delta_table=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.delta.my_local_delta_table; SELECT 'my_local_nonnull_name=' || CAST(COUNT(name) AS VARCHAR) AS check FROM iceberg_floecat.delta.my_local_delta_table; SELECT 'dv_demo_delta=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.delta.dv_demo_delta; SELECT 'dv_content=' || CAST(MIN(id) AS VARCHAR) || ',' || CAST(MAX(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) AS check FROM iceberg_floecat.delta.dv_demo_delta; SELECT 'empty_join=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.iceberg.trino_types i JOIN iceberg_floecat.delta.call_center d ON 1=0; DROP TABLE IF EXISTS iceberg_floecat.iceberg.duckdb_mutation_smoke; CREATE TABLE iceberg_floecat.iceberg.duckdb_mutation_smoke (id INTEGER, v VARCHAR); SELECT 'mut_after_create=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.iceberg.duckdb_mutation_smoke; INSERT INTO iceberg_floecat.iceberg.duckdb_mutation_smoke VALUES (1, 'a'), (2, 'b'), (3, 'c'); SELECT 'mut_after_insert=' || CAST(COUNT(*) AS VARCHAR) || ',' || CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) AS check FROM iceberg_floecat.iceberg.duckdb_mutation_smoke; DELETE FROM iceberg_floecat.iceberg.duckdb_mutation_smoke WHERE id = 2; SELECT 'mut_after_delete=' || CAST(COUNT(*) AS VARCHAR) || ',' || CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) AS check FROM iceberg_floecat.iceberg.duckdb_mutation_smoke; UPDATE iceberg_floecat.iceberg.duckdb_mutation_smoke SET v = 'c2' WHERE id = 3; SELECT 'mut_after_update=' || CAST(COUNT(*) AS VARCHAR) || ',' || CAST(SUM(id) AS VARCHAR) || ',' || MIN(v) || ',' || MAX(v) AS check FROM iceberg_floecat.iceberg.duckdb_mutation_smoke;"

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
    assert_contains "$label duckdb mutation create" "$duckdb_out" "mut_after_create=0"
    assert_contains "$label duckdb mutation insert" "$duckdb_out" "mut_after_insert=3,6,a,c"
    assert_contains "$label duckdb mutation delete" "$duckdb_out" "mut_after_delete=2,4,a,c"
    assert_contains "$label duckdb mutation update" "$duckdb_out" "mut_after_update=2,4,a,c2"

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
  fi

  trap - ERR
  trap - RETURN
  save_mode_logs "$compose_cmd" "$label"
  cleanup_mode "$compose_cmd"
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
