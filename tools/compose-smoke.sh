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

cleanup_mode() {
  local env_file="$1"
  local profile="$2"
  local mode_env="FLOECAT_ENV_FILE=$env_file COMPOSE_PROFILES=$profile"
  eval "$mode_env $DOCKER_COMPOSE_MAIN down --remove-orphans -v" >/dev/null 2>&1 || true
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

  local mode_env="FLOECAT_ENV_FILE=$env_file COMPOSE_PROFILES=$profile"
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
    eval "$compose_cmd logs --no-color --tail=300" || true
    cleanup_mode "$env_file" "$profile"
  }
  trap on_mode_error ERR

  echo "==> [SMOKE] mode=$label"
  cleanup_mode "$env_file" "$profile"

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
  echo "$out_iceberg" | grep -q "account set:"
  echo "$out_iceberg" | grep -q "table id:"

  local out_delta
  out_delta=$(printf "account t-0001\nresolve table examples.delta.call_center\nquit\n" | eval "$compose_cmd run --rm -T cli")
  echo "$out_delta"
  echo "$out_delta" | grep -q "account set:"
  echo "$out_delta" | grep -q "table id:"

  cleanup_mode "$env_file" "$profile"
  trap - ERR
}

echo "==> [COMPOSE] smoke (inmem + localstack + localstack-oidc)"
run_mode ./env.inmem "" inmem ""
run_mode ./env.localstack localstack localstack "localstack"
run_mode ./env.localstack-oidc localstack-oidc localstack-oidc "localstack keycloak" "keycloak" "8080"
