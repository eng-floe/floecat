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
IFS=$'\n\t'

# -----------------------------------------------------------------------------
# Floecat telemetry demo launcher
#
# Starts: Prometheus + Grafana + Tempo + Loki + OTEL Collector (Docker Compose)
# Expects: Floecat service already running so Prometheus can scrape /q/metrics.
# -----------------------------------------------------------------------------

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${ROOT}/../.." && pwd)
cd "$ROOT"

# ---- logging helpers ---------------------------------------------------------
info()  { echo "[demo] $*"; }
warn()  { echo "[demo] WARN: $*" >&2; }

die()   { echo "[demo] ERROR: $*" >&2; exit 1; }

step()  {
  echo
  echo "[demo] ──────────────────────────────────────────────────────────────"
  echo "[demo]  $*"
  echo "[demo] ──────────────────────────────────────────────────────────────"
}

# ---- load generation config --------------------------------------------------
# Account seeded by SeedRunner (used by generate_load_burst)
: "${DEMO_ACCOUNT_ID:=5eaa9cd5-7d08-3750-9457-cfe800b0b9d2}"
# How long to run the load burst (seconds)
: "${DEMO_LOAD_DURATION_S:=15}"

# Concurrency for load generation (number of concurrent CLI workers)
: "${DEMO_LOAD_CONCURRENCY:=4}"

# Delay between CLI iterations per worker (seconds, can be fractional)
: "${DEMO_LOAD_SLEEP_S:=0}"

# When using the single-session CLI load generator, how many times each worker repeats
# the command mix (catalogs + information_schema + table get). Defaults to ~2x duration.
: "${DEMO_CLI_REPEATS_PER_WORKER:=}"

# Centralized curl defaults so demo never hangs on network/Docker issues.
CURL_CONNECT_TIMEOUT=${CURL_CONNECT_TIMEOUT:-2}
CURL_MAX_TIME=${CURL_MAX_TIME:-5}

curl_quiet() {
  # shellcheck disable=SC2086
  curl -fsS --connect-timeout "${CURL_CONNECT_TIMEOUT}" --max-time "${CURL_MAX_TIME}" "$@"
}

curl_json() {
  curl_quiet -H 'Accept: application/json' "$@"
}

# ---- command helpers ---------------------------------------------------------
require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

compose_cmd() {
  # Prefer Docker Compose v2 (docker compose), fall back to v1 (docker-compose)
  if docker compose version >/dev/null 2>&1; then
    COMPOSE_ARR=(docker compose)
    return 0
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_ARR=(docker-compose)
    return 0
  fi
  die "Neither 'docker compose' nor 'docker-compose' is available"
}

wait_for_http_ok() {
  local url=$1
  local attempts=${2:-12}
  local sleep_s=${3:-5}
  local i
  for i in $(seq 1 "$attempts"); do
    if curl_quiet "$url" >/dev/null 2>&1; then
      return 0
    fi
    info "(${i}/${attempts}) waiting for ${url} ..."
    sleep "$sleep_s"
  done
  return 1
}

wait_for_prom_target_up() {
  local targets_url=$1
  local job=${2:-floecat-service}
  local attempts=${3:-12}
  local sleep_s=${4:-5}
  local i

  for i in $(seq 1 "$attempts"); do
    local targets
    # Time-bound the call so we never hang if Prometheus/Docker networking is flaky.
    if targets=$(curl_json "$targets_url" 2>/dev/null); then
      # Prometheus targets payload includes "activeTargets" objects with "labels":{"job":...} and "health":"up".
      # Minimal grep/awk parsing: good enough for demo, avoids jq dependency.
      if echo "$targets" | grep -q '"health"\s*:\s*"up"' \
        && echo "$targets" | grep -q '"job"\s*:\s*"'"$job"'"'; then
        return 0
      fi
    fi
    info "(${i}/${attempts}) waiting for Prometheus target '${job}' to become UP ..."
    sleep "$sleep_s"
  done
  return 1
}

# ---- load generation helpers -------------------------------------------------

find_java_bin() {
  # Returns the path to a JDK 21+ java binary.
  # The CLI JAR is compiled for class file version 65+ (JDK 21+), so an older
  # JDK will fail at startup.  We prefer JAVA_HOME, then the Homebrew JDK path,
  # then fall back to whatever 'java' resolves to.
  local candidate ver
  for candidate in \
      "${JAVA_HOME:+$JAVA_HOME/bin/java}" \
      "/opt/homebrew/opt/openjdk/bin/java" \
      "$(command -v java 2>/dev/null)"; do
    [[ -n "$candidate" && -x "$candidate" ]] || continue
    # Extract the major version number (e.g. "25" from "openjdk version \"25.0.2\"")
    ver=$("$candidate" -version 2>&1 | head -1 | grep -oE '[0-9]+' | head -1)
    if [[ -n "$ver" ]] && (( ver >= 21 )); then
      echo "$candidate"
      return 0
    fi
  done
  # Last resort: trust whatever is on PATH
  echo "java"
}

generate_load_burst() {
  # Runs a load burst using the CLI for the given number of seconds, issuing:
  #   - account (once per worker)
  #   - catalogs
  #   - tables examples.information_schema
  #   - table get examples.iceberg.orders
  # The catalogs/information_schema/table get commands are repeated in a loop.
  # Designed to be called in the foreground to populate the dashboard with live data.
  local duration_s=${1:-10}
  local java_bin cli_jar
  java_bin=$(find_java_bin)
  cli_jar="$REPO_ROOT/client-cli/target/quarkus-app/quarkus-run.jar"

  if [[ ! -f "$cli_jar" ]]; then
    warn "CLI JAR not found at $cli_jar — skipping load generation"
    warn "  Build it first: make cli"
    return 0
  fi

  info "Load burst: ${duration_s}s of gRPC traffic (Catalogs · information_schema · table get)"

  local concurrency=${DEMO_LOAD_CONCURRENCY:-4}
  local sleep_s=${DEMO_LOAD_SLEEP_S:-0}

  # Each worker runs a single CLI session for the whole burst.
  # That means `account ...` runs once, and we then issue commands repeatedly.
  # We shard the Quarkus HTTP port so multiple CLI instances can run concurrently.
  local pids=()
  local i
  for i in $(seq 1 "$concurrency"); do
    (
      local port=$(( 18080 + i ))

      # Build a CLI script: select account once, then repeat a small command mix.
      # We approximate "duration" by repeating the block N times; this keeps the demo
      # deterministic while producing more varied gRPC traffic.
      local repeats=${DEMO_CLI_REPEATS_PER_WORKER:-$(( duration_s * 20 ))}
      if (( repeats < 1 )); then repeats=1; fi

      if [[ "$sleep_s" != "0" && -n "$sleep_s" ]]; then
        warn "DEMO_LOAD_SLEEP_S=${sleep_s} is ignored in single-session load mode (account runs once)."
      fi

      local script
      script=$(mktemp)
      {
        printf "account %s\n" "$DEMO_ACCOUNT_ID"
        local r
        for r in $(seq 1 "$repeats"); do
          # Occasionally refresh catalog + namespace listings (nice for dashboards)
          if (( r % 10 == 1 )); then
            printf "catalogs\n"
            printf "namespaces examples\n"
            printf "tables examples.iceberg\n"
          fi

          # Rotate across multiple tables to reduce cache-hit bias.
          # Keep information_schema reads (good for “catalog explorer” style demos)
          # and add a second iceberg table to diversify call stacks.
          case $(( (r - 1) % 5 )) in
            0) printf "table get examples.iceberg.orders\n" ;;
            1) printf "table get examples.iceberg.lineitem\n" ;;
            2) printf "table get examples.information_schema.tables\n" ;;
            3) printf "table get examples.information_schema.columns\n" ;;
            4) printf "table get examples.information_schema.schemata\n" ;;
          esac

          # Heavier calls (run only on real Iceberg tables). These tend to exercise
          # more CPU/allocations than cached table-get alone.
          if (( r % 5 == 0 )); then
            printf "snapshots examples.iceberg.orders\n"
            printf "stats files examples.iceberg.orders --limit 50\n"
            printf "stats columns examples.iceberg.orders --limit 200\n"
          fi
          if (( r % 7 == 0 )); then
            printf "snapshots examples.iceberg.lineitem\n"
            printf "stats files examples.iceberg.lineitem --limit 50\n"
            printf "stats columns examples.iceberg.lineitem --limit 200\n"
          fi
        done
        printf "quit\n"
      } >"$script"

      "$java_bin" -Dquarkus.http.port="$port" -jar "$cli_jar" <"$script" \
        >/dev/null 2>&1 || true

      rm -f "$script" 2>/dev/null || true
      echo "$repeats" >"${TMPDIR:-/tmp}/floecat-demo-load-${$}-${i}.count" 2>/dev/null || true
    ) &
    pids+=("$!")
  done

  # Wait for all workers to finish.
  local total=0
  for i in "${!pids[@]}"; do
    wait "${pids[$i]}" 2>/dev/null || true
    local cfile="${TMPDIR:-/tmp}/floecat-demo-load-${$}-$((i+1)).count"
    if [[ -f "$cfile" ]]; then
      local c
      c=$(cat "$cfile" 2>/dev/null || echo 0)
      rm -f "$cfile" 2>/dev/null || true
      if [[ "$c" =~ ^[0-9]+$ ]]; then
        total=$(( total + c ))
      fi
    fi
  done

  info "Load burst complete: ${total} CLI command-block repeats (concurrency=${concurrency})"
}

# ---- configuration -----------------------------------------------------------
# Optional env file for stable port defaults
if [[ -f "$ROOT/demo.env" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT/demo.env"
fi

: "${PROMETHEUS_PORT:=9092}"
: "${GRAFANA_PORT:=3001}"
: "${TEMPO_HTTP_PORT:=3102}"
: "${TEMPO_GRPC_PORT:=9095}"
: "${LOKI_PORT:=3101}"
: "${OTLP_HTTP_PORT:=4318}"
: "${DS_PROMETHEUS:=prometheus}"

export PROMETHEUS_PORT GRAFANA_PORT TEMPO_HTTP_PORT TEMPO_GRPC_PORT LOKI_PORT OTLP_HTTP_PORT DS_PROMETHEUS

PROP_FILE="$REPO_ROOT/service/src/main/resources/application.properties"
[[ -f "$PROP_FILE" ]] || die "Cannot find Quarkus properties at: $PROP_FILE"

# Reads the last 'key=...' match from application.properties.
# Also supports simple Quarkus-style env substitution: ${ENV_VAR:default}
get_property() {
  local key=$1
  local default=$2
  local line

  line=$(grep -E "^\s*${key}\s*=" "$PROP_FILE" | tail -n1 || true)
  if [[ -z "${line}" ]]; then
    printf '%s' "$default"
    return
  fi

  line=${line#*=}
  line=${line//\"/}
  line=${line//$'\r'/}
  line=${line%%#*}
  # trim leading/trailing whitespace
  line=$(echo "$line" | sed -e 's/^\s\+//' -e 's/\s\+$//')

  if [[ "$line" =~ ^\$\{([^:}]+):([^}]+)\}$ ]]; then
    local envvar=${BASH_REMATCH[1]}
    local fallback=${BASH_REMATCH[2]}
    printf '%s' "${!envvar:-$fallback}"
  else
    printf '%s' "$line"
  fi
}

SERVICE_HTTP_PORT=$(get_property "quarkus.http.port" "9100")
SERVICE_HOST_RAW=$(get_property "quarkus.http.host" "localhost")
SERVICE_HOST=$SERVICE_HOST_RAW
if [[ "$SERVICE_HOST_RAW" == "0.0.0.0" || -z "$SERVICE_HOST_RAW" ]]; then
  SERVICE_HOST="localhost"
fi

SERVICE_METRICS_URL="http://${SERVICE_HOST}:${SERVICE_HTTP_PORT}/q/metrics"
PROMETHEUS_URL="http://localhost:${PROMETHEUS_PORT}"
PROMETHEUS_TARGETS_URL="${PROMETHEUS_URL}/api/v1/targets"
GRAFANA_URL="http://localhost:${GRAFANA_PORT}"
TEMPO_URL="http://localhost:${TEMPO_HTTP_PORT}"
LOKI_URL="http://localhost:${LOKI_PORT}"

OTLP_ENDPOINT=$(get_property "quarkus.otel.exporter.otlp.endpoint" "http://localhost:4317")
# Extract port from OTLP endpoint (works for http(s)://host:port and host:port)
OTLP_GRPC_PORT=$(echo "$OTLP_ENDPOINT" | sed -E 's|^[a-zA-Z]+://||' | awk -F: '{print $NF}' | tr -d '/')
export OTLP_GRPC_PORT

# Derive the service log directory so the collector's filelog receiver keeps working
# even if the Quarkus log path changes.
LOG_FILE_KEY="quarkus.log.file.path"
LOG_FILE_LINE=$(grep -E "^\s*${LOG_FILE_KEY}\s*=" "$PROP_FILE" | tail -n1 || true)
LOG_FILE_VALUE=${LOG_FILE_LINE#*=}
LOG_FILE_VALUE=${LOG_FILE_VALUE:-log/floecat-service.log}
LOG_FILE_VALUE=${LOG_FILE_VALUE//\"/}
LOG_FILE_VALUE=${LOG_FILE_VALUE//$'\r'/}
LOG_FILE_VALUE=${LOG_FILE_VALUE%%#*}
LOG_FILE_VALUE=$(echo "$LOG_FILE_VALUE" | sed -e 's/^\s\+//' -e 's/\s\+$//')

LOG_DIR=$(dirname "$LOG_FILE_VALUE")
if [[ "$LOG_FILE_VALUE" == /* ]]; then
  SERVICE_LOG_DIR=$LOG_DIR
else
  SERVICE_LOG_DIR="$REPO_ROOT/service/$LOG_DIR"
fi

# If the directory doesn't exist yet (fresh checkout), don't hard-fail;
# the collector will still run and will start tailing once the service creates the file.
if [[ -d "$SERVICE_LOG_DIR" ]]; then
  SERVICE_LOG_DIR=$(cd "$SERVICE_LOG_DIR" && pwd)
else
  warn "Service log directory does not exist yet: $SERVICE_LOG_DIR"
fi

SERVICE_LOG_FILE=${LOG_FILE_VALUE##*/}
SERVICE_LOG_PATH="$SERVICE_LOG_DIR/$SERVICE_LOG_FILE"
export SERVICE_LOG_PATH SERVICE_LOG_FILE SERVICE_LOG_DIR

# ---- preflight ---------------------------------------------------------------
require_cmd docker
require_cmd curl
require_cmd grep
require_cmd awk
require_cmd sed
require_cmd seq

compose_cmd
if [[ ${#COMPOSE_ARR[@]} -eq 0 ]]; then
  die "Failed to resolve docker compose command"
fi
# Print without being affected by global IFS (we set IFS=$'\n\t' above)
COMPOSE_PRETTY="${COMPOSE_ARR[0]}"
if [[ ${#COMPOSE_ARR[@]} -gt 1 ]]; then
  COMPOSE_PRETTY+=" ${COMPOSE_ARR[1]}"
fi
info "Using compose command: ${COMPOSE_PRETTY}"

# Render collector config from template if available
if [[ -f "$ROOT/config/otel-collector-config.yaml.template" ]]; then
  if command -v envsubst >/dev/null 2>&1; then
    envsubst < "$ROOT/config/otel-collector-config.yaml.template" > "$ROOT/config/otel-collector-config.yaml"
  else
    warn "envsubst not found; cannot render otel-collector-config.yaml from template"
    warn "Install gettext (envsubst) or pre-generate $ROOT/config/otel-collector-config.yaml"
  fi
fi

info "Checking Floecat service metrics endpoint: ${SERVICE_METRICS_URL}"
if ! wait_for_http_ok "${SERVICE_METRICS_URL}" 3 1; then
  die "Floecat service is not reachable at ${SERVICE_METRICS_URL}. Start it first (e.g., QUARKUS_PROFILE=telemetry-otlp mvn -pl service -am quarkus:dev)."
fi

info "Ports configured for demo stack:"
info "  PROMETHEUS_PORT=${PROMETHEUS_PORT}"
info "  GRAFANA_PORT=${GRAFANA_PORT}"
info "Load generation:"
info "  DEMO_LOAD_DURATION_S=${DEMO_LOAD_DURATION_S}"
info "  DEMO_LOAD_CONCURRENCY=${DEMO_LOAD_CONCURRENCY}"
info "  DEMO_LOAD_SLEEP_S=${DEMO_LOAD_SLEEP_S}"
info "  TEMPO_HTTP_PORT=${TEMPO_HTTP_PORT}"
info "  TEMPO_GRPC_PORT=${TEMPO_GRPC_PORT}"
info "  LOKI_PORT=${LOKI_PORT}"
info "  OTLP_GRPC_PORT=${OTLP_GRPC_PORT} (derived from ${OTLP_ENDPOINT})"
info "  OTLP_HTTP_PORT=${OTLP_HTTP_PORT}"
info "Service log path: ${SERVICE_LOG_PATH}"

# ---- run stack ---------------------------------------------------------------
step "STEP 1/3 · Starting telemetry stack (Prometheus · Grafana · OTLP · Tempo · Loki)"
info "Starting the telemetry stack (Prometheus, Grafana, OTLP collector, Tempo, Loki)..."
"${COMPOSE_ARR[@]}" up -d --remove-orphans >/dev/null 2>&1

# Basic health signal: containers present (never stream logs / spam output).
# We only check that at least one container id is reported.
RUNNING_IDS=$(("${COMPOSE_ARR[@]}" ps -q 2>/dev/null || true) | tr -d '\r')
if [[ -z "${RUNNING_IDS}" ]]; then
  warn "Compose stack started, but no containers detected — check Docker Desktop / docker logs if something looks off."
fi

PROM_WAIT_ATTEMPTS=12
PROM_WAIT_SLEEP=5

step "STEP 2/3 · Waiting for Prometheus to scrape the service"
info "Waiting for Prometheus to scrape the service (/q/metrics on port ${SERVICE_HTTP_PORT})..."
if wait_for_prom_target_up "${PROMETHEUS_TARGETS_URL}" "floecat-service" "${PROM_WAIT_ATTEMPTS}" "${PROM_WAIT_SLEEP}"; then
  info "Prometheus target is UP."
else
  warn "Prometheus target never reached UP state. Check ${PROMETHEUS_URL}/targets for details."
fi

step "STEP 3/3 · Telemetry stack ready"

# Generate a burst of gRPC traffic so the Grafana dashboard has live data
# (RPC latency, request rate, etc.) the moment you open it.
generate_load_burst "${DEMO_LOAD_DURATION_S}"

echo "The telemetry stack is running (Prometheus + Grafana + OTLP collector + Tempo + Loki):"
echo "  • Service metrics: ${SERVICE_METRICS_URL}"
echo "  • Prometheus UI: ${PROMETHEUS_URL}"
echo "  • Grafana UI: ${GRAFANA_URL} (admin/admin)"
echo "    → Open the 'Floecat demo' dashboard. Start at Service health (latency, errors),"
echo "      then drill into RPC (per-operation)."
echo "  • Tempo trace explorer: ${TEMPO_URL}"
echo "  • Loki log browser: ${LOKI_URL}"
echo

echo "Notes:"
echo "  • Grafana is pre-provisioned with Prometheus/Tempo/Loki datasources from examples/telemetry/provisioning."
echo "  • The dashboard uses the DS_PROMETHEUS variable (currently '${DS_PROMETHEUS}')."
echo "  • Run the service with QUARKUS_PROFILE=telemetry-otlp so it exports spans/logs to the collector."
echo

echo "Clean up:"
echo "  ${COMPOSE_PRETTY} down"
echo

exit 0
