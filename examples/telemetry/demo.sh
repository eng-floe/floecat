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
    if curl -fsS "$url" >/dev/null 2>&1; then
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
    if targets=$(curl -fsS "$targets_url" 2>/dev/null); then
      # Minimal JSON grep: good enough for demo, avoids jq dependency.
      if echo "$targets" | grep -q '"health"\s*:\s*"up"' && echo "$targets" | grep -q '"job"\s*:\s*"'"$job"'"'; then
        return 0
      fi
    fi
    info "(${i}/${attempts}) waiting for Prometheus target '${job}' to become UP ..."
    sleep "$sleep_s"
  done
  return 1
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
info "  TEMPO_HTTP_PORT=${TEMPO_HTTP_PORT}"
info "  TEMPO_GRPC_PORT=${TEMPO_GRPC_PORT}"
info "  LOKI_PORT=${LOKI_PORT}"
info "  OTLP_GRPC_PORT=${OTLP_GRPC_PORT} (derived from ${OTLP_ENDPOINT})"
info "  OTLP_HTTP_PORT=${OTLP_HTTP_PORT}"
info "Service log path: ${SERVICE_LOG_PATH}"

# ---- run stack ---------------------------------------------------------------
info "Starting the telemetry stack (Prometheus, Grafana, OTLP collector, Tempo, Loki)..."
"${COMPOSE_ARR[@]}" up -d

# Basic health signal: containers present
if ! "${COMPOSE_ARR[@]}" ps >/dev/null 2>&1; then
  warn "Compose stack started, but 'compose ps' failed — check Docker logs if something looks off."
fi

info "Waiting for Prometheus to scrape the service (/q/metrics on port ${SERVICE_HTTP_PORT})..."
if wait_for_prom_target_up "${PROMETHEUS_TARGETS_URL}" "floecat-service" 12 5; then
  info "Prometheus target is UP."
else
  warn "Prometheus target never reached UP state. Check ${PROMETHEUS_URL}/targets for details."
fi

cat <<MSG
The telemetry stack is running (Prometheus + Grafana + OTLP collector + Tempo + Loki):
  • Service metrics: ${SERVICE_METRICS_URL}
  • Prometheus UI: ${PROMETHEUS_URL}
  • Grafana UI: ${GRAFANA_URL} (admin/admin)
  • Tempo trace explorer: ${TEMPO_URL}
  • Loki log browser: ${LOKI_URL}

Notes:
  • Grafana is pre-provisioned with Prometheus/Tempo/Loki datasources from examples/telemetry/provisioning.
  • The dashboard uses the DS_PROMETHEUS variable (currently '${DS_PROMETHEUS}').
  • Run the service with QUARKUS_PROFILE=telemetry-otlp so it exports spans/logs to the collector.

Generate traffic:
  make cli-run
  then at the floecat> prompt execute:
    account 5eaa9cd5-7d08-3750-9457-cfe800b0b9d2
    tables examples.iceberg

Clean up:
  ${COMPOSE_PRETTY} down
MSG
