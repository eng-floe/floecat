#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "$0")" && pwd)
cd "$ROOT"

PROMETHEUS_URL=http://localhost:9091
echo "Checking Floecat service on http://localhost:9100/q/metrics..."
if ! curl -sf http://localhost:9100/q/metrics >/dev/null; then
  echo "ERROR: Floecat service is not running on http://localhost:9100/q/metrics" >&2
  exit 1
fi

echo "Starting the telemetry stack (Prometheus + Grafana)..."
docker compose up -d

echo "Waiting for Prometheus to scrape the service (/q/metrics on port 9100)..."
for i in {1..12}; do
  targets=$(curl -s http://localhost:9091/api/v1/targets)
  if echo "$targets" | grep -q '"health":"up"' && echo "$targets" | grep -q '"job":"floecat-service"'; then
    echo "Prometheus target is UP."
    break
  fi
  echo "  (${i}/12) waiting for target to become UP..."
  sleep 5
done

cat <<'MSG'
The telemetry stack is running:
  • Service metrics: http://localhost:9100/q/metrics
  • Prometheus UI: ${PROMETHEUS_URL}
  • Grafana UI: http://localhost:3000 (admin/admin)
Import telemetry/demo-dashboard.json into Grafana (Create → Import) and point it at the Prometheus datasource.
Use the URL above when you configure the datasource (e.g. `http://host.docker.internal:9091`).

Want immediate metrics data? Run one of the CLI commands below while the stack is running:
  make run-cli
  # then at the floecat> prompt execute `account 5eaa9cd5-7d08-3750-9457-cfe800b0b9d2` followed by `tables examples.iceberg`.
  
Once done, don't forget to clean up : `docker compose down`
MSG
