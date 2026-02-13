# Telemetry Dashboard Demo

This page explains how to launch a complete telemetry stack (service, Prometheus, Grafana) and import a sample dashboard so you can see the hub metrics instantly.

## Quick start stack
1. Build and run the service with telemetry enabled:
   ```bash
   mvn -pl service -am -DskipTests quarkus:dev
   ```
   Quarkus exposes metrics via Micrometer at `http://localhost:9100/q/metrics`.

2. In a second shell, launch Prometheus + Grafana via the helper script (which brings up the compose stack and waits for Prometheus to see the target):
   ```bash
   ./demo.sh
   ```
   - Prometheus listens on `http://localhost:9091` and scrapes the service at `http://host.docker.internal:9100/q/metrics`.
   - Grafana is available at `http://localhost:3000` (admin/admin).

3. Configure Grafana:
   - Add a Prometheus datasource pointing to `http://host.docker.internal:9091`.
   - Import the dashboard in `demo-dashboard.json` (Dashboard → Import in Grafana).

4. Observe the panels. They query Prometheus using the default Micrometer name transformations (dots → underscores, counters → `_total`, timers become `_seconds_count`/`_sum`/`_max`). Grafana already uses the correct `_seconds_` variants (see `demo-dashboard.json`).

5. Generate RPC traffic via the Floecat CLI so the panel queries have data:

   ```bash
   cd client-cli
   mvn io.quarkus:quarkus-maven-plugin:3.29.4:dev
   ```
   At the `floecat>` prompt type commands such as `account 5eaa9cd5-7d08-3750-9457-cfe800b0b9d2` and `tables examples.iceberg`. Each invocation issues gRPC requests that produce the `floecat_core_*` metrics the dashboard queries.

## Metric naming guide
- Micrometer renames `floecat.core.rpc.latency` to `floecat_core_rpc_latency_seconds_count`/`_sum`/`_max`. Use `_seconds_` guard when calculating averages:
  ```promql
  rate(floecat_core_rpc_latency_seconds_sum[5m]) / rate(floecat_core_rpc_latency_seconds_count[5m])
  ```
- Counters become `floecat_core_rpc_requests_total` (note `_total`). Add `by(component,status)` if you want breakdowns.
- Service metrics follow the same pattern (`floecat_service_cache_hits_total`, `floecat_service_storage_account_bytes`).
- The Prometheus label `telemetry_contract_version="v1"` corresponds to the contract version tag; use it whenever you need to pin a panel to v1.


## Cleanup
```bash
docker compose down
```

## Notes
- The storage gauges refresh every `floecat.metrics.storage.refresh` (30 s default). Timestamps for GC metrics update once per tick (`floecat.gc.*.tick-every`).
