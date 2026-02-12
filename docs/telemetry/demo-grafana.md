# Grafana demo dashboard

This page describes the official Grafana dashboard that demos the Floecat telemetry hub. The dashboard is stored in `telemetry/demo-dashboard.json` and expects a Prometheus datasource that scrapes `http://host.docker.internal:9100/q/metrics`.

## Quick start

Use the **Component** and **Operation** dropdowns to scope every RPC/storage panel to the workload you are debugging.

1. Start the Floecat service with telemetry enabled:
   ```bash
   mvn -pl service -am -DskipTests quarkus:dev
   ```
   Quarkus exposes Micrometer metrics on `http://localhost:9100/q/metrics`.
2. Start Prometheus/Grafana (you can reuse `telemetry/demo.sh` if the folder exists or run Prometheus + Grafana locally) and point it at `http://host.docker.internal:9100/q/metrics`.
3. Import `telemetry/demo-dashboard.json` into Grafana.
   - When Grafana asks for the Prometheus datasource, pick whichever name you created; the dashboard uses `${DS_PROMETHEUS}` so it works with any datasource name.
4. Drive some traffic through the CLI (`floecat> account <id>`, `floecat> tables examples.iceberg`) to populate the panels.

## Dashboard layout

The dashboard has a quick-start markdown panel at the top (useful when you ship the JSON to teammates) and the following telemetry-focused panels arranged in a grid:

- **RPC Active** (`floecat_core_rpc_active`) — in-flight RPC counts broken down by component/operation.
- **RPC Latency (avg)** (`rate(floecat_core_rpc_latency_seconds_sum[5m]) / rate(floecat_core_rpc_latency_seconds_count[5m])`) — average latency per RPC.
- **RPC Throughput** (`rate(floecat_core_rpc_requests_total[1m])`) — requests-per-second, useful to spot spikes.

These storage panels honor the same **Component** / **Operation** templates that drive the RPC graphs, so you can slice both RPC and storage traffic by the same workload tags.
- **Storage Throughput** (`rate(floecat_core_store_requests_total[1m])` vs `rate(floecat_core_store_bytes_total[1m])`) — compares refresh call volume and bytes transferred for the selected component/operation set.
- **Storage Latency (avg)** (`rate(floecat_core_store_latency_seconds_sum[5m]) / rate(floecat_core_store_latency_seconds_count[5m])`) — surfaces slow storage jobs for the chosen tags.
- **Dropped tags** (`floecat_core_observability_dropped_tags_total`) — stat panel highlighting how many telemetry tags were dropped in lenient mode.
- **Storage Pointers (per account)** (`floecat_service_storage_account_pointers`) — shows pointer counts per account tag value.
- **Cache health** (`floecat_service_cache_enabled`, `floecat_service_cache_entries`, `floecat_service_cache_max_size`) — ensures the graph cache is enabled and sized as expected.
- **GC activity** (`floecat_service_gc_pointer_running`, `floecat_service_gc_idempotency_running`, `floecat_service_gc_cas_running`) — quick visibility into whether the GC workers are active.

Every metric includes `telemetry_contract_version="v1"`, so the panels inherently focus on the current contract version. Use this dashboard as the first screen when debugging RPC/Storage regressions.

## Notes

- Micrometer translates the logical contract names (`floecat.core.*`, `floecat.service.*`) into Prometheus-friendly names (`floecat_core_*` and `floecat_service_*`). Use `_seconds_count/_sum` for timers and `_total` for counters (the dashboard already references these variants).
- If you ship your own Grafana instance, you can re-export the JSON and share it inside your team so everyone gets the same overview.
