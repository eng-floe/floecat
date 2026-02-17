# Grafana demo dashboard

This page describes the official Grafana dashboard that demos the Floecat telemetry hub. The dashboard is stored in `examples/telemetry/demo-grafana/demo-dashboard.json` and expects a Prometheus datasource that scrapes `http://host.docker.internal:9100/q/metrics`.

## Quick start

Use the **Component** and **Operation** dropdowns to scope every RPC/storage panel to the workload you are debugging.

1. Start the Floecat service with telemetry enabled:
   ```bash
   mvn -pl service -am -DskipTests quarkus:dev
   ```
   Quarkus exposes Micrometer metrics on `http://localhost:9100/q/metrics`.
2. Start Prometheus/Grafana (you can reuse `telemetry/demo.sh` if the folder exists or run Prometheus + Grafana locally) and point it at `http://host.docker.internal:9100/q/metrics`.
3. Import `demo-dashboard.json` into Grafana.
   - When Grafana asks for the Prometheus datasource, pick whichever name you created; the dashboard uses `${DS_PROMETHEUS}` so it works with any datasource name.
4. Drive some traffic through the CLI (`floecat> account <id>`, `floecat> tables examples.iceberg`) to populate the panels.

## Dashboard layout

The dashboard has a quick-start markdown panel at the top and six sections:

### Service health (top row)

Seven stat panels for at-a-glance triage:

- **Req/s** (`rate(floecat_core_rpc_requests_total)`) — aggregate RPC throughput.
- **Avg latency (ms)** — average RPC latency across all operations.
- **Error %** — ratio of RPC errors to total requests.
- **Active RPC** (`floecat_core_rpc_active`) — in-flight RPC count.
- **Cache saturation** — cache entries as a percentage of max entries.
- **Heap used** — JVM heap live data vs max capacity.
- **Dropped tags** (`floecat_core_observability_dropped_tags_total`) — telemetry contract violations. Non-zero means a metric was emitted with disallowed or missing tags.

### RPC

- **Requests by operation** — req/s broken down by operation tag.
- **Errors by status** — error rate grouped by gRPC status.
- **Avg latency by operation** — per-operation average latency.
- **Status distribution** — req/s grouped by status code.

### Storage

- **Store requests (req/s)** — request rate by operation.
- **Store throughput (bytes/s)** — byte throughput via `floecat_core_store_bytes_total`.
- **Store latency (ms)** — per-operation average latency.
- **Store errors** — error rate.
- **Per-account storage** — table showing `floecat_service_storage_account_bytes` per account.

### Cache

- **Cache entries** — entry count by cache name (`floecat_core_cache_entries`).
- **Cache weight (bytes)** — weighted size for Caffeine caches (`floecat_core_cache_weighted_size_bytes`).
- **Cache hit rate (%)** — `hits / (hits + misses)` by cache name.
- **Cache enabled** — per-cache enabled/disabled status via `floecat_core_cache_enabled`.

### Background tasks

- **Tasks running** — count of active scheduled task ticks (`floecat_core_task_running`).
- **Last task tick age** — seconds since the most recent task tick ended (`floecat_core_task_last_tick_end_ms`).
- **GC collections (rate by type)** — GC collection rate grouped by gc type (`floecat_core_gc_collections_total`).

### System (JVM / host) — collapsed by default

- **CPU usage** — system + process CPU.
- **Load avg (1m)** — 1-minute load average.
- **Threads** — live and daemon thread counts.
- **Open files** — process file descriptor count.

## Template variables

| Variable | Type | Purpose |
| --- | --- | --- |
| `DS_PROMETHEUS` | datasource | Prometheus datasource selector |
| `rate_interval` | interval | Rate window (30s, 1m, 5m) |
| `component` | query | Filter by component tag |
| `operation` | query | Filter by operation tag (scoped to component) |

Every metric includes `telemetry_contract_version="v1"`, so the panels inherently focus on the current contract version. Use this dashboard as the first screen when debugging RPC/Storage regressions.

## Notes

- Micrometer translates the logical contract names (`floecat.core.*`, `floecat.service.*`) into Prometheus-friendly names (`floecat_core_*` and `floecat_service_*`). Use `_seconds_count/_sum` for timers and `_total` for counters (the dashboard already references these variants).
- If you ship your own Grafana instance, you can re-export the JSON and share it inside your team so everyone gets the same overview.
