# Grafana demo dashboard

This page describes the official Grafana dashboard that demos the Floecat telemetry hub. The dashboard is stored in `examples/telemetry/dashboards/demo-dashboard.json` and expects a Prometheus datasource that scrapes `http://host.docker.internal:9100/q/metrics`. To start the full stack, run `examples/telemetry/demo.sh`.

## Demo flow

The dashboard tells a single story: **signal → trace → log → profile**.

Use the **Component** and **Operation** dropdowns to scope every RPC/storage panel to the workload you are debugging.

1. **Start the service** with profiling enabled:
   ```bash
   QUARKUS_PROFILE=telemetry-otlp,telemetry-prof mvn -pl service -am -DskipTests \
     -Dfloecat.profiling.enabled=true \
     -Dfloecat.profiling.endpoints-enabled=true \
     quarkus:dev
   ```
   Quarkus exposes Micrometer metrics on `http://localhost:9100/q/metrics`.

2. **Start the telemetry stack:**
   ```bash
   cd examples/telemetry && ./demo.sh
   ```
   Brings up Prometheus (9092), Grafana (3001), Tempo (3102), Loki (3101), and the OTLP collector (4317). The dashboard auto-loads via Grafana provisioning.

3. **Generate traffic** to populate the panels:

   `demo.sh` generates a 15-second burst of gRPC calls automatically after the stack is ready (no manual step needed). For continued or manual exploration:
   ```
   make cli-run
   # at the floecat> prompt:
   account 5eaa9cd5-7d08-3750-9457-cfe800b0b9d2
   tables examples.iceberg
   ```

4. **Trigger a profiling capture** — either let a latency policy fire under load, or trigger one explicitly:
   ```bash
   # Automated: starts the stack and triggers a capture in one step
   ./demo.sh --profiling-demo

   # Or manually via REST:
   curl -X POST http://localhost:9100/profiling/captures \
     -H 'Content-Type: application/json' \
     -d '{"trigger":"manual","mode":"jfr","requestedBy":"demo","duration":"PT5S"}'
   ```

5. **Download the JFR artifact and open in JMC:**
   ```bash
   # Replace {id} with the capture ID returned by POST or from GET /profiling/captures/latest
   curl http://localhost:9100/profiling/captures/{id}/artifact -o demo.jfr
   jmc -open demo.jfr
   ```
   In JMC: check **Method Profiling** (hot call stacks), **Allocations**, **GC** (pause patterns), and **Threads** (concurrency bottlenecks). The capture's `traceId` field from `GET /profiling/captures/latest` correlates the JFR directly to the Tempo trace visible on the latency panel.

## Dashboard layout

Start at **Service health** to spot a problem, drill into **RPC** to scope it by operation, then open **Profiling** (collapsed by default) to capture and download a JFR artifact.

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

### Profiling (on-demand & policy)

This row sits immediately below the Service health row so it feels like the natural escalation path. Keep it collapsed by default and open it only when you want to step from the high-level stats into a capture workflow.

- **Captures by trigger/policy** — table/series showing
  ```promql
  sum by (result, trigger, policy)(increase(floecat_profiling_captures_total[5m]))
  ```
  It shows whether captures ran manually or automatically and whether they started/completed/failed/dropped.
- **Capture in flight** — stat panel showing `sum(floecat_profiling_captures_total{result="started"}) - sum(floecat_profiling_captures_total{result="completed"})`, so you immediately see when a recording is active.
- **Latency policy overlay** — add `increase(floecat_profiling_captures_total{trigger="policy"}[1m])` to the RPC latency chart (secondary axis) or its own small panel so captures show up aligned with latency/GC spikes.

Document this row in the quick-start instructions: after you trigger a capture (either manually or via a policy), fetch `/profiling/captures/latest` and share the artifact URL that `latest` supplies.

```
POST /profiling/captures
GET /profiling/captures/latest
```

The `/latest` response lists metadata such as `policy`, `requestedByType`, and `traceId`/`spanId`, which link directly into Tempo traces.

Once the capture finishes, download `/profiling/captures/{id}/artifact` and open it in Java Mission Control or a flamegraph viewer. This lets you close the loop from dashboard spike → trace → JVM-level evidence.

### RPC

- **Requests by operation** — req/s broken down by operation tag.
- **Errors by status** — error rate grouped by gRPC status.
- **Avg latency by operation** — per-operation average latency.
- **Status distribution** — req/s grouped by status code.

#### Trace links

The operation-scoped panels (*Requests by operation*, *Avg latency by operation*) include a **View traces in Tempo** data link. Clicking a series opens Grafana Explore with a TraceQL query that matches the clicked operation:

```traceql
{ resource.service.name = "floecat-service" && span.floecat.operation = "<operation>" }
```

This works because `GrpcTelemetryServerInterceptor` sets the custom span attribute `floecat.operation` to the same simplified name used for the metric `operation` label (e.g. `CatalogService.ListCatalogs`). The *Errors by status* panel links to errored traces (`status = error`) instead.

Standard OTel `rpc.*` attributes (`rpc.service`, `rpc.method`, `rpc.system`) are also present on every gRPC span and can be used for ad-hoc Tempo queries.

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

- **Background task health** — seconds since each scheduled task last completed a tick, broken down per task (`floecat_core_task_last_tick_end_ms`). Each task has its own schedule (idempotency GC: 60s, cas/pointer GC: 10m) — a rising line beyond the expected interval means a task is stalling.
- **GC collections (rate by type)** — GC collection rate grouped by gc type (`floecat_core_gc_collections_total`). Types are `cas`, `pointer`, and `idempotency` — Floecat's application-level GC, not JVM GC.
- **Executor queue depth** — pending task count per executor pool (`floecat_core_exec_queue_depth`). Rising depth alongside rising RPC latency is a saturation signal — a good moment to trigger a profiling capture.

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
