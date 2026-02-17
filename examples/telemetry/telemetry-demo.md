# Telemetry Dashboard Demo

This page explains how to launch the full observability stack (Prometheus, Grafana, Tempo, Loki, OTLP collector) and see metrics, traces, and logs from the Floecat service.

## Quick start

1. **Start the service** with the `telemetry-otlp` profile so it exports spans to the collector:
   ```bash
   QUARKUS_PROFILE=telemetry-otlp mvn -pl service -am -DskipTests quarkus:dev
   ```
   Quarkus exposes Micrometer metrics at `http://localhost:9100/q/metrics`.

2. **Launch the telemetry stack** from this folder:
   ```bash
   ./demo.sh
   ```
   The script brings up Prometheus, Grafana, Tempo, Loki, and the OTLP collector, then waits for Prometheus to scrape the service.

   | Component        | URL                            | Notes                            |
   |------------------|--------------------------------|----------------------------------|
   | Service metrics  | http://localhost:9100/q/metrics | Micrometer / Prometheus scrape   |
   | Prometheus       | http://localhost:9092           | Metric queries                   |
   | Grafana          | http://localhost:3001           | admin / admin                    |
   | Tempo            | http://localhost:3102           | Trace search                     |
   | Loki             | http://localhost:3101           | Log search                       |
   | OTLP collector   | localhost:4317 (gRPC)          | Receives spans from the service  |

3. **Generate traffic** via the CLI:
   ```bash
   make cli-run
   ```
   At the `floecat>` prompt run: `account 5eaa9cd5-7d08-3750-9457-cfe800b0b9d2` then `tables examples.iceberg`.

4. **Explore in Grafana** (`http://localhost:3001`):
   - **Dashboard** — *Dashboards → Floecat → Floecat Telemetry Hub — Demo*. The RPC panels (*Requests by operation*, *Avg latency by operation*) have **View traces in Tempo** data links — click a series to jump to matching traces.
   - **Traces** — *Explore → Tempo*. Search by `Service Name = floecat-service`. Each trace shows `floecat.component` and `floecat.operation` span attributes. Click **Logs for this trace** (automatic via the provisioned `tracesToLogsV2` config) to see correlated Loki logs.
   - **Logs** — *Explore → Loki*. Query `{service_name="floecat-service"}` to browse service logs.

## How it works

```
Service ──OTLP/gRPC──▶ Collector ──▶ Tempo   (traces)
   │                      │
   │                      ├──▶ Loki    (logs via filelog + OTLP)
   │                      │
   └──/q/metrics──▶ Prometheus         (metrics)
                      │
                      ▼
                   Grafana  (dashboards, Explore, trace↔log links)
```

- **Traces**: The Quarkus OTel integration creates spans for every gRPC call. The `GrpcTelemetryServerInterceptor` adds `floecat.component` and `floecat.operation` span attributes. The `ServiceTelemetryInterceptor` mirrors these into MDC for log correlation (`floecat_component`, `floecat_operation`, `traceId`, `spanId`).
- **Logs**: The OTLP collector ingests service log files via the `filelog` receiver and forwards them to Loki via its native OTLP endpoint.
- **Metrics**: Prometheus scrapes the Micrometer endpoint. The dashboard uses `${DS_PROMETHEUS}` so any Prometheus datasource name works.

## Configuration

The `telemetry-otlp` Quarkus profile (defined in `service/src/main/resources/application.properties`) overrides the OTLP exporter endpoint and timeout. The exporter (`cdi`) is always built-in; the profile simply points it at `localhost:4317` where the collector listens. Without the profile, exports go to the same endpoint but silently fail since no collector is running (the warnings are suppressed in logging config).

Key properties:
```properties
# Always on — exporter is build-time, endpoint is runtime
quarkus.otel.enabled=true
quarkus.otel.traces.enabled=true
quarkus.otel.exporter.otlp.endpoint=http://localhost:4317

# telemetry-otlp profile overrides
%telemetry-otlp.telemetry.exporters=prometheus,otlp
%telemetry-otlp.quarkus.otel.exporter.otlp.timeout=10s
```

## Metric naming guide

- Micrometer renames `floecat.core.rpc.latency` → `floecat_core_rpc_latency_seconds_count`/`_sum`/`_max`. Use `_seconds_` when calculating averages:
  ```promql
  rate(floecat_core_rpc_latency_seconds_sum[5m]) / rate(floecat_core_rpc_latency_seconds_count[5m])
  ```
- Counters become `floecat_core_rpc_requests_total` (note `_total`).
- The Prometheus label `telemetry_contract_version="v1"` pins all metrics to the current contract version.

## Cleanup

```bash
docker compose down
```
