# Telemetry Hub Contract

This lists all metrics available in the repository right now:

<!-- METRICS:START -->
## Core Metrics

| Metric | Type | Unit | Since | Description | Required Tags | Allowed Tags |
| --- | --- | --- | --- | --- | --- | --- |
| floecat.core.cache.accounts | GAUGE | count | v1 | Number of accounts with an active cache entry, tagged by cache name. | cache, component, operation | account, cache, component, operation |
| floecat.core.cache.enabled | GAUGE |  | v1 | Indicator that the cache is enabled (1=enabled, 0=disabled). | cache, component, operation | account, cache, component, operation |
| floecat.core.cache.entries | GAUGE | count | v1 | Approximate number of entries in the cache, tagged by cache name. | cache, component, operation | account, cache, component, operation |
| floecat.core.cache.errors | COUNTER |  | v1 | Number of cache operation failures (load errors), tagged by cache name. | cache, component, operation, result | account, cache, component, exception, operation, result |
| floecat.core.cache.hits | COUNTER |  | v1 | Number of cache lookup hits, tagged by cache name. | cache, component, operation | account, cache, component, operation |
| floecat.core.cache.latency | TIMER | seconds | v1 | Cache latency distribution for operations. | cache, component, operation, result | account, cache, component, exception, operation, result |
| floecat.core.cache.max.entries | GAUGE | count | v1 | Configured max entries for the cache. | cache, component, operation | account, cache, component, operation |
| floecat.core.cache.max.weight.bytes | GAUGE | bytes | v1 | Configured maximum weight (bytes) for the cache. | cache, component, operation | account, cache, component, operation |
| floecat.core.cache.misses | COUNTER |  | v1 | Number of cache lookup misses, tagged by cache name. | cache, component, operation | account, cache, component, operation |
| floecat.core.cache.weighted.size.bytes | GAUGE | bytes | v1 | Total weight (bytes) of cache entries, tagged by cache name. | cache, component, operation | account, cache, component, operation |
| floecat.core.exec.active | GAUGE | count | v1 | Number of threads actively executing tasks per pool. | component, operation, pool | component, operation, pool |
| floecat.core.exec.queue.depth | GAUGE | count | v1 | Number of work items waiting in the executor queue per pool. | component, operation, pool | component, operation, pool |
| floecat.core.exec.rejected | COUNTER |  | v1 | Number of task submissions rejected by the executor. | component, operation, pool | component, operation, pool |
| floecat.core.exec.task.run | TIMER | seconds | v1 | Duration spent running the task on a worker thread. | component, operation, pool | component, operation, pool, result |
| floecat.core.exec.task.wait | TIMER | seconds | v1 | Duration spent waiting in the queue before execution starts. | component, operation, pool | component, operation, pool, result |
| floecat.core.gc.collections | COUNTER |  | v1 | Number of GC collections per GC type. | component, gc, operation, result | component, exception, gc, operation, result |
| floecat.core.gc.errors | COUNTER |  | v1 | GC failures per GC type. | component, gc, operation, result | component, exception, gc, operation, result |
| floecat.core.gc.pause | TIMER | seconds | v1 | GC pause time per GC type. | component, gc, operation, result | component, exception, gc, operation, result |
| floecat.core.gc.retries | COUNTER |  | v1 | GC retries per component/operation. | component, operation | component, operation |
| floecat.core.observability.dropped.metric.total | COUNTER |  | v1 | Total number of metric emissions rejected because validation failed. |  | reason |
| floecat.core.observability.dropped.tags.total | COUNTER |  | v1 | Total number of tags dropped because they violated telemetry contracts. |  |  |
| floecat.core.observability.duplicate.gauge.total | COUNTER |  | v1 | Count of duplicate gauge registration attempts. |  | reason |
| floecat.core.observability.invalid.metric.total | COUNTER |  | v1 | Total number of metrics rejected because they were not registered. |  | reason |
| floecat.core.observability.registry.size | GAUGE | count | v1 | Current size of the telemetry registry. |  |  |
| floecat.core.rpc.active | GAUGE |  | v1 | Number of in-flight RPCs per component/operation. | component, operation | component, operation |
| floecat.core.rpc.errors | COUNTER |  | v1 | Count of RPC failures per component/operation. | component, operation, result | account, component, exception, operation, result, status |
| floecat.core.rpc.latency | TIMER | seconds | v1 | Latency distribution for RPC operations. | component, operation, result | account, component, exception, operation, result, status |
| floecat.core.rpc.requests | COUNTER |  | v1 | Total RPC requests processed, tagged by account and status. | account, component, operation, status | account, component, operation, status |
| floecat.core.rpc.retries | COUNTER |  | v1 | Number of RPC retries invoked. | component, operation | component, operation |
| floecat.core.store.bytes | COUNTER | bytes | v1 | Count of bytes processed by store operations. | component, operation, result | account, component, exception, operation, result |
| floecat.core.store.errors | COUNTER |  | v1 | Store failure count per component/operation. | component, operation, result | account, component, exception, operation, result |
| floecat.core.store.latency | TIMER | seconds | v1 | Store operation latency distribution. | component, operation, result | account, component, exception, operation, result |
| floecat.core.store.requests | COUNTER |  | v1 | Number of store requests emitted per component/operation. | component, operation, result | account, component, exception, operation, result |
| floecat.core.store.retries | COUNTER |  | v1 | Store retries per component/operation. | component, operation | component, operation |
| floecat.core.task.enabled | GAUGE |  | v1 | Indicator that a scheduled task is enabled (1=enabled, 0=disabled). | component, operation, task | account, component, operation, task |
| floecat.core.task.last.tick.end.ms | GAUGE | milliseconds | v1 | Timestamp (ms since epoch) when the scheduled task last finished a tick. | component, operation, task | account, component, operation, task |
| floecat.core.task.last.tick.start.ms | GAUGE | milliseconds | v1 | Timestamp (ms since epoch) when the scheduled task last started a tick. | component, operation, task | account, component, operation, task |
| floecat.core.task.running | GAUGE |  | v1 | Number of active ticks for the scheduled task (usually 0 or 1). | component, operation, task | account, component, operation, task |

## Extra JVM Metrics

| Metric | Type | Unit | Since | Description | Required Tags | Allowed Tags |
| --- | --- | --- | --- | --- | --- | --- |
| floecat.jvm.gc.live.data.bytes | GAUGE | bytes | v1 | Estimated live data (bytes) held by each garbage collector. | component, gc, operation | component, gc, operation |
| floecat.jvm.gc.live.data.growth.rate | GAUGE | bytes_per_second | v1 | Live data growth rate (bytes/second) for GC-managed pools. | component, gc, operation | component, gc, operation |

## Profiling Metrics

| Metric | Type | Unit | Since | Description | Required Tags | Allowed Tags |
| --- | --- | --- | --- | --- | --- | --- |
| floecat.profiling.captures.total | COUNTER |  | v1 | Profiling capture lifecycle counts (started/completed/failed/dropped). | component, mode, operation, result, scope, trigger | component, mode, operation, policy, reason, result, scope, trigger |

## Service Metrics

| Metric | Type | Unit | Since | Description | Required Tags | Allowed Tags |
| --- | --- | --- | --- | --- | --- | --- |
| floecat.service.storage.account.bytes | GAUGE | bytes | v1 | Estimated per-account storage byte consumption (sampled, not exact). | account | account |
| floecat.service.storage.account.pointers | GAUGE |  | v1 | Per-account pointer count stored in the service. | account | account |

<!-- METRICS:END -->

## Standard JVM metrics

Quarkus enables the standard Micrometer JVM binders (`JvmMemoryMetrics`, `JvmThreadMetrics`, `JvmGcMetrics`, `ProcessorMetrics`, etc.) by default, so the `jvm.*`, `process_cpu_usage`, `system_cpu_usage`, and related runtime gauges are still emitted alongside Floecat’s custom GC live-data metrics. Refer to the OpenTelemetry JVM runtime semantic conventions for the full list of names and tags: https://opentelemetry.io/docs/specs/semconv/runtime/jvm-metrics/.


## Correlation contract

The hub expects metrics, traces, and logs to share a small, predictable key set so dashboards can link between systems:

- **Spans** carry `floecat.component` and `floecat.operation` (plus `floecat.rpc.status` for RPC spans and `floecat.store.operation` for storage observations) so Tempo queries can reuse the same component/operation filters as the Prometheus panels.
- **Logs** expose `floecat_component` and `floecat_operation` via MDC (and can emit `traceId`/`spanId` if your JSON logging pipeline is configured to capture OpenTelemetry context). That keeps Loki’s **Logs for this trace** button and Tempo’s trace-to-log links usable even when you jump directly from a metric graph, while making it clear the trace identifiers only show up when your logging stack surfaces them.
- **Metrics** retain their canonical `component`/`operation` tags from the catalog, and the Micrometer backend logs the current trace/span IDs at `TRACE` level while it records latency or error counters so you can correlate back to the active span.
- **Telemetry contract version** (`telemetry.contract.version`) travels with every meter (and can be mirrored into spans/logs or log labels) so dashboards know which catalog version produced a series.

Adhering to this contract means a slow RPC bucket in Grafana can open Tempo with the matching span, jump from the span to Loki logs, and still point back to the same metric tags.
