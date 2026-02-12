# Telemetry Hub Contract

This lists all metrics available in the repository right now:

<!-- METRICS:START -->
**Core Metrics**
| Metric | Type | Unit | Since | Description | Required Tags | Allowed Tags |
| --- | --- | --- | --- | --- | --- | --- |
| floecat.core.cache.hits | COUNTER |  | v1 | Cache hits count for the graph cache. | cache, component, operation | account, cache, component, operation, result |
| floecat.core.cache.misses | COUNTER |  | v1 | Cache misses count for the graph cache. | cache, component, operation | account, cache, component, operation, result |
| floecat.core.cache.size | GAUGE |  | v1 | Approximate graph cache size by cache name. | cache, component, operation | account, cache, component, operation, result |
| floecat.core.gc.collections | COUNTER |  | v1 | Number of GC collections per GC type. | component, gc, operation | component, exception, gc, operation, result |
| floecat.core.gc.pause | TIMER | seconds | v1 | GC pause time per GC type. | component, gc, operation | component, exception, gc, operation, result |
| floecat.core.observability.dropped.tags.total | COUNTER |  | v1 | Total number of tags dropped because they violated telemetry contracts. |  |  |
| floecat.core.rpc.active | GAUGE |  | v1 | Number of in-flight RPCs per component/operation. | component, operation | component, operation |
| floecat.core.rpc.errors | COUNTER |  | v1 | Count of RPC failures per component/operation. | component, operation, result | account, component, exception, operation, result, status |
| floecat.core.rpc.latency | TIMER | seconds | v1 | Latency distribution for RPC operations. | component, operation, result | account, component, exception, operation, result, status |
| floecat.core.rpc.requests | COUNTER |  | v1 | Total RPC requests processed, tagged by account and status. | account, component, operation, status | account, component, operation, status |
| floecat.core.rpc.retries | COUNTER |  | v1 | Number of RPC retries invoked. | component, operation | component, operation |
| floecat.core.store.bytes | COUNTER | bytes | v1 | Count of bytes processed by store operations. | component, operation, result | account, component, exception, operation, result, status |
| floecat.core.store.latency | TIMER | seconds | v1 | Store operation latency distribution. | component, operation, result | account, component, exception, operation, result, status |
| floecat.core.store.requests | COUNTER |  | v1 | Number of store requests emitted per component/operation. | component, operation, result | account, component, exception, operation, result, status |

**Service Metrics**
| Metric | Type | Unit | Since | Description | Required Tags | Allowed Tags |
| --- | --- | --- | --- | --- | --- | --- |
| floecat.service.cache.accounts | GAUGE |  | v1 | Number of accounts with active caches. |  |  |
| floecat.service.cache.enabled | GAUGE |  | v1 | Indicator that the graph cache is enabled. |  |  |
| floecat.service.cache.entries | GAUGE |  | v1 | Estimated total graph cache entries across accounts. |  |  |
| floecat.service.cache.load.latency | TIMER | seconds | v1 | Latency for loading graph entries when caching is enabled. |  |  |
| floecat.service.cache.max.size | GAUGE |  | v1 | Configured max entries for the graph cache. |  |  |
| floecat.service.gc.cas.enabled | GAUGE |  | v1 | Indicator that the CAS GC scheduler is enabled (1=enabled, 0=disabled). |  |  |
| floecat.service.gc.cas.last.tick.end.ms | GAUGE | milliseconds | v1 | Timestamp when the CAS GC last finished. |  |  |
| floecat.service.gc.cas.last.tick.start.ms | GAUGE | milliseconds | v1 | Timestamp when the CAS GC last started. |  |  |
| floecat.service.gc.cas.running | GAUGE |  | v1 | Indicator that the CAS GC tick is currently active (1 when running, 0 when idle). |  |  |
| floecat.service.gc.idempotency.enabled | GAUGE |  | v1 | Indicator that the idempotency GC scheduler is enabled (1=enabled, 0=disabled). |  |  |
| floecat.service.gc.idempotency.last.tick.end.ms | GAUGE | milliseconds | v1 | Timestamp when the idempotency GC last finished. |  |  |
| floecat.service.gc.idempotency.last.tick.start.ms | GAUGE | milliseconds | v1 | Timestamp when the idempotency GC last started. |  |  |
| floecat.service.gc.idempotency.running | GAUGE |  | v1 | Indicator that the idempotency GC tick is currently active (1 when running, 0 when idle). |  |  |
| floecat.service.gc.pointer.enabled | GAUGE |  | v1 | Indicator that the pointer GC scheduler is enabled (1=enabled, 0=disabled). |  |  |
| floecat.service.gc.pointer.last.tick.end.ms | GAUGE | milliseconds | v1 | Timestamp when the pointer GC last finished. |  |  |
| floecat.service.gc.pointer.last.tick.start.ms | GAUGE | milliseconds | v1 | Timestamp when the pointer GC last started (ms since epoch from the most recent tick). Tick interval is controlled via \`floecat.gc.pointer.tick-every\`. |  |  |
| floecat.service.gc.pointer.running | GAUGE |  | v1 | Indicator that the pointer GC tick is currently active (1 when running, 0 when idle). |  |  |
| floecat.service.hint.cache.hits | COUNTER |  | v1 | Engine hint cache hits. |  |  |
| floecat.service.hint.cache.misses | COUNTER |  | v1 | Engine hint cache misses. |  |  |
| floecat.service.hint.cache.weight | GAUGE | bytes | v1 | Estimated weight of the hint cache. |  |  |
| floecat.service.storage.account.bytes | GAUGE | bytes | v1 | Per-account byte consumption for storage. | account | account |
| floecat.service.storage.account.pointers | GAUGE |  | v1 | Per-account pointer count stored in the service. | account | account |

<!-- METRICS:END -->
