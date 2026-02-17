# Telemetry Hub Contract

This lists all metrics available in the repository right now:

<!-- METRICS:START -->
**Core Metrics**
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
| floecat.core.gc.collections | COUNTER |  | v1 | Number of GC collections per GC type. | component, gc, operation, result | component, exception, gc, operation, result |
| floecat.core.gc.errors | COUNTER |  | v1 | GC failures per GC type. | component, gc, operation, result | component, exception, gc, operation, result |
| floecat.core.gc.pause | TIMER | seconds | v1 | GC pause time per GC type. | component, gc, operation, result | component, exception, gc, operation, result |
| floecat.core.gc.retries | COUNTER |  | v1 | GC retries per component/operation. | component, operation | component, operation |
| floecat.core.observability.dropped.tags.total | COUNTER |  | v1 | Total number of tags dropped because they violated telemetry contracts. |  |  |
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

**Service Metrics**
| Metric | Type | Unit | Since | Description | Required Tags | Allowed Tags |
| --- | --- | --- | --- | --- | --- | --- |
| floecat.service.storage.account.bytes | GAUGE | bytes | v1 | Estimated per-account storage byte consumption (sampled, not exact). | account | account |
| floecat.service.storage.account.pointers | GAUGE |  | v1 | Per-account pointer count stored in the service. | account | account |

<!-- METRICS:END -->
