# Telemetry Hub Contract

Here is a global overview of all metrics defined in the repo right now:

<!-- METRICS:START -->
| Metric | Type | Unit | Required Tags | Allowed Tags |
| --- | --- | --- | --- | --- |
| cache.hits | COUNTER | count | cache, component, operation | account, cache, component, operation, result |
| cache.misses | COUNTER | count | cache, component, operation | account, cache, component, operation, result |
| cache.size | GAUGE | count | cache, component, operation | account, cache, component, operation, result |
| floecat.service.cache.accounts | GAUGE | count |  |  |
| floecat.service.cache.enabled | GAUGE | count |  |  |
| floecat.service.cache.entries | GAUGE | count |  |  |
| floecat.service.cache.load.latency | TIMER | seconds |  | result |
| floecat.service.cache.max.size | GAUGE | count |  |  |
| floecat.service.gc.cas.enabled | GAUGE | count |  |  |
| floecat.service.gc.cas.last.tick.end.ms | GAUGE | milliseconds |  |  |
| floecat.service.gc.cas.last.tick.start.ms | GAUGE | milliseconds |  |  |
| floecat.service.gc.cas.running | GAUGE | count |  |  |
| floecat.service.gc.idempotency.enabled | GAUGE | count |  |  |
| floecat.service.gc.idempotency.last.tick.end.ms | GAUGE | milliseconds |  |  |
| floecat.service.gc.idempotency.last.tick.start.ms | GAUGE | milliseconds |  |  |
| floecat.service.gc.idempotency.running | GAUGE | count |  |  |
| floecat.service.gc.pointer.enabled | GAUGE | count |  |  |
| floecat.service.gc.pointer.last.tick.end.ms | GAUGE | milliseconds |  |  |
| floecat.service.gc.pointer.last.tick.start.ms | GAUGE | milliseconds |  |  |
| floecat.service.gc.pointer.running | GAUGE | count |  |  |
| floecat.service.hint.cache.hits | COUNTER | count |  |  |
| floecat.service.hint.cache.misses | COUNTER | count |  |  |
| floecat.service.hint.cache.weight | GAUGE | bytes |  |  |
| floecat.service.storage.account.bytes | GAUGE | bytes | account | account |
| floecat.service.storage.account.pointers | GAUGE | count | account | account |
| gc.collections | COUNTER | count | component, gc, operation | component, exception, gc, operation, result |
| gc.pause | TIMER | seconds | component, gc, operation | component, exception, gc, operation, result |
| observability.dropped.tags.total | COUNTER | count |  |  |
| rpc.active | GAUGE | count | component, operation | component, operation |
| rpc.errors | COUNTER | count | component, operation, result | account, component, exception, operation, result, status |
| rpc.latency | TIMER | seconds | component, operation, result | account, component, exception, operation, result, status |
| rpc.requests | COUNTER | count | account, component, operation, status | account, component, operation, status |
| rpc.retries | COUNTER | count | component, operation | component, operation |
| store.bytes | COUNTER | bytes | component, operation, result | account, component, exception, operation, result, status |
| store.latency | TIMER | seconds | component, operation, result | account, component, exception, operation, result, status |
| store.requests | COUNTER | count | component, operation, result | account, component, exception, operation, result, status |
<!-- METRICS:END -->
