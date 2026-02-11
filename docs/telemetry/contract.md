# Telemetry Hub Contract

Here is a global overview of all metrics defined in the repo right now:

<!-- METRICS:START -->
| Metric | Type | Unit | Required Tags | Allowed Tags |
| --- | --- | --- | --- | --- |
| cache.hits | COUNTER | count | cache, component, operation | account, cache, component, operation, result |
| cache.misses | COUNTER | count | cache, component, operation | account, cache, component, operation, result |
| cache.size | GAUGE | count | cache, component, operation | account, cache, component, operation, result |
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
