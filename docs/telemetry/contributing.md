# Contributing metrics

Each module that wants to add metrics to the shared registry implements `TelemetryContributor`.
The easiest path is:

- Define the contract explicitly: add a new `MetricId`/`MetricDef` entry in your module’s telemetry holder (`MyModuleTelemetry`); core metrics live in `Telemetry.Metrics`. An empty `allowedTags` set means “allow any tag” (the contract only enforces required tags unless you enumerate the allowlist). Prefer enumerating `allowedTags` to avoid accidental high-cardinality tags in production.
2. Implement `TelemetryContributor` (for example, `MyModuleTelemetryContributor`) and register it via `Telemetry.register(...)` or a framework integration.
3. Provide tests that assert the metric exists in the registry (via `Telemetry.metricCatalog(registry)` or `Telemetry.requireMetricDef(registry, metric)`) and that its `requiredTags` are a subset of `allowedTags`.

```
public final class MyModuleTelemetryContributor implements TelemetryContributor {
  @Override
  public void contribute(TelemetryRegistry registry) {
    MyModuleTelemetry.definitions().values().forEach(registry::register);
  }
}
```

Because `TelemetryRegistry` enforces duplicate names, you can safely register new contributors without colliding with the core catalog.

Core RPC metrics:
- `rpc.requests` counts every gRPC invocation; additional tags (like `status`) differentiate success/failure.
- `rpc.errors` is the dedicated failure counter so dashboards can subscribe to errors directly without filtering `status`.
Timers use a base unit of seconds even if names (e.g., `rpc.latency.ms`) retain the legacy suffix. Backends should interpret the `seconds` unit and convert as needed, but the name helps operators quickly understand the intent.

## Helper families

Prefer using the provided `RpcMetrics`, `StoreMetrics`, `CacheMetrics`, and `GcMetrics` helpers so instrumentation automatically applies the canonical names/tag sets and complies with each metric’s required tags; they wrap the `Observability` API but keep required/allowed tag logic centralized.
