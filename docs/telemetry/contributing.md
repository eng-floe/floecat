# Contributing metrics

Each module that wants to add metrics to the shared registry implements `TelemetryContributor`.
The easiest path is:

1. Add a new metric to your module’s telemetry holder (e.g., `MyModuleTelemetry`) with `MetricId` and `MetricDef`; core metrics live in `Telemetry.Metrics`.
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
