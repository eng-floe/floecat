# Telemetry Hub Overview

The telemetry hub lives under `telemetry-hub/` and provides:

- `telemetry-hub-core`: the framework-neutral contract (metric IDs, registry, helpers, validators).
- `telemetry-hub-backend-micrometer`: the Micrometer backend that enforces `STRICT` vs `LENIENT` policies.
- `telemetry-hub-integration-grpc`: interceptors/helpers that reuse the core API to instrument gRPC.
- `telemetry-hub-tool-docgen`: generates the canonical `docs/telemetry/contract.*` artifacts from the registry.

Use this overview to understand how metrics are named, tagged, versioned, contributed, and documented. The contract tables in `docs/telemetry/contract.md`/`contract.json`, the docgen regression test, and the demo dashboards all derive from the same registry, so keep those artifacts in sync with the ideas outlined below.

## Core naming & units

All built-in metrics start with `floecat.core.` so dashboards and tooling can easily filter hub-supplied data. Service-specific metrics follow `floecat.service.*` (and any future origins should use `floecat.<origin>.*`) so it remains obvious where each metric comes from and there are no collisions between module contracts. For example, `floecat.core.rpc.requests` counts RPC requests while `floecat.service.gc.pointer.running` tracks the service pointer GC. Cache instrumentation now exposes the full set of `floecat.core.cache.*` gauges and counters (configuration knobs, account counts, entry counts, weighted size, hits/misses, latency, errors) so operators can monitor any cache with the same metric names.

- Counters and gauges that represent raw counts omit a unit (`unit = ""`); note that empty string is intentional (not `null`) and backends may still treat those metrics as dimensionless counts if they need to pick a base. For timers or byte counters, declare the appropriate unit (`seconds`, `bytes`).
- Each metric has a `since` version (currently `v1`). Bump that value if you rename, retype, or otherwise break the public contract.
- Every core metric also declares its `origin` (`core`, `service`, etc.). The doc generator groups metrics by origin so consumers can focus on the subsystem they care about.

## Tags: required vs. allowed

The contract differentiates:

- **Required tags** – callers **must** provide these tags when emitting the metric. They are enforced even in lenient mode.
- **Allowed tags** – the only extra tags permitted beyond the required set. An empty allowed set means “no extra tags” unless the contributor explicitly allows more.

This two-set approach keeps cardinality in check while giving you control over which tags can vary. Prefer enumerating allowed tags if you expect optional attributes (account, status, result) because that prevents unbounded cardinality (e.g., user IDs or request IDs). When you need a completely open tag set, omit both required and allowed tags, and the validator will only enforce the metric type. Strict mode is especially useful during development because it fails fast when a caller accidentally emits a disallowed or high-cardinality tag.

### Result/Status conventions

 - `result` (used in RPC scopes, store/GC helpers) conveys the logical outcome and expects values such as `success`, `error`, `retry`, `unknown`, etc. Keep the values lowercase and stable; `unknown` is reserved for lenient mode or when `ObservationScope.close()` runs without an explicit `success()` or `error()` so dashboards can distinguish missing outcomes.
 - `status` (typically the gRPC code name like `OK`, `INVALID_ARGUMENT`, `UNKNOWN`) maps to transport-layer status codes. Tags are case-sensitive (Micrometer doesn’t normalize them), so we standardize on uppercase gRPC names throughout the helpers.
 - `result` is meant for business outcomes, while `status` captures the transport-level code; both help you filter dashboards more precisely. The hub enforces these tag keys via required/allowed tag sets so reports stay consistent across modules.

## Breaking changes & Versioning

Metric stability is critical for dashboards. The `since` column on each `MetricId` identifies when the metric entered the contract. When you change a metric’s name, type, units, or required tags, update the `since` value and regenerate the documentation so downstream consumers know a new version exists. The service exposes `telemetry.contract.version` (default `v1`) as a configuration property that the Quarkus Micrometer backend adds as a common tag on every meter, so Prometheus/OTLP collectors can filter or group by the catalog that produced the data. OTLP resource attributes are configured separately through the OpenTelemetry configuration if you want a dedicated resource field.

In addition to metrics, service spans now include `floecat.component` and `floecat.operation` attributes (matching the measurement dimensions). RPC spans also set `floecat.rpc.status` to the gRPC status name. Storage observations emit child spans with a `floecat.store.operation` attribute so latency/throughput links land on the correct store trace. Logs can expose those values as `floecat_component`/`floecat_operation` MDC keys, along with `traceId`/`spanId`, whenever Quarkus JSON logging (default `log-format`) writes them under the `mdc` field—Loki can then derive fields for Tempo’s **Logs for this trace** button and jump-to-trace/log links stay reliable.

The new `floecat.jvm.*` gauges capture JVM saturation signals (process CPU, heap/non-heap/direct memory, live thread count, and GC live data plus growth rate) through the same component/operation tags so dashboards can pivot from RPC/store/cache panels to runtime pressure without learning another naming scheme.

Executor timers (`floecat.core.exec.task.wait`/`task.run`) currently come from the Mutiny default executor wrapper; the Vert.x pool instrumentation still only backs the queue-depth/active/rejected gauges until future work hooks task submissions running through those pools.

## Correlation contract

Every metric-emitting scope, span, and log entry participates in a small correlation contract:

- **Span attributes** – `floecat.component`, `floecat.operation`, and (for RPCs) `floecat.rpc.status` appear on every span so a trace explorer can filter down to the exact RPC/store cache operation tied to a metric series.
- **Log fields** – the service mirrors `floecat_component` and `floecat_operation` into MDC, and with Quarkus JSON logging (default `log-format`) those values show up under the `mdc` field along with any `traceId`/`spanId` that your OpenTelemetry pipeline emits. Keeping that field lets Tempo’s **Logs for this trace** and Loki queries stay usable even when you jump directly from a metric graph.
- **Metric tags** – component/operation tags on timers/counters link to their span equivalents, and the Micrometer backend also logs the current trace/span IDs at `TRACE` level so dashboards that surface the logs can still surface the identifiers.
- **Telemetry contract version** – every meter carries `telemetry.contract.version` so you can distinguish `v1` data from future contract revisions; spans/logs should surface the same version via attributes or MDC if you rely on multiple catalog versions in the same cluster.

Keeping these keys consistent lets Grafana/Tempo/Loki dashboards present a seamless “metric spike → trace → log” workflow without chasing per-module naming quirks.

## Profiling capture metadata

Policy-driven captures now emit richer metadata so dashboards can explain why a recording exists:

- `requestedBy` describes the actor (e.g., `cli`, `policy/latency_threshold`) that asked for the capture.
- `requestedByType` distinguishes manual actors (`manual`) from automated policies (`policy`).
- `policyName` and `policySignal` record the specific monitor that triggered the capture, and the metric `policy` tag mirrors that value so you can filter the `floecat.profiling.captures.total` counter right in Prometheus/Grafana.

Keeping those fields synced with the REST API plus the `policy` tag lets dashboards surface a “jump to profile” link annotated with the exact latency/queue/GC signal that fired the capture.

## Strict vs Lenient mode

The Micrometer backend supports two policies:

- **STRICT** – contract violations (missing required tags, disallowed tags, duplicate non-canonical tag keys, missing success/error before closing an observation) throw immediate exceptions so developers catch telemetry errors early.
- **LENIENT** – invalid tags are dropped, and the hub increments `floecat.core.observability.dropped.tags.total`. Measurements are still emitted with the remaining tags, but missing **required** tags drop the emission entirely (required tags are always enforced). Lenient mode keeps production workloads moving while still counting telemetry mistakes.

Use strict mode in local/dev/test profiles (`telemetry.strict=true`) to fail fast; lenient mode (`telemetry.strict=false`) is the default for prod exports.

## Helper families and instrumentation

The hub ships several helper classes whose job is to translate your telemetry intent into the contract:

- **`RpcMetrics`** powers gRPC + RPC-level instrumentation. It maintains the active request gauge (`floecat.core.rpc.active`), observes latency/error/retry metrics, normalizes component/operation tags, and exposes `observe(...)` and `recordRequest(...)` helpers so interceptors can focus on status parsing instead of meter plumbing.
- **`CacheMetrics`** (and its derivatives like `GraphCacheManager` helpers) expose canonical cache gauges/counters such as hits, misses, size, and load latency. They automatically register the necessary meters, enforce the required tags, and forward every emission through the hub’s `Observability` so contract validation and strict/lenient policies apply.
- **`StoreMetrics`** wraps storage layer counters/timers (`bytes`, `requests`, `latency`) with the right tag set (`component`, `operation`, `result`, `status`). Callers record bytes or durations and the helper ensures every emission matches the `floecat.core.store.*` definitions.
- **`GcMetrics`** handles scheduler health (enabled, running state, last tick timestamps) for pointer, CAS, and idempotency collectors. Each helper knows its `component`/`operation` pair and tags results/exceptions consistently.
- **Scheduler-driven gauges** – the GC schedulers (`floecat.service.gc.*`) and storage refresher expose gauges that update when those schedulers run. The GC metrics (`enabled`, `running`, `last.tick.start.ms`, `last.tick.end.ms`) reflect the most recent tick and remain unchanged when the scheduler is disabled (`enabled=0`, `running=0`). The storage metrics update on the refresh schedule controlled by `floecat.metrics.storage.refresh` (default `30s`), so per-account gauges (`floecat.service.storage.account.*`) represent the last sampled snapshot rather than every write.
- **`ObservationScope`** is the standard pattern for RPC/async spans: call `observability.observe(category, component, operation, tags...)`, use the returned scope to mark success/error/retry, then `close()`. Scopes emit latency, error, and retry metrics in addition to optional timers, so helpers prefer them to manual timer/counter manipulation. The Micrometer implementation now provides real scopes for `Category.RPC`, `Category.STORE`, `Category.CACHE`, and `Category.GC`; only `BACKGROUND` and `OTHER` still return a `NOOP_SCOPE`. This lets the helpers listed above reuse the same lifecycle hooks rather than sprinkling ad-hoc counters/timers around the code.

Gauges emitted through these helpers can represent different sampling types:

- **Instantaneous** (e.g., `floecat.core.rpc.active`) reports live state directly from an `AtomicInteger` or other source.
- **Sampled/refresh** (e.g., storage account bytes) update on a scheduled refresh, so values reflect the most recent poll not necessarily every write.
- **Estimated** (e.g., cache entry counts derived from `Caffeine.estimatedSize()`) may over-approximate; we document the semantics so dashboards know what to expect.

All these helpers accept an `Observability` implementation (typically the Micrometer backend via `ObservabilityProducer`) and never require a `MeterRegistry`. That keeps the service code framework-neutral and lets the hub control validation, dropped-tag tracking, and exporter wiring.

## Adding new metrics

1. **Define the metadata** – create a module-specific telemetry holder (e.g., `MyModuleTelemetry`) that declares the `MetricId`s and `MetricDef`s. Link them to your chosen origin (`service`, `integration`, etc.). Supply a description, since version, tags, and unit while keeping the name prefixed appropriately.
2. **Implement `TelemetryContributor`** – create `MyModuleTelemetryContributor` that registers the definitions into `TelemetryRegistry`. Example:
   ```java
   public final class MyModuleTelemetryContributor implements TelemetryContributor {
     @Override
     public void contribute(TelemetryRegistry registry) {
       MyModuleTelemetry.definitions().values().forEach(registry::register);
     }
   }
   ```
3. **Publish via `ServiceLoader`** – place `META-INF/services/ai.floedb.floecat.telemetry.TelemetryContributor` in your module listing the contributor’s class. The hub loads every contributor on the classpath when `Telemetry.newRegistryWithCore()` runs, so both doc generation and runtime see your metrics.
4. **Instrument using helpers** – prefer the helper families (`RpcMetrics`, `GcMetrics`, `CacheMetrics`, `StoreMetrics`) wherever possible; they already know the canonical metric IDs, tag sets, and helper methods (`recordHit`, `recordLatency`, `observe`), so you stay consistent without re-implementing tag logic.
5. **Verify coverage** – add a unit test that builds a fresh registry (`Telemetry.newRegistryWithCore()`) and asserts your metric is present (use `Telemetry.metricCatalog(...)` or `Telemetry.requireMetricDef(...)`). This proves the contributor loads and the contract entry exists before anything runs.

## Ensuring metrics are available

- Make sure the telemetry classifier jar (`floecat-service-<ver>-telemetry.jar`) is produced during `process-classes` so docgen/test modules (and Quarkus builds) can load your contributor via ServiceLoader. This lightweight classifier artifact contains only the metric definitions and `META-INF/services/ai.floedb.floecat.telemetry.TelemetryContributor`, which lets other modules load your metrics without depending on the full service runtime.
- Use the docgen regression test (`telemetry-hub/tool-docgen/src/test/.../MetricCatalogDocgenTest`) to guard against disappearing metrics.
- When running locally, start the service with `telemetry.strict=true` in dev or test profiles to catch contract violations early.

## Generating documentation

The contract is the single source of truth. Regenerate the catalog whenever you add or change metrics:

```
mvn -pl telemetry-hub/tool-docgen -am process-classes
```

The exec plugin populates `docs/telemetry/contract.md` and `docs/telemetry/contract.json` by building a registry, grouping metrics by origin, and emitting the columns: Metric, Type, Unit, Since, Description, Required Tags, Allowed Tags. Commit the updated artifacts along with your code so reviewers can verify the new names/tags.

## Runtime wiring

- The service sets `telemetry.strict` (true in `%dev`/`%test`, false in prod) so strict mode throws on contract violations while lenient mode only increments `floecat.core.observability.dropped.tags.total`.
- Observability instrumentation should never refer to Micrometer directly; only the extensions (e.g., `telemetry-hub-backend-micrometer`, `telemetry-hub-integration-quarkus`) need backend dependencies.
- Helpers like `GraphCacheManager`, `EngineHintManager`, and `StorageUsageMetrics` call into the hub’s `Observability` API and rely on the metric definitions described above.

## Backend/exporter behavior

- The hub records timers via `Observability.timer(metricId, Duration)`. A backend such as Micrometer can register a `Timer`, so any exporter (Prometheus, OTLP, Datadog, etc.) sees the usual `_count`, `_sum`, and optionally `_max` series after the logical name (`floecat.core.rpc.latency`) is transformed into the wire format used by that exporter (e.g., dots → underscores for Prometheus).
- Buckets or percentiles are **not enabled by default**; they only appear if you enable distribution statistics/percentile collection in your backend’s configuration (for example via Micrometer’s `distributionStatisticConfig`, Quarkus properties, or the exporter’s own histogram flags). The hub leaves these knobs to the backend so the core remains simple.
- **Prometheus name transformation** – when you scrape `/q/metrics`, Micrometer has already mapped the logical name to Prometheus-compatible tags. Dots become underscores (`floecat.core.rpc.requests` → `floecat_core_rpc_requests_total`), counters gain `_total`, and timers produce `_count`/`_sum` (and `_max`/`_bucket` if you enable distribution statistics). Exact suffixes depend on the registry configuration, but the example above is what the default Micrometer Prometheus registry emits.
- **Histograms & summaries** – timers publish Micrometer timers, and Prometheus consumes them as histogram-like `_count`/`_sum` (or `_bucket`/`_quantile` if distribution statistics are enabled via `quarkus.micrometer.export.prometheus.distribution-statistics.enabled` or the baseline `distributionStatisticConfig`). Micrometer doesn’t emit Prometheus Summaries by default; if you want summaries instead of histograms you must reconfigure the Prometheus registry/filters in Quarkus. The core API remains agnostic—use the exporter config knobs to toggle histogram buckets or percentiles.

## Metric lifecycle & registration

- Meters are registered lazily: a meter is created in the backend registry the first time `Observability.counter`/`gauge`/`timer` is called for that `MetricId`. Helpers such as `CacheMetrics` may register their canonical meters up front, but otherwise unused metrics never make it into the registry.
- Per-account gauges/counters (e.g., storage/account pointers/bytes, cache hit/miss counters) create one time series per distinct account tag value. Most metric registries do not garbage-collect these series automatically, so cardinality grows with the number of accounts observed. If an account disappears, the series typically just stops updating; you must explicitly remove it if you truly need to reclaim the cardinality.
