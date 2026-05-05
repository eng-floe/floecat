# Operations

## Docker Compose Modes

Docker build, compose modes, and AWS credential options live in [`docs/docker.md`](docker.md).

## Testing

```bash
# Unit + integration tests across modules (in-memory)
make test

# Run full test suite against LocalStack (fixtures + catalog storage)
make test-localstack

# Only run unit or integration suites
make unit-test
make integration-test
```

## Builtin Catalog Validator

Validate bundled or bespoke builtin catalog protobufs without running the service.

```bash
mvn -pl tools/builtin-validator package
```

**Engine mode** – load via `ServiceLoader` (extension JAR must be on the classpath):

```bash
java -jar tools/builtin-validator/target/builtin-validator.jar \
  --engine example
```

**Directory mode** – point at a catalog directory; the validator reads `_index.txt` automatically:

```bash
java -jar tools/builtin-validator/target/builtin-validator.jar \
  /path/to/builtins/example
```

**File mode** – validate a single merged protobuf file (binary or text format):

```bash
java -jar tools/builtin-validator/target/builtin-validator.jar \
  /path/to/catalog.pbtxt
```

Flags:

- `--engine <kind>` – load the registered `EngineSystemCatalogExtension` for the given engine kind via `ServiceLoader` instead of reading a file.
- `--json` – emit machine-readable output (for CI or scripting).
- `--strict` – fail the run when warnings are present (warnings are currently reserved for future checks).

## Observability & Operations

### Reconciler deployment modes

The reconciler runs in three shapes from the same artifact:

- **All-in-one**: default profile; public APIs, durable queue ownership, and local executor polling stay in one runtime.
- **Control plane**: `QUARKUS_PROFILE=reconciler-control`; owns the queue, automatic enqueue, public reconcile APIs, and executor-control RPCs.
- **Executor plane**: `QUARKUS_PROFILE=reconciler-executor`; disables local queue ownership and automatic scheduling, then leases work remotely from the control plane over gRPC.

Key reconciler mode flags live in `service/src/main/resources/application.properties`:

```properties
floecat.reconciler.worker.mode
reconciler.max-parallelism
floecat.reconciler.executor.remote-planner.enabled
floecat.reconciler.executor.remote-default.enabled
floecat.reconciler.executor.remote-snapshot-planner.enabled
floecat.reconciler.executor.remote-file-group.enabled
floecat.reconciler.authorization.header
floecat.reconciler.authorization.token
floecat.reconciler.auto.execution-class
floecat.reconciler.auto.execution-lane
```

Recommended split deployment:

- Control plane: `QUARKUS_PROFILE=reconciler-control`
- Executor plane: `QUARKUS_PROFILE=reconciler-executor`
- Shared settings: same blob/kv backend, same reconcile auth token, executor nodes pointed at the control-plane gRPC host/port
- Control-plane-specific setting: `reconciler.max-parallelism=0`
- Executor-plane-specific setting: `floecat.reconciler.worker.mode=remote`

In the split model, the control plane owns top-level `PLAN_CONNECTOR` jobs and public reconcile
APIs, while executor-plane nodes primarily run child `PLAN_TABLE`, `PLAN_VIEW`, `PLAN_SNAPSHOT`,
and `EXEC_FILE_GROUP` work. `CaptureNow` uses the same plan-plus-child execution path. File-group workers submit results through
`SubmitLeasedFileGroupExecutionResult`, which requires `result_id` so the control plane can
enforce replay safety across worker retries.

If `PLAN_CONNECTOR` jobs can be enqueued, at least one enabled executor must support that job kind.
Planner jobs also need to remain leaseable under the configured execution lane semantics: planner
executors advertise wildcard lane support, while child planning and file-group execution jobs carry
the concrete lane policy that remote or local workers enforce later.

To scale executors horizontally, add more executor-plane instances. They greedily lease eligible jobs from the shared durable queue, so no leader election is required at the executor layer.

- **Logging** – JSON console logs plus rotating files under `log/`. Audit logs route gRPC request
  summaries to `log/audit.json`; see [`docs/log.md`](log.md).
- **Metrics** – Micrometer/Prometheus exporters expose gRPC, storage, and GC metrics at the
  `/q/metrics` endpoint (see the telemetry hub contract in `docs/telemetry/contract.md`).
- **Tracing** – OpenTelemetry (TraceContext propagator) is always enabled for MDC correlation
  (`traceId`/`spanId`). The OTLP exporter is built-in; activate the `telemetry-otlp` profile to
  ship spans to a collector. See [`telemetry-demo.md`][telemetry-demo-doc]
  for the full Prometheus + Tempo + Loki + Grafana demo stack.

### Telemetry hub configuration
The service uses the telemetry hub core + Micrometer backend. The following flags are available in
`service/src/main/resources/application.properties` (the `telemetry-otlp` profile toggles OTLP tracing/log exports):

```
telemetry.strict=false
telemetry.exporters=prometheus
telemetry.contract.version=v1
%dev.telemetry.strict=true
%test.telemetry.strict=true
%telemetry-otlp.telemetry.exporters=prometheus,otlp
```

These settings keep production lenient (dropped-tag counters are exposed) while blowing up early in dev/test
when metrics violate their contract. Run the docgen tool if you change any metrics to regenerate the catalog:

```
mvn -pl telemetry-hub/tool-docgen -am process-classes
```

[telemetry-demo-doc]: https://github.com/eng-floe/floecat/blob/main/examples/telemetry/telemetry-demo.md

Documented metrics are listed in `docs/telemetry/contract.md` and generated JSON `docs/telemetry/contract.json`.

Configuration flags are documented per module (for example storage backend selection in
[`docs/storage-spi.md`](storage-spi.md), GC cadence in [`docs/service.md`](service.md),
Secrets Manager in [`docs/secrets-manager.md`](secrets-manager.md)).

### Telemetry exporter matrix
The `telemetry.exporters` flag (defined in `service/src/main/resources/application.properties`) tells the hub which backends to activate. Supported values are:

| Exporter | Description | Activation | Notes |
| --- | --- | --- | --- |
| `prometheus` | Micrometer Prometheus registry that exposes `floecat.core.*` and `floecat.service.*` metrics via `/q/metrics`. | Enabled by default and controlled on the Micrometer side via `quarkus.micrometer.export.prometheus.enabled=true`. | This exporter simply scrapes the Micrometer registry that Observability feeds. Keep `telemetry.exporters` set to include `prometheus` in dev/test to keep dashboards working. |
| `otlp` | OpenTelemetry exporter that forwards traces (and hub metrics if wired) to an OTLP collector via gRPC. | Enabled only with the `telemetry-otlp` profile (e.g., `QUARKUS_PROFILE=telemetry-otlp mvn -pl service -am quarkus:dev`). | Configure the OTLP endpoint with `quarkus.otel.exporter.otlp.endpoint` (runtime property). The exporter itself is `cdi` (Quarkus built-in, build-time); do **not** set `quarkus.otel.traces.exporter=otlp`. Run the full telemetry demo stack (`examples/telemetry/docker-compose.yml`) to bring up the collector, Tempo, Loki, and Grafana. |

Drop an exporter by removing it from `telemetry.exporters` (or setting the property to the empty string), e.g., `%dev.telemetry.exporters=prometheus` keeps strict mode local without OTLP traffic. The hub simply skips wiring backends it isn’t asked for, so only the listed exporters get the meters/traces.

Spans emitted by the service carry custom attributes `floecat.component` and `floecat.operation` (set by `GrpcTelemetryServerInterceptor`), matching the `component`/`operation` labels on Prometheus metrics. The Grafana dashboard uses this bridge to build TraceQL links: clicking a series on an RPC panel opens Tempo Explore with `span.floecat.operation = "<operation>"`. Standard OTel `rpc.*` attributes are also present on every gRPC span for ad-hoc queries. Logs expose the same values through `floecat_component` and `floecat_operation` MDC keys, plus `traceId` and `spanId` for trace ↔ log correlation. The Tempo datasource is provisioned with `tracesToLogsV2` so you can click through from a trace span to the correlated Loki logs.

### Metrics
Micrometer + Prometheus export is enabled by default. The scrape endpoint is:

```
GET http://<host>:<http-port>/q/metrics
```

See [`docs/telemetry/overview.md`](telemetry/overview.md) for the naming, tagging, and contribution rules, and view the generated catalog (`docs/telemetry/contract.md` and `docs/telemetry/contract.json`) for the current set of metrics. Regenerate the catalog any time you add or modify a metric:

```
mvn -pl telemetry-hub/tool-docgen -am process-classes
```
