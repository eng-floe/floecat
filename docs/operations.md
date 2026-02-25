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

Validate bundled or bespoke builtin catalog protobufs without running the service (merge the
fragments listed in `_index.txt` before you run the validator; e.g., use the index as the source of truth:

```
dir=service/src/main/resources/builtins/floe-demo
awk '!/^\s*($|#)/{print}' "$dir/_index.txt" | sed "s|^|$dir/|" | xargs cat > /tmp/floe-demo.pbtxt
```

Then point the validator at the merged file:

```bash
mvn -pl tools/builtin-validator package
java -jar tools/builtin-validator/target/builtin-validator.jar \
  /tmp/floe-demo.pbtxt
```

Flags:

- `--json` – emit machine-readable output (for CI or scripting).
- `--strict` – fail the run when warnings are present (warnings are currently reserved for future checks).

## Observability & Operations

- **Logging** – JSON console logs plus rotating files under `log/`. Audit logs route gRPC request
  summaries to `log/audit.json`; see [`docs/log.md`](log.md).
- **Metrics** – Micrometer/Prometheus exporters expose gRPC, storage, and GC metrics at the
  `/q/metrics` endpoint (see the telemetry hub contract in `docs/telemetry/contract.md`).
- **Tracing** – OpenTelemetry (TraceContext propagator) is always enabled for MDC correlation
  (`traceId`/`spanId`). The OTLP exporter is built-in; activate the `telemetry-otlp` profile to
  ship spans to a collector. See [`examples/telemetry/telemetry-demo.md`](../examples/telemetry/telemetry-demo.md)
  for the full Prometheus + Tempo + Loki + Grafana demo stack.

### Telemetry hub configuration
The service now uses the telemetry hub core + Micrometer backend. The following flags are available in
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
