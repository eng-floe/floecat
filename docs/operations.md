# Operations

## Docker Compose Modes

Docker build, compose modes, and AWS credential options live in [`docs/docker.md`](docker.md).

## Testing

```bash
# Unit + integration tests across modules (in-memory)
make test

# Run full test suite against LocalStack (upstream + catalog)
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
- **Tracing** – OpenTelemetry (TraceContext propagator) is enabled; export pipelines can be wired by
  overriding `quarkus.otel.traces.exporter`.

### Telemetry hub configuration
The service now uses the telemetry hub core + Micrometer backend. The following flags are available in
`service/src/main/resources/application.properties`:

```
telemetry.strict=false
telemetry.exporters=prometheus,otlp
telemetry.contract.version=v1
%dev.telemetry.strict=true
%test.telemetry.strict=true
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
| `otlp` | OpenTelemetry exporter that forwards the hub’s metrics (and any traces/span data if you instrument other flows) to an OTLP collector via gRPC or HTTP. | Requires you to set `quarkus.otel.metrics.exporter=otlp` (and `quarkus.otel.traces.exporter=otlp` if you want traces). | Configure the OTLP endpoint with `quarkus.otel.exporter-otlp.endpoint`; switch protocols via `quarkus.otel.exporter-otlp.protocol=http/protobuf`. The hub doesn’t itself emit OTEL spans—`ObservationScope` currently powers Micrometer metrics, while Quarkus OpenTelemetry (plus your own instrumentation) controls tracing. |

Drop an exporter by removing it from `telemetry.exporters` (or setting the property to the empty string), e.g., `%dev.telemetry.exporters=prometheus` keeps strict mode local without OTLP traffic. The hub simply skips wiring backends it isn’t asked for, so only the listed exporters get the meters/traces.

### Metrics
Micrometer + Prometheus export is enabled by default. The scrape endpoint is:

```
GET http://<host>:<http-port>/q/metrics
```

See [`docs/telemetry/overview.md`](telemetry/overview.md) for the naming, tagging, and contribution rules, and view the generated catalog (`docs/telemetry/contract.md` and `docs/telemetry/contract.json`) for the current set of metrics. Regenerate the catalog any time you add or modify a metric:

```
mvn -pl telemetry-hub/tool-docgen -am process-classes
```
