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

Validate bundled or bespoke builtin catalog protobufs without running the service:

```bash
mvn -pl tools/builtin-validator package
java -jar tools/builtin-validator/target/builtin-validator.jar \
  service/src/main/resources/builtins/floe-demo.pbtxt
```

Flags:

- `--json` – emit machine-readable output (for CI or scripting).
- `--strict` – fail the run when warnings are present (warnings are currently reserved for future checks).

## Observability & Operations

- **Logging** – JSON console logs plus rotating files under `log/`. Audit logs route gRPC request
  summaries to `log/audit.json`; see [`docs/log.md`](log.md).
- **Metrics** – Micrometer/Prometheus exporters summarise pointer/blob IO and reconciliation stats.
- **Tracing** – OpenTelemetry (TraceContext propagator) is enabled; export pipelines can be wired by
  overriding `quarkus.otel.traces.exporter`.

Configuration flags are documented per module (for example storage backend selection in
[`docs/storage-spi.md`](storage-spi.md), GC cadence in [`docs/service.md`](service.md)).
