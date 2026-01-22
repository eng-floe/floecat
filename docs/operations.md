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
  `/q/metrics` endpoint.
- **Tracing** – OpenTelemetry (TraceContext propagator) is enabled; export pipelines can be wired by
  overriding `quarkus.otel.traces.exporter`.

Configuration flags are documented per module (for example storage backend selection in
[`docs/storage-spi.md`](storage-spi.md), GC cadence in [`docs/service.md`](service.md)).

### Metrics
Micrometer + Prometheus export is enabled by default. The scrape endpoint is:

```
GET http://<host>:<http-port>/q/metrics
```

GC counters:
- `floecat_gc_cas_ticks`
- `floecat_gc_cas_accounts`
- `floecat_gc_cas_pointers_scanned`
- `floecat_gc_cas_blobs_scanned`
- `floecat_gc_cas_blobs_deleted`
- `floecat_gc_cas_referenced`
- `floecat_gc_cas_tables_scanned`
- `floecat_gc_pointer_ticks`
- `floecat_gc_pointer_accounts`
- `floecat_gc_pointer_pointers_scanned`
- `floecat_gc_pointer_pointers_deleted`
- `floecat_gc_pointer_missing_blobs`
- `floecat_gc_pointer_stale_secondaries`
- `floecat_gc_idempotency_ticks`
- `floecat_gc_idempotency_slices`
- `floecat_gc_idempotency_scanned`
- `floecat_gc_idempotency_expired`
- `floecat_gc_idempotency_ptr_deleted`
- `floecat_gc_idempotency_blob_deleted`

RPC metrics (via `MeteringInterceptor`):
- `rpc_requests` (counter; tags: `account`, `op`, `status`)
- `rpc_latency_seconds` (timer; tags: `account`, `op`, `status`)
- `rpc_active_seconds` (long task timer; tags: `account`, `op`)

Storage usage metrics (via `StorageUsageMetrics`):
- `account.storage.refresh.errors` (counter)
- `account.storage.refresh.ms` (timer)
- `account.pointers.count` (gauge; tag: `account`)
- `account.storage.bytes` (gauge; tag: `account`)
