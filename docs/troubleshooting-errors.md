# Troubleshooting Floecat errors

Floecat enforces a single error contract so every non-OK gRPC response carries:

1. An official `grpc.Status` canonical code (INVALID_ARGUMENT, NOT_FOUND, etc.).
2. A packed `ai.floedb.floecat.common.rpc.Error` detail that includes:
   * application `ErrorCode` (e.g. `MC_INVALID_ARGUMENT`),
   * `correlation_id`,
   * `message_key`,
   * `message` (rendered text or safe fallback),
   * `params` map with the fields/ids tied to the failure.
3. Optional standard details such as `BadRequest` violations, `ErrorInfo`, and (in dev/test via `floecat.errors.debug-details=true`) `DebugInfo`.

### Finding the correlation ID

Every interceptor ensures the correlation ID:

* is generated/extracted at the top of the call (`x-correlation-id` header, or a UUID fallback),
* is persisted in the request MDC/otel span,
* is echoed back in gRPC trailers,
* is the `correlation_id` field of the packed Floecat `Error`.

### Decoding errors in Java

Use `FloecatStatus.fromThrowable(Throwable)` to unpack a gRPC error:

```java
try {
  serviceStub.doWork(request);
} catch (StatusRuntimeException sre) {
  FloecatStatus decoded = FloecatStatus.fromThrowable(sre);
  if (decoded != null) {
    log.warnf(
        "rpc failed code=%s app=%s msg=%s params=%s corr=%s",
        decoded.canonicalCode(),
        decoded.errorCode(),
        decoded.message(),
        decoded.params(),
        decoded.correlationId());
  }
  throw sre;
}
```

`FloecatStatus` also exposes `debugInfo()` when `floecat.errors.debug-details=true` so engineers can optionally read stack snippets without leaking them in production.

### Inspecting via gRPC tools

`grpcurl` automatically shows the packed details:

```bash
grpcurl -d @ \
  -H 'x-correlation-id: abc123' \
  -import-path ... -proto ... \
  localhost:9000 ai.floedb.floecat.CatalogService/GetCatalog
```

A non-OK response will include `google.rpc.Status` details; strip out the `Any` with `ai.floedb.floecat.common.rpc.Error` to see `message_key`, params, and correlation ID. If `BadRequest` is present, note each `field_violations`.

### Validation failures

Use `RequestValidation.requireNonBlank` and `RequestValidation.requireHeader` to raise consistent `INVALID_ARGUMENT` errors with the `field` parameter populated and the `BadRequest` detail crafted automatically. That makes UI tooling and SDKs reliably show what input to fix.

### Internal errors

Unexpected exceptions are wrapped as `GrpcErrors.internal(...)`. `Status.message` remains safe (`Internal error. correlation_id=...`) while the packed `Error` carries the root `error_class`, `root_class`, and `root_message`. Toggle `floecat.errors.debug-details=true` to populate `DebugInfo` for deeper investigation.
