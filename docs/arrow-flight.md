# Arrow Flight integration

This document describes the Arrow Flight contract used for system-table scans and how to:

1. implement a producer, and
2. implement an ingester/consumer.

For the cross-transport overview, see [system-scans.md](system-scans.md). For gRPC details, see
[system-scans-grpc.md](system-scans-grpc.md).

## Scope

Floecat uses a two-phase Flight flow for system tables:

1. `GetFlightInfo`: validate command + auth, resolve schema, return an opaque ticket.
2. `GetStream`: redeem ticket, build scan plan, stream Arrow batches.

The command/ticket schema is defined in `system_scan_flight.proto` (`SystemTableFlightCommand` and
`SystemTableFlightTicket`).

## Transport flow details

### Phase 1: `GetFlightInfo`

- Decode `SystemTableFlightCommand` from descriptor command bytes.
- Validate query identity (`query_id` vs `x-query-id` if both are present).
- Resolve table handle (`target.name` or `target.id`) and project schema.
- Return a `FlightInfo` with projected schema and a versioned opaque ticket.
- No data scan happens in this phase.

### Phase 2: `GetStream`

- Decode ticket (`SystemTableFlightTicket`) and validate version.
- Resolve/authorize table access again.
- Run plan on worker executor thread (not event-loop thread).
- Stream Arrow schema/batches through `FlightArrowBatchSink`.
- Ensure sink/allocator cleanup on complete, cancel, or error.

## Producer guide

Use `SystemTableFlightProducerBase` from `floecat-flight`.

### 1) Extend the base class

Implement:

- `resolveCallContext(...)`
- `tableNames(...)` (canonical lowercase names, for example `sys.session`)
- `schemaColumns(...)`
- `buildPlan(...)`
- `selfLocation()`

Optional:

- `authorize(...)` for permission checks.
- `resolveSystemTableId(...)` / `resolveSystemTableName(...)` if ID routing is needed.

### 2) Constructor wiring

Use one of the base constructors:

- `super(allocator, flightExecutor)` when allocator is provided directly by host runtime.
- `super(allocatorProvider, flightExecutor)` when allocator is managed by a provider bean.

### 3) List flights correctly

Use `descriptorForTable(tableName, callCtx)` so descriptors carry protocol-correct command bytes.

### 4) Build plans with cancellation

`buildPlan(...)` receives `BooleanSupplier cancelled`; pass it into paged iterators and stop quickly
when cancelled.

### 5) Memory model

The base allocates a per-stream child allocator from the parent allocator and closes it after the
stream ends or fails. Producer code should not close the parent allocator.

## Ingester / consumer guide

Use `SystemTableCommands` + standard Flight client calls.

### 1) Build command descriptor

Use canonical table name and query id:

```java
FlightDescriptor d = SystemTableCommands.descriptor("sys.query", queryId);
```

### 2) Send required headers/context

At minimum, provide query identity either:

- in command `query_id` (default via `SystemTableCommands`), or
- via `x-query-id` header.

If both are present, they must match.

For authenticated services, propagate the same auth/session headers used on gRPC paths.

Common headers:

| Header | Purpose |
|---|---|
| `authorization` or session header | OIDC bearer token or session token |
| `x-engine-kind` | Engine kind (optional) |
| `x-engine-version` | Engine version (optional) |
| `x-query-id` | Query ID (optional if command carries `query_id`) |
| `x-correlation-id` | Correlation ID (generated if absent; propagated in call context) |

### 3) Two-phase call flow

1. `FlightInfo info = client.getInfo(d, callOptions...)`
2. `Ticket ticket = info.getEndpoints().get(0).getTicket()`
3. `FlightStream stream = client.getStream(ticket, callOptions...)`
4. Read schema + batches from stream.

### 4) Retry guidance

Retry only transient failures (`UNAVAILABLE`), with bounded timeout/retry budget. Do not retry:

- `INVALID_ARGUMENT` (bad command/query id/ticket version)
- `NOT_FOUND` (unknown table/route)
- `UNAUTHENTICATED` / `UNAUTHORIZED`

## Protocol notes

- Routing is by canonical table name ownership (`tableNames()`), not opaque payload heuristics.
- Ticket format is versioned (`SystemTableFlightTicket.version`); mismatched versions return
  `INVALID_ARGUMENT`.
- Producers may accept name-only targets (no ID) for external endpoints.
- `required_columns` projection is applied to both `GetFlightInfo` schema and stream data.
- Arrow schema generation uses Floecat `SchemaColumn.logical_type` semantics via
  `ArrowSchemaUtil`:
  - use Floecat canonical logical types (or supported aliases),
  - integer aliases collapse to Arrow `Int64`,
  - `JSON` maps to Arrow `Utf8`; `BINARY` maps to Arrow `Binary`; `UUID` maps to
    `FixedSizeBinary(16)`,
  - `INTERVAL` and complex container kinds (`ARRAY`, `MAP`, `STRUCT`, `VARIANT`) are not supported
    and must be omitted or cast to `STRING`/`BINARY`,
  - unknown/null/blank logical types fail fast instead of defaulting to `Utf8`.

Auth/authorization behavior:

- `catalog.read` is enforced on both `GetFlightInfo` and `GetStream`.
- Failures map to typed statuses (`UNAUTHENTICATED`, `UNAUTHORIZED`, `NOT_FOUND`,
  `INVALID_ARGUMENT`) rather than generic internal errors.

## Configuration

| Property | Default | Purpose |
|---|---|---|
| `floecat.flight.port` | `47470` | Flight server port |
| `floecat.flight.host` | `localhost` | Host returned in `FlightEndpointRef` |
| `floecat.flight.tls` | `false` | TLS toggle (currently unsupported) |
| `floecat.flight.memory.max-bytes` | `0` | Parent allocator cap (0 = unbounded) |
| `ai.floedb.floecat.arrow.max-bytes` | `1073741824` | Per-stream allocator cap |

The configured `floecat.flight.host` is published in catalog bundles as `FlightEndpointRef`. In
non-dev/test profiles, avoid loopback values (`localhost` / `127.0.0.1`) for distributed clients.

## Scanner and engine-context resolution

Flight and gRPC share scanner resolution logic:

- `SystemScannerResolver.resolve(correlationId, tableId, engineContext)`
- Includes cross-engine table-id translation (`translateToDefault`) when engine-specific IDs are
  presented.

## Discovery model

When catalog bundles include `flight_endpoint`, workers can route to the declared endpoint and use
this same command/ticket flow independent of backing service implementation.

## Troubleshooting quick checks

- `INVALID_ARGUMENT: query_id is required`:
  missing both command `query_id` and `x-query-id`.
- `INVALID_ARGUMENT: query_id mismatch`:
  header and command query ids differ.
- `NOT_FOUND` on supported table:
  producer `tableNames()` does not include canonical name used by descriptor.
- Empty batches:
  verify writer calls `root.setRowCount(...)`.
