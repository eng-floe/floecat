# Statistics Engine SPI

This document describes the Floecat-side statistics engine SPI, what it does
today, and how to extend it safely.

## Why this exists

Historically, statistics reads/writes were wired directly to repository and service code paths.
That worked for persisted table/column stats, but did not provide a generic provider-selection
layer for:

- capability-based provider selection
- sync vs async capture routing
- connector-specific strategies
- expression-target compute providers

This SPI introduces that layer while keeping current planner contracts stable.

## What is added

Core interfaces and models:

- `StatsCaptureEngine`
- `StatsStore`
- `StatsCapabilities`
- `StatsCaptureRequest`
- `StatsCaptureResult`
- `StatsEngineRegistry`

Module boundaries:

- `core/stats` defines SPI contracts and capability/request/result models.
- `service` owns runtime orchestration (`StatsEngineRegistry`) and provider implementations.

Current runtime engines:

- `IcebergNativeStatsCaptureEngine` (source-native capture for Iceberg/Glue connectors)
- `DeltaNativeStatsCaptureEngine` (source-native capture for Delta/Unity connectors)

Current target support in native engines:

- `TABLE`
- `COLUMN`
- `FILE`

`EXPRESSION` capture engines are future work.

`StatsCaptureRequest.columnSelectors`:

- optional connector-native selector hints for scoped table/column/file capture
- used to preserve selector-scoped reconcile behavior while routing through orchestrator/control
  plane

Default store:

- `StatsRepository` (default OSS `StatsStore` implementation)

Service wiring:

- `StatsOrchestrator` is the internal resolution entrypoint for planner/query reads.
- `StatsEngineRegistry` handles capability/priority engine selection for compute capture.
- `StatsCaptureControlPlane` is the explicit capture entrypoint for control-plane callers
  (for example reconciler `STATS_ONLY` routing); current service implementation delegates to
  `StatsOrchestrator.trigger(request)`.
- Public read/list RPCs remain `StatsStore` authoritative reads.

Authoritative model:

- Providers produce stats records through the SPI.
- Floecat defines canonical stats record semantics and serves authoritative reads; storage backend
  is pluggable (default OSS deployment uses Floecat-managed persistence).
- Floecat serves persisted authoritative stats through public APIs.

## What this does not do yet

This SPI does **not** implement by itself:

- target-native planner payload evolution
- compute expression engines
- sync budget orchestration
- async reconcile policy
- multi-provider field-level composition

This is intentionally a thin orchestration layer; engine quality/policy evolves independently.

## Capability model

Each `StatsCaptureEngine` advertises:

- supported connectors (`connectors`)
- supported target types (`TABLE`, `FILE`, `COLUMN`, `EXPRESSION`)
- supported statistic kinds per target (`statisticKindsByTarget`)
- supported execution modes (`SYNC`, `ASYNC`)
- sampling support (`NONE`, `RANDOM_ROW_GROUP`, `METADATA_GUIDED`)
- snapshot awareness

Kind routing note:

- `StatsEngineCapabilities.statisticKindsByTarget` is target-specific.
- Engine selection checks requested kinds against the map entry for the requested target type.
- Engines should provide a mapping entry for each declared target type.

`StatsEngineRegistry` uses these capabilities plus `priority()` to select candidate engines in
order.

Snapshot-awareness rule:

- for concrete snapshot requests (`snapshot_id > 0`), only `snapshotAware=true` engines are eligible
- for unresolved/current sentinel requests (`snapshot_id = 0`), both engine types may be eligible

## Routing behavior

`StatsOrchestrator.resolve(request)`:

1. checks `StatsStore` for an existing record
2. for `SYNC`, calls `StatsEngineRegistry.capture(request)` on miss
3. if still missing, enqueues table-scoped `STATS_ONLY` capture for the requested snapshot and
   returns empty

Current enqueue policy:

- enqueue fallback is currently miss-based
- enqueue is capability-driven: orchestrator checks whether the registry has at least one ASYNC
  candidate for the requested target
- async enqueue scope is currently table+snapshot (not target-granular follow-up yet)
- target-level async follow-up reasons/scopes are planned in later PRs

`StatsEngineRegistry.capture(request)`:

1. filters engines by `supports(request)` / capabilities
2. sorts by `priority()` then `id()`
3. calls each engine until one returns a non-empty result
4. throws `StatsUnsupportedTargetException` when no engine supports the request

The registry is selection-only and does not merge multi-engine outputs.

`StatsCaptureControlPlane.trigger(request)`:

1. serves as the explicit capture control-plane entrypoint (non-query callers)
2. in service runtime, delegates to `StatsOrchestrator.trigger(request)`
3. executes one registry-routed capture attempt (no store-read short-circuit, no enqueue fallback)

Result model:

- `StatsCaptureResult` returns one canonical `TargetStatsRecord` plus engine attributes.
- Column and expression targets share the same canonical `ScalarStats` payload.
- Canonical metadata for all targets is stored at `TargetStatsRecord.metadata`.
- `ScalarStats.display_name` is presentation-only (column name/alias) and not part of identity.
- Target/payload compatibility is strict at persistence boundaries:
  - `TABLE` target must carry `table` payload
  - `COLUMN` and `EXPRESSION` targets must carry `scalar` payload
  - `FILE` target must carry `file` payload with matching canonical file path
- Table targets use `TableValueStats` (value only) with metadata at record level.
- `StatsCaptureRequest` is intentionally target-native: one request resolves one concrete target.
  The request also carries `columnSelectors` so one reconcile/capture request can scope work to
  multiple relevant columns in that target context.
  Scope-level orchestration may issue multiple requests for a table/snapshot, and engines may still
  persist additional related records from a single source read when available.

## How to add a new engine

1. Implement `StatsCaptureEngine`.
2. Define accurate `StatsCapabilities`.
3. Set `priority()` (lower is higher priority).
4. Return `Optional.empty()` when unsupported/unavailable for a specific request.
5. Register as CDI bean (`@ApplicationScoped`), no manual registry wiring needed.

Guidelines:

- Keep `capture()` deterministic for a given snapshot/target.
- Do not return stats from a different snapshot.
- Prefer `Optional.empty()` over throwing for normal "not available" outcomes.
- Use `attributes` in `StatsCaptureResult` for lightweight diagnostics.

## Existing baseline behavior

`StatsRepository` is the default OSS authoritative `StatsStore` implementation.
Both native engines persist captured records back into `StatsStore` before returning.
This keeps reads and writes aligned to one authoritative store contract.

## Plugging your own StatsStore

You can replace the default store with your own CDI bean implementing
`ai.floedb.floecat.stats.spi.StatsStore`.

Requirements:

- implement all `StatsStore` operations (`put`, `get`, `list`, `count`, `delete`, metadata)
- enforce the same target identity semantics (`StatsTargetIdentity.storageId(...)`)
- keep record-level metadata intact (`TargetStatsRecord.metadata`)
- preserve snapshot consistency (`table_id + snapshot_id + target`)

CDI guidance:

- register your implementation as `@ApplicationScoped`
- ensure only one active `StatsStore` bean is selected (use alternatives if needed)
- no changes are required in service code; all public reads/listing and capture fallback already
  resolve through `StatsStore`

## Typical extensions

- Add compute-backed expression engines through this SPI.
- Add budgeted sync orchestration and async follow-up on partial outcomes.
