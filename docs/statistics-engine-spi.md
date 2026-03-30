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

Current baseline engine:

- `PersistedStatsCaptureEngine` (capture SPI fallback reading persisted target stats from store)
- configured as a lowest-priority fallback so specialized providers win when available

Default store:

- `StatsRepository` (default OSS `StatsStore` implementation)

Service wiring:

- `StatsEngineRegistry` is internal capture orchestration for provider routing.
- Public read/list RPCs resolve through `StatsStore`.
- Planner/query reads resolve through `StatsStore`.

Authoritative model:

- Providers produce stats records through the SPI.
- Floecat defines canonical stats record semantics and serves authoritative reads; storage backend
  is pluggable (default OSS deployment uses Floecat-managed persistence).
- Floecat serves persisted authoritative stats through public APIs.

## What this does not do yet

This SPI does **not** implement by itself:

- target-native planner payload evolution
- source-native Iceberg/Delta engines
- compute expression engines
- sync budget orchestration
- async reconcile policy
- multi-provider field-level composition

This is intentionally a thin orchestration layer with one baseline engine.

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

`StatsEngineRegistry.capture(request)`:

1. filters engines by `supports(request)` / capabilities
2. sorts by `priority()` then `id()`
3. calls each engine until one returns a non-empty result
4. if no engine supports the request, returns a clear `NON_IMPLEMENTED` error

The registry is responsible for discovery, ordering, and selection only. It is not a merge
framework and does not assemble a final user-facing response.

Result model:

- `StatsCaptureResult` returns one canonical `TargetStatsRecord` plus engine attributes.
- Column and expression targets share the same canonical `ScalarStats` payload.
- Canonical metadata for all targets is stored at `TargetStatsRecord.metadata`.
- `ScalarStats.display_name` is presentation-only (column name/alias) and not part of identity.
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

`PersistedStatsCaptureEngine` supports store-backed table, column, expression, and file targets
for `SYNC` capture mode.
`StatsRepository` is the default OSS authoritative `StatsStore` implementation.
This ensures no regression while keeping reads/writes aligned to one store contract.

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

- Add source-native engines (Iceberg/Delta) through this SPI.
- Add compute-backed expression engines through this SPI.
- Move service/planner payload handling to target-native request/response models.
- Add budgeted sync orchestration and async follow-up on partial outcomes.
