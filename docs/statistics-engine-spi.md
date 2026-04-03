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
- `StatsEngineCapabilities`
- `StatsCaptureRequest`
- `StatsCaptureResult`
- `StatsCaptureValue` (exactly one payload: table, file, column, expression)
- `StatsValueSummary` (shared value-level metrics for column/expression payloads)
- `StatsEngineRegistry`

Module boundaries:

- `core/stats` defines SPI contracts and capability/request/result models.
- `service` owns runtime orchestration (`StatsEngineRegistry`) and provider implementations.

Current baseline engine:

- `PersistedStatsCaptureEngine` (reads already persisted table/column stats and internal file stats from repository)
- configured as a lowest-priority fallback so specialized providers win when available

Service wiring:

- `StatsEngineRegistry` is internal orchestration infrastructure for provider routing.
- Public stats RPCs remain repository-backed in this stage; they are not yet routed through the
  registry.

Authoritative model:

- Providers produce stats records through the SPI.
- Floecat persists canonical stats as the system of record.
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

The registry is responsible for discovery, ordering, and selection only. It is not a merge
framework and does not assemble a final user-facing response.

Payload model:

- `StatsCaptureResult` carries a single typed `StatsCaptureValue`.
- Column and expression payloads share `StatsValueSummary` to avoid duplicated metric schemas.
- `StatsCaptureRequest` is intentionally target-native: one request resolves one concrete target.
  Scope-level orchestration may issue multiple requests for a table/snapshot, and engines may still
  persist additional related records from a single source read when available.

## How to add a new engine

1. Implement `StatsCaptureEngine`.
2. Define accurate `StatsEngineCapabilities`.
3. Set `priority()` (lower is higher priority).
4. Return `Optional.empty()` when unsupported/unavailable for a specific request.
5. Register as CDI bean (`@ApplicationScoped`), no manual registry wiring needed.

Guidelines:

- Keep `capture()` deterministic for a given snapshot/target.
- Do not return stats from a different snapshot.
- Prefer `Optional.empty()` over throwing for normal "not available" outcomes.
- Use `attributes` in `StatsCaptureResult` for lightweight diagnostics.

## Existing baseline behavior

`PersistedStatsCaptureEngine` supports repository-backed table and column targets, and can also
serve file stats for internal/reconciliation capture paths.
This ensures no regression while making provider selection real.

## Typical extensions

- Add source-native engines (Iceberg/Delta) through this SPI.
- Add compute-backed expression engines through this SPI.
- Move service/planner payload handling to target-native request/response models.
- Add budgeted sync orchestration and async follow-up on partial outcomes.
