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

- optional connector-native selector hints
- explicit target selection uses batch requests (`StatsCaptureBatchRequest`) and per-item
  `StatsTarget` payloads

Default store:

- `StatsRepository` (default OSS `StatsStore` implementation)

Service wiring:

- `StatsOrchestrator` is the internal resolution entrypoint for planner/query reads.
- `StatsEngineRegistry` handles capability/priority engine selection for compute capture.
- `StatsCaptureControlPlane` is the explicit capture entrypoint for control-plane callers
  (for example reconciler `CAPTURE_ONLY` routing); current service implementation delegates to
  `StatsOrchestrator.triggerBatch(batchRequest)`.
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

Kind routing note:

- `StatsEngineCapabilities.statisticKindsByTarget` is target-specific.
- Engine selection checks requested kinds against the map entry for the requested target type.
- Engines should provide a mapping entry for each declared target type.

`StatsEngineRegistry` uses these capabilities plus `priority()` to select candidate engines in
order.

## Routing behavior

`StatsOrchestrator.resolve(request)` / `resolveBatch(batchRequest)`:

1. checks `StatsStore` for an existing record
2. for `SYNC`, calls `StatsEngineRegistry.captureBatch()` on miss (single-item requests are
   wrapped via `StatsCaptureBatchRequest.of(request)`)
3. if still missing (UNCAPTURABLE/DEGRADED), enqueues scoped `CAPTURE_ONLY` follow-up carrying the
   unresolved table-scoped stats requests and returns empty for unresolved items

Current enqueue policy:

- enqueue fallback is currently miss-based
- enqueue is capability-driven: orchestrator checks whether the registry has at least one ASYNC
  candidate for the requested target
- async enqueue scope is table-scoped and uses reconcile `ScopedStatsRequest` payloads:
  `table_id` + `snapshot_id` + encoded `target_spec` + `column_selectors`

`StatsEngineRegistry.captureBatch(batchRequest)`:

1. filters candidates per request by `supports(request)` / capabilities
2. applies deterministic priority ordering (`priority()` then `id()`)
3. routes batched stage groups per engine and preserves request order in results
4. returns per-item outcomes (`CAPTURED`, `UNCAPTURABLE`, `DEGRADED`, `QUEUED`)

The registry is selection-only and does not merge multi-engine outputs.

`StatsCaptureControlPlane.triggerBatch(batchRequest)`:

1. serves as the explicit capture control-plane entrypoint (non-query callers)
2. in service runtime, delegates to `StatsOrchestrator.triggerBatch(...)`
3. executes registry-routed capture attempt(s) (no store-read short-circuit, no enqueue fallback)

Reconciler policy on top of control-plane results:

- all items non-captured (`UNCAPTURABLE` / `DEGRADED`) => reconcile table execution fails
- mixed captured + non-captured => reconcile table execution succeeds in degraded state
- `statsProcessed` counts captured snapshots, not attempted requests

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
- Single request API is target-native (`StatsCaptureRequest`).
- Batch API is first-class (`StatsCaptureBatchRequest`) and is the primary path for reconciler/query
  orchestration so engines can process multiple targets in one pass.

## Batch contract for engine implementors

Implementations of `StatsCaptureEngine.captureBatch()` must:

1. **Size invariant**: `results().size() == batchRequest.requests().size()` in all cases.
2. **Order invariant**: `result[i]` must correspond to `requests.get(i)`.
3. **No throws**: represent failures as per-item outcomes (typically `DEGRADED`) instead of
   throwing exceptions.
4. **Item independence**: failure for one item must not prevent attempts for remaining items.
5. **UNCAPTURABLE semantics**: use `UNCAPTURABLE` only for capability mismatches. Registry routing
   retries `UNCAPTURABLE` items with the next candidate engine; `DEGRADED` is final.

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

When a TABLE request is included in a native engine batch, the engine may opportunistically persist
additional COLUMN/FILE records returned in the same connector capture bundle. This is intentional
to avoid extra source scans for follow-up target requests.

Native connector target-kind expansion:

- when `StatsTargetKind.TABLE` is requested, native engines may also request `COLUMN` and `FILE`
  from `captureSnapshotTargetStats(...)` so full-bundle persistence can happen in one connector
  pass

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
