# Transactions (gRPC Core)

This document describes Floecat's core gRPC transaction mechanism used for Iceberg REST commits.
It is the backend used by both:
- `POST /v1/{prefix}/transactions/commit` (multi-table)
- `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}` (single-table commit)

## Overview

Transactions provide optimistic, pointer-CAS-based atomic commit across one or more tables:

- `BeginTransaction` creates a transaction in `TS_OPEN`.
- `PrepareTransaction` validates changes, writes intent records, then moves to `TS_PREPARED`.
- `CommitTransaction` applies intents synchronously and returns a terminal apply state.
- `AbortTransaction` marks `TS_ABORTED` and cleans up intents.

## State Model

- `TS_OPEN`: new transaction, no committed prepare yet.
- `TS_PREPARED`: intents are persisted and ready to apply.
- `TS_ABORTED`: transaction aborted explicitly or due to expiration handling.
- `TS_APPLIED`: all intent pointer updates applied.
- `TS_APPLY_FAILED_RETRYABLE`: apply failed with a retryable/storage conflict.
- `TS_APPLY_FAILED_CONFLICT`: apply failed with deterministic conflict semantics.

## Request/Data Model

`TxChange`:
- Targets a table by `table_id` or `table_fq`.
- `table_fq` format is `catalog.namespace1.namespace2.table` (dot-separated, no escaping).
- Payload is one of: `table`, opaque `payload`, or `intended_blob_uri`.
- Optional pointer `precondition.expected_version`.

`TransactionIntent`:
- One intent per target pointer key.
- Stores target pointer key, blob URI, and optional expected version.
- On failed apply, stores diagnostic fields (`apply_error_*`).

## Flow Details

### BeginTransaction
1. Authorize `table.write`; require account ID.
2. Create transaction (`TS_OPEN`) with TTL:
   - request TTL if provided
   - otherwise default 600s
3. Persist transaction and return it.
4. If idempotency key is supplied, wrapped by `IdempotencyGuard.runOnce`.

### PrepareTransaction
1. Load transaction by `tx_id`.
2. Expired transaction:
   - transition to `TS_ABORTED`
   - return error (`transaction expired`)
3. State handling:
   - `TS_PREPARED` => return existing transaction
   - non-`TS_OPEN` => error
4. Build intents from all changes:
   - resolve table ID
   - reject duplicate target pointer keys in one request
   - read current pointer version
   - validate `precondition.expected_version` (if set)
   - materialize payload blob URI:
     - `table`: validate resource ID/account match, write content-addressed table blob
     - `payload`: write tx-scoped content-addressed blob
     - `intended_blob_uri`: must be non-empty and within account prefix
5. Before creating each intent, enforce target lock availability:
   - if existing lock belongs to another tx and owner tx is stale/terminal/missing/expired, clean it up and continue
   - if owner tx is still active (including `TS_APPLY_FAILED_RETRYABLE`), fail with lock error
6. Create intents and transition transaction to `TS_PREPARED`.
7. On failures after partial creation, delete created intent indices best-effort.

Note: prepared blob URIs are content-addressed; cleanup intentionally does not delete blobs on failure.

### CommitTransaction
1. Load transaction by `tx_id`.
2. Expired transaction:
   - transition to `TS_ABORTED`
   - clean up intents
   - return error (`transaction expired`)
3. State handling:
   - `TS_APPLIED` => return as-is
   - `TS_APPLY_FAILED_CONFLICT` => return as-is
   - allowed apply states: `TS_PREPARED`, `TS_APPLY_FAILED_RETRYABLE`
   - other states => error
4. Load intents by tx; empty intent set is an error.
5. Apply intents via pointer batch CAS:
   - table intents include by-id pointer plus table name-pointer ownership updates
   - max pointer CAS ops per apply is 100
6. Commit result mapping:
   - apply success => `TS_APPLIED`
   - deterministic conflict => annotate intents, `TS_APPLY_FAILED_CONFLICT`
   - retryable apply failure => annotate intents, `TS_APPLY_FAILED_RETRYABLE`
7. On successful apply, intent index cleanup is best-effort; warnings are logged if not fully removed.

### AbortTransaction
1. Load transaction by `tx_id`.
2. State handling:
   - already `TS_ABORTED` => return as-is
   - committed family (`TS_APPLIED`, `TS_APPLY_FAILED_CONFLICT`) => error
   - `TS_APPLY_FAILED_RETRYABLE` => abort is allowed to release intent locks
3. Transition to `TS_ABORTED`.
4. Delete intents for that tx (best-effort via index deletes).

## Idempotency

Idempotency keys are supported on:
- `BeginTransaction`
- `PrepareTransaction`
- `CommitTransaction`

`AbortTransaction` has no idempotency key in the proto API.

## REST `/transactions/commit` Bridge (Current Behavior)

The Iceberg REST gateway uses this gRPC flow as follows:

1. Begins a transaction and stores `iceberg.commit.request-hash` in transaction properties.
   - If `Idempotency-Key` is absent, begin idempotency falls back to `req:<catalog>:<request-hash>` so retries can resume the same backend transaction without cross-catalog key collisions.
2. Reads current transaction state:
   - `TS_APPLIED` => returns HTTP 204 immediately.
   - `TS_APPLY_FAILED_CONFLICT` => returns HTTP 409 immediately.
3. For open/retryable paths, plans table changes, prepares intents, applies pre-commit snapshot ops, then calls commit.
4. During prepare, each table change now includes `precondition.expected_version` sourced from table `MutationMeta.pointer_version` fetched at planning time.
5. REST returns 204 only when backend is `TS_APPLIED`; deterministic failures map to 409.
   Unknown commit state maps are:
   - 503 for unavailable/retryable unknown state
   - 502 for upstream unknown response
   - 504 for deadline exceeded
   - 500 fallback for other unknowns
   Auth failures map to:
   - 401 for unauthenticated
   - 403 for permission denied
6. Post-commit side effects (snapshot prune + connector stats sync/reconcile trigger) are outside backend atomic CAS boundary and are best-effort after apply (they no longer downgrade a committed apply to non-204).
7. Stage-create materialization (`stage-id`) is not supported in multi-table `/transactions/commit`; staged create should use single-table commit flow.
8. Unknown requirement types and update actions are rejected with HTTP 400 before commit orchestration, including replay (`TS_APPLIED`) paths.

## REST Single-Table Commit Bridge (Current Behavior)

Single-table commit uses the same backend transaction flow as multi-table commit, with a single
planned table change:

1. `TableCommitService` forwards the incoming table commit request to `TransactionCommitService`.
2. `TransactionCommitService` runs begin/prepare/commit against the same gRPC transaction APIs
   described above, using one `table-change` entry.
3. Atomicity guarantees are therefore identical at the backend CAS/apply layer; only the REST
   request/response envelope differs (single-table commit returns table metadata in its response).

## Failure and Concurrency Notes

- Prepare may write blobs before intent creation; blob cleanup is intentionally skipped because blobs are content-addressed and may be shared.
- Intent target locks are reclaimed from stale owners during prepare.
- `TS_APPLY_FAILED_RETRYABLE` owners are reclaimable once expired; before expiry they are treated as active.
- Commit conflict/retryable diagnostics are persisted onto each intent (`apply_error_*`) when possible.
- Intent index cleanup is retried and verified but remains best-effort.

## Relevant Code

- gRPC service: `service/src/main/java/ai/floedb/floecat/service/transaction/impl/TransactionsServiceImpl.java`
- Intent applier: `service/src/main/java/ai/floedb/floecat/service/transaction/impl/TransactionIntentApplierSupport.java`
- Intent repo: `service/src/main/java/ai/floedb/floecat/service/repo/impl/TransactionIntentRepository.java`
- REST bridge: `protocol-gateway/iceberg-rest/src/main/java/ai/floedb/floecat/gateway/iceberg/rest/services/table/TransactionCommitService.java`
- Single-table entrypoint: `protocol-gateway/iceberg-rest/src/main/java/ai/floedb/floecat/gateway/iceberg/rest/services/table/TableCommitService.java`
