# Transactions (gRPC Core)

This document describes Floecat's core gRPC transaction mechanism used for multi-table commits.
It is the backend for the Iceberg REST `/transactions/commit` flow.

## Overview

Transactions provide an optimistic, lock-free, multi-table commit mechanism:

- **Begin** creates a transaction in `TS_OPEN`.
- **Prepare** validates preconditions, persists intent payloads, and moves to `TS_PREPARED`.
- **Commit** validates intent versions, marks `TS_COMMITTED`, and applies intents asynchronously.
- **Abort** marks `TS_ABORTED` and cleans up intents.

All operations are **idempotent** when an idempotency key is provided.

## State Model

- `TS_OPEN`: new transaction, no intents persisted yet.
- `TS_PREPARED`: intents persisted and validated.
- `TS_COMMITTED`: transaction commit decision is durable; intents may still be pending apply.
- `TS_ABORTED`: transaction was aborted (explicitly or due to conflicts).
- `TS_APPLY_FAILED_CONFLICT`: commit decision is durable, but apply failed due to irreconcilable
  conflicts (intents retain diagnostics).

## Data Model

**TxChange**
- Targets a table by ID or fully qualified name.
- `table_fq` format is `catalog.namespace1.namespace2.table` (dot-separated, no escaping).
- Includes exactly one payload source: full Table proto, opaque bytes, or an `intended_blob_uri`.
- Optional precondition on the target pointer version.

**TransactionIntent**
- One intent per target pointer.
- Stores the blob URI to apply and the expected pointer version.

## Flow Details

### BeginTransaction
1. Validate caller permissions and account.
2. Create a `Transaction` with state `TS_OPEN`.
3. Persist the transaction record.
4. Return the transaction (idempotent by key).

### PrepareTransaction
1. Load the transaction (must be `TS_OPEN`).
2. For each `TxChange`:
   - Resolve table ID.
   - Read current pointer version.
   - Validate precondition (if provided).
   - Persist payload to blob store (table proto or raw payload).
   - Create a `TransactionIntent` with target pointer + expected version.
3. Persist intents.
4. Transition the transaction to `TS_PREPARED`.
5. Return the updated transaction (idempotent by key).

### CommitTransaction
1. Load the transaction (must be `TS_PREPARED`).
2. Re-check intent expected versions against current pointer versions.
3. Transition the transaction to `TS_COMMITTED`.
4. Apply intents best-effort (may be incomplete):
   - CAS the target pointer to the intent blob URI.
   - If table-by-id pointer changed, update table-by-name pointer.
5. Return the committed transaction (idempotent by key).

### AbortTransaction
1. Load the transaction.
2. Transition to `TS_ABORTED`.
3. Delete intents for the transaction.
4. Return the aborted transaction.

## Idempotency

Begin, prepare, and commit accept idempotency keys. If a request is retried:

- The same transaction record is returned.
- The operation will not be re-applied.

## Failure and Concurrency Behavior

- **Precondition failure** during prepare aborts the operation and the transaction remains open. Intents
  are persisted only if all changes validate; payload blobs may already be written and are not
  referenced by any intent.
- **Version conflict** during commit aborts the transaction and returns a conflict error.
- **Apply state** is derived from intents: a transaction is fully applied once it has no intents.
- **Best-effort apply** means post-commit pointer updates are retried by background workers if needed.
- **Irreconcilable apply conflicts** transition the transaction to `TS_APPLY_FAILED_CONFLICT` and
  persist per-intent diagnostics for inspection.

## Relevant Code

- gRPC service: `service/src/main/java/ai/floedb/floecat/service/transaction/impl/TransactionsServiceImpl.java`
- Intent applier: `service/src/main/java/ai/floedb/floecat/service/transaction/impl/TransactionIntentApplier.java`
- Storage keys: `service/src/main/java/ai/floedb/floecat/service/repo/model/Keys.java`
