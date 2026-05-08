# Rust Remote Capture Executor

## Overview
This page describes how to replace the current Java `EXEC_FILE_GROUP` worker with a Rust remote
worker that speaks Floecat's leased reconcile protocol directly.

The goal is not to embed Rust into the JVM. The goal is to run a separate Rust process that:

1. Leases eligible reconcile jobs from the control plane.
2. Fetches the standalone file-group execution payload for each leased job.
3. Reads parquet files and computes stats and parquet page-index sidecars.
4. Submits success or failure back through the control plane.

If you only need file-group capture replacement, you do not need to replace the Java planner
workers. `PLAN_CONNECTOR`, `PLAN_TABLE`, `PLAN_VIEW`, and `PLAN_SNAPSHOT` can remain in the
existing JVM control plane or executor fleet.

## What You Are Replacing
The current JVM path for file-group execution is:

- `RemoteReconcileExecutorPoller` leases `EXEC_FILE_GROUP` jobs.
- `RemoteFileGroupReconcileExecutor` fetches `LeasedFileGroupExecution`.
- `StandaloneJavaFileGroupExecutionRunner` performs the actual parquet work.
- `SubmitLeasedFileGroupExecutionResult` persists stats, index artifacts, and per-file results.

A Rust worker replaces the execution portion of that flow. It should behave like an external
implementation of the current worker contract, not like a new public API.

## Required Protocol Surface
At minimum, the Rust worker must implement these `ReconcileExecutorControl` RPCs from
[`docs/proto.md`](proto.md):

- `LeaseReconcileJob`
- `StartLeasedReconcileJob`
- `RenewReconcileLease`
- `ReportReconcileProgress`
- `GetReconcileCancellation`
- `GetLeasedFileGroupExecution`
- `SubmitLeasedFileGroupExecutionResult`
- `CompleteLeasedReconcileJob`

For a file-group-only worker, lease only `RJK_EXEC_FILE_GROUP`.

## Control-Plane Configuration
Run the service as the reconcile control plane:

```properties
%reconciler-control.floecat.reconciler.worker.mode=remote
%reconciler-control.reconciler.max-parallelism=0
%reconciler-control.floecat.reconciler.auto.enabled=true
```

Relevant shared settings:

```properties
floecat.reconciler.job-store=durable
floecat.reconciler.authorization.header=authorization
floecat.reconciler.oidc.issuer=https://<issuer>/realms/<realm>
floecat.reconciler.oidc.client-id=<reconcile-worker-client-id>
floecat.reconciler.oidc.client-secret=<reconcile-worker-client-secret>
floecat.reconciler.oidc.token-refresh-skew-seconds=30
floecat.reconciler.job-store.lease-ms=30000
```

The Rust worker should target the control-plane gRPC endpoint and acquire bearer tokens for the
configured reconciler service principal via client credentials. Worker RPCs should attach that
bearer token explicitly; they should not rely on propagated inbound user auth or global client
interceptor behavior for correctness.

## Worker Identity and Leasing
The lease request supports:

- execution classes
- lanes
- job kinds
- `executor_id`
- repeated `executor_ids`

For a Rust file-group worker, use:

- `job_kinds = [RJK_EXEC_FILE_GROUP]`
- `executor_id = <stable worker instance id>`
- `executor_ids` containing the executor implementations this process can satisfy

The current Java poller advertises local executor ids so pinned jobs can route to compatible
workers. A Rust fleet should do the same if you intend to use pinned executor routing.

## Execution Loop
The happy-path loop is:

```text
LeaseReconcileJob
  → StartLeasedReconcileJob
  → GetLeasedFileGroupExecution
  → run parquet capture
  → SubmitLeasedFileGroupExecutionResult(success)
  → CompleteLeasedReconcileJob(RCS_SUCCEEDED)
```

The failure path is:

```text
LeaseReconcileJob
  → StartLeasedReconcileJob
  → GetLeasedFileGroupExecution
  → run parquet capture
  → SubmitLeasedFileGroupExecutionResult(failure)
  → CompleteLeasedReconcileJob(RCS_FAILED)
```

During execution:

- renew the lease before expiry
- report progress periodically
- check cancellation periodically
- stop work if the lease is no longer valid

## Standalone File-Group Payload
`GetLeasedFileGroupExecution` returns the standalone worker payload. The important fields are:

- `job_id`
- `lease_epoch`
- `parent_job_id`
- `source_connector`
- `source_namespace`
- `source_table`
- `table_id`
- `snapshot_id`
- `plan_id`
- `group_id`
- `file_paths`
- `capture_policy`

For a Rust worker, `source_connector` is important because it carries the resolved upstream
connector definition and auth material needed to read source files.

## Result Contract
`SubmitLeasedFileGroupExecutionResult` has two outcomes:

- `success`
- `failure`

Both require `result_id`.

Success carries:

- `result_id`
- `stats_records`
- `index_artifacts`

Failure carries:

- `result_id`
- `message`

The service enforces top-level idempotency on `job_id + result_id` and also keeps per-item
idempotency for stats and artifact writes. This gives you safe replay semantics if the worker loses
the gRPC response and retries the same submission.

## Result ID Rules
For the same durable outcome, reuse the same `result_id` on retries.

Recommended shape:

```text
<job_id>:<plan_id>:<group_id>:success
<job_id>:<plan_id>:<group_id>:failure
```

That is the same stability rule the current Java file-group executor follows when `plan_id`
and `group_id` are available.

Do not reuse one `result_id` for different payloads. The control plane rejects replay with the
same `result_id` if the full request payload changes.

## Idempotency and Retry Semantics
The worker should assume the following:

- `SubmitLeasedFileGroupExecutionResult` is safe to retry only if the same `result_id` and the
  same payload are reused.
- success and failure are different outcomes and must not share a `result_id`.
- `CompleteLeasedReconcileJob` is a separate terminal-state RPC. Do not assume a successful result
  submit also marks the job terminal.

Recommended retry behavior:

1. Generate one stable `result_id` per durable success or failure outcome.
2. If the submit RPC times out or the response is lost, retry the same request unchanged.
3. If `CompleteLeasedReconcileJob` times out after a successful submit, retry completion with the
   same terminal counters/message.

## Cancellation and Lease Handling
The worker should treat lease expiry and cancellation as first-class control signals.

Recommended loop:

1. Start a heartbeat task after `StartLeasedReconcileJob`.
2. Call `RenewReconcileLease` on a cadence comfortably below `lease-ms`.
3. Treat `renewed=false` as loss of ownership and stop work.
4. Poll `GetReconcileCancellation` or rely on the cancellation flag returned by renew/progress.
5. If cancellation is requested, stop execution and submit:
   - `SubmitLeasedFileGroupExecutionResult(failure)` only if you want a durable failure payload, or
   - no result payload if no per-file result should be persisted
6. Finish with `CompleteLeasedReconcileJob(RCS_CANCELLED)` when appropriate.

## What the Rust Worker Must Produce
The service expects the same logical outputs the Java runner currently produces:

- `TargetStatsRecord` values for requested capture outputs
- `LeasedFileGroupIndexArtifact` records with:
  - `IndexArtifactRecord`
  - raw artifact bytes
  - content type

The worker is responsible for ensuring:

- every planned file requested for page-index capture gets a matching artifact
- artifact metadata matches the target file identity
- null or missing outputs are not sent for required planned files

## Minimal Architecture
A practical Rust implementation usually has these pieces:

- protobuf-generated Rust client/server types for `core/proto`
- a gRPC client for `ReconcileExecutorControl`
- a lease manager
- a heartbeat/cancellation task
- a parquet execution engine
- an adapter that converts engine outputs into Floecat protobuf messages

Keep the protobuf adapter isolated from the parquet engine. That makes it easier to test retry and
idempotency behavior separately from file scanning logic.

## Recommended Integration Strategy
Start small:

1. Implement a Rust worker that only leases `RJK_EXEC_FILE_GROUP`.
2. Initially support `requestsStats=false` / `capturePageIndex=false` no-op file groups correctly.
3. Add stats capture.
4. Add parquet page-index artifact generation.
5. Run the Rust worker alongside the existing JVM planner workers.
6. Disable `floecat.reconciler.executor.remote-file-group.enabled` on JVM executor nodes once the
   Rust worker is ready to own all file-group jobs.

This keeps the planner/control-plane behavior stable while you replace only the parquet execution
layer.

## Non-Goals
This worker does not need to:

- implement public catalog CRUD APIs
- replace `ReconcileControl`
- replace planner workers unless you want full non-JVM reconcile
- embed into the Quarkus service process

## Troubleshooting
### Duplicate submit rejected
Likely cause:

- same `result_id`, different success/failure payload

Fix:

- make `result_id` stable per durable outcome
- retry with identical payload bytes

### Lease lost during long parquet work
Likely cause:

- renew cadence too slow
- worker blocked heartbeat thread

Fix:

- renew on a dedicated async task
- renew well before `lease-ms`

### Job completed but artifacts missing
Likely cause:

- page-index capture returned incomplete artifact set

Fix:

- validate one artifact per planned file before calling success submit

## Cross-References
- Reconcile architecture: [`reconciler.md`](reconciler.md)
- RPC contracts: [`proto.md`](proto.md)
- Operations and split deployment: [`operations.md`](operations.md)
- Docker split deployment examples: [`docker.md`](docker.md)
