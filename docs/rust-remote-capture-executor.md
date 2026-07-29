# Rust Remote Capture Executor

## Overview
This page describes how to replace the current Java `EXEC_FILE_GROUP` worker with a Rust remote
worker that speaks Floecat's leased reconcile protocol directly.

> [!WARNING]
> This protocol revision is breaking. `SubmitLeasedFileGroupExecutionResult` and its chunked
> result flow were removed and replaced by `CommitLeasedFileGroupResult`, which commits
> executor-written immutable artifact descriptors. Old workers receive `UNIMPLEMENTED` from a new
> control plane, and new workers cannot submit results to an old control plane. Mixed-version
> worker/control-plane deployments are unsupported: drain leased work, stop the old worker fleet,
> deploy the control plane and matching workers as one coordinated cutover, then resume leasing.

The current protocol also adds content-state, realized-stats-selector, and
realized-index-selector fields. These protobuf additions are wire-compatible, but some values are
conditionally required by the control plane. Regenerate Rust protobuf bindings from `core/proto`
and deploy matching workers with the control plane. An old file-group worker can continue to
submit stats-only results, but without `FileGroupResultPayload.realized_stats_selectors` the
control plane cannot record the concrete name/field-ID aliases or resolved default columns that
were materialized. That can cause a later request for equivalent coverage to be captured again.
An old worker also cannot complete default page-index capture because it cannot populate
`FileGroupResultPayload.realized_index_selectors`.

The file-group result contract also requires file stats for auxiliary delete artifacts recorded in
`file_execution_plans`: Iceberg position/equality delete files and on-disk Delta deletion vectors.
The protobuf fields are not new, but treating them as required stats coverage is a semantic contract
change. A worker that publishes only the paths in `file_paths` can commit an incomplete result that
is later rejected by snapshot finalization.

The goal is not to embed Rust into the JVM. The goal is to run a separate Rust process that:

1. Leases eligible reconcile jobs from the control plane.
2. Fetches the standalone file-group execution payload for each leased job.
3. Reads parquet files and computes stats and parquet page-index sidecars.
4. Submits success or failure back through the control plane.

If you only need file-group capture replacement, you do not need to replace the Java planner
workers. `PLAN_CONNECTOR`, `PLAN_TABLE`, `PLAN_VIEW`, and `PLAN_SNAPSHOT` can remain in the
existing JVM control plane or executor fleet.

Query-driven stats-only work does not carry a request-origin marker. Content-state coverage decides
whether execution is required. For genuinely missing coverage, the JVM snapshot planner attempts
connector-native direct stats first and can enqueue `EXEC_FILE_GROUP` jobs when the connector cannot
satisfy the request directly.

If a remote implementation also owns `PLAN_SNAPSHOT`, it must preserve the leased snapshot task's
`source_revision`, `metadata_fingerprint`, and complete `requested_coverage` in its successful
planned task. Dropping those fields disables or corrupts content-state deduplication. A remote
snapshot finalizer must likewise populate the realized-selector fields described below.

## What You Are Replacing
The current JVM path for file-group execution is:

- `RemoteReconcileExecutorPoller` leases `EXEC_FILE_GROUP` jobs.
- `RemoteFileGroupReconcileExecutor` fetches `LeasedFileGroupExecution`.
- `StandaloneJavaFileGroupExecutionRunner` performs the actual parquet work.
- `CommitLeasedFileGroupResult` durably accepts the immutable result and completes the job, then
  stages its stats and index-artifact pointer metadata and writes a prepared marker.

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
- `CommitLeasedFileGroupResult`
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

The worker participates only in the lease-coordination domain. Canonical reconcile job indexes stay
owned by control-plane job-state transitions, and remote workers should not assume reads or
maintenance will repair queue drift for them.

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
  → CommitLeasedFileGroupResult(success)
```

The failure path is:

```text
LeaseReconcileJob
  → StartLeasedReconcileJob
  → GetLeasedFileGroupExecution
  → run parquet capture
  → CommitLeasedFileGroupResult(failure)
  → CompleteLeasedReconcileJob(RCS_FAILED)
```

During execution:

- renew the lease before expiry
- report progress periodically
- check cancellation periodically
- stop work if the lease is no longer valid
- once the worker has durably submitted a handled completion, stop heartbeats and do not perform a
  post-completion lease confirmation renew

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
- `execution_schema_json`
- `file_execution_plans`
- `capture_policy`
- `stats_object_prefix`

For a Rust worker, `source_connector` is important because it carries the resolved upstream
connector definition and auth material needed to read source files.

Treat the complete `capture_policy` as the execution contract. Its outputs, per-column settings,
default column scope, maximum default-column count, and opaque `properties` map are all forwarded
to the worker. Engines may interpret property keys they own and should preserve unknown keys when
passing the policy between worker components.

`file_paths` contains the data files assigned to the group. `file_execution_plans` carries the
per-data-file format metadata and any attached delete artifacts:

- `iceberg_delete_files` contains Iceberg position/equality delete files. One delete file may be
  attached to multiple data files and therefore may occur in more than one file group.
- `deletion_vector` contains the Delta deletion vector attached to that data file. Storage types
  `u` and `p` are on-disk vectors; use the exact `path_or_inline_dv` value as the file-stat target
  path. Storage type `i` is inline and is not currently supported by the JVM capture path.

The data-file paths remain the unit of execution progress and `ReconcileFileResult` reporting.
Attached delete artifacts add stats objects, but do not add successful-file results and do not get
page-index artifacts.

## Result Contract
`CommitLeasedFileGroupResult` has two outcomes:

- `success`
- `failure`

Both require `result_id`.

The worker uploads immutable stats and index-artifact wrapper objects, then sends their compact
descriptors with `success`. Floecat durably accepts the immutable result and makes the file-group
job terminal, then idempotently protects the referenced objects, stages their generation-scoped
pointer mappings without reading them, and writes a digest-bound prepared marker. Completion and
pointer staging are ordered, not one atomic storage transaction. The
`artifact_uri` inside an index wrapper may name external storage; Floecat does not read, copy, or
clean up that sidecar.

The sidecar and its wrapper have separate placement rules:

- The worker may choose the actual sidecar URI recorded in `IndexArtifactRecord.artifact_uri`.
- The serialized `IndexArtifactRecord` wrapper must be written below
  `<stats_object_prefix>index-artifacts/`.
- Its wrapper URI must be
  `<stats_object_prefix>index-artifacts/<sha256(target_storage_id)>/<payload_sha256>.pb`, using
  lowercase hexadecimal SHA-256 values.

The fenced wrapper prefix prevents one lease from registering another worker's metadata. It does
not move, copy, or constrain the referenced sidecar.

Success carries:

- `result_id`
- `result_descriptor`
- `file_stats`
- `index_artifacts`

Each `StatsObjectDescriptor` carries the immutable object's target storage ID, payload URI, byte
length, and SHA-256. File-stats and index descriptors for the same source file may share a target
path, but stats and index target storage IDs use different formats. A file-stats target is
`file-<sha256>` where the digest is SHA-256 over the byte `F`, byte `0x1f`, and the UTF-8 bytes of
the trimmed file path. A file index target is `file:<source-file-path>`. Their payload URIs identify
the distinct protected objects.

Failure carries:

- `result_id`
- `message`

The service enforces top-level idempotency on `job_id + result_id` and also keeps per-item
idempotency for stats and artifact writes. This gives you safe replay semantics if the worker loses
the gRPC response and retries the same submission.

## Result ID Rules
Scope the `result_id` to a single **execution attempt** by including the `lease_epoch`. Within one
lease — including network retries of the same submission — reuse the same `result_id`. A re-leased
retry runs under a fresh `lease_epoch`, and therefore a fresh `result_id`.

Required shape:

```text
<job_id>:<plan_id>:<group_id>:<lease_epoch>:success
<job_id>:<plan_id>:<group_id>:<lease_epoch>:failure
```

Both the Java file-group executor and the remote executor follow this shape.

Do not reuse one `result_id` for different payloads. The control plane rejects replay with the same
`result_id` if the immutable result descriptor or either artifact-descriptor list changes. Two
different lease attempts must not share a `result_id`, because the later attempt would conflict with
the durable result accepted for the earlier attempt. Including the `lease_epoch` guarantees that.

## Idempotency and Retry Semantics
The worker should assume the following:

- `CommitLeasedFileGroupResult` is safe to retry only if the same `result_id` and the
  same descriptor and descriptor lists are reused.
- success and failure are different outcomes and must not share a `result_id`.
- a successful commit does not mark the file-group job terminal until pointer staging and the
  prepared marker are complete.
- a retry of the exact success submission is required after a timeout, retryable error, or uncertain
  outcome, even when the job is already terminal. This is a replay of the accepted result, not a
  second logical completion.
- a failed or uncertain commit may leave partial pointer or protection metadata. An exact replay
  resumes staging; finalization waits for the prepared marker. Finalization or
  abandoned-generation cleanup removes the protections.

Recommended retry behavior:

1. Generate one `result_id` per execution attempt (include the `lease_epoch`); a re-lease produces a new one.
2. If the submit RPC times out or the response is lost, retry the same request unchanged.
3. Once the durable result is accepted, stop heartbeats. If staging is incomplete, continue retrying
   the same success request without renewing the cleared lease.
4. Treat `accepted=true` as confirmation that both durable completion and metadata staging
   succeeded.

## Cancellation and Lease Handling
The worker should treat lease expiry and cancellation as first-class control signals.

Recommended loop:

1. Start a heartbeat task after `StartLeasedReconcileJob`.
2. Call `RenewReconcileLease` on a cadence comfortably below `lease-ms`.
3. Treat `renewed=false` as loss of ownership and stop work.
4. Poll `GetReconcileCancellation` or rely on the cancellation flag returned by renew/progress.
5. If cancellation is requested, stop execution and submit:
   - `CommitLeasedFileGroupResult(failure)` only if you want a durable failure payload, or
   - no result payload if no per-file result should be persisted
6. Finish with `CompleteLeasedReconcileJob(RCS_CANCELLED)` when appropriate.

For worker implementations that use handled completion semantics, lease ownership ends when the
handled completion RPC is durably accepted by the control plane. After that point the worker should
not send another `RenewReconcileLease` as a final confirmation step, because the service may have
already cleared the lease as part of successful completion.

## What the Rust Worker Must Produce
The service expects the same logical outputs the Java runner currently produces:

- one directly uploaded protobuf blob per requested `TargetStatsRecord`
- worker-chosen parquet sidecars plus one fenced, hash-addressed `IndexArtifactRecord` wrapper per
  sidecar
- a bounded `FileGroupResultPayload` containing compact file-stats and index-wrapper descriptors
- the concrete column selectors represented by the published column-stat aggregates, repeated in
  `FileGroupResultPayload.realized_stats_selectors`
- the concrete index selectors present in the published wrappers, repeated in
  `FileGroupResultPayload.realized_index_selectors`
- a `ReconcileFileGroupResultDescriptor` sent in the success RPC

The worker is responsible for ensuring:

- when stats are requested, `file_stats` contains exactly one target per planned data file plus one
  target for every distinct attached Iceberg delete file and on-disk Delta deletion vector in the
  group
- duplicate auxiliary targets within a group are emitted only once; the same Iceberg delete file
  may legitimately recur in different groups when it applies to data files in those groups
- auxiliary delete stats carry `FC_POSITION_DELETES` or `FC_EQUALITY_DELETES` as appropriate and
  are excluded from table/column aggregate partials
- `file_results`, planned/succeeded file counts, and page-index coverage continue to count only the
  planned data files, while file-stats descriptor counts include auxiliary delete targets
- every planned file requested for page-index capture gets a matching artifact
- artifact metadata matches the target file identity
- every referenced stats object, sidecar, and index wrapper is committed before the success RPC
- every index wrapper uses the leased `stats_object_prefix` plus the required
  `index-artifacts/<target-hash>/<payload-hash>.pb` suffix
- every index wrapper records its concrete comma-separated selector set in the
  `indexed_columns` property
- `realized_stats_selectors` is the sorted, distinct selector set represented by the file group's
  column-stat aggregates; omit it when column-stats output was not requested
- report every equivalent selector known to the worker, such as both an Iceberg `#<field-id>` and
  its column name, so content-state deduplication can recognize later requests using either form
- `realized_index_selectors` is the sorted, distinct selector set represented by the file group's
  index wrappers; omit it only when page-index output was not requested
- explicit selectors need not appear verbatim when the persisted artifacts use an equivalent
  alias; an explicit field-ID request may therefore report its resolved name alias, but page-index
  capture for a non-empty group must still report at least one realized selector
- default index selection resolves to a non-empty selector set for non-empty snapshots, uses the
  same set for every file in the group, and does not exceed `max_default_columns` for `FIRST_N`
- every stats descriptor identifies its target storage ID and the object size and SHA-256
- the result descriptor's `artifact_references_sha256` is the canonical digest of its file-stats
  and index-artifact descriptor sets
- the result manifest size and SHA-256 match the uploaded payload

`CommitLeasedFileGroupResult` durably accepts the immutable result before staging its bounded
pointers and metadata-only prepared marker. Snapshot finalization waits for that marker. If the RPC
outcome is uncertain or retryable, submit the exact same result ID, descriptor, and descriptor lists
again; exact replay resumes staging without allowing a rejected lease to mutate generation pointers
and without re-reading the worker objects.

Compute `artifact_references_sha256` by feeding the following canonical bytes to SHA-256:

1. Encode the file-stats group, then the index-artifact group.
2. For each group, write its one-byte kind (`1` for file stats or `2` for index artifacts), followed
   by its descriptor count as an unsigned 32-bit big-endian integer.
3. Sort descriptors by target storage ID, payload URI, payload byte count, then lowercase payload
   SHA-256 hex. Compare the two string fields lexicographically as unsigned UTF-8 bytes; do not
   use UTF-16 code-unit ordering.
4. For each descriptor, write the UTF-8 target storage ID and payload URI as a 32-bit big-endian
   byte length followed by the bytes, write the payload byte count as a 64-bit big-endian integer,
   then write the binary payload SHA-256 as a 32-bit big-endian byte length followed by the bytes.

## File-Group Size Ceiling

Snapshot planning currently limits each file group to 128 files by default. Configure the planning
ceiling with:

```properties
floecat.reconciler.snapshot-plan.max-files-per-group=128
```

The planner clamps the configured value to at least one and partitions the immutable snapshot plan
accordingly. The service rejects submitted plans containing a group above the same configured
ceiling and validates each submitted result against that planned group, so an executor cannot add
files beyond its lease. Increasing this setting increases the maximum descriptor count,
pointer-staging work, request size, and resident metadata for one `CommitLeasedFileGroupResult`
call. Attached delete artifacts can make the file-stats descriptor count larger than the planned
data-file count. Keep the value bounded to the RPC deadline and message-size limits of the worker
deployment.

## Snapshot Finalizer Implications

The snapshot finalizer reads and SHA-verifies each bounded file-group result payload. It validates
the referenced stats and index descriptors but does not download their immutable objects. Its
worker-side workload is therefore proportional to planned-file, descriptor, and partial-aggregate
counts rather than to the referenced objects' byte volume. For each successful group it derives the
exact expected stats-target set from the immutable `file_execution_plans`: successful data files,
attached Iceberg delete files, and attached on-disk Delta deletion vectors. Missing or extra targets
are invalid.

An Iceberg delete file may be referenced by data files in different groups. Repeated references to
that target are execution overhead rather than additional logical files: a finalizer should verify
that repeated descriptors identify identical stats content and retain one snapshot-level target.
This comparison uses the descriptor's existing target ID, payload size, and payload SHA-256; it does
not require reading or hashing the delete-file content.

The finalizer's `SnapshotCaptureManifest` must carry each durable file-group descriptor, including
its `artifact_references_sha256`, but must not repeat the per-file stats or index descriptor lists.
It carries only file-group descriptors, snapshot-wide aggregate descriptors, and counts.
Data-file source/success counts do not include auxiliary delete artifacts; file-stats record counts
do include their group-level descriptors.

For column-stats capture, the finalizer must populate
`SnapshotCaptureManifest.realized_stats_selectors` with the sorted, distinct union reported by the
file groups for explicit selection. When default stats selection is in use, every file group must
report the same resolved selector set. A Delta column present in the snapshot schema but absent
from an older Parquet file is materialized as all-null stats rather than dropped. For `FIRST_N`,
the realized column count must not exceed `max_default_columns`.
Name/field-ID aliases for the same columns may both be present; when field-ID selectors are
available, the control plane counts those IDs rather than double-counting their name aliases.

For page-index capture, the finalizer must populate
`SnapshotCaptureManifest.realized_index_selectors` with the sorted, distinct selectors represented
by the activated index generation. Explicitly requested selectors need not be repeated verbatim
when the persisted artifacts use a different or unknown alias. For default selection on a
non-empty snapshot, the realized set must be non-empty, every file group must report the same set,
and the realized column count must not exceed `max_default_columns` for `FIRST_N`. A Delta column
present in the snapshot schema but absent from an older Parquet file is represented by synthetic
all-null V1 sidecar rows covering that file's row groups. The Java snapshot finalizer derives both
stats and index sets from the file-group payloads; a non-Java finalizer must perform the same
validation and aggregation.

The manifest must also repeat the leased capture policy exactly, including outputs, column
policies, default column scope, maximum default-column count, and the complete opaque properties
map. The control plane rejects policy drift during finalization.

`SubmitLeasedSnapshotFinalizeResult` reads the manifest once and performs one metadata-pointer
lookup per file group. It does not read file-group payloads or per-file objects. If any accepted
file group has not written its digest-bound prepared marker, the service returns a retryable error.
The finalizer must retry the exact same finalization result; it must not regenerate a different
result ID or manifest for that retry.

During successful publication, the control plane commits the stats-generation root and the index
generation's active and capture-manifest pointers in one atomic pointer batch. Readers therefore
cannot observe a finalized snapshot with only one generation activated. This publication fence is
internal to the control plane and requires no additional RPC or sequencing step from the external
finalizer.

The digest field is required and has no legacy fallback. Existing in-flight or persisted
file-group results without it must be drained or replanned before a finalizer using this contract
can complete them.

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
