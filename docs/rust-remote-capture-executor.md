# Rust Remote Capture Executor

## Overview
This page describes a Rust remote `EXEC_FILE_GROUP` worker that speaks Floecat's leased reconcile
protocol directly.

Generate Rust protobuf bindings from `core/proto` and deploy the worker with the matching control
plane. The control plane validates conditionally required fields, bundle metadata, immutable plan
identity, realized selector coverage, and artifact digests.

File-group results include file stats for all requested data files and for auxiliary delete
artifacts recorded in `file_execution_plans`: Iceberg position/equality delete files and on-disk
Delta deletion vectors. Page indexes remain data-file outputs only.

The goal is not to embed Rust into the JVM. The goal is to run a separate Rust process that:

1. Leases eligible reconcile jobs from the control plane.
2. Fetches the standalone file-group execution payload for each leased job.
3. Loads selected reusable artifact bundles and reads source parquet only for outputs that are not
   reusable.
4. Computes missing stats and parquet page-index sidecars.
5. Publishes one immutable artifact bundle and submits success or failure to the control plane.

File-group execution is independently deployable. `PLAN_CONNECTOR`, `PLAN_TABLE`, `PLAN_VIEW`, and
`PLAN_SNAPSHOT` can run in the JVM control plane or executor fleet while Rust workers lease
`EXEC_FILE_GROUP` jobs.

Query-driven stats-only work does not carry a request-origin marker. Content-state coverage decides
whether execution is required. For genuinely missing coverage, the JVM snapshot planner attempts
connector-native direct stats first and can enqueue `EXEC_FILE_GROUP` jobs when the connector cannot
satisfy the request directly.

If a remote implementation also owns `PLAN_SNAPSHOT`, it must preserve the leased snapshot task's
`source_revision`, `metadata_fingerprint`, and complete `requested_coverage` in its successful
planned task. Dropping those fields disables or corrupts content-state deduplication. A remote
snapshot finalizer must likewise populate the realized-selector fields described below. Planner
and finalizer implementations must use bindings generated from the same protobuf version as the
control plane: reusable-artifact indexes use format-1 immutable sorted runs, and the former
trie-root format is not accepted.

## Execution Architecture
The leased file-group execution path is:

- `RemoteReconcileExecutorPoller` leases `EXEC_FILE_GROUP` jobs.
- `RemoteFileGroupReconcileExecutor` fetches `LeasedFileGroupExecution`.
- `StandaloneJavaFileGroupExecutionRunner` performs the actual parquet work.
- `CommitLeasedFileGroupResult` durably accepts the immutable result and completes the job, then
  stages its stats and index-artifact pointer metadata and writes a prepared marker.

A Rust worker implements the execution portion of that flow as an external implementation of the
worker contract.

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
- `storage_location`
- `table_id`
- `snapshot_id`
- `plan_id`
- `group_id`
- `file_paths`
- `result_payload_uri`
- `stats_object_prefix`
- `execution_schema_json`
- `file_execution_plans`
- `capture_policy`
- `index_predecessor`
- `predecessor_index_artifacts`

For a Rust worker, `source_connector` is important because it carries the resolved upstream
connector definition and auth material needed to read source files.

Treat the complete `capture_policy` as the execution contract. Its outputs, per-column settings,
default column scope, maximum default-column count, and opaque `properties` map are all forwarded
to the worker. Engines may interpret property keys they own and should preserve unknown keys when
passing the policy between worker components.

`result_payload_uri` is the server-allocated destination for `FileGroupResultPayload`, and
`stats_object_prefix` fences the bundle objects published by this execution. When page indexes are
requested, echo the pinned `index_predecessor` through the result descriptor. The predecessor
artifact list contains active index-wrapper metadata; it does not require reading the sidecars
named by those wrappers.

`file_paths` contains the data files assigned to the group. `file_execution_plans` carries the
per-data-file format metadata, reuse identity, and any attached delete artifacts:

- `iceberg_delete_files` contains Iceberg position/equality delete files. One delete file may be
  attached to multiple data files and therefore may occur in more than one file group. Each delete
  file carries its own `content_identity`.
- `deletion_vector` contains the Delta deletion vector attached to that data file. Storage types
  `u` and `p` are on-disk vectors; use the exact `path_or_inline_dv` value as the file-stat target
  path. Storage type `i` is inline and is not currently supported by the JVM capture path.
- `content_identity` is connector-planned immutable physical identity. An empty value forbids
  cross-snapshot reuse for that data file.
- `source_fingerprint` and `auxiliary_stats_fingerprints` bind stats to the complete source context,
  including schema, partitions, delete files, and deletion vectors.
- `index_source_fingerprint` binds page-index reuse to the physical data file.
- `stats_capture_signature` and `index_capture_signature` bind artifacts to the requested capture
  policy.
- `reusable_artifact_bundle_selections` identifies the stats and index records that can be loaded
  from previously finalized group bundles.

The data-file paths remain the unit of execution progress and `ReconcileFileResult` reporting.
Attached delete artifacts add stats objects, but do not add successful-file results and do not get
page-index artifacts.

## Reuse Execution

Bundle selections are authoritative inputs from snapshot planning. Deduplicate selections by
`artifact.payload_uri`, fetch each selected object once for the file group, verify its byte length
and SHA-256, parse `ReusableArtifactBundlePayload` format version 1, and index its records by file
path.

For every selected record, validate the target path, source fingerprint, and capture signature
against its `FileExecutionPlan`. Rebind valid records to the leased destination table and snapshot.
Stats and page indexes are selected independently, so a file may reuse either output while the
worker computes the other. Reused page indexes retain their `artifact_uri`; the worker does not
read or rewrite the sidecar bytes.

If a required output has no bundle selection, execute the corresponding source-file capture. The
reuse path reads the compact protobuf bundle, not the source parquet data or the page-index
sidecar.

## Result Contract
`CommitLeasedFileGroupResult` has two outcomes:

- `success`
- `failure`

Both require `result_id`.

The worker serializes all file-stat records and index-artifact wrappers for the group into one
`ReusableArtifactBundlePayload` with `format_version = 1`. Publish it at:

```text
<stats_object_prefix>reuse-bundles/<payload_sha256_hex>.pb
```

The bundle's `StatsObjectDescriptor` uses `target_storage_id = reuse-bundle:<group_id>` and records
the exact payload URI, byte length, and binary SHA-256. A `ReusableArtifactBundleReference` combines
that descriptor with manifest-resident compatibility metadata for every bundled stats and index
record.

The worker also publishes `FileGroupResultPayload` at the leased `result_payload_uri`. Its
`file_stats` and `index_artifacts` entries are target mappings: each entry has its target storage ID
and the shared bundle URI, size, and digest. The payload includes the complete
`reusable_artifact_bundle` reference, per-file results, aggregate partials, realized selectors, and
the pinned index predecessor when page indexes are requested.

The actual page-index sidecar URI remains worker-controlled through
`IndexArtifactRecord.artifact_uri`. Commit newly generated sidecars before publishing the bundle.
For a reused index record, retain the existing sidecar URI and publish no sidecar bytes. Floecat
does not copy, move, or read sidecars during group commit or snapshot finalization.

Success carries:

- `result_id`
- `result_descriptor`
- `artifact_bundle`

`FileGroupArtifactBundleDescriptor.artifact` is the bundle descriptor. Its
`file_stats_target_storage_ids` and `index_artifact_target_storage_ids` fields enumerate the target
mappings carried by that bundle. A file-stats target is `file-<sha256>` where the digest is SHA-256
over the byte `F`, byte `0x1f`, and the UTF-8 bytes of the trimmed file path. A file-index target is
`file:<source-file-path>`.

Floecat durably accepts the result and makes the file-group job terminal, then idempotently
protects the bundle, stages all generation-scoped target mappings without reading the bundle, and
writes a digest-bound prepared marker. Completion and pointer staging are ordered operations.

Failure carries:

- `result_id`
- `message`

The service enforces idempotency on `job_id + result_id` and on the staged target mappings. This
provides safe replay when the worker loses the gRPC response and retries the same submission.

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
`result_id` if the immutable result descriptor or artifact-bundle mapping changes. Two different
lease attempts must not share a `result_id`, because the later attempt would conflict with the
durable result accepted for the earlier attempt. Including the `lease_epoch` guarantees that.

## Idempotency and Retry Semantics
The worker should assume the following:

- `CommitLeasedFileGroupResult` is safe to retry only if the same `result_id` and the
  same result descriptor and artifact-bundle descriptor are reused.
- success and failure are different outcomes and must not share a `result_id`.
- the durable result makes the file-group job terminal before pointer staging; `accepted=true` is
  returned only after staging and the prepared marker are complete.
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

## Worker Outputs
The service requires:

- one `ReusableArtifactBundlePayload` containing the group's file stats and index wrappers
- worker-chosen parquet sidecars for newly generated page indexes
- a bounded `FileGroupResultPayload` containing target mappings and compatibility metadata for the
  artifact bundle
- the concrete column selectors represented by the published column-stat aggregates, repeated in
  `FileGroupResultPayload.realized_stats_selectors`
- the concrete index selectors present in the published wrappers, repeated in
  `FileGroupResultPayload.realized_index_selectors`
- a `ReconcileFileGroupResultDescriptor` and `FileGroupArtifactBundleDescriptor` sent in the
  success RPC

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
- every referenced stats object, generated sidecar, index wrapper, and artifact bundle is
  committed before the success RPC
- every index wrapper uses the leased `stats_object_prefix` plus the required
  `index-artifacts/<target-hash>/<payload-hash>.pb` suffix
- the artifact bundle uses the leased `stats_object_prefix` and the required
  `reuse-bundles/<payload-sha256>.pb` suffix
- each `TargetStatsRecord` carries `floedb.reconcile.source-fingerprint-v1`,
  `floedb.reconcile.stats-signature-v1`, and
  `floedb.reconcile.realized-stats-selectors-v2` properties matching its execution plan
- each `IndexArtifactRecord` carries `floedb.reconcile.source-fingerprint-v1` and
  `floedb.reconcile.index-signature-v1` properties matching its execution plan
- every index wrapper records its concrete selector set as a JSON string array in the shared
  `indexed_columns` property
- every reusable index metadata entry repeats that wrapper's sorted, distinct selector set in
  `realized_index_selectors`; entries with an empty set cannot be selected for reuse
- `realized_stats_selectors` is the sorted, distinct selector set represented by the file group's
  column-stat aggregates; omit it when column-stats output was not requested
- report every equivalent selector known to the worker, such as both an Iceberg `#<field-id>` and
  its column name, so content-state deduplication can recognize later requests using either form
- `realized_index_selectors` is the sorted, distinct selector set represented by the file group's
  index wrappers; omit it only when page-index output was not requested
- every explicitly requested selector must appear verbatim in the corresponding realized-selector
  list; report equivalent aliases in addition so later requests can reuse the same artifacts
- default index selection resolves to a non-empty selector set for non-empty snapshots, uses the
  same set for every file in the group, and does not exceed `max_default_columns` for `FIRST_N`
- the reusable bundle reference contains exactly one compatibility entry for each bundled target
- the commit's artifact-bundle target ID lists exactly match the target mappings in
  `FileGroupResultPayload`
- the result descriptor's `artifact_references_sha256` is the canonical digest of its file-stats
  and index-artifact descriptor sets
- the result descriptor's payload size and SHA-256 match the uploaded `FileGroupResultPayload`

`CommitLeasedFileGroupResult` durably accepts the immutable result before staging its bounded
pointers and metadata-only prepared marker. Snapshot finalization waits for that marker. If the RPC
outcome is uncertain or retryable, submit the exact same result ID, result descriptor, and
artifact-bundle descriptor again. Exact replay resumes staging without reading the bundle.

Compute `artifact_references_sha256` over expanded target mappings. For each file-stats and index
target ID in `FileGroupArtifactBundleDescriptor`, copy the shared bundle artifact descriptor and
replace `target_storage_id` with that target ID. Feed those descriptors to SHA-256 as follows:

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
the artifact-bundle reference and target mappings but does not read the bundle payload, source
parquet files, or page-index sidecars. Its worker-side workload is therefore proportional to
planned-file, mapping, compatibility-metadata, and partial-aggregate counts. For each successful
group it derives the exact expected stats-target set from the immutable `file_execution_plans`:
successful data files, attached Iceberg delete files, and attached on-disk Delta deletion vectors.
Missing or extra targets are invalid.

An Iceberg delete file may be referenced by data files in different groups. Repeated references to
that target are execution overhead rather than additional logical files. The finalizer verifies
equivalent reusable stats metadata, selects one canonical bundle mapping for the snapshot-level
target, and retains that target's compatibility metadata on its owning bundle. It does not read or
hash delete-file content.

The finalizer's `SnapshotCaptureManifest` must carry each durable file-group descriptor, including
its `artifact_references_sha256`, and one normalized `reusable_artifact_bundles` entry per current
file group. These bundles are a publication delta. When the leased plan has an `append_only_base`,
the finalizer must return it unchanged, including its opaque format-1 `reusable_artifact_index`,
but must leave `SnapshotCaptureManifest.reusable_artifact_index` unset. That output field is service-owned;
the service rejects a worker-supplied value, authenticates the prior index, applies the delta, and
publishes the complete immutable run set.
The manifest also carries snapshot-wide aggregate descriptors and counts. Data-file source/success
counts do not include auxiliary delete artifacts; file-stats record counts include their
group-level target mappings.

An external `PLAN_SNAPSHOT` implementation that offers reuse must use the same lookup semantics as
the control plane: authenticate every fetched index object by length and SHA-256, batch-fetch run
filters and manifests, and read only candidate data blocks. It must not issue an object-store
request per planned source file, and it never needs to read source Parquet, bundle payloads, or
page-index sidecars to decide compatibility. A missing run object makes the base unavailable for
reuse; corrupt authenticated metadata is a terminal invalid-base error. The run filter encoding
and compaction layout are internal, so an external implementation should use a shared compatible
index library rather than manufacture or compact run objects itself.

These requirements do not change the standalone Rust `EXEC_FILE_GROUP` contract. A deployment
that externalizes only file-group execution can treat the run index as opaque and needs no new
object-store access pattern.

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
by the activated index generation. Every explicitly requested selector must be repeated verbatim;
known equivalent aliases may be included in addition. For default selection on a
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
lookup per file group. It does not read file-group payloads, artifact bundles, source files, or
sidecars. If any accepted file group has not written its digest-bound prepared marker, the service
returns a retryable error. The finalizer must retry the exact same finalization result and manifest.

During successful publication, the control plane commits the stats-generation root and the index
generation's active and capture-manifest pointers in one atomic pointer batch. Readers therefore
cannot observe a finalized snapshot with only one generation activated. This publication fence is
internal to the control plane and requires no additional RPC or sequencing step from the external
finalizer.

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

## Recommended Deployment Strategy
Start small:

1. Implement a Rust worker that only leases `RJK_EXEC_FILE_GROUP`.
2. Initially support `requestsStats=false` / `capturePageIndex=false` no-op file groups correctly.
3. Add stats capture.
4. Add parquet page-index artifact generation.
5. Run the Rust worker with the JVM planner workers.
6. Disable `floecat.reconciler.executor.remote-file-group.enabled` on JVM executor nodes once the
   Rust worker is ready to own all file-group jobs.

This assigns parquet execution to Rust while retaining the planner and control-plane services.

## Non-Goals
This worker does not need to:

- implement public catalog CRUD APIs
- implement `ReconcileControl`
- implement planner workers unless the deployment also moves planning out of the JVM
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
