# Reconciler

## Overview
The `reconciler/` module automates ingestion from upstream connectors. It manages a durable queue of
reconciliation jobs, leases work to workers, instantiates connectors via the SPI, and calls the
service's gRPC APIs to create or update tables, views, snapshots, statistics, and index artifacts.

This component decouples connector execution from the main service so long-running metadata and
file-scoped execution work do not block public gRPC threads. The service submits capture jobs via
`ReconcileControl.StartCapture`, which creates jobs in the reconciler's store.

The current job model is split by responsibility:

- **`PLAN_CONNECTOR`**: top-level connector discovery job. Plans table and view work and enqueues
  child planning jobs.
- **`PLAN_TABLE`**: child table planning job. Ensures destination table metadata exists and enqueues
  snapshot planning work for snapshots that need processing.
- **`PLAN_VIEW`**: child view planning job. Creates or updates exactly one destination view.
- **`PLAN_SNAPSHOT`**: child snapshot planning job. Freezes the immutable snapshot plan on the
  parent job payload, records explicit file-group coverage metadata, and enqueues
  `EXEC_FILE_GROUP` plus `FINALIZE_SNAPSHOT_CAPTURE` children.
- **`EXEC_FILE_GROUP`**: child execution job. Reads the planned source parquet files through
  `FloecatConnector` when required, reuses compatible finalized artifacts selected by the snapshot
  planner, captures missing file-target stats, generates missing parquet sidecar index artifacts,
  and publishes one immutable artifact bundle for the group.
- **`FINALIZE_SNAPSHOT_CAPTURE`**: child finalization job. Validates the persisted snapshot
  coverage, waits for all planned `EXEC_FILE_GROUP` children to finish with persisted success
  results, and publishes the immutable capture manifest containing reusable bundle references and
  snapshot-wide aggregate outputs. Explicit-empty snapshots publish a zero-group manifest.

## Architecture & Responsibilities
- **`ReconcileJobStore`**: interface abstracting job persistence and leasing. In service runtime,
  the default is the durable implementation (`DurableReconcileJobStore`) selected by
  `floecat.reconciler.job-store=durable`; in-memory (`InMemoryReconcileJobStore`) remains available
  for lightweight/local usage when `floecat.reconciler.job-store=memory`.
- **`RemoteReconcileExecutorPoller`**: Quarkus scheduled bean that leases reconcile jobs through
  the `ReconcileExecutorControl` gRPC control plane, starts heartbeats, invokes the matching local
  executor implementation, reports progress/completion, and repolls while worker capacity is
  available. In `worker.mode=local`, the poller talks to the colocated service over gRPC; in
  `worker.mode=remote`, executor-only nodes use the same lease protocol against a separate control
  plane.
- **`ReconcilerService`**: core planning/orchestration:
  1. Resolves connector metadata via the service's `Connectors` RPC.
  2. Plans connector-scoped table and view work while preserving destination namespace/table
     overrides.
  3. Ensures destination catalogs/namespaces/tables exist and updates connector metadata with
     resolved destination IDs.
  4. Instantiates the connector via `ConnectorFactory`.
  5. Handles incremental vs full-rescan logic.
  6. Supports explicit capture modes:
     - `METADATA_ONLY`: advances catalog, table, view, and snapshot metadata without capture.
     - `CAPTURE_ONLY`: captures stats / index artifacts for explicitly scoped destination tables
       without reconciling view metadata.
     - `METADATA_AND_CAPTURE`: ingests metadata and runs capture for matching table work in the same
       job tree.
- **Lease executors**:
  - `RemotePlannerReconcileExecutor` handles `PLAN_CONNECTOR`.
  - `RemoteDefaultReconcileExecutor` handles `PLAN_TABLE` and `PLAN_VIEW`.
  - `RemoteSnapshotPlanningReconcileExecutor` handles `PLAN_SNAPSHOT`.
  - `RemoteFileGroupReconcileExecutor` handles `EXEC_FILE_GROUP`.
  - `RemoteSnapshotFinalizeReconcileExecutor` handles file-group
    `FINALIZE_SNAPSHOT_CAPTURE` jobs with non-empty file-group plans.
  - `SnapshotFinalizeReconcileExecutor` handles direct-stats finalization and explicit-empty
    file-group plans.
- **`GrpcClients`**: provides blocking stubs for all service RPCs (Catalog, Namespace, Table,
  Snapshot, Statistics, Directory, Connectors, ReconcileExecutorControl).
- **`FloecatConnector`**: remains the only component allowed to touch upstream catalogs, table
  metadata, and object storage. Reconcile planning and file-group execution both go through the
  connector instance; reconcile does not use `ScanBundleService`.

## Public API / Surface Area
While the reconciler itself runs as an internal Quarkus app, it exposes behavior through the
reconcile control RPCs:

- `ReconcileControl.StartCapture(scope, mode, full_rescan, execution_policy)`: enqueues a
  top-level `PLAN_CONNECTOR` job via `ReconcileJobStore`.
- `ReconcileControl.CaptureNow(...)`: uses the same split path, but waits for the aggregated
  outcome of the top-level plan job plus any child planning/execution jobs.
- `ReconcileControl.GetReconcileJob(job_id)` / `ListReconcileJobs(...)`: expose both top-level and
  child jobs. Parent-capable jobs (`PLAN_CONNECTOR`, `PLAN_TABLE`, `PLAN_SNAPSHOT`) surface
  aggregate child status through eventually consistent projection/root-summary read models rather
  than synchronous parent-canonical rollups.
- `ReconcileControl.GetFinalizedSnapshotStatus(table_id, snapshot_id)`: returns whether one
  destination snapshot has completed reconcile finalization. `FSS_PENDING` means no finalized
  snapshot record exists yet for that table/snapshot pair. `FSS_FINALIZED` includes the
  `finalized_at` timestamp and `finalizer_job_id` for the job that committed finalization.

Internally, the worker poller exposes `pollEvery` via `@Scheduled` (default every second).

## Important Internal Details
- **Destination binding**: when reconciling, the service ensures the connector's declared
  destination catalog/namespace/table IDs align with actual resources. Any mismatch triggers a
  `ConnectorState` update or raises conflicts.
- **Statistics ingestion**: stats persistence is centralized behind the stats control plane
  and the reconcile executor control plane. `CAPTURE_ONLY` routes capture planning through the
  same reconcile job tree without metadata reconciliation. `METADATA_AND_CAPTURE` performs metadata
  reconciliation and capture within the same planner/executor job tree. Remote file-group workers
  publish one group artifact bundle and submit its descriptor and target mappings through
  `CommitLeasedFileGroupResult`. The service protects the bundle and stages generation-scoped
  pointers without reading its payload before `FINALIZE_SNAPSHOT_CAPTURE` writes snapshot-wide
  aggregate stats.
- **Snapshot planning persistence**: the immutable snapshot plan is stored in a content-bound blob
  referenced by the compact parent `PLAN_SNAPSHOT` job. Its identity includes the complete
  file-execution plans and, for append-only plans, the inherited base descriptor and persistent
  reusable-artifact sorted-run index. Child
  `EXEC_FILE_GROUP` and `FINALIZE_SNAPSHOT_CAPTURE` jobs reference that plan through `parentJobId`.
  File-group execution resolves `(planId, groupId)` through a bounded in-process index over the
  durable blob; a cold cache reloads the immutable plan. There is no expanded parent-task scan or
  legacy fallback.
- **File-group execution**:
  - `EXEC_FILE_GROUP` resolves its indexed immutable planned group, captures file-target stats, and
    records per-file execution results on the child job payload.
  - For a connector-proven append-only Delta or Iceberg change with unchanged schema, capture
    policy, and delete artifacts, the plan contains groups only for net-new parquet data files.
    Finalization inherits the prior stats/index generations through durable base-generation edges
    and publishes only the delta bundle references. The trusted finalizer writes one immutable level-zero
    sorted run and carries forward the authenticated prior run references, so the new manifest represents complete
    reuse coverage without repeating inherited bundle metadata or reopening inherited parquet
    artifacts. Any
    removed data file, changed Delta deletion vector, or added/removed Iceberg delete file rejects
    the append-only path and uses ordinary delete-aware file-group planning instead.
  - Each file execution plan carries connector-provided physical `content_identity`, source
    fingerprints, stats/index capture signatures, auxiliary-target fingerprints, and any selected
    reusable bundle records. Empty physical content identity disables cross-snapshot reuse for that
    file.
  - Snapshot planning reads the compact capture manifest from the newest root-published finalized
    reusable candidate at or before the committed current snapshot. Candidate selection is bounded
    by the table root and does not scan snapshot history. The lightweight snapshot reference carries
    the current manifest-contract version, so older references are rejected before their manifest
    objects are fetched or parsed. A missing, unavailable, malformed, incomplete, or unsupported
    manifest or persistent index is ineligible for reuse and triggers an ordinary full capture.
    Append-only overlap checks use point lookups in the persistent artifact index through
    Bloom-filtered, batched run probes and scale with delta files and the compacted run count.
    Ordinary full-file planning resolves reuse in bounded file-group batches instead of loading the
    complete index. Planning does not read reusable bundle payloads, source files, or page-index
    sidecars.
    The file-group worker fetches each selected compact bundle once, verifies its size and SHA-256,
    rebinds selected records to the destination snapshot, and captures only missing outputs.
  - The immutable per-file execution plans also define auxiliary stats coverage. Stats-enabled
    groups publish one target for each planned data file, each distinct attached Iceberg
    position/equality delete file, and each attached on-disk Delta deletion vector (`u` or `p`).
    Inline Delta deletion vectors (`i`) remain unsupported. Auxiliary delete targets contribute to
    file-stats descriptor counts but not planned/succeeded data-file counts, page-index coverage, or
    table/column aggregate rollups.
  - An Iceberg delete file can apply to data files in multiple groups. Repeated group-level
    references are duplicate execution work, not additional logical files; snapshot publication
    verifies equivalent source fingerprints and stats signatures, selects one canonical bundle
    mapping, and retains one target without reading or hashing delete-file content. File-group
    descriptors retain their group-level counts; the snapshot manifest reports the deduplicated
    unique-target count.
  - Snapshot-wide aggregate outputs are intentionally deferred to
    `FINALIZE_SNAPSHOT_CAPTURE`, which acts as the barrier for complete snapshot capture.
  - Newly generated sidecars are written per source parquet file. Reused index records retain the
    existing `artifact_uri`, and the worker does not read or rewrite their sidecar bytes.
  - File stats and index wrappers are serialized into one `ReusableArtifactBundlePayload` per group
    beneath the leased `stats_object_prefix`. `FileGroupResultPayload` carries target mappings to
    that shared object plus a lightweight compatibility index. Aggregate table/column outputs are
    recomputed once at snapshot finalization time from group partials.
  - `CommitLeasedFileGroupResult` requires `result_id` and a canonical
    `artifact_references_sha256` over the expanded stats and index target mappings. Success carries
    the shared bundle descriptor and the stats/index target IDs stored in that bundle. The service
    first durably accepts the immutable result and completes the child job, then idempotently
    protects the bundle, stages bounded generation-scoped pointer batches without reading it, and
    writes a metadata-only prepared marker. An exact retry resumes any incomplete staging.
    `FINALIZE_SNAPSHOT_CAPTURE` requires the digest-bound prepared marker for every file group,
    stages only snapshot-wide aggregate pointers, and activates the prepared stats and index
    generations. The finalization submission service performs one small pointer lookup per file
    group and does not read result payloads, artifact bundles, source files, sidecars, or per-file
    pointers. Successful finalization clears protection metadata; abandoned unpublished full-rescan
    generations are deleted after terminal failure or cancellation.
  - Snapshot planning limits each file group to 128 files by default. The ceiling is configurable
    with `floecat.reconciler.snapshot-plan.max-files-per-group`; values are clamped to at least one.
    The service rejects submitted plans containing a group above its configured ceiling and
    enforces membership and counts against the resulting immutable planned group. Raising the
    setting increases the descriptor count, pointer-metadata work, request size, and resident
    metadata for one file-group commit.
  - Append-only reuse chains checkpoint with a full capture after 16 links by default. The planner
    persists and authenticates predecessor depth in the immutable plan and manifest, and
    `floecat.reconciler.snapshot-plan.max-append-only-chain-depth` configures the maximum. Set it to
    zero to disable append-only chaining.
  - The finalizer reads and verifies each bounded `FileGroupResultPayload`, derives exact file-stats
    coverage from the immutable data-file execution plans and their attached delete artifacts, and
    merges the embedded aggregate partials. It validates one current bundle reference per file group
    without reading bundle payloads. Before submission, the trusted finalizer rejects duplicate
    typed file paths against authenticated inherited runs, writes the delta through bounded 8 MiB
    sorted L0 runs plus bounded level compaction, and stores the complete structurally shared run set in the durable
    `SnapshotCaptureManifest`. Sorted entries remain addressable in roughly 512 KiB logical blocks,
    but adjacent blocks are concatenated into content-addressed packs of up to 64 MiB. Each block
    reference records its pack, byte range, and SHA-256. Point probes range-read one block, while
    sequential compaction and paging coalesce adjacent blocks into windows of up to 8 MiB, targeting
    a 64 MiB operation-wide read budget while always allowing one indivisible block per run. Run objects no larger than
    64 KiB are embedded in that manifest; larger objects use content-addressed blob storage. The
    service binds the inactive generation to
    the capture manifest and reads file mappings through its authenticated run index after fenced
    activation. It does not traverse and copy inherited mappings. A reuse manifest without a
    persistent run index is invalid; planning rejects it instead of falling back to the former
    manifest-level bundle index.
  - Reusable compatibility metadata is executor-authored publication metadata. The control plane
    validates its structure, leased ownership, counts, content-addressed bundle descriptor, and
    staged target mappings, but deliberately does not GET bundle payloads to reconstruct or compare
    that metadata. A later file-group worker is the payload consumer: it verifies the selected
    bundle's byte length and SHA-256 and rejects missing or incompatible records before reuse.
  - Successful publication stores the capture-manifest descriptor in the snapshot's system-owned
    `reuse_manifest_ref` and heads the reusable stats generation from the table root. Garbage
    collection traverses that manifest's run manifests, filters, and block packs and treats them and referenced
    reusable bundle payloads—and the stats generations that own those payloads—as live publication
    state.
  - Current snapshot reads surface `file_groups_total`, `file_groups_completed`,
    `file_groups_failed`, `files_total`, `files_completed`, and `files_failed`.
- **Index artifacts**:
  - sidecars are parquet artifacts written by execution workers and registered through
    `IndexArtifactRecord`.
  - The actual sidecar URI remains worker-controlled. Floecat never copies, moves, or treats the
    sidecar named by `artifact_uri` as Floecat-owned cleanup state.
  - The serialized `IndexArtifactRecord` wrapper is stored in the file group's reusable artifact
    bundle. The service stages the file-index target ID against that shared bundle object.
  - service-side lookup/list/read is exposed by `TableIndexService`.
- **Capture-engine SPI contract**:
  - `CaptureEngine.capture` accepts a `CaptureFileResultConsumer`. Engines emit file-scoped stats
    progressively through that consumer instead of retaining them in `CaptureEngineResult`.
  - The terminal result may contain compact group-level aggregate partials and staged index
    outputs, but it must not contain file stats or page-index rows. It also reports the sorted,
    distinct concrete selectors represented by its column-stat aggregates so finalization can
    preserve resolved default columns and name/field-ID aliases.
  - Capture engines advertise `PROGRESSIVE_FILE_OUTPUTS` and implement the progressive
    `capture(request, consumer)` contract.
  - `ReconcilerBackend.indexCaptureComplete` is a snapshot-level completeness proof. Backend
    implementations must not replace it with one remote artifact lookup per source file.
- **External capture policy**:
  - leased file-group and snapshot-finalize payloads carry the complete policy, including its
    opaque properties map.
  - trusted snapshot finalizers enforce outputs, column policies, default scope, maximum default
    columns, and properties while validating realized selector coverage.
  - query-driven stats requests do not implicitly request Parquet page indexes and do not encode
    their request origin in capture-policy properties.
  - content-state coverage is checked before execution. Missing coverage attempts connector-native
    direct stats first and uses file-group capture when direct stats cannot satisfy the request.
- **External reusable-index contract**:
  - external `EXEC_FILE_GROUP` workers do not read, construct, or compact reusable-index runs. They
    continue to consume the bundle selections in their leased execution payload and publish the
    current file group's bundle metadata.
  - external `PLAN_SNAPSHOT` implementations must regenerate their protobuf bindings for the
    format-1 sorted-run messages and preserve the complete `AppendOnlySnapshotBase`, including its
    `reusable_artifact_index`, through planning and finalization. There is no reader for the former
    trie-root format and no dual-read fallback.
  - a planner that performs reusable-artifact lookup must authenticate object length and SHA-256,
    batch-fetch run filters and manifests, and fetch only candidate data blocks. It must not turn
    lookup into one object-store request per source file. A missing index object makes the base
    unavailable for reuse. A reuse manifest without an artifact index is likewise ineligible and
    falls back to a full capture. Any reusable index that is invalid under the current contract,
    including malformed or digest-invalid index metadata, is discarded and causes a full capture;
    it is not interpreted through a legacy reader. Missing, unavailable, malformed, digest-invalid,
    incomplete, or unsupported capture manifests likewise make the base ineligible and cause a
    full capture.
  - Bloom-filter references larger than the format's 16 MiB bitset ceiling are rejected before any
    object read. Activation streams and authenticates every referenced external pack before the
    generation becomes visible; generation-to-capture-manifest bindings verify the content digest
    encoded in the immutable manifest URI as well as its recorded length.
  - an external snapshot finalizer submits the current `reusable_artifact_bundles` delta and the
    leased `append_only_base`, constructs `SnapshotCaptureManifest.reusable_artifact_index`, and
    owns run creation and bounded compaction. At a chain reset it authenticates and traverses the
    inherited index and stages references through bounded control-plane calls. The service owns
    fenced activation and garbage-collection reachability; it does not reopen inherited bundles.
  - run objects are immutable and content-addressed. Their filter encoding and storage layout are
    internal implementation details, not an extension point for independently constructed runs.
- **Connector security boundary**: all upstream I/O remains inside `FloecatConnector`.
  `ScanBundleService` stays query-plane only; reconcile snapshot planning uses connector-native
  snapshot file planning.
- **Mode-aware behavior**:
  - in `CAPTURE_ONLY`, destination-table misses are treated as skip/no-op rather than job-fatal
    errors.
  - `CAPTURE_ONLY` is table-scoped: view scope is rejected, and scoped capture requests must
    resolve to explicit destination table IDs.
  - in `METADATA_AND_CAPTURE`, planner/executor availability is validated up front based on scope:
    table scope requires `PLAN_TABLE`, view scope requires `PLAN_VIEW`, and broad metadata reconcile
    requires both.
- **Plan failure behavior**: if a parent plan job fails or is cancelled after enqueuing child jobs,
  the control plane cancels those children.
- **View reconciliation semantics**: reconcile is current-state, not history-preserving. When an
  upstream view already exists in Floecat, reconcile updates the stored canonical definition in
  place rather than appending a backend version history.
- **Durable queue ownership model**: `DurableReconcileJobStore` is split into explicit state
  domains with native durable-store boundaries:
  - canonical job state owns the job-index domain transactionally (`lookup`, `state`, `dedupe`,
    parent, connector, job-local counters, payload references, and related canonical indexes)
  - ready-queue state is a separate due-ordered domain used for leasing eligibility and queue
    slicing
  - lease coordination owns runtime worker-ownership state separately (`lease`, `lease-expiry`,
    lane lease, snapshot lease)
  - payload blobs are canonical task artifacts referenced from canonical rows (snapshot plans,
    file-group results, direct stats, and reusable file-group artifact bundles)
  - projection/root-summary state mirrors canonical parent rollups for eventual-consistent
    observability (root-job list summaries and tree/list aggregate counters)
- **Stats-only enqueue coalescing**: active stats-only `CAPTURE_ONLY` root jobs use a normalized
  capture identity containing the account, connector, table/snapshot pairs, requested outputs, and
  capture-policy properties, in addition to the normal root-job identity fields. Column target and
  selector differences therefore do not create parallel roots for overlapping requests against the
  same snapshot. A different snapshot or output family receives a distinct root job.
- **Job leasing**: workers lease from persisted ready pointers, mark jobs
  running/succeeded/failed through transactional state transitions, and reclaim expired leases on a
  configured interval. Failed jobs are retried with backoff up to configured attempt limits before
  terminal failure.
- **Execution vs observability split**:
  - queue correctness depends only on canonical state plus execution payload references
  - projection refresh is best-effort and may lag without affecting leasing, retries,
    cancellation, reclaim, or completion
  - detailed read APIs use best-effort payload hydration and may degrade detail when payload blobs
    are missing
- **No queue self-healing**: read paths, lease scans, and maintenance do not rebuild missing or
  stale job indexes. Canonical job-index pointers are expected to stay correct because the owning
  job-state transitions update them together. Lease maintenance reclaims expired leases and
  projection maintenance repairs dirty parent/root summaries, but neither path is part of queue
  correctness.
- **Canonical parent projection maintenance**:
  - canonical child records are the sole rollup input; stored child projections are never scanned
    or used to decide parent state
  - child changes coalesce through a versioned dirty-parent marker
  - one maintenance operation reads the direct children once, computes the exact rollup, then
    atomically commits the canonical parent, its projection, dirty-marker consumption, and the
    immediate ancestor marker
  - canonical parent records therefore carry both scheduling state and exact aggregate counters;
    projections mirror that committed state for list/tree reads
  - list/get/tree read paths never recursively scan descendants or repair projections
- **Backend shape**:
  - in `floecat.kv=dynamodb`, the durable store hot paths use native Dynamo-style partition/sort-key
    layouts for job indexes, ready slices, and lease rows/expiry scans
  - in `floecat.kv=memory`, the same domain model is preserved, but the physical implementation is
    in-memory rather than a literal Dynamo simulator

### gRPC auth
- Reconcile workers use the gRPC control plane for leasing, progress, and standalone worker
  payload/result exchange.
- Worker auth is attached explicitly by the reconcile executor client. The global outbound gRPC
  interceptor is not responsible for minting or attaching worker tokens.
- In OIDC mode, background workers obtain a machine token via client credentials using
  `floecat.reconciler.oidc.issuer`, `client-id`, `client-secret`,
  `token-refresh-skew-seconds`, and `connect-timeout`.
- If `floecat.interceptor.session.header` is configured, worker RPCs attach the token there
  (typically `x-floe-session`). Otherwise they fall back to
  `floecat.reconciler.authorization.header`.
- `ReconcileExecutorControl` accepts only the dedicated internal worker permission carried by the
  reconciler service principal.
- Other internal gRPC fanout paths may still propagate request-scoped user/session headers where
  that is the actual call contract, but that is separate from reconcile worker auth.

## Data Flow & Lifecycle
```text
Connector StartCapture / CaptureNow
  → ReconcileJobStore.enqueue(PLAN_CONNECTOR)
  → RemoteReconcileExecutorPoller.pollOnce
      → leaseNext
      → markRunning
      → if PLAN_CONNECTOR:
          → ReconcilerService.planTableTasks / planViewTasks
          → enqueue PLAN_TABLE / PLAN_VIEW children
      → if PLAN_TABLE:
          → ensure destination table metadata
          → enumerate snapshots via FloecatConnector
          → enqueue PLAN_SNAPSHOT children
      → if PLAN_VIEW:
          → describeView
          → ensure destination namespace exists
          → create or update the destination view
      → if PLAN_SNAPSHOT:
          → ask FloecatConnector for planned parquet file membership
          → read the newest root-published finalized reuse manifest and select compatible bundle records
          → persist grouped file plan on parent job payload
          → enqueue EXEC_FILE_GROUP children
          → enqueue FINALIZE_SNAPSHOT_CAPTURE child
      → if EXEC_FILE_GROUP:
          → resolve parent PLAN_SNAPSHOT payload
          → instantiate FloecatConnector
          → read each selected reusable bundle once
          → capture missing file-target stats and page indexes
          → publish one group bundle and bounded result payload
          → commit the result descriptor and bundle target mappings
      → if FINALIZE_SNAPSHOT_CAPTURE:
          → validate explicit planned coverage metadata
          → wait for all planned EXEC_FILE_GROUP children to succeed with persisted results
          → read and verify each bounded group result payload
          → roll up snapshot-wide aggregate outputs and publish the capture manifest
          → activate prepared stats/index generations and snapshot reuse metadata
      → markSucceeded or markFailed
```

Jobs include `fullRescan`, `executionPolicy`, `jobKind`, and optional task payloads. Snapshot plan
jobs, file-group jobs, and snapshot finalization jobs surface file-group/file counters in projected
public views; parent canonical records do not store rolled-up aggregate counters.
`RemoteReconcileExecutorPoller` uses `AtomicBoolean` and in-flight counters to avoid over-leasing
within the same instance while continuing to repoll until worker slots are full.
For handled remote completions, workers stop heartbeats before the handled success RPC and do not
perform a post-completion final lease confirmation after that RPC has durably completed the job.

## Configuration & Extensibility
- Scheduling cadence via `reconciler.pollEvery` (defaults to `1s`).
- Empty-queue polling uses one probe sweep per worker JVM and exponentially backs off with jitter
  between `reconciler.empty-poll-backoff-initial-ms` (default `500`) and
  `reconciler.empty-poll-backoff-max-ms` (default `5000`). Leasing work resets the backoff and
  immediately fills available worker capacity.
- Worker mode via `floecat.reconciler.worker.mode`:
  - `local` runs the lease poller in the same JVM as the control plane.
  - `remote` keeps the same gRPC lease protocol but is intended for executor-only nodes. Set
    `reconciler.max-parallelism=0` on control-plane-only nodes.
- Worker capacity via `reconciler.max-parallelism`.
- Job store selection:
  - `floecat.reconciler.job-store=durable` (service default) uses persisted queue records plus
    retry/lease tuning via:
    `floecat.reconciler.job-store.max-attempts`, `base-backoff-ms`, `max-backoff-ms`, `lease-ms`,
    `reclaim-interval-ms`, and `ready-scan-limit`.
  - `floecat.reconciler.job-store=memory` uses the in-memory queue implementation.
- Accepted snapshot-finalize results are published independently of the originating worker lease.
  Publication recovery and throughput are tuned with
  `floecat.reconciler.snapshot-finalize-publication.tick-every`, `page-size`, and
  `max-parallelism`.
- Executor toggles:
  - `floecat.reconciler.executor.remote-default.enabled`
  - `floecat.reconciler.executor.remote-planner.enabled`
  - `floecat.reconciler.executor.remote-snapshot-planner.enabled`
  - `floecat.reconciler.executor.remote-file-group.enabled`
  - `floecat.reconciler.executor.remote-snapshot-finalize.enabled`
  - `floecat.reconciler.executor.snapshot-finalize.enabled`
  - File-group snapshots, including explicit-empty snapshots, use the descriptor-driven
    `RemoteSnapshotFinalizeReconcileExecutor` when they contain file groups. Direct-stats snapshots
    and explicit-empty file-group snapshots use `SnapshotFinalizeReconcileExecutor`.
- Swap out `ReconcileJobStore` for additional backends by providing a CDI alternative (job ID
  references must remain stable for `GetReconcileJob`).
- Extend `FloecatConnector` to add richer snapshot planning or file execution behavior. Query scan
  planning remains separate behind `ScanBundleService`.

## Examples & Scenarios
- **Full metadata rescan**: operator triggers
  `connector trigger demo-glue --full --all --mode metadata-only`. The job store enqueues a
  full `PLAN_CONNECTOR` job, the worker poller leases it, and the reconciler walks connector
  discovery and metadata planning across the full upstream history.
- **Incremental capture run**: operator triggers
  `connector trigger demo-glue --incremental --current --mode metadata-and-capture --capture stats`.
  The reconcile path
  captures table/file/column stats for matching table work while still allowing metadata mutation.
- **Incremental run**: `--incremental` enumerates snapshots selected by the explicit snapshot scope
  (`--current`, `--latest-n`, `--snapshot`, or `--all`). Durable content state then skips snapshots
  whose metadata fingerprint and requested capture coverage are already complete, or narrows work
  to only the missing coverage. Materialized coverage records the concrete stats and index
  selectors reported by executors, allowing a field-ID/name alias to satisfy an equivalent later
  request. An `ALL` default covers any narrower default, and a larger `FIRST_N` covers a smaller
  `FIRST_N`; the reverse does not hold. Full rescans bypass this content-state deduplication.

## Cross-References
- Connector SPI details: [`docs/connectors-spi.md`](connectors-spi.md)
- Service connector/query RPCs: [`docs/service.md`](service.md)
- Rust file-group worker implementation guide:
  [`docs/rust-remote-capture-executor.md`](rust-remote-capture-executor.md)
- Concrete connectors: [`docs/connectors-iceberg.md`](connectors-iceberg.md),
  [`docs/connectors-delta.md`](connectors-delta.md)
