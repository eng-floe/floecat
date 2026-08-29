# Storage Layout

## Resource Model & Storage Layout

Floecat normally stores catalog entities as immutable protobuf payloads in a BlobStore and exposes
them through versioned PointerStore entries for hierarchical lookup and CAS updates. Operational
state also uses pointer-only indexes, content-addressed blobs, and small inline payloads as described
below. This design keeps history in blobs while enabling fast name-based resolution via pointers.

This inventory is authoritative and should remain in sync with
`service/src/main/java/ai/floedb/floecat/service/repo/model/Keys.java`. Values shown in braces are
percent-encoded path segments unless the description says otherwise. Snapshot IDs and sortable
timestamps marked `:019d` are zero-padded to 19 decimal digits. Snapshot `by-time` keys store the
inverted timestamp and snapshot ID (`Long.MAX_VALUE - value`) so ascending scans return newest
entries first.

### Blob keys

Resource and transaction blobs:

```
/accounts/{account_id}/account/{sha}.pb
/accounts/{account_id}/catalogs/{catalog_id}/catalog/{sha}.pb
/accounts/{account_id}/storage-authorities/{authority_id}/storage-authority/{sha}.pb
/accounts/{account_id}/namespaces/{namespace_id}/namespace/{sha}.pb
/accounts/{account_id}/tables/{table_id}/table/{sha}.pb
/accounts/{account_id}/views/{view_id}/view/{sha}.pb
/accounts/{account_id}/connectors/{connector_id}/connector/{sha}.pb
/accounts/{account_id}/catalog-integrations/{integration_id}/integration/{sha}.pb
/accounts/{account_id}/catalog-overlays/{overlay_id}/overlay/{sha}.pb
/accounts/{account_id}/transactions/{tx_id}/transaction/{sha}.pb
/accounts/{account_id}/transactions/{tx_id}/intent/{sha}.pb
/accounts/{account_id}/transactions/{tx_id}/objects/{sha}.bin
/accounts/{account_id}/transactions/{tx_id}/delete/{encoded_target_pointer_key}
```

Table, snapshot, stats, constraints, and index blobs:

```
/accounts/{account_id}/tables/{table_id}/snapshots/{snapshot_id:019d}/snapshot/{sha}.pb
/accounts/{account_id}/tables/{table_id}/snapshots/current/{sha}.pb
/accounts/{account_id}/tables/{table_id}/root/{sha}.pb
/accounts/{account_id}/tables/{table_id}/root/manifest/{sha}.pb
/accounts/{account_id}/tables/{table_id}/target-stats/{snapshot_id:019d}/manifests/{generation_id}.pb
/accounts/{account_id}/tables/{table_id}/target-stats/{snapshot_id:019d}/generations/{generation_id}/{target_id_sha256}/{sha}.pb
/accounts/{account_id}/tables/{table_id}/target-stats/{snapshot_id:019d}/generations/{generation_id}/index-artifacts/{target_id_sha256}/{sha}.pb
/accounts/{account_id}/tables/{table_id}/index-sidecars/{snapshot_id:019d}/{target_id}/{sha}.parquet
/accounts/{account_id}/tables/{table_id}/snapshots/{snapshot_id:019d}/index-artifacts/capture-manifests/{sha}.pb
/accounts/{account_id}/tables/{table_id}/reusable-artifact-index/runs/run-manifests/{sha}.pb
/accounts/{account_id}/tables/{table_id}/reusable-artifact-index/runs/filters/{sha}.bf
/accounts/{account_id}/tables/{table_id}/reusable-artifact-index/runs/packs/{sha}.pack
/accounts/{account_id}/tables/{table_id}/constraints/{snapshot_id:019d}/{sha}.pb
```

`{target_id_sha256}` is the lowercase SHA-256 hex digest of the logical target identity. Reusable
artifact-index objects at or below the inline threshold are embedded in their protobuf references
and therefore have no blob key.

Idempotency and reconcile blobs:

```
/accounts/{account_id}/idempotency/{key}/idempotency.pb
/accounts/{account_id}/idempotency/{key}/idempotency-{suffix}.pb
/accounts/{account_id}/reconcile/jobs/{job_id}/job-{suffix}.json
/accounts/{account_id}/reconcile/jobs/{job_id}/result-{suffix}.json
/accounts/{account_id}/reconcile/jobs/{job_id}/result-payloads/v1/snapshot-plans/{parent_job_id}/executions/{lease_epoch_sha256}.pb
/accounts/{account_id}/tables/{table_id}/target-stats/{snapshot_id:019d}/generations/full-rescan-{parent_job_id}/worker-uploads/{job_id}/{lease_epoch_sha256}/...
/accounts/{account_id}/tables/{table_id}/target-stats/{snapshot_id:019d}/generations/full-rescan-{parent_job_id}/finalizer-outputs/...
/accounts/{account_id}/tables/{table_id}/target-stats/{snapshot_id:019d}/generations/full-rescan-{parent_job_id}/finalizer-outputs/reuse-manifests/{manifest_sha256}.pb
```

### Pointer keys

Catalog hierarchy, lookup indexes, and maintenance markers:

```
/accounts/by-id/{account_id}
/accounts/by-name/{account_name}
/accounts/{account_id}
/accounts/{account_id}/deleting
/accounts/{account_id}/catalogs/by-id/{catalog_id}
/accounts/{account_id}/catalogs/by-name/{catalog_name}
/accounts/{account_id}/storage-authorities/by-id/{authority_id}
/accounts/{account_id}/storage-authorities/by-name/{authority_name}
/accounts/{account_id}/namespaces/by-id/{namespace_id}
/accounts/{account_id}/catalogs/{catalog_id}/namespaces/by-path/{path...}
/accounts/{account_id}/tables/by-id/{table_id}
/accounts/{account_id}/catalogs/{catalog_id}/namespaces/{namespace_id}/tables/by-name/{table_name}
/accounts/{account_id}/views/by-id/{view_id}
/accounts/{account_id}/catalogs/{catalog_id}/namespaces/{namespace_id}/views/by-name/{view_name}
/accounts/{account_id}/catalogs/{catalog_id}/namespaces/{namespace_id}/relations/by-name/{relation_name}
/accounts/{account_id}/connectors/by-id/{connector_id}
/accounts/{account_id}/connectors/by-name/{connector_name}
/accounts/{account_id}/catalog-integrations/by-id/{integration_id}
/accounts/{account_id}/catalog-integrations/by-name/{integration_name}
/accounts/{account_id}/catalog-integrations/overlays-marker/{integration_id}
/accounts/{account_id}/catalog-integrations/deleting/{integration_id}
/accounts/{account_id}/catalog-overlays/by-id/{overlay_id}
/accounts/{account_id}/catalog-overlays/by-name/{overlay_name}
/accounts/{account_id}/catalog-overlays/by-integration/{integration_id}/{overlay_id}
/accounts/{account_id}/catalog-overlays/by-catalog/{catalog_id}/{overlay_id}
/accounts/{account_id}/catalog-overlays/deleting/{overlay_id}
/accounts/{account_id}/catalogs/overlays-marker/{catalog_id}
/accounts/{account_id}/deleting
/accounts/{account_id}/catalogs/{catalog_id}/markers/children
/accounts/{account_id}/namespaces/{namespace_id}/markers/children
/accounts/{account_id}/gc/cas/generation-cursor
/accounts/{account_id}/root-resyncs/by-table/{table_id}
```

An overlay references an existing Catalog. Its `by-integration` and `by-catalog` dependency
pointers are written and removed atomically with the overlay resource. Fixed generation markers on
both parents close create/delete races, and the per-overlay deletion marker fences reconciliation
while an overlay is retired. Catalog and overlay names and lifecycles remain independent.

Catalog integration secret payloads are stored outside the resource blob through SecretsManager:

```
accounts/{account_id}/catalog-integrations/{integration_id}.credentials.{credential_generation}
```

The key is derived internally from the persisted integration identity and credential generation; it
is not present in the resource payload. A new secret is written before publishing the matching
generation; an old secret is deleted only after the resource mutation commits. Definite failures
delete an unpublished generation. Durable cleanup records cover acknowledgement-uncertain failures
and are drained once the generation is provably superseded.

Transaction and idempotency pointers:

```
/accounts/{account_id}/transactions/by-id/{tx_id}
/accounts/{account_id}/transactions/by-target/{encoded_target_pointer_key}
/accounts/{account_id}/transactions/{tx_id}/intents/{encoded_target_pointer_key}
/accounts/{account_id}/idempotency/{operation}/{key}
```

Snapshot, stats-generation, constraint, compatibility, and index pointers:

```
/accounts/{account_id}/tables/{table_id}/snapshots/by-id/{snapshot_id:019d}
/accounts/{account_id}/tables/{table_id}/snapshots/by-time/{inverted_timestamp:019d}-{inverted_snapshot_id:019d}
/accounts/{account_id}/tables/{table_id}/snapshots/current
/accounts/{account_id}/tables/{table_id}/root/current
/accounts/{account_id}/tables/{table_id}/snapshots/{snapshot_id:019d}/stats/targets-active
/accounts/{account_id}/tables/{table_id}/snapshots/{snapshot_id:019d}/stats/target-generations/{generation_id}/targets/{target_id}
/accounts/{account_id}/tables/{table_id}/snapshots/{snapshot_id:019d}/stats/target-generations/{generation_id}/protections/{protection_id}/...
/accounts/{account_id}/tables/{table_id}/snapshots/{snapshot_id:019d}/stats/target-generations/{generation_id}/lifecycle
/accounts/{account_id}/tables/{table_id}/snapshots/{snapshot_id:019d}/stats/target-generations/{generation_id}/artifact-map
/accounts/{account_id}/tables/{table_id}/snapshots/{snapshot_id:019d}/stats/target-generations/{generation_id}/publication-intent
/accounts/{account_id}/tables/{table_id}/snapshots/{snapshot_id:019d}/stats/target-generations/{generation_id}/prepared-file-groups/{job_id}/{lease_epoch_sha256}
/accounts/by-id/{account_id}/reconcile/deleted-stats-generations/{table_id}/{snapshot_id}/{generation_id}
/accounts/{account_id}/tables/{table_id}/snapshots/{snapshot_id:019d}/stats/target-generations/{generation_id}/index-artifacts/{target_id}
/accounts/{account_id}/tables/{table_id}/snapshots/{snapshot_id:019d}/index-artifacts/active-generation
/accounts/{account_id}/tables/{table_id}/snapshots/{snapshot_id:019d}/index-artifacts/capture-manifest
/accounts/{account_id}/tables/{table_id}/snapshots/{snapshot_id:019d}/compat/iceberg-rest/...
/accounts/{account_id}/tables/{table_id}/snapshots/{snapshot_id:019d}/stats/constraints
/accounts/{account_id}/tables/{table_id}/constraints/by-snapshot/{snapshot_id:019d}
```

Stats are generation-scoped. There is no standalone `file-stats` blob or pointer family: logical
table, column, expression, and file target identities all enter through `{target_id}` in the active
generation, while their blobs use `{target_id_sha256}`.

Reconcile job pointers:

```
/accounts/{account_id}/reconcile/jobs/by-id/{job_id}
/accounts/by-id/reconcile/jobs/by-id/{job_id}
/accounts/{account_id}/reconcile/finalized-snapshots/by-id/{table_id}/{snapshot_id:019d}
/accounts/by-id/reconcile/jobs/dirty-parents-by-worker-affinity/{worker_affinity}/{account_id}/{parent_job_id}
/accounts/by-id/reconcile/jobs/cancellation-cleanup-by-worker-affinity/{worker_affinity}/{account_id}/{root_job_id}
/accounts/{account_id}/reconcile/jobs/projections/by-id/{job_id}
/accounts/{account_id}/reconcile/jobs/root-summaries/by-account/{sortable_job_token}
/accounts/{account_id}/reconcile/jobs/root-summaries/by-connector/{connector_id}/{sortable_job_token}
/accounts/{account_id}/reconcile/jobs/gc-quarantine-by-worker-affinity/{worker_affinity}/canonical/{canonical_key_hash}
/accounts/{account_id}/reconcile/jobs/by-parent/{parent_job_id}/{job_id}
/accounts/{account_id}/reconcile/jobs/by-connector/{connector_id}/{sortable_job_token}
/accounts/by-id/reconcile/jobs/by-state-by-worker-affinity/{worker_affinity}/{state}/{sortable_timestamp:019d}/{account_id}/{job_id}
/accounts/{account_id}/reconcile/jobs/by-state/{state}/{sortable_timestamp:019d}/{job_id}
/accounts/{account_id}/reconcile/jobs/terminal-retention-by-worker-affinity/{worker_affinity}/{terminal_timestamp:019d}/{job_id}
/accounts/{account_id}/reconcile/jobs/by-connector-state/{connector_id}/{state}/{sortable_timestamp:019d}/{job_id}
/accounts/{account_id}/reconcile/job-leases/by-id/{job_id}
/accounts/by-id/reconcile/job-leases/by-expiry-by-worker-affinity/{worker_affinity}/{expiry_timestamp:019d}/accounts/{account_id}/jobs/{job_id}
/accounts/by-id/reconcile/jobs/ready/{due_timestamp:019d}/{account_id}/{lane_key}/{job_id}
/accounts/by-id/reconcile/jobs/ready/by-execution-class/{execution_class}/{due_timestamp:019d}/{account_id}/{job_id}
/accounts/by-id/reconcile/jobs/ready/by-execution-lane/{execution_lane}/{due_timestamp:019d}/{account_id}/{job_id}
/accounts/by-id/reconcile/jobs/ready/by-pinned-executor/{executor_id}/{due_timestamp:019d}/{account_id}/{job_id}
/accounts/by-id/reconcile/jobs/ready/by-job-kind/{job_kind}/{due_timestamp:019d}/{account_id}/{job_id}
/accounts/{account_id}/reconcile/dedupe/{dedupe_key_hash}
/accounts/{account_id}/reconcile/snapshot-owners/{table_id}/{snapshot_id:019d}
/accounts/{account_id}/reconcile/snapshot-coverage-claims/{connector_id}/{source_namespace}/{source_table}/{table_id}/{snapshot_id:019d}/{source_revision}/{semantics_hash}
/accounts/{account_id}/reconcile/lanes/{lane_key}
/accounts/{account_id}/reconcile/jobs/gc-blob-cleanup-by-worker-affinity/{worker_affinity}/{job_id}
```

Affinity-qualified indexes encode an absent or disabled affinity as the reserved `_none_` segment;
operators cannot configure `_none_` as a named affinity. Production startup still requires a
non-blank configured affinity.

Constraint storage semantics:

- The persisted constraints unit is per table per snapshot (`.../constraints/by-snapshot/{snapshot_id}`).
- The `SnapshotConstraints` payload can contain both table constraints and column-level constraints
  represented in table scope (for example `NOT NULL`).
- Ingestion policy is intentionally asymmetric today:
  - `PutTableConstraints` is strict and requires an existing snapshot row.
  - `PutTargetStats` is lenient and may accept writes before snapshot materialization.
  - Rationale: preserve existing stats capture ordering while keeping constraints explicitly
    attached to materialized snapshots.

Each pointer carries a monotonically increasing version; repositories use compare-and-set to enforce
idempotency and optimistic concurrency. Two storage implementations ship with the repo:

- **Memory** – `InMemoryPointerStore` + `InMemoryBlobStore` (default for `make run`).
- **AWS** – DynamoDB pointer table + S3 blob bucket (see `service/src/main/resources/application.properties`).
