# Storage Layout

## Resource Model & Storage Layout

Floecat stores every entity twice: the immutable protobuf payload lives in a BlobStore while a
PointerStore entry (with versions) exposes hierarchical lookup and CAS updates. This design keeps
history in blobs while enabling fast name-based resolution via pointers.

This list is authoritative and should remain in sync with `service/src/main/java/ai/floedb/floecat/service/repo/model/Keys.java`.

Blobs follow deterministic prefixes:

```
/accounts/{account_id}/account/{sha}.pb
/accounts/{account_id}/catalogs/{catalog_id}/catalog/{sha}.pb
/accounts/{account_id}/namespaces/{namespace_id}/namespace/{sha}.pb
/accounts/{account_id}/tables/{table_id}/table/{sha}.pb
/accounts/{account_id}/tables/{table_id}/snapshots/{snapshot_id}/snapshot/{sha}.pb
/accounts/{account_id}/tables/{table_id}/target-stats/{target_id}/{sha}.pb
/accounts/{account_id}/tables/{table_id}/file-stats/{file_path}/{sha}.pb
/accounts/{account_id}/tables/{table_id}/constraints/{snapshot_id}/{sha}.pb
/accounts/{account_id}/views/{view_id}/view/{sha}.pb
/accounts/{account_id}/connectors/{connector_id}/connector/{sha}.pb
/accounts/{account_id}/idempotency/{key}/idempotency.pb
/accounts/{account_id}/idempotency/{key}/idempotency-{suffix}.pb
```

Pointers capture hierarchy and name lookups:

```
/accounts/by-id/{account_id}
/accounts/by-name/{account_name}
/accounts/{account_id}
/accounts/{account_id}/catalogs/by-id/{catalog_id}
/accounts/{account_id}/catalogs/by-name/{catalog_name}
/accounts/{account_id}/namespaces/by-id/{namespace_id}
/accounts/{account_id}/catalogs/{catalog_id}/namespaces/by-path/{path}
/accounts/{account_id}/tables/by-id/{table_id}
/accounts/{account_id}/catalogs/{catalog_id}/namespaces/{namespace_id}/tables/by-name/{table_name}
/accounts/{account_id}/views/by-id/{view_id}
/accounts/{account_id}/catalogs/{catalog_id}/namespaces/{namespace_id}/views/by-name/{view_name}
/accounts/{account_id}/tables/{table_id}/snapshots/by-id/{snapshot_id}
/accounts/{account_id}/tables/{table_id}/snapshots/by-time/{timestamp}-{snapshot_id}
/accounts/{account_id}/tables/{table_id}/snapshots/{snapshot_id}/stats/targets/{target_id}
/accounts/{account_id}/tables/{table_id}/snapshots/{snapshot_id}/stats/files/{file_path}
/accounts/{account_id}/tables/{table_id}/snapshots/{snapshot_id}/stats/constraints
/accounts/{account_id}/tables/{table_id}/constraints/by-snapshot/{snapshot_id}
/accounts/{account_id}/connectors/by-id/{connector_id}
/accounts/{account_id}/connectors/by-name/{connector_name}
/accounts/{account_id}/idempotency/{operation}/{key}
/accounts/{account_id}/catalogs/{catalog_id}/markers/children
/accounts/{account_id}/namespaces/{namespace_id}/markers/children
```

Stats target identity is encoded in `{target_id}` and is the storage key component for all
persisted table/column/expression stats at a given `(table_id, snapshot_id)`.

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
