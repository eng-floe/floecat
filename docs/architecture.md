# Architecture

## System Overview

Floecat tracks a resource graph anchored at Accounts and spanning Catalogs, Namespaces, Tables,
Views, Snapshots, statistics, and query-lifecycle artifacts.

```
Account
 └── Catalog (logical catalog-of-catalogs)
      └── Namespace (hierarchical path)
           ├── Table (Iceberg/Delta metadata, upstream reference)
           │    └── Snapshot (immutable upstream checkpoints)
           └── View (stored definition)
```

Two storage primitives underpin every service:

- **BlobStore** – immutable protobuf payloads such as `catalog.pb`, `table.pb`,
  `snapshots/{snapshot_id}/snapshot/{sha}.pb`, and stats blobs. Blobs are content-addressed via
  SHA256 ETAGs.
- **PointerStore** – versioned key→blob indirection to support atomic compare-and-set (CAS),
  hierarchical listing, and name→ID lookups. Keys use deterministic prefixes such as
  `/accounts/{account_id}/catalogs/by-name/{name}` and
  `/accounts/{account_id}/tables/{table_id}/snapshots/by-id/{snapshot_id}`.

The gRPC service (Quarkus) enforces tenancy, authorization, and idempotency while orchestrating
connectors that ingest upstream metadata, reconciling it into the canonical blob/pointer stores, and
serving execution-ready scan bundles.

## Components

The following modules compose the system (see linked docs for deep dives):

| Component | Responsibility |
|-----------|----------------|
| [`proto/`](proto.md) | All protobuf/gRPC contracts (catalog, query lifecycle, execution scans, connectors, statistics, types). |
| [`service/`](service.md) | Quarkus runtime, resource repositories, query lifecycle service, GC, security, metrics. |
| [`client-cli/`](client-cli.md) | Interactive shell for humans; exercises every public RPC. |
| [`core/connectors/spi/`](connectors-spi.md) | Connector interfaces, stats engines, NDV helpers, auth shims. |
| [`connectors/catalogs/iceberg/`](connectors-iceberg.md) | Iceberg REST + AWS Glue connector implementation. |
| [`connectors/catalogs/delta/`](connectors-delta.md) | Unity Catalog/Delta Lake connector using Delta Kernel + Databricks APIs. |
| [`core/connectors/common/`](connectors-common.md) | Shared connector utilities (Parquet stats, NDV sketches, planners). |
| [`reconciler/`](reconciler.md) | Connector scheduler/worker, reconciliation orchestration, job store. |
| [`core/storage-spi/`](storage-spi.md) | Blob/pointer persistence contracts shared by service and GC. |
| [`storage/memory/`](storage-memory.md) | In-memory dev/test stores (CAS semantics maintained). |
| [`storage/aws/`](storage-aws.md) | Production DynamoDB pointer store + S3 blob store. |
| [`types/`](types.md) | Logical type system utilities, coercions, min/max encoding. |
| [`extensions/builtin/`](builtin-catalog.md) | Plugin architecture for engine-specific builtin catalogs (functions, operators, types, etc.). |
| [`log/`](log.md) | Runtime log directory layout (service log + audit channel). |

## Data & Control Flow

1. **Connectors** (Delta/Iceberg) enumerate upstream namespaces, tables, snapshots, and file-level
   stats via the shared SPI.
2. The **Reconciler** schedules connector runs, materializes local Tables/Snapshots/Stats through
   repository APIs, and records incremental NDV, histograms, and scan manifests.
3. The **Service** exposes CRUD RPCs for catalogs/namespaces/tables/views, plus query-lifecycle and
   statistics APIs. Requests traverse interceptors that inject `PrincipalContext`, correlation IDs,
   and optional query leases before hitting service implementations.
4. **Repositories** translate RPCs into pointer/blob mutations, enforce optimistic concurrency, and
   update idempotency records.
5. **Query lifecycle RPCs** hand planners lease descriptors (snapshot pins, obligations) plus any
   connector-provided scan metadata needed before execution.
