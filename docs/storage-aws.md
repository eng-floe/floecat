# AWS Storage Backend

## Overview
`storage/aws/` contains production-grade implementations of the storage SPI backed by Amazon
DynamoDB (pointer store) and Amazon S3 (blob store). It also includes bootstrapping helpers to ensure
DynamoDB tables exist before the service starts.

## Architecture & Responsibilities
- **`AwsClients`** – Centralises AWS SDK v2 clients (DynamoDB, S3) configured via Quarkus properties
  (`aws.region`, credentials providers). Injected into both stores.
- **`DynamoPointerStore`** – Stores pointer entries in DynamoDB tables (partition key = pointer key).
  Implements compare-and-set using conditional expressions, handles pagination via DynamoDB queries,
  and supports TTL fields using native DynamoDB TTL attributes.
- **`DynamoPointerTableBootstrap`** – Optional bootstrapper that creates the pointer table if missing
  when `metacat.kv.auto-create=true`. Configures throughput, TTL, and key schema.
- **`S3BlobStore`** – Stores protobuf blobs inside an S3 bucket. `put` writes objects with
  server-side encryption (if configured) and stores metadata (ETag, content-type). `head` uses
  `HeadObject` to fetch metadata and returns `BlobHeader`.

## Public API / Surface Area
Implements `PointerStore` and `BlobStore` exactly as defined in the SPI. Additional behaviours:
- `DynamoPointerStore` honours TTL via `expires_at` (optional) and exposes metrics hooks for list and
  write throughput.
- `S3BlobStore.list` uses `ListObjectsV2` with continuation tokens to support GC scanning.

## Important Internal Details
- **CAS semantics** – DynamoDB’s `ConditionExpression` enforces pointer versions. Creation uses
  `attribute_not_exists(key)`; updates compare the stored `version` attribute.
- **Pagination tokens** – DynamoDB returns `LastEvaluatedKey`; the store encodes this as the
  `nextToken`. The service stores these tokens in page responses without modification.
- **Data integrity** – `S3BlobStore.put` calculates SHA256 locally, compares it against the returned
  ETag (or `x-amz-meta-sha256`), and throws `StorageAbortRetryableException` on mismatch, allowing
  `BaseServiceImpl.runWithRetry` to reattempt.
- **Bootstrap** – `DynamoPointerTableBootstrap` checks `metacat.kv.auto-create` and `metacat.kv.table`.
  When enabled, it creates the table with TTL support (`expires_at`).

## Data Flow & Lifecycle
```
Repository write → DynamoPointerStore.compareAndSet (PutItem with conditions)
  → S3BlobStore.put (PutObject) + head verification
  → Metadata reads (GetItem/HeadObject)
GC → DynamoPointerStore.listPointersByPrefix (Query) → deleteByPrefix (BatchWrite)
```

## Configuration & Extensibility
Key properties (see `service/application.properties` for defaults):
- `metacat.kv=dynamodb`, `metacat.kv.table=<tableName>`, `metacat.kv.auto-create=true|false`,
  `metacat.kv.ttl-enabled=true|false`.
- `metacat.blob=s3`, `metacat.blob.s3.bucket=<bucket>`, `aws.region=<region>`.
- Provide AWS credentials via the default SDK provider chain (env vars, profiles, IAM roles) or
  override the client builders inside `AwsClients`.

Extensibility:
- Adjust throughput and TTL policies inside `DynamoPointerTableBootstrap` if you need custom
  provisioning.
- Wrap additional S3 options (encryption, storage classes) inside `S3BlobStore` by extending metadata
  tags.

## Examples & Scenarios
- **Production deployment** – Set `metacat.kv=dynamodb`, `metacat.blob=s3`, supply the DynamoDB table
  name and S3 bucket. On startup, `AwsClients` initialises AWS SDK clients and repositories begin
  writing to DynamoDB/S3. Stats ingestion and scan bundles now persist durably.
- **Disaster recovery** – Because blobs are immutable and pointers encode versions, restoring a table
  involves rehydrating DynamoDB pointers (versions) and replaying snapshots stored in S3. GC settings
  control how long idempotency records and tombstones linger.

## Cross-References
- SPI contract: [`docs/storage-spi.md`](storage-spi.md)
- Repository usage: [`docs/service.md`](service.md)
