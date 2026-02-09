# AWS Storage Backend

## Overview
`storage/aws/` contains production-grade implementations of the storage SPI backed by Amazon
DynamoDB (pointer store) and Amazon S3 (blob store). It also includes bootstrapping helpers to ensure
DynamoDB tables exist before the service starts, plus a Secrets Manager-backed secrets store.

## Architecture & Responsibilities
- **`AwsClients`** – Centralises AWS SDK v2 clients (DynamoDB, S3) configured via Quarkus properties
  (`floecat.fileio.override.*`, credentials providers). Injected into both stores.
- **`DynamoPointerStore`** – Stores pointer entries in DynamoDB tables (partition key = pointer key).
  Implements compare-and-set using conditional expressions, handles pagination via DynamoDB queries,
  and supports TTL fields using native DynamoDB TTL attributes.
- **`DynamoPointerTableBootstrap`** – Optional bootstrapper that creates the pointer table if missing
  when `floecat.kv.auto-create=true`. Configures throughput, TTL, and key schema.
- **`S3BlobStore`** – Stores protobuf blobs inside an S3 bucket. `put` writes objects with
  server-side encryption (if configured) and stores metadata (ETag, content-type). `head` uses
  `HeadObject` to fetch metadata and returns `BlobHeader`.
- **`ProdSecretsManager`** – Stores secret payloads in AWS Secrets Manager with ABAC-friendly tags.

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
- **Bootstrap** – `DynamoPointerTableBootstrap` checks `floecat.kv.auto-create` and `floecat.kv.table`.
  When enabled, it creates the table with TTL support (`expires_at`).

## Data Flow & Lifecycle
```
Repository write → DynamoPointerStore.compareAndSet (PutItem with conditions)
  → S3BlobStore.put (PutObject) + head verification
  → Metadata reads (GetItem/HeadObject)
GC → DynamoPointerStore.listPointersByPrefix (Query) → deleteByPrefix (BatchWrite)
CAS blob GC → S3BlobStore.list (ListObjectsV2) → deletePrefix
```

## Configuration & Extensibility
Key properties (see `service/application.properties` for defaults):
- `floecat.kv=dynamodb`, `floecat.kv.table=<tableName>`, `floecat.kv.auto-create=true|false`,
  `floecat.kv.ttl-enabled=true|false`.
- `floecat.blob=s3`, `floecat.blob.s3.bucket=<bucket>`,
  `floecat.fileio.override.s3.region=<region>`.
- Provide AWS credentials via the default SDK provider chain (env vars, profiles, IAM roles) or
  override the client builders inside `AwsClients`.

Extensibility:
- Adjust throughput and TTL policies inside `DynamoPointerTableBootstrap` if you need custom
  provisioning.
- Wrap additional S3 options (encryption, storage classes) inside `S3BlobStore` by extending metadata
  tags.

## Examples & Scenarios
- **Production deployment** – Set `floecat.kv=dynamodb`, `floecat.blob=s3`, supply the DynamoDB table
  name and S3 bucket. On startup, `AwsClients` initialises AWS SDK clients and repositories begin
  writing to DynamoDB/S3. Stats ingestion and scan bundles now persist durably.
- **Disaster recovery** – Because blobs are immutable and pointers encode versions, restoring a table
  involves rehydrating DynamoDB pointers (versions) and replaying snapshots stored in S3. GC settings
  control how long idempotency records and tombstones linger.

## Local Testing with LocalStack
- `make localstack-up` – Starts a LocalStack container (DynamoDB + S3) and provisions buckets/tables.
- `make run-localstack-localstack` – Runs `quarkus:dev` with both upstream and catalog pointing at
  LocalStack. Uses `floecat.kv=dynamodb` / `floecat.blob=s3` and path-style S3 access.
- `make run-localstack-aws` – Upstream LocalStack, catalog real AWS.
- `make run-aws-localstack` – Upstream real AWS, catalog LocalStack.
- `make test-localstack` – Runs unit + IT suites against LocalStack backends.
- `make localstack-down` – Stops the LocalStack container started by the Make targets.

Use the default credentials (`test`/`test`) or override them via `LOCALSTACK_ACCESS_KEY` /
`LOCALSTACK_SECRET_KEY` when invoking Make. The Make target idempotently provisions the S3 bucket,
DynamoDB table, and TTL attribute using the bundled `awslocal` CLI.

## Cross-References
- SPI contract: [`docs/storage-spi.md`](storage-spi.md)
- Repository usage: [`docs/service.md`](service.md)
- Secrets Manager integration: [`docs/secrets-manager.md`](secrets-manager.md)
