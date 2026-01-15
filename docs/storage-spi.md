# Storage SPI

## Overview
The storage SPI defines how Floecat persists immutable protobuf blobs and mutable pointer entries.
It abstracts the underlying medium (in-memory, DynamoDB/S3, or future stores) so repositories inside
`service/` can compose high-level behaviours (idempotency, pagination, hierarchical lookups) without
being tied to specific infrastructure.

Two interfaces power the system:
- `BlobStore` – Byte-addressable storage for protobuf payloads.
- `PointerStore` – Versioned key/value metadata for name and hierarchy indexes.

Both live under `core/storage-spi/src/main/java/ai/floedb/floecat/storage`.

## Architecture & Responsibilities
- **BlobStore** responsibilities:
  - `get(uri)` – Fetch blob bytes by URI.
  - `put(uri, bytes, contentType)` – Store new version (overwrites allowed).
  - `head(uri)` – Return `BlobHeader` (ETag, size, timestamps, tags).
  - `delete(uri)` / `deletePrefix(prefix)` – Remove single blobs or a subtree.
  - Optional `getBatch` helper.
  - `list(prefix, limit, pageToken)` – Enumerate keys for GC.
- **PointerStore** responsibilities:
  - `get(key)` – Retrieve pointer (key, blob URI, version, optional expiry).
  - `compareAndSet(key, expectedVersion, nextPointer)` – CAS semantics for pointer creation/update.
  - `compareAndDelete(key, expectedVersion)` – CAS deletion.
  - `listPointersByPrefix(prefix, limit, pageToken, nextTokenOut)` – Hierarchical listing.
  - `deleteByPrefix(prefix)` / `countByPrefix(prefix)` – Multi-delete and counting.
- **Error contracts** – SPI declares checked runtime exceptions (e.g.
  `StorageNotFoundException`, `StorageConflictException`, `StoragePreconditionFailedException`,
  `StorageAbortRetryableException`, `StorageCorruptionException`) so the service can map them to
  gRPC status codes.

## Public API / Surface Area
Repositories call the SPI through the `PointerStore`/`BlobStore` interfaces; GC and seeding code also
use them directly. The service expects the following guarantees:
- Compare-and-set is linearizable (no lost updates).
- `head()` returns strong ETAGs for verification.
- `listPointersByPrefix` returns results in lexicographic order and supports opaque page tokens.
- `deletePrefix` and `deleteByPrefix` clean entire hierarchies (used when dropping catalogs).

## Important Internal Details
- Repositories rely on deterministic pointer keys. Implementations must treat keys as arbitrary
  strings; they often contain `/` or `:` characters.
- `Pointer.version` starts at `1` when created. `compareAndSet(key, 0, pointer)` is used to reserve a
  brand new entry; attempts with wrong versions must fail without side effects.
- `BlobStore.put` MUST behave atomically relative to `head()`; `BaseResourceRepository` verifies the
  ETag after writing.
- Stores may implement TTL by populating `Pointer.expires_at`; GC code respects this field.

## Data Flow & Lifecycle
```
Repository write → pointerStore.compareAndSet(key, expected, pointer)
  → blobStore.put(uri, bytes, contentType)
  → blobStore.head(uri) for verification
  → pointerStore.get(key) for `MutationMeta`
GC → pointerStore.listPointersByPrefix + pointerStore.deleteByPrefix
CAS blob GC → blobStore.list + blobStore.deletePrefix
```

## Configuration & Extensibility
- Select an implementation via `floecat.kv` (`memory`, `dynamodb`, ...)
  and `floecat.blob` (`memory`, `s3`, ...).
- Add new backends by implementing the interfaces and wiring them with CDI qualifiers (for example
  `@IfBuildProperty(name = "floecat.kv", stringValue = "foo")`).
- Ensure new stores honour the SPI’s CAS semantics and throw the defined exceptions.

## Examples & Scenarios
- **Creating a catalog** – `CatalogRepository.create` reserves keys
  (`/accounts/{account}/catalogs/by-name/{name}`), writes a blob (`catalog.pb`), and advances pointer
  versions. If another client created the catalog concurrently, `compareAndSet` fails and the service
  translates it to `MC_CONFLICT`.
- **GC** – `IdempotencyGc` iterates pointer prefixes representing idempotency records, deletes stale
  entries, and relies on `deleteByPrefix` to remove entire account subtrees efficiently. `CasBlobGc`
  lists blob prefixes to clean up unreferenced CAS blobs.

## Cross-References
- In-memory implementation: [`docs/storage-memory.md`](storage-memory.md)
- AWS implementation: [`docs/storage-aws.md`](storage-aws.md)
- Repository usage: [`docs/service.md`](service.md)
