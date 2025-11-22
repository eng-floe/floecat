# In-Memory Storage

## Overview
`storage/memory/` provides reference implementations of the storage SPI for local development and
unit tests. It stores pointers and blobs in concurrent in-memory maps while preserving the atomic
semantics expected by the service.

The module registers its beans under the build properties `metacat.kv=memory` and
`metacat.blob=memory`, making it the default backend for `make run`.

## Architecture & Responsibilities
- **`InMemoryPointerStore`** – Maintains a `ConcurrentHashMap<String, Pointer>` and implements CAS by
  using `Map.compute` to examine/update entries atomically. Supports prefix listing by filtering keys,
  sorting lexicographically, and materialising `Pointer` snapshots.
- **`InMemoryBlobStore`** – Stores blobs inside a `ConcurrentHashMap<String, byte[]>` plus metadata
  (ETag, timestamps). Implements `head`, `get`, `put`, and prefix deletion by scanning key sets.

Both classes are annotated with `@Singleton` and `@IfBuildProperty` so Quarkus only wires them when
`metacat.kv`/`metacat.blob` match `memory`.

## Public API / Surface Area
Implements the SPI without deviation. `InMemoryPointerStore.listPointersByPrefix` throws an
`IllegalArgumentException` for invalid page tokens (matching service expectations). `countByPrefix`
provides fast enumerations for list APIs.

## Important Internal Details
- **Concurrency guarantees** – `compareAndSet` uses `ConcurrentHashMap.compute` to avoid race
  conditions. Versions start at `1` for new pointers, mirroring persistent stores.
- **Pagination tokens** – Tokens are the last key from the previous page (lexicographically sorted).
- **Deletion semantics** – `deleteByPrefix` removes every key that starts with the prefix; deleting
  `/` nukes the entire store (used during tests).

## Data Flow & Lifecycle
```
Repository writes pointer → InMemoryPointerStore.compareAndSet (atomic map compute)
  → store blob bytes in InMemoryBlobStore
  → reads fetch from the same maps, enabling deterministic tests
```
Because everything resides in memory, data resets whenever the process restarts.

## Configuration & Extensibility
- Enable by setting `metacat.kv=memory` and `metacat.blob=memory` (default in
  `service/src/main/resources/application.properties`).
- Ideal for unit tests; integration tests like `TableRepositoryTest` use it to validate repository
  logic without external dependencies.

## Examples & Scenarios
- **Quarkus dev mode** – Running `make run` launches the service with in-memory stores. Seed data is
  written into the maps during startup, and developers can inspect pointer counts via CLI commands.
- **Testing** – Repository tests instantiate `InMemoryPointerStore`/`InMemoryBlobStore` directly to
  assert behaviours such as CAS conflicts, ETag validation, and prefix listing.

## Cross-References
- Storage SPI contract: [`docs/storage-spi.md`](storage-spi.md)
- AWS implementation for production: [`docs/storage-aws.md`](storage-aws.md)
