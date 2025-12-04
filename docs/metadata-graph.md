# Metadata Graph

## Overview
Metacat’s query-facing services share a common metadata cache called the **Metadata Graph**. It sits
between the pointer/blob repositories and any RPC that needs to inspect catalogs, namespaces, tables,
or views. The graph provides:

- Immutable node models that can be safely reused across requests.
- A per-tenant Caffeine-backed cache keyed by resource ID + pointer version so invalidation is
  deterministic (can be disabled by setting `metacat.metadata.graph.cache-max-size` to 0; the limit
  applies to each tenant independently).
- Helper APIs for name resolution (Directory RPC parity) and snapshot pinning (Snapshot RPC parity).
- Extension points (`EngineHint`) so planners/executors can attach engine-specific payloads without
  mutating the base metadata structures.

```
┌────────────┐      ┌────────────────────┐      ┌───────────────────────┐
│ gRPC RPCs  │ ---> │ MetadataGraph APIs │ ---> │ Repositories / RPCs   │
│ (Query,    │      │  - resolve()       │      │  - Catalog/Table/View │
│  Planner,  │      │  - catalog()/...   │      │  - Directory/Snapshot │
│  Executors)│      │  - snapshotPinFor  │      │  - Storage backends   │
└────────────┘      └────────────────────┘      └───────────────────────┘
```

### Implementation Structure
The `service/query/graph` package is split into a few focused modules:

- `model/` – Immutable node records (`CatalogNode`, `NamespaceNode`, `TableNode`, `ViewNode`,
  `SystemViewNode`) plus shared enums (`RelationNodeKind`, `EngineKey`, `EngineHint`, etc.).
- `cache/` – `GraphCacheManager` + `GraphCacheKey` implement the per-tenant cache-of-caches and
  expose meters for hit/miss counts, tenant count, and total entries.
- `loader/` – `NodeLoader` wraps the catalog/namespace/table/view repositories to hydrate immutable
  nodes from protobuf metadata (`metaForSafe` + pointer fetches).
- `resolver/` – `NameResolver` handles catalog/namespace/table/view lookups, while
  `FullyQualifiedResolver` mirrors DirectoryService’s ResolveFQ list/prefix semantics.
- `snapshot/` – `SnapshotHelper` encapsulates snapshot pinning and schema resolution, wrapping the
  SnapshotService RPC stub.
- `MetadataGraph` – Thin façade that composes the helpers above and exposes the public APIs consumed
  by DirectoryService, QueryInputResolver, SchemaService, etc.

## Node Model
Nodes live under `service/query/graph` and each implements `RelationNode`. They are Java records with
defensive copies to guarantee immutability.

- `CatalogNode` – Lightweight display + connector/policy metadata. Optionally exposes namespace IDs
  for listing RPCs.
- `NamespaceNode` – Captures catalog ancestry, path segments, display name, and optional child IDs.
- `TableNode` – Holds logical schema JSON, partition keys, field-ID map, snapshot pointers (current,
  previous, resolved sets), optional stats summary, dependent view IDs, and engine hints.
- `ViewNode` – Stores SQL text, dialect, output columns, base relation IDs, creation search path, and
  optional owner.
- `SystemViewNode` – Reserved for virtual/system relations (e.g., `$files`, `$snapshots`).

Common fields:

| Field                  | Description                                                                 |
|------------------------|-----------------------------------------------------------------------------|
| `id()`                 | Stable `ResourceId` carrying tenant/kind/UUID.                              |
| `version()`            | Pointer version used to compose cache keys.                                 |
| `metadataUpdatedAt()`  | Repository mutation timestamp; informative only (not tied to snapshots).    |
| `engineHints()`        | Map keyed by `EngineKey(engineKind, engineVersion)` → opaque `EngineHint`.  |

### Engine Hints
`EngineHint` is a small struct with `contentType`, `payload`, and `source`. Planners can register
adapters that compute hints on demand and stash them inside the node map. Consumers should treat the
payload as immutable and versioned.

## Graph APIs
`MetadataGraph` (CDI `@ApplicationScoped`) exposes the APIs that higher layers call. Key methods:

| Method | Purpose |
|--------|---------|
| `Optional<RelationNode> resolve(ResourceId)` | Loads a node from cache or repository by ID/kind. |
| `Optional<CatalogNode> catalog(...)` / `namespace` / `table` / `view` | Typed convenience wrappers around `resolve`. |
| `ResourceId resolveName(String cid, NameRef ref)` | Mirrors DirectoryService semantics for planner RPCs (NameRef → ID). |
| `ResolveResult resolveTables(String cid, List<NameRef> list, int limit, String token)` | Resolves explicit table names (DirectoryService parity) with best-effort semantics. |
| `ResolveResult resolveTables(String cid, NameRef prefix, int limit, String token)` | Lists tables under a namespace prefix while enforcing Directory pagination contracts. |
| `ResolveResult resolveViews(String cid, List<NameRef> list, int limit, String token)` | Resolves explicit view names, returning canonical `NameRef`s and resource IDs. |
| `ResolveResult resolveViews(String cid, NameRef prefix, int limit, String token)` | Lists views below a prefix with next-page tokens and total counts. |
| `SnapshotPin snapshotPinFor(String cid, ResourceId tableId, SnapshotRef override, Optional<Timestamp> asOfDefault)` | Normalises snapshot selection (override → as-of → current). |
| `void invalidate(ResourceId id)` | Evicts every cached version of an ID (call after successful mutations). |

Internally the graph:

1. Calls the matching repository’s `metaForSafe` to fetch pointer version and mutation metadata.
2. Composes a cache key `(ResourceId, pointerVersion)`.
3. Rehydrates the protobuf record (`Catalog`, `Namespace`, `Table`, `View`) into the immutable node.
4. Returns cached nodes for future lookups until the pointer version changes.

### Snapshot Pinning Semantics
- Explicit snapshot ID overrides always win.
- Explicit AS-OF timestamps produce pins with `snapshot_id=0` and `as_of` set.
- `asOfDefault` is applied when no overrides exist (to support `BEGIN QUERY AS OF ...` semantics).
- Otherwise the graph calls `SnapshotService.GetSnapshot(SS_CURRENT)` to discover the latest ID.

### Name Resolution Semantics
`resolveName` first short-circuits when the NameRef embeds a `ResourceId`. Otherwise it performs the
table/view lookups directly (using the same repositories DirectoryService previously used) and
throws the same ambiguity/unresolved error codes as `DirectoryService.Resolve*`. Graph callers get
consistent NameRef → ResourceId translations without depending on a secondary RPC hop.

### Fully Qualified (ResolveFQ*) Semantics
`resolveTables/resolveViews` mirror the `ResolveFQ*` RPCs. The helpers accept either a list selector
or a namespace prefix, apply input validation, paginate using Directory-compatible tokens, and
return canonical `NameRef` + `ResourceId` pairs. DirectoryService now delegates to these helpers so
the graph defines the single source of truth for list/prefix resolution.

## Usage Guidelines
- **Always go through the graph** for read paths instead of hitting repositories directly. This keeps
  cache hit rate predictable and ensures planner/executor code sees immutable snapshots.
- **Call `invalidate`** whenever a catalog/namespace/table/view mutation succeeds. Pointer version
  bumps will naturally invalidate cache entries, but eviction shortens the window before readers see
  the new data.
- **Treat node instances as read-only**. They are immutable records but they may still be shared
  across requests via the cache, so do not mutate maps or lists after retrieval.
- **Attach engine hints sparingly**. Hints should be small (think JSON blobs or compact protobufs)
  and versioned so planners/executors can safely down-level or up-level between releases.

## Metrics
The graph surfaces a couple of Micrometer gauges so operators can verify cache state at runtime:

| Metric | Type | Description |
|--------|------|-------------|
| `metacat.metadata.graph.cache.enabled` | Gauge | 1.0 when caching is enabled, 0.0 when `cache-max-size=0`. |
| `metacat.metadata.graph.cache.max_size` | Gauge | Per-tenant configured max size (0 when caching is disabled). |
| `metacat.metadata.graph.cache.tenants` | Gauge | Number of tenant cache partitions that currently exist. |
| `metacat.metadata.graph.cache.entries` | Gauge | Total estimated entries across all tenant caches. |

These gauges complement the per-tenant `metacat.metadata.graph.cache{result=hit|miss,tenant=<id>}`
counters and the `metacat.metadata.graph.load` timer that track cache effectiveness.

## Testing
`MetadataGraphTest` uses in-memory repository/snapshot/directory fakes to exercise cache behavior and
helper semantics without Mockito or bytecode agents. Any new helper should be covered there. Higher
level components (e.g., `QueryInputResolverTest`) rely on lightweight graph fakes to validate their
own logic while still mirroring real graph responses.

## Future Work
- Add traversal helpers that expand view dependency trees into stable `RelationInfo` products.
- Surface resolved snapshot sets on `TableNode` for multi-table AS OF operations.
- Provide SPI hooks so connectors can contribute engine hints lazily.
- Harden per-tenant cache lifecycle (limit live tenant shards, auto-evict idle shards, and release
  tenant-specific metrics) so multi-tenant churn cannot exhaust heap or Micrometer registries.
