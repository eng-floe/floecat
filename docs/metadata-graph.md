# Metadata Graph

## Overview
Floecat’s query-facing services share a common metadata cache called the **Metadata Graph**. It sits
between the pointer/blob repositories and any RPC that needs to inspect catalogs, namespaces, tables,
or views. The graph provides:

- Immutable node models that can be safely reused across requests.
- A per-account Caffeine-backed cache keyed by resource ID + pointer version so invalidation is
  deterministic (can be disabled by setting `floecat.metadata.graph.cache-max-size` to 0; the limit
  applies to each account independently).
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
Immutable node models live under `core/metagraph/model`, while the runtime helpers and
facade sit inside `service/metagraph`. The split looks like this:

- `core/metagraph/model/` – Immutable node records (`CatalogNode`, `NamespaceNode`, `TableNode`,
  `ViewNode`, `SystemViewNode`) plus shared enums (`GraphNodeKind`, `EngineKey`, `EngineHint`,
  `GraphNodeOrigin`, etc.).
- `service/metagraph/cache/` – `GraphCacheManager` + `GraphCacheKey` implement the per-account
  cache-of-caches and expose meters for hit/miss counts, account count, and total entries.
- `service/metagraph/loader/` – `NodeLoader` wraps the catalog/namespace/table/view repositories to
  hydrate immutable nodes from protobuf metadata (`metaForSafe` + pointer fetches).
- `service/metagraph/resolver/` – `NameResolver` handles catalog/namespace/table/view lookups, while
  `FullyQualifiedResolver` mirrors DirectoryService’s ResolveFQ list/prefix semantics.
- `service/metagraph/snapshot/` – `SnapshotHelper` encapsulates snapshot pinning and schema resolution,
  wrapping the SnapshotService RPC stub.
- `service/metagraph/hint/` – `EngineHintManager` routes registered hint providers, matches them
  against `EngineKey`, and caches payloads so planners never race over engine versions.
- `service/metagraph/overlay/` – `UserGraph` (the Metadata Graph façade, see
  `service/metagraph/overlay/user/UserGraph.java`) composes the helpers above, exposes the public API,
  and keeps a `CatalogOverlay`-friendly view via `MetaGraph`. `SystemGraph` (in
  `overlay/systemobjects/SystemGraph.java`) consumes `SystemNodeRegistry` snapshots so pg_catalog-style
  system tables/views merge with the user metadata when callers go through the overlay.

## Node Model
Nodes live under `core/metagraph/model` and each implements `GraphNode`. They are Java records with
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
| `id()`                 | Stable `ResourceId` carrying account/kind/UUID.                              |
| `version()`            | Pointer version used to compose cache keys.                                 |
| `metadataUpdatedAt()`  | Repository mutation timestamp; informative only (not tied to snapshots).    |
| `engineHints()`        | Map keyed by `EngineHintKey(engineKind, engineVersion, payloadType)` → opaque `EngineHint`.  |

### Engine Hints
`EngineHint` is a small struct with `payloadType`, `payload`, and optional metadata; the `payloadType` string
matches the hint provider’s advertised `payloadType`/`payload_type` so callers can fetch the right payload. Planners can register
adapters that compute hints on demand and stash them inside the node map. Consumers should treat the
payload as immutable and versioned.

### Version‑Specificity and Matching Semantics
Engine‑specific hint providers rely on `EngineSpecificMatcher`, which compares an engine’s
(engine_kind, engine_version) against each rule’s declared constraints. Version matching is
inclusive: `min_version` and `max_version` both participate in ≥ / ≤ comparisons. Versions are
compared using natural ordering: numeric segments compare by magnitude, while mixed alphanumeric
segments (e.g., `16.0beta2`, `16.0rc1`) follow prefix ordering where numeric segments always sort
after alphabetic suffixes. Pre‑release versions (`alpha`, `beta`, `rc`) therefore sort strictly
before the corresponding final release. Rules omitting `engine_kind` inherit the catalog file’s
engine kind. Hint providers compute a fingerprint per node and engine so cached hints remain
isolated across engine versions and hint revisions.

### Builtin Nodes & Engine Filtering
Builtin SQL objects (types, functions, operators, casts, collations, aggregates) never hit the
pointer/blob repositories. Instead, `SystemNodeRegistry` (core/catalog) loads the pb/pbtxt catalogs once
per engine kind, materialises immutable relation nodes, and caches the result per
`(engine_kind, engine_version)`. Catalog files live under `resources/builtins` and follow the
`<engine_kind>.pb[pbtxt]` naming convention. Each builtin definition can declare one or more
`engine_specific` rules (engine kind + min/max versions + optional properties). The registry filters
definitions using those rules so a planner that sets `x-engine-kind=postgres` and
`x-engine-version=16.0` only sees builtin nodes that actually exist in that release. Callers that omit
either header simply receive an empty builtin bundle (the catalog files stay untouched), and
`GetSystemObjects` rejects the request until both headers are provided.

Each `engine_specific` block may also attach arbitrary key/value `properties`. When the registry
materialises a `(engine_kind, engine_version)` bundle it keeps only the rules that match the requested
engine/version, so the filtered catalog (and `GetSystemObjects` response) contains exactly the
entries that apply to the caller. Pbtxt authors rarely need to repeat the engine kind in every rule;
entries that omit it inherit the file’s engine kind. Builtin nodes intentionally stay rule-free; the
`SystemCatalogHintProvider` exposes the matching rules’ properties through publisher-defined payload
types (the `payload_type` field in the catalog rule), so planners request the hint whose `payloadType`
matches the catalog payload. Documented payload types live alongside the catalog definitions. Catalog
authors should prefer stable, namespaced strings (e.g., `builtin.systemcatalog.function.semantic+json`
or `floe.type+proto`) so that consumers can register decoders per payload family and avoid accidental
collisions. Catalog authors control override behavior by ordering `engine_specific` rules intentionally:
entries stay in pbtxt order (including overlay merges), and the provider treats the *last* matching
rule as the one to publish.

The matcher applies all engine-specific constraints eagerly when materialising builtin bundles. For a
given `(engine_kind, engine_version)` pair, only the rules that match the naturally-ordered version
boundaries are retained. The `BuiltinNodes` returned by `SystemNodeRegistry.nodesFor` therefore already
represent the exact set applicable for that engine release. `SystemGraph` consumes those nodes to build
a `_system` `GraphSnapshot` that `MetaGraph` exposes via `CatalogOverlay`, so pg_catalog-style system
objects live alongside the user metadata when scanners run. `SystemObjectsServiceImpl` reuses the same
`SystemNodeRegistry`/`SystemCatalogProtoMapper` pipeline to answer `GetSystemObjects()` calls without
recomputing the catalog data, and because builtin catalogs are immutable per engine version the registry
keeps them entirely in memory until FloeCAT restarts.

### Deterministic Hint Caching
The hint system used by builtin catalog providers and other planners is backed by a weight-bounded
Caffeine cache keyed on `(resourceId, pointerVersion, engineKey, payloadType, fingerprint)`. The
fingerprint is provider‑defined and ensures that changes in provider logic (e.g., version of a
builtin definition, rule filtering logic, or planner‑specific metadata) produce new cached entries.
Cache eviction is weight‑aware: inserts that exceed the configured maximum immediately trigger
synchronous eviction when running in test mode, and asynchronous eviction in production. Eviction
can invalidate old hints even when pointer versions are unchanged, ensuring stale engine‑specific
metadata is not reused beyond its boundary conditions.

## Graph APIs
The `UserGraph` façade (CDI `@ApplicationScoped`, see `service/metagraph/overlay/user/UserGraph.java`)
exposes the Metadata Graph APIs that higher layers call. Key methods:

| Method | Purpose |
|--------|---------|
| `Optional<GraphNode> resolve(ResourceId)` | Loads a node from cache or repository by ID/kind. |
| `Optional<CatalogNode> catalog(...)` / `namespace` / `table` / `view` | Typed convenience wrappers around `resolve`. |
| `ResourceId resolveName(String cid, NameRef ref)` | Mirrors DirectoryService semantics for planner RPCs (NameRef → ID). |
| `ResolveResult resolveTables(String cid, List<NameRef> list, int limit, String token)` | Resolves explicit table names (DirectoryService parity) with best-effort semantics. |
| `ResolveResult resolveTables(String cid, NameRef prefix, int limit, String token)` | Lists tables under a namespace prefix while enforcing Directory pagination contracts. |
| `ResolveResult resolveViews(String cid, List<NameRef> list, int limit, String token)` | Resolves explicit view names, returning canonical `NameRef`s and resource IDs. |
| `ResolveResult resolveViews(String cid, NameRef prefix, int limit, String token)` | Lists views below a prefix with next-page tokens and total counts. |
| `SnapshotPin snapshotPinFor(String cid, ResourceId tableId, SnapshotRef override, Optional<Timestamp> asOfDefault)` | Normalises snapshot selection (override → as-of → current). |
| `void invalidate(ResourceId id)` | Evicts every cached version of an ID (call after successful mutations). |

### Engine Hint Retrieval
All tables and views participating in planning may embed engine‑specific hints. The Metadata Graph
delegates hint evaluation to the EngineHintManager, which selects providers based on node kind,
hint type, and engine availability. Hints are cached per fingerprint and engine key so that
planners requesting different engine versions or planner modes never interfere with one another.

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

## Query Catalog Service
`UserObjectsService.GetUserObjects` now streams `UserObjectsBundleChunk`s directly from the metadata
graph. Each chunk carries a header, batched relation resolutions (`RelationResolutions`) and a final 
summary, so planners can start binding as soon as the service resolves each relation. The service 
shares the same `QueryContext` as the other query RPCs and relies on `CatalogOverlay.resolve`, 
`snapshotPinFor`, and view metadata stored in `ViewNode` to produce canonical names, pruned schemas,
and view definitions without issuing a second RPC batch.
Resolved tables/views also go through `QueryInputResolver` so their snapshot pins are merged into
`QueryContext` before the response hits the planner—`FetchScanBundle` can therefore find the same
pins later in the lifecycle. Builtins remain behind `GetSystemObjects`.

## Metrics
The graph surfaces a couple of Micrometer gauges so operators can verify cache state at runtime:

| Metric | Type | Description |
|--------|------|-------------|
| `floecat.metadata.graph.cache.enabled` | Gauge | 1.0 when caching is enabled, 0.0 when `cache-max-size=0`. |
| `floecat.metadata.graph.cache.max_size` | Gauge | Per-account configured max size (0 when caching is disabled). |
| `floecat.metadata.graph.cache.accounts` | Gauge | Number of account cache partitions that currently exist. |
| `floecat.metadata.graph.cache.entries` | Gauge | Total estimated entries across all account caches. |

These gauges complement the per-account `floecat.metadata.graph.cache{result=hit|miss,account=<id>}`
counters and the `floecat.metadata.graph.load` timer that track cache effectiveness.

## Testing
`MetadataGraphTest` uses in-memory repository/snapshot/directory fakes to exercise cache behavior and
helper semantics without Mockito or bytecode agents. Any new helper should be covered there. Higher
level components (e.g., `QueryInputResolverTest`) rely on lightweight graph fakes to validate their
own logic while still mirroring real graph responses.

## Future Work
- Add traversal helpers that expand view dependency trees into stable `RelationInfo` products.
- Surface resolved snapshot sets on `TableNode` for multi-table AS OF operations.
- Provide SPI hooks so connectors can contribute engine hints lazily.
- Harden per-account cache lifecycle (limit live account shards, auto-evict idle shards, and release
  account-specific metrics) so multi-account churn cannot exhaust heap or Micrometer registries.
