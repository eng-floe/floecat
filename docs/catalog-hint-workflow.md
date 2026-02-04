# Shared hint workflow

This repository now has one compute layer that produces Floe engine hints plus two sinks (scanners and decorators) that consume the same data. Here's how the flow works end-to-end:

```
          Request headers           EngineContext
        ┌──────────────┐        ┌──────────────┐
        │  planner /   │ ─────>│ EngineContext │
        │  bundle RPC  │        └──────────────┘
        └──────────────┘               │
                │                      ▼
                ▼              SystemDefinitionRegistry
   Builder fetch →            (kind-level cache + fingerprint)
                ▼                      │
                │                      ▼
        SystemNodeRegistry ─────────▶ BuiltinNodes (kind+version cache)
               /│\
              / │ \                   
             /  │  \  (FloeHintResolver is external; both scanners and decorator call it)
        Scanners Decorator ...
           │        │
  pg_* rows produced   Catalog bundle decorated
```

## 1. EngineContext and system catalog layers

`EngineContext` is built once from request headers; missing headers map to `floecat_internal`. `SystemDefinitionRegistry` caches the raw plugin snapshot per normalized kind (no provider merges). `SystemNodeRegistry` fetches that snapshot, seeds it with `FloecatInternalProvider` (the shared `information_schema`), overlays the plugin catalog when available, and, if overlays are enabled, merges any `SystemObjectScannerProvider.definitions(kind, version)` entries before filtering by `EngineSpecificMatcher`. Layer‑1 fingerprints reflect the raw snapshot, layer‑2 fingerprints reflect the merged catalog. Headers that mention an unknown engine kind still fall back to `floecat_internal`: the provider returns the fallback catalog, the cache key is the requested kind, and `SystemNodeRegistry` treats the result as `floecat_internal`, so overlays/decorators are skipped while the shared `information_schema` layer remains visible.

## 2. Shared hint resolver

`FloeHintResolver` lives under `extensions/floedb/.../hints/`. Every scanner plus the decorator calls it with:

* `MetadataResolutionContext` (which contains the active `EngineContext`, overlay map, and `CatalogOverlay`),
* the node being described (`NamespaceNode`, `FunctionNode`, `GraphNode`, `TypeNode`, `SchemaColumn`, …),
* optional resolution helpers (type resolver, parsed `LogicalType`).

The resolver:

* looks for existing Floe payloads via `ScannerUtils`.
* applies overlay values if present.
* fills deterministic defaults for OIDs, typmods, lengths, collision, etc. When overlays are missing, ScannerUtils now defers to a pluggable `EngineOidGenerator` (currently configured through `floecat.extensions.floedb.engine.oid-generator=default`), and there is a payload-aware `fallbackOid(id, payloadType)` overload so different hint categories for the same resource hash to distinct values. The default generator compresses its UUID into ~31 bits, so collisions can still occur at scale; any persistence layer that stores generated OIDs may need to detect duplicates and retry with a new payload tag or salt. If you retry with a new salt (or payload tag), be sure to persist the resulting OID alongside the object before you rely on it again—otherwise you break the deterministic guarantee that future scans expect.
* returns the canonical `Floe*Specific` proto (e.g., `FloeColumnSpecific`, `FloeRelationSpecific`).

It never mutates scan builders or catalog responses, and all callers share the same defaults.

## 3. PG scanners

Each `pg_*` scanner:

* extracts the current `MetadataResolutionContext` (and thus the normalized `EngineContext`) and parses the logical type once, reusing a cached `TypeResolver` (these resolvers are request-scoped and typically stored per relation/decoration, not globally).
* calls `FloeHintResolver` (e.g., `columnSpecific`) with the context + resolver + parsed type.
* projects the returned FloeColumnSpecific fields into the row (attname, atttypid, atttypmod, attnum, attnotnull, attisdropped, attcollation, atthasdef) and synthesizes safe defaults for the remaining PG-only columns (attlen, attalign, attstorage, attndims).

The scanner never invents OIDs/typmods itself; it relies entirely on the shared resolver. When overlays are missing, the resolver still returns safe defaults (typmod = -1, attlen = -1, attalign = 'i', attstorage = 'p', attndims = 0, attcollation = 0).

## 4. Catalog bundle decoration

When `UserObjectBundleService` builds responses it:

* collects cached relation/column metadata from `RelationDecoration` + `ColumnDecoration`.
* asks `EngineMetadataDecorator` (e.g., `FloeEngineSpecificDecorator`) for Floe hints when overlays are enabled.
* the decorator calls `FloeHintResolver`, wraps the resulting proto in `EngineSpecific` via `FloePayloads`, and attaches it to the bundle.
* if snapshot-aware stats exist for the relation, the bundle includes them in `RelationInfo.stats` (a new `RelationStats` proto carrying best-effort `row_count` and `total_size_bytes`). This field is optional: resolvers may see `RelationInfo.stats` unset for views, system tables, or any unpinned relation, and should treat missing stats as a best-effort decoration rather than required data.

The decorator is a pure sink: it never parses schemas nor recomputes defaults, so the bundle payloads match exactly what `pg_*` scanners would have emitted for the same column in the same engine context.

## 5. Results for the planner

The planner now receives two consistent views:

* `pg_*` scanners stream `FloeColumnSpecific` fields into rows.
* Catalog bundles attach the same Floe payloads to `ColumnInfo`/`RelationInfo`, ensuring RPC consumers see the same metadata as scanners.

Both flows rely on:

* Normalized `EngineContext` (kind+version),
* `FloeHintResolver` for overlay/default fusion,
* A shared `FloePayloads` registry that knows how to wrap protos into `EngineSpecific`.

Because the resolver caches parsed schema info and the decorator is lightweight, the entire workflow scales to large catalogs without redundant recomputation.

## 6. Optional relation statistics

Catalog bundles now attach optional relation statistics via `RelationInfo.stats`
(`core/proto/src/main/proto/query/catalog_bundle.proto`). Each `RelationStats`
message carries the best-effort `row_count` and `total_size_bytes` values that were recorded for
the pinned snapshot. Because the `RelationStats` wrapper is optional, callers must guard
`relation.hasStats()` before reading the values. When stats are present, both counters are
populated (though `0` may represent “unknown” depending on the upstream source), so downstream
code can safely rely on the reported `row_count`/`total_size_bytes` when non-zero.
Planners should treat missing stats as a decorative hint and never rely on them for correctness.

Scanners get the same best-effort information through `MetadataResolutionContext.statsProvider()`
(`core/catalog/src/main/java/ai/floedb/floecat/systemcatalog/spi/scanner/MetadataResolutionContext.java:27`,
which always returns a non-null provider). The provider itself is defined in
`core/catalog/src/main/java/ai/floedb/floecat/systemcatalog/spi/scanner/StatsProvider.java:24`
and returns `Optional<TableStatsView>`/`Optional<ColumnStatsView>` so that callers can gracefully
handle the absence of data. The service layer wires `StatsProviderFactory`
(`service/src/main/java/ai/floedb/floecat/service/query/catalog/StatsProviderFactory.java:33`)
to cache results per `(tableId, snapshotId)` without mutating `QueryContext` or creating implicit
snapshot pins. These caches only emit values for pinned tables, and the provider always returns
`Optional.empty()` when the pin is missing, so system tables/views/unpinned relations continue to
flow through the bundle/scan paths without failure. Callers that need the data can request it via the
shared provider and add the resulting `RelationStats` to the bundle; everyone else is free to ignore
the field.

Because the stats plumbing is best-effort and stateless, it can be reused by scanners, decorators,
and catalog bundles alike without introducing new failure modes.
