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
* fills deterministic defaults for OIDs, typmods, lengths, collision, etc.
* returns the canonical `Floe*Specific` proto (e.g., `FloeColumnSpecific`, `FloeRelationSpecific`).

It never mutates scan builders or catalog responses, and all callers share the same defaults.

## 3. PG scanners

Each `pg_*` scanner:

* extracts the current `MetadataResolutionContext` (and thus the normalized `EngineContext`) and parses the logical type once, reusing a cached `TypeResolver` (these resolvers are request-scoped and typically stored per relation/decoration, not globally).
* calls `FloeHintResolver` (e.g., `columnSpecific`) with the context + resolver + parsed type.
* projects the returned fields into the row (attrelid/attnum/attlen/attalign/attstorage/attcollation/attbyval/type OID/typmod/ndims/etc.).

The scanner never invents OIDs/typmods itself; it relies entirely on the shared resolver. When overlays are missing, the resolver still returns safe defaults (typmod = -1, attlen = -1, attalign = 'i', attstorage = 'p', attndims = 0, attcollation = 0).

## 4. Catalog bundle decoration

When `CatalogBundleService` builds responses it:

* collects cached relation/column metadata from `RelationDecoration` + `ColumnDecoration`.
* asks `EngineMetadataDecorator` (e.g., `FloeEngineSpecificDecorator`) for Floe hints when overlays are enabled.
* the decorator calls `FloeHintResolver`, wraps the resulting proto in `EngineSpecific` via `FloePayloads`, and attaches it to the bundle.

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
