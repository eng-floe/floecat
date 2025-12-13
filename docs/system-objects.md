# System Objects

System objects are the catalog-level row sources that live inside the `_system` account: `information_schema.*`, `pg_catalog.*`, and any engine/plugin-provided tables or views. Unlike user tables, these objects are not backed by persisted blobs – they are synthesised from the builtin catalog metadata, cached graph snapshots, and bespoke scanners. The builtin catalog load/caching pipeline that feeds these snapshots is described in [Builtin catalog architecture](builtin-catalog.md), and `EngineSystemCatalogExtension` is the single SPI that now ships both the builtin data and the corresponding system-object definitions.

### Architecture overview

```
┌────────────────────────────────────────────┐
│ ServiceLoaderSystemCatalogProvider         │
│ - discovers EngineSystemCatalogExtension   │
│ - merges SystemObjectScannerProvider defs  │
└────────────────────────────────────────────┘
                    ↓
┌────────────────────────────────────────────┐
│ SystemCatalogData + SystemEngineCatalog     │
└────────────────────────────────────────────┘
                    ↓
┌────────────────────────────────────────────┐
│ SystemNodeRegistry → BuiltinNodes          │
│ - filters EngineSpecific rules             │
└────────────────────────────────────────────┘
                    ↓
┌────────────────────────────────────────────┐
│ SystemGraph (GraphSnapshot cache)           │
│ - reuses BuiltinNodes to build namespace    │
│   buckets & SystemTableNode instances       │
└────────────────────────────────────────────┘
                    ↓
┌────────────────────────────────────────────┐
│ MetaGraph / CatalogOverlay                  │
│ (implements SystemObjectGraphView)          │
│ - resolves `_system` nodes via SystemGraph  │
│ - delegates to MetadataGraph for user data  │
└────────────────────────────────────────────┘
                    ↓
┌────────────────────────────────────────────┐
│ SystemObjectScanContext (ctx)               │
│ - built with CatalogOverlay/SystemObjectGraph │
└────────────────────────────────────────────┘
                    ↓
┌────────────────────────────────────────────┐
│ SystemObjectScanner → SystemObjectRow       │
└────────────────────────────────────────────┘
```

The core pieces:

- **`SystemObjectDef`** – Describes a system row source (`NameRef`, `SchemaColumn[]`, `scannerId`, `TableBackendKind`, etc.). Implemented by `SystemNamespaceDef`, `SystemTableDef`, `SystemViewDef`.
- **`SystemObjectScanner`** (`core/catalog`, `ai.floedb.floecat.systemcatalog.spi.scanner`) – Exposes `schema()` and a lazy `scan(SystemObjectScanContext)` that returns `Stream<SystemObjectRow>`. Scanners must be allocation-light, respect the Arrow schema, and keep rows as `Object[]`.
- **`SystemObjectScannerProvider`** – SPI for providers of definitions and scanners. `definitions()` lists every `SystemObjectDef` (no filtering). `supportsEngine`/`supports(NameRef, engineKind)` gate when a definition applies. `provide(scannerId, engineKind, engineVersion)` lets the runtime look up the scanner for a node’s `scannerId`.
- **`ServiceLoaderSystemCatalogProvider`** – Discovers `EngineSystemCatalogExtension`s (which already extend `SystemObjectScannerProvider`), merges their definitions with the default `InformationSchemaProvider`, fingerprints the `SystemCatalogData`, and hands the snapshot to `SystemDefinitionRegistry`.
- **`SystemNodeRegistry` + `SystemGraph`** – The registry filters the snapshot for the requested engine/version, materialises `GraphNode`s (functions, types, aggregates) and `SystemTableNode`s (with their `scannerId`s), and caches the result. `SystemGraph` builds `GraphSnapshot`s from those nodes and keeps them in an LRU `LinkedHashMap` keyed by `(engineKind, engineVersion)`. Each snapshot stores namespace buckets, table relations, and a `nodesById` map for constant-time resolution.
- **`CatalogOverlay` / `MetaGraph`** – The overlay implements `SystemObjectGraphView` and exposes an immutable view over `MetadataGraph` plus the `_system` snapshot from `SystemGraph`. `SystemObjectScanContext` receives this view and uses it for every lookup, so scanners consistently benefit from the metadata graph’s caches.

### Built-in information_schema

`InformationSchemaProvider` is the default `SystemObjectScannerProvider`. It unconditionally registers:

1. The `information_schema` namespace (`SystemNamespaceDef`).
2. Tables `information_schema.tables`, `information_schema.columns`, and `information_schema.schemata` (`SystemTableDef`s) with identifiers like `tables_scanner`.

Each table wires to a lightweight scanner (`TablesScanner`, `ColumnsScanner`, `SchemataScanner`). These scanners rely on `SystemObjectScanContext` for cached lookups:

1. `ctx.listNamespaces()`/`ctx.listTables()` reuse the overlay, so the catalog’s graph snapshot is only enumerated once per scan.
2. `columns` calls `ctx.columnTypes(table.id())`, which delegates to `CatalogOverlay.tableColumnTypes` → `MetadataGraph.table(...)` → `LogicalSchemaMapper`, ensuring schema JSON is parsed once per table.

### Writing your own provider

1. **Implement `SystemObjectScannerProvider`** (or build this into your `EngineSystemCatalogExtension`). Provide every `SystemObjectDef`/`SchemaColumn` pair, return the matching scanner from `provide(...)`, and let `supports(...)` gate whether your definition overrides a builtin.
2. **Optional: implement `EngineSystemCatalogExtension`** – it already extends `SystemObjectScannerProvider`, so you can ship a catalog + system tables in one jar. The unified loader merges your definitions with `InformationSchemaProvider` and fingerprints the snapshot.
3. **Emit rows with `SystemObjectRow`** – `SystemObjectRow` is a cheap wrapper around `Object[]`. Use `SystemObjectScanContext` for every graph lookup (catalog, namespace, table, schema) so you benefit from `CatalogOverlay`’s caches.
4. **Register via `META-INF/services/ai.floedb.floecat.systemcatalog.provider.SystemObjectScannerProvider`** (and/or `EngineSystemCatalogExtension`). CDI exposes the provider list through `ServiceLoaderSystemCatalogProvider.providers()`, so downstream services can resolve scanners by ID.

### Performance & scalability notes

- `SystemGraph` caches snapshots in an access-ordered `LinkedHashMap` sized by `floecat.system.graph.snapshot-cache-size` (default `16`). Each snapshot already buckets namespaces, relations, and node lookups, so repeated `_system` scans never rebuild the graph.
- `SystemObjectScanContext` keeps the catalog/namespace `ResourceId`s, reuses `CatalogOverlay` enumeration methods, and caches `listNamespaces`, `listTables`, and `columnTypes`. Scanners should not duplicate this caching logic – the context forwards to the cached overlay that already talks to `MetadataGraph`.
- `CatalogOverlay`/`MetaGraph` implement `SystemObjectGraphView` (`ai.floedb.floecat.systemcatalog.spi.scanner`) so that the core catalog module stays unaware of the full metadata graph. `SystemGraph` answers `_system` requests while `MetadataGraph` handles user objects, but both feed into the same overlay.
- `SystemTableNode.scannerId()` carries the bridge between metadata and row generation. Row-oriented components can look up the correct `SystemObjectScanner` by calling `provide(scannerId, engineKind, engineVersion)` on the discovered providers list.
- Keep scanners lazy and stateless. Every `SystemObjectScanner` should stream rows, avoid boxing, and match its `SchemaColumn[]` exactly. The shared `SystemObjectScanContext` is the only place scanners should touch metadata – everything else (name resolution, schema parsing, column typing) is already cached.
