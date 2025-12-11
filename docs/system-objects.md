## System Objects

System objects are the catalog-level tables that live alongside traditional SQL metadata: `information_schema.*`, `pg_catalog.*`, and any custom engine-provided relations. This subsystem keeps the catalog layer _pure_ (no Quarkus, no service wiring) while still letting the service layer supply cached metadata and letting extensions ship new tables.

### Architecture overview

- **`SystemObjectDefinition`** describes a single row source: the canonical `NameRef`, a `SystemObjectColumnSet`, and the scanner ID.
- **`SystemObjectScanner`** produces `SystemObjectRow` streams when a query needs real data. The scanner receives a `SystemObjectScanContext`, which is the catalog’s only bridge to runtime metadata.
- **`SystemObjectRegistry`** aggregates every provider discovered via `ServiceLoaderSystemObjectProvider` (builtins and plugin extensions). CDI produces the registry once per application and exposes it to the runtime metadata query path.
- **`SystemObjectGraphView` / `SystemObjectScanContext`** are deliberately thin abstractions over `MetadataGraph`. Providers rely on them for resolution, enumeration, schema JSON, and typed column maps.

### Built-in information_schema

`InformationSchemaProvider` is the default provider; it registers `schemata`, `tables`, and `columns`. Each scanner tightly controls caching:

1. `SystemObjectScanContext` keeps catalog/namespace IDs so scanners can reuse them across rows.
2. Namespace and table enumerations call through to `SystemObjectGraphView`, which in turn uses the cached `MetadataGraph` in `service`.
3. `columns` now also calls `ctx.columnTypes(table.id())`, which uses `MetadataGraphView.tableColumnTypes` and the shared `LogicalSchemaMapper` so we never reparse schema JSON for each column.

This setup keeps the core module free of editor or Quarkus dependencies while letting service stay in charge of expensive metadata.

### Writing your own provider

1. **Implement `SystemObjectProvider`** for addon tables. Provide definitions for all `NameRef`/`column set` pairs, return the matching scanner from `provide(...)`, and respect `supports(...)` so you can override builtins.
2. **Optional: implement `EngineSystemTablesExtension`** if you want engine-specific overrides; the provided `ServiceLoaderSystemObjectProvider` merges builtin and plugin providers, so the extra interface simply marks that your provider should load after the builtins.
3. **Emit rows with `SystemObjectRow`**. Use `SystemObjectScanContext` for every graph lookup (catalog, namespace, table, schema). The context caches `listNamespaces`, `listTables`, and `columnTypes`, so keep your scanner short and rely on provided helpers rather than your own caches.
4. **Register via `META-INF/services/ai.floedb.floecat.catalog.system_objects.spi.SystemObjectProvider`** (and/or the `EngineSystemTablesExtension` variant) inside your plugin jar. Plugins can commit a single jar that bundles connectors, system tables, and builtin extensions just by shipping this service file.

### Performance & scalability notes

- `SystemObjectGraphView` is an immutable adapter over `MetadataGraph`, so it benefits from the graph’s caches (catalog lookup, namespaces, tables, snapshots). Scanners should never resolve nodes by hitting the repository directly: always go through the context.
- `SystemObjectScanContext.columnTypes(tableId)` gives you the `{physicalPath → logicalType}` map computed by `LogicalSchemaMapper`, which reuses schema parsing logic already exercised by the query resolver.
- Enumerations within a scan occur once per runtime call (see `listTables`, `listNamespaces`), and these methods are intentionally simple backends for `SystemObjectRow`. Avoid additional global caching layers inside the scanner; let the graph keep hot data in memory.
- The registry merges builtin/extension providers lazily via `ServiceLoader`, so adding a new provider only affects startup (discovery) time. Keep scanner scans streaming and stateless to maintain low latency.

With these hooks documented, extensions can implement any new system table without disturbing the core catalog, and the service layer retains control over the underlying metadata caches and schema parsing.
