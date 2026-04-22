# Builtin Catalog Architecture

## Overview

Floecat's builtin catalog system provides engine-specific metadata (functions, operators, types, casts, collations, aggregates) to query planners. This enables the planner to understand and rewrite queries against heterogeneous engines (Floe, Postgres, Trino, Spark, etc.) without embedding engine-specific logic.

The architecture is **plugin-based**: each engine implements a builtin catalog plugin that provides structured, versioned metadata matching the engine's capabilities.

## Design Principles

1. **Engine-Agnostic Core** – The core proto (`engine_specific.proto`) defines only the envelope; engines plug in without modifying the core.
2. **ServiceLoader Discovery** – Plugins are discovered automatically via Java's `ServiceLoader` mechanism at runtime.
3. **Proto Extensions** – Plugins define proto extensions on the core `EngineSpecific` message to allow rich PBtxt files while preserving core simplicity.
4. **Versioned Metadata** – Each plugin defines metadata per engine version; the planner can request version-specific capabilities.

## Architecture

```
┌──────────────┐
│ Planner      │
│ (engine_kind,│
│  engine_ver) │
└──────┬───────┘
       │ gRPC GetSystemObjects
       ▼
┌────────────────────────────────────────────────────────────┐
│ SystemObjectsServiceImpl (service)                        │
│ - validates headers + correlation id                        │
│ - calls SystemNodeRegistry.nodesFor(kind, version)          │
│ - maps SystemCatalogData → SystemObjectsRegistry via              │
│   SystemCatalogProtoMapper                                    │
└──────┬──────────────────────────────────────────────────────┘
       │
       ▼
┌────────────────────────────────────────────────────────────┐
│ SystemNodeRegistry (core/catalog)                           │
│ - caches BuiltinNodes per (engineKind, engineVersion)       │
│ - filters SystemCatalogData with EngineSpecificMatcher       │
│ - materialises GraphNodes + SystemTable/Table/View defs     │
└──────┬──────────────────────────────────────────────────────┘
       │
       ▼
┌────────────────────────────────────────────────────────────┐
│ SystemDefinitionRegistry                                    │
│ - caches SystemEngineCatalog per engine kind                │
│ - hands off to SystemCatalogProvider                        │
└──────┬──────────────────────────────────────────────────────┘
       │
       ▼
┌────────────────────────────────────────────────────────────┐
│ ServiceLoaderSystemCatalogProvider                          │
│ - discovers EngineSystemCatalogExtension via ServiceLoader   │
│ - returns the raw SystemCatalogData for the normalized kind  │
│ - fingerprints the kind-level catalog without provider merges│
└──────┬──────────────────────────────────────────────────────┘
       │
   ┌───┴───┬───────────────────────┐
   │Plugin │InformationSchema      │
   │Catalog│Provider → scanners     │
   └───────┴───────────────────────┘
       │
       ▼
┌────────────────────────────────────────────────────────────┐
│ SystemCatalogData + SystemEngineCatalog (immutable snapshot)│
└────────────────────────────────────────────────────────────┘
```

### Builtin-loading details

Builtins live on the classpath under `builtins/<engineKind>/`. Each engine directory contains a lexically-ordered `_index.txt` file plus one or more `.pbtxt` fragments:

```
builtins/
  floecat_internal/
    _index.txt
    00_system_relations.pbtxt
  floedb/
    _index.txt
    00_registry.pbtxt
    10_types.pbtxt
    …
```

The `_index.txt` file lists fragments in the order they should be merged. Lines that are blank or start with `#` are ignored. Each fragment is a proto-text encoding of `SystemObjectsRegistry` (see [`system_objects_registry.proto`](https://github.com/eng-floe/floecat/blob/main/core/proto/src/main/proto/floecat/query/system_objects_registry.proto)). During startup Floe catalogs parse each fragment into a `SystemObjectsRegistry.Builder` and apply them sequentially via `SystemObjectsRegistryMerger` (which now merges builder→builder to avoid extra allocations). The merged result is then rewritten by `SystemCatalogProtoMapper` and cached as `SystemCatalogData`.

The loader always applies `floecat_internal` first, then overlays the engine-specific catalog (for instance, `floedb`), and finally allows scanner-provided overlays from `SystemObjectScannerProvider` implementations when the request carried engine headers. Overrides happen deterministically because each stage stores entries in a `LinkedHashMap` keyed by canonical names; we also log overrides at DEBUG to make the behavior visible during debugging.

#### Override precedence contract

1. `floecat_internal` (shared information_schema) seeds every catalog with namespaces/tables/views.
2. The plugin catalog returned by `SystemDefinitionRegistry` overlays the base definitions.
3. When engine headers are present and the normalized kind is not `floecat_internal`, each `SystemObjectScannerProvider` that supports that kind can overlay definitions for the specific `(engineKind, engineVersion)` tuple.
4. Within each stage, later fragments override earlier ones (controlled by `_index.txt` ordering); identical canonical names always respect the last writer.

When headers are absent or the engine kind is unknown, `EngineContext.effectiveEngineKind()` resolves to `floecat_internal`, so the overlay steps beyond the base layer are skipped and only the shared relations remain available.

### System table backend contract

`SystemTableDef` now records a `TableBackendKind` (proto `TABLE_BACKEND_KIND_*`) plus backend-specific metadata:  
| Backend | Description | Required field | Scanner policy |
| --- | --- | --- | --- |
| `FLOECAT` | Rows produced by Floecat scanners (information_schema, system tables, plugin metadata tables). | `scannerId` (non-blank) | `SystemScannerResolver` accepts only `FloeCatSystemTableNode` instances, so only FLOECAT tables can be scanned through `SystemScannerResolver`. |
| `ENGINE` | Rows produced directly by an engine backend (e.g., engine information tables). | - | Metadata-only: not scanable through `SystemScannerResolver` and exposed solely for planners to reason about engine-provided hints. |
| `STORAGE` | Tables whose rows are stored on disk. | `storagePath` | Metadata-only: not scanable via `SystemScannerResolver` and often serve as hints in downstream planning (e.g., partitions or external tables). |

 `SystemTableDef` throws at construction time when the required backend-specific value is missing, guaranteeing the graph never exposes partially-specified tables.

### Engine-specific hint contract

Every `EngineSpecificRule` with a `payloadType` is mapped to a metagraph `EngineHint` whose key is `(engineKind, engineVersion, payloadType)` and whose value contains the payload bytes plus properties. The `EngineHintsMapper` replaces null payloads with an empty byte array to avoid NPEs, and it throws `IllegalStateException` if two rules share the same `(engineKind, engineVersion, payloadType)` triple. Column-level hints are grouped per column name; duplicate column names are already rejected by `SystemTableDef` so the per-column maps stay one-to-one with the schema. These hints drive scanner/table metadata, so when you add engine-specific definitions ensure each `EngineSpecificRule` has a unique payload type per engine/version.

`ServiceLoaderSystemCatalogProvider` is the gatekeeper for engine plugins: it discovers every `EngineSystemCatalogExtension`, loads the normalized catalog snapshot for each engine kind, and fingerprints the raw data without applying any provider overlays. `SystemDefinitionRegistry` caches those snapshots keyed by `EngineContext.effectiveEngineKind()` (so blank headers collapse to `floecat_internal`) so the kind-level catalog only needs to parse once. The real layering happens in `SystemNodeRegistry`: on a cache miss it seeds the result with `FloecatInternalProvider` (the `floecat_internal` base that always brings `information_schema`), overlays the plugin catalog, and finally applies `SystemObjectScannerProvider.definitions(engineKind, engineVersion)` entries when overlays are enabled (i.e., headers are present and the plugin exists). The floecat_internal layer only contributes namespace/table/view metadata (the shared `information_schema`/`pg_catalog` relations and their hints) so functions/operators/types/casts/aggregates are never merged from this internal layer; those object classes must come from engine plugins/providers. Overrides happen deterministically because each step puts entries into a LinkedHashMap keyed by canonical names; we also log overrides at DEBUG to make the behavior visible during debugging. When headers are absent or the engine kind is unknown, `EngineContext.effectiveEngineKind()` resolves to `floecat_internal` and overlays are skipped, but the base definitions (and the shared `information_schema`) remain available. `SystemGraph` continues to reuse the merged `BuiltinNodes` to build `_system` snapshots (namespace buckets, relation map, `SystemTableNode`s) that `MetaGraph` exposes as `CatalogOverlay`/`SystemObjectGraphView`. That merged `_system` view (load + scan) is documented in [System objects](system-objects.md).

### Plugin Implementations

Plugins implement `EngineSystemCatalogExtension` directly and provide:

1. **Engine Kind** – A stable identifier (e.g., `”example”`)
2. **Catalog Data** – Loads `.pbtxt` resources and returns a `SystemCatalogData` snapshot
3. **Discovery** – Registered via ServiceLoader for automatic runtime discovery

See `extensions/example/` for a complete reference implementation.

### Hint Lifecycle

`EngineSystemCatalogExtension` extends `SystemObjectScannerProvider`, so every plugin can also
serve system-table definitions and scanners. Plugins that persist engine hints (metadata attached
to catalog objects at runtime) can control when those hints are invalidated by implementing
`decideHintClear`:

```java
default HintClearDecision decideHintClear(EngineContext ctx, HintClearContext context) {
  return HintClearDecision.dropAll();   // safe default: clear everything on any schema change
}
```

`HintClearContext` carries what changed: `resourceId`, field `mask`, and before/after `Table`/`View`
snapshots. `HintClearDecision` controls what to clear:

- `HintClearDecision.dropAll()` — clears all relation and column hints (safe default for simple plugins)
- Fine-grained constructor — clears only specific `payloadType` sets or individual column IDs, for
  plugins that want to avoid unnecessary hint recomputation on unrelated schema changes

`SystemCatalogHintProvider` (in `ai.floedb.floecat.systemcatalog.hint`) automatically publishes
each object's `engine_specific` rules as `EngineHint` entries keyed by `(engineKind, engineVersion,
payloadType)`. Plugins that rely solely on `properties`-based metadata in their `.pbtxt` files get
hint publishing for free without implementing `decideHintClear`.

## Core Components

### EngineSpecific (Proto)

The core envelope in `proto/src/main/proto/floecat/query/engine_specific.proto`:

```proto
message EngineSpecific {
  string engine_kind = 10;       // "postgres", ...
  string min_version = 11;       // min engine version (inclusive)
  string max_version = 12;       // max engine version (inclusive)
  string payload_type = 20;      // e.g., "floe.function+proto"
  bytes payload = 21;            // Opaque binary data
  map<string,string> properties = 100;

  // Reserve extension numbers for plugins
  extensions 1000 to 2000;
}
```

**Key Design**: The message is engine-agnostic. Extensions (defined by plugins) are allowed in range 1000–2000 to support rich PBtxt files during parsing.

### SystemCatalogProvider (SPI)

Interface for loading catalogs:

```java
public interface SystemCatalogProvider {
  SystemEngineCatalog load(EngineContext ctx);
  List<String> engineKinds();
}
```

**Implementations**:
- **ServiceLoaderSystemCatalogProvider** – Discovers plugin catalogs via ServiceLoader, exposes the list of available engine kinds, and returns the raw `SystemCatalogData` for the normalized kind. It does not merge provider definitions—the merged view is computed later in `SystemNodeRegistry`—so the kind-level fingerprint stays stable.
- **StaticSystemCatalogProvider** – For tests; allows programmatic registration

### EngineContext & header semantics

Every `SystemCatalogProvider` receives an `EngineContext` (engine kind + version) derived from request headers. `EngineContext.effectiveEngineKind()` folds missing or blank headers to `floecat_internal` (which is also the registered `FloeCatInternalProvider`), so the base catalog is always available even when no headers are sent. When headers are present the normalized engine kind identifies the plugin whose catalog is cached in `SystemDefinitionRegistry`; the normalized version is used later by `SystemNodeRegistry` for version filtering. `SystemNodeRegistry` seeds every catalog build with the floecat_internal definitions, overlays the plugin snapshot (if one exists), and only applies `SystemObjectScannerProvider` overlays when `EngineContext.enginePluginOverlaysEnabled()` is true (i.e., headers were present and the normalized kind is not `floecat_internal`). Headers that mention an unknown engine kind still fall back to `floecat_internal`: the provider returns the fallback catalog, `SystemDefinitionRegistry` caches it under the requested kind, and `SystemNodeRegistry` treats it as `floecat_internal` so provider overlays are skipped while `information_schema` remains visible.

### Caching Architecture

Builtins are cached at every stage of the pipeline:

```
┌──────────────────────────────────┐
│ SystemDefinitionRegistry         │
│ key = effectiveEngineKind        │
│ value = SystemEngineCatalog       │
│ (raw SystemCatalogData snapshot)  │
└──────────────────────────────────┘
                │
┌──────────────────────────────────┐
│ SystemNodeRegistry               │
│ key = (engineKind, engineVersion) │
│ value = BuiltinNodes (GraphNodes +│
│         merged SystemCatalogData │
│         and overlays)             │
└──────────────────────────────────┘
                │
┌──────────────────────────────────┐
│ SystemGraph snapshot cache       │
│ key = (engineKind, engineVersion) │
│ value = GraphSnapshot (namespace →│
│         relations + lookup map)   │
└──────────────────────────────────┘
```

1. **SystemDefinitionRegistry** – normalizes the engine kind (`Locale.ROOT`, case-insensitive) and caches the immutable `SystemEngineCatalog` produced by the `SystemCatalogProvider` under `EngineContext.effectiveEngineKind()`, so the layer-1 cache is kind-only. Every catalog is fingerprinted once before provider overlays are applied, making the kind-level fingerprint stable even if later merges change the result. Tests can reset this cache via `clear()`.

2. **SystemNodeRegistry** – filters the cached catalog through `EngineSpecificMatcher` (per `min_version`, `max_version` rules) and merges floecat_internal + plugin catalogs + provider overlays when overlays are enabled. The resulting `BuiltinNodes` record keeps copies of the filtered and merged `SystemCatalogData` (functions, types, casts, tables, etc.), so the same snapshot serves both the catalog service and the system graph; this layer’s fingerprint reflects the post-merge catalog that includes overlays. `VersionKey` is the `(engineKind, engineVersion)` tuple stored in a `ConcurrentHashMap`.
   Canonical names remain fully qualified for identity and namespace mapping, while node display labels are materialized separately (functions/operators/types/collations/aggregates default to leaf names unless a provider overrides them).
   Catalog validation is enforced at load time: providers fail fast on `Severity.ERROR` issues. The default namespace-scope policy currently requires known namespaces for `function/type/table/view` and leaves `operator/cast/collation/aggregate` relaxed unless a stricter policy is selected.

3. **SystemGraph snapshot cache** – `SystemGraph` consumes `BuiltinNodes` to build a `GraphSnapshot` that buckets namespace→relations, indexes every `GraphNode` by `ResourceId`, and keeps `_system` catalog metadata ready for `CatalogOverlay`. Snapshots are stored in a synchronized `LinkedHashMap` configured by `floecat.system.graph.snapshot-cache-size` (defaults to 16) and evict the oldest entry when the cache is full.

`SystemObjectsServiceImpl` itself stays cache-less: it simply fetches the prebuilt `BuiltinNodes`, hands the embedded `SystemCatalogData` to `SystemCatalogProtoMapper.toProto()`, and responds. Because all heavy work (parsing, filtering, node construction, snapshotting) happens before the gRPC layer, repeated requests hit the cache in <1ms.

## Plugin Architecture

### EngineBuiltinExtension (SPI)

Plugins implement this interface (defined in `extensions/builtin/spi/`):

```java
public interface EngineBuiltinExtension {
  String engineKind();
  SystemCatalogData loadSystemCatalog();
  default void onLoadError(Exception e) { }
}
```

### Reference Implementation

The bundled `extensions/example/` module (`ExampleCatalogExtension`) is the canonical reference.
It loads `.pbtxt` files from either a configured filesystem directory or from classpath resources
under `builtins/<engine-kind>/`, reading fragment order from `_index.txt`.

Each fragment independently defines the repeated field(s) it needs; the loader appends them in
order to a single `SystemObjectsRegistry.Builder`.

Resource layout:

```
resources/
  builtins/
    <engine-kind>/
      _index.txt             # lists fragments in merge order
      00_registry.pbtxt
      10_types.pbtxt
      20_functions.pbtxt
      30_operators.pbtxt
      40_casts.pbtxt
      50_collations.pbtxt
      60_aggregates.pbtxt
```

### ServiceLoader Registration

Each plugin registers itself in `META-INF/services/ai.floedb.floecat.systemcatalog.spi.EngineSystemCatalogExtension`:

```
com.example.MyEngineCatalogExtension
```

## Data Flow

### Request Flow (Planner → Floecat)

1. **Planner** sends `GetSystemObjectsRequest` with headers:
   - `x-engine-kind: "example"`
   - `x-engine-version: "1.0"`
2. **SystemObjectsServiceImpl** validates the headers and calls `SystemNodeRegistry.nodesFor("example", "1.0")`.
3. **SystemNodeRegistry** normalizes the kind, looks up `(engineKind, engineVersion)` in its cache, and, on a miss, asks `SystemDefinitionRegistry` for the raw `SystemEngineCatalog`.
4. **SystemDefinitionRegistry** delegates to `ServiceLoaderSystemCatalogProvider` when it needs to load a raw catalog snapshot for the normalized engine kind.
5. **SystemNodeRegistry** takes that snapshot, seeds it with `floecat_internal` + `InformationSchema` definitions, overlays the plugin catalog and any `SystemObjectScannerProvider.definitions(engineKind, engineVersion)` entries, re-fingerprints the merged catalog, and caches the result per `(engineKind, engineVersion)`. Plugin/provider overlays only occur when engine headers are present (engine-plugin overlays).
6. **SystemNodeRegistry** filters the catalog by version (`EngineSpecificMatcher`), applies engine-specific rules, and materialises `BuiltinNodes` (graph nodes + filtered `SystemCatalogData`). The `BuiltinNodes` instance is cached for future requests for the same version.
7. **SystemObjectsServiceImpl** receives the cached `BuiltinNodes`, hands its embedded `SystemCatalogData` to `SystemCatalogProtoMapper.toProto()`, and streams the `GetSystemObjectsResponse` back to the planner.
8. **SystemGraph** reuses the same `BuiltinNodes` to build `_system` catalog snapshots (namespace buckets, relation map, `SystemTableNode`s) that `MetaGraph` exposes as `CatalogOverlay`/`SystemObjectGraphView` for system object scanning.
   * The scanner-visible system relations (information_schema, pg_catalog, etc.) are seeded from `floecat_internal` and merged into every engine namespace for `_system` scans. They are **not** emitted by `GetSystemObjects`, which only returns engine-visible SQL objects produced by plugins/providers.

### SystemNodeRegistry Caching

All caches are case-normalized and thread-safe:
* `SystemDefinitionRegistry` keeps one `SystemEngineCatalog` per engine kind in a `ConcurrentHashMap`. Loading once per kind is enough because `SystemEngineCatalog` references a parsed `SystemCatalogData` snapshot that contains every supported version.
* `SystemNodeRegistry` caches `BuiltinNodes` per `VersionKey(engineKind, engineVersion)` via `ConcurrentHashMap.computeIfAbsent`. The result stores stable `ResourceId`s (via `SystemNodeRegistry.resourceId`) and a copy of the filtered `SystemCatalogData`.  
  `SystemNodeRegistry.resourceId` now derives a deterministic UUID (engine kind + resource kind + object signature) instead of concatenating readable `engine:suffix` strings, so every owner of a system node should call the helper rather than inventing their own IDs.
* `SystemGraph` keeps a synchronized, access-ordered `LinkedHashMap` of `GraphSnapshot`s per version. Each snapshot already groups namespace relations and indexes every `GraphNode` so that `_system` list/lookups take constant time.

## Version Matching

Each rule in the builtin catalog carries `min_version` and `max_version` constraints:

```java
EngineSpecific {
  min_version: "9.5"
  max_version: "13.0"
  // ...
}
```

The planner can filter or match rules based on the requested version. The versioning semantics are engine-defined; plugins document their version scheme in their documentation.

The actual predicate is implemented by `EngineSpecificMatcher.matches(rules, engineKind, engineVersion)`, which is used by `SystemNodeRegistry` to compute exactly which objects survive the version filter.

## Scalability & Performance

### Catalog Size Considerations

PostgreSQL has a massive builtin catalog:
- **Functions**: ~6,000+ builtin functions
- **Operators**: ~1,000+
- **Types**: ~200+
- **Casts**: ~500+
- **Aggregates**: ~100+
- **Total**: ~8,000-10,000 objects per major PG version

A PG-scale engine catalog inherits this scale. The system is **designed to handle this**:

### Layer 1 (Engine-Kind Cache) – PG-Scale Performance

| Metric | Small Catalog (example extension) | PG-Scale (Postgres) |
|--------|---------------------------|-------------------|
| **Raw catalog size** | ~100 objects | ~8,000-10,000 objects |
| **Parsed JAR size** | ~20-50KB | ~2-5MB |
| **Parse time (first load)** | ~10ms | ~100-200ms |
| **In-memory size** | ~1-2MB | ~20-50MB |
| **Layer 1 hit (repeated)** | <0.1ms | <0.1ms |

PG-scale catalogs load once and stay cached for service lifetime; subsequent access is instant.

### Layer 2 (Version-Specific Cache) – PG-Scale Performance

When planner requests a version, Layer 2 filters Layer 1's full catalog:

| Metric | Small Catalog | PG-Scale |
|--------|---------------|----------|
| **Filtering cost** | ~5-10ms per new version | ~50-100ms per new version |
| **Filtered result** | ~30-50% of raw | ~30-50% of raw |
| **Node construction** | ~5ms | ~50ms |
| **In-memory (per version)** | ~500KB-2MB | ~10-30MB |
| **Layer 2 hit (repeated)** | <0.1ms | <0.1ms |

**Critical point**: Filtering is done **once per version**, then memoized. All subsequent requests for PG 13.0 hit Layer 2 cache instantly.

### Proto Conversion & gRPC Response

| Scenario | Time | Size |
|----------|------|------|
| Layer 2 cache hit + proto conversion | ~1-2ms | ~500KB-5MB per response |
| Full PG catalog wire format | ~50-100ms (first ever) | ~5-20MB compressed |
| Subsequent PG requests | ~2ms (Layer 2 cache + proto) | Same size |

### Memory Scaling (Typical Deployment)

```
3 engines × (1 raw catalog + 5 versions average):
  Example:       2MB (raw) + 2MB×5 (versions)      = 12MB
  Postgres:     40MB (raw) + 25MB×5 (versions)    = 165MB
  Trino:        30MB (raw) + 20MB×5 (versions)    = 130MB
  ──────────────────────────────────────────────────────
  Total:                                           ~307MB

This is acceptable for a service with 8GB+ heap. Typical Floecat deployments
allocate 16GB+ to handle metadata graph + execution plans.
```

### Thread Safety

Both Layer 1 and Layer 2 use `ConcurrentHashMap` with atomic `computeIfAbsent()`, ensuring:
- No duplicate plugin loads if multiple threads request same engine concurrently
- No duplicate filtering if multiple threads request same version concurrently
- Safe concurrent reads after cache population
- **PG-scale benefit**: With 8,000+ objects, preventing duplicate parses saves 100-200ms per redundant load

### Scalability Assessment
 
**PG-scale catalogs scale well** if:
- Planner requests stable set of engine versions (typical: 3-5 versions per engine)
- Service runs for hours/days (cache warm)
- Metadata graph + other components can absorb 300-500MB memory overhead

**Potential bottlenecks** at extreme scales:
- **Many versions per engine** (> 15 versions): Layer 2 cache grows; implement LRU if needed
- **Many engines** (> 10 engines): ~500MB+ memory; consider partitioning
- **Very large .pbtxt files** (> 10MB): Parsing could timeout; consider compression or lazy loading

**Recommendation**: For PG-scale engines, pre-warm cache by loading all expected versions at service startup rather than lazy-loading on first planner request.

## Proto Extensions (Advanced)

Plugins can define proto extensions on `EngineSpecific` to support rich PBtxt syntax. The Floe plugin defines:

```proto
// my_engine.proto — define in your own plugin proto
extend ai.floedb.floecat.query.EngineSpecific {
  MyFunctionSpecific  my_function  = 1001;
  MyOperatorSpecific  my_operator  = 1002;
  MyCastSpecific      my_cast      = 1003;
  MyTypeSpecific      my_type      = 1004;
  MyAggregateSpecific my_aggregate = 1005;
  MyCollationSpecific my_collation = 1006;
}
```

**Important**: Extensions use range 1000–2000 reserved in the core proto. Plugins must coordinate to avoid collisions (e.g., Postgres uses 1100–1199, Trino uses 1200–1299, etc.).

## Validation

The `SystemCatalogValidator` validates loaded catalogs:

```java
public static List<String> validate(SystemCatalogData catalog) { ... }
```

Checks include:
- Types defined before use
- Function/operator argument types exist
- Cast source/target types exist
- No duplicate names
- Required fields present

Validation errors are logged; invalid catalogs are still returned (planner must handle gracefully).

## Creating a New Plugin

### Step 1: Implement EngineSystemCatalogExtension

`EngineSystemCatalogExtension` lives in `ai.floedb.floecat.systemcatalog.spi` and already extends `SystemObjectScannerProvider`, so every plugin can also supply system table definitions and scanners without extra wiring.

```java
public class MyEngineCatalogExtension implements EngineSystemCatalogExtension {
  @Override
  public String engineKind() {
    return "my-engine";
  }

  @Override
  public SystemCatalogData loadSystemCatalog() {
    // Load and parse your catalog
    return new SystemCatalogData(...);
  }

  // Implement SystemObjectScannerProvider methods if you expose new rows.
}
```

### Step 2: Register with ServiceLoader

Create `resources/META-INF/services/ai.floedb.floecat.systemcatalog.spi.EngineSystemCatalogExtension`:

```
com.example.MyEngineCatalogExtension
```

### Step 3: Define Proto Extensions (Optional)

If using PBtxt with custom fields, define proto extensions:

```proto
import "query/engine_specific.proto";

message MyEngineFunction { /* ... */ }

extend ai.floedb.floecat.query.EngineSpecific {
  MyEngineFunction my_function = 1100;  // Use allocated range for your engine
}
```

### Step 4: Ship with Service Module

Add the plugin JAR as a runtime dependency of the service module so it's available at startup.

## Testing

### Plugin-Side Validation Tests

Each plugin should validate its `.pbtxt` files using the `SystemCatalogValidator`:

**Example: CatalogExtensionTest**

```java
class CatalogExtensionTest {
  @Test
  void extensionLoadsAndValidates() {
    var extension = new ExampleCatalogExtension();

    // Load the catalog (`_index.txt` + fragments)
    SystemCatalogData catalog = extension.loadSystemCatalog();

    // Validate structural integrity
    var errors = SystemCatalogValidator.validate(catalog);
    assert errors.isEmpty() : "catalog must pass validation, got: " + errors;
  }

  @Test
  void catalogDataPreservesEngineSpecificRules() {
    var extension = new ExampleCatalogExtension();
    SystemCatalogData catalog = extension.loadSystemCatalog();

    // Ensure engine-specific rules have non-blank payloadType
    var functionsWithRules = catalog.functions().stream()
        .filter(f -> !f.engineSpecific().isEmpty())
        .toList();

    for (var func : functionsWithRules) {
      for (var rule : func.engineSpecific()) {
        assert !rule.payloadType().isBlank() :
            "engine_specific rules must have a non-blank payload_type";
      }
    }
  }

  @Test
  void missingResourceFileThrows() {
    var extension = new TestExtensionWithMissingResource();

    try {
      extension.loadSystemCatalog();
      assert false : "Expected IllegalStateException";
    } catch (IllegalStateException e) {
      assert e.getMessage().contains("Builtin file not found");
    }
  }
}
```

**What this validates:**
- `.pbtxt` file parses without errors
- All objects (functions, types, operators, etc.) pass structural validation
- Engine-specific rules have a non-blank `payload_type` (required by the validator)
- Resource files are present and readable
- Invalid/malformed `.pbtxt` syntax fails fast

### Core Engine Tests

**SystemObjectsServiceIT** – Full gRPC flow:
- Valid engine headers return full catalog
- Version filtering returns only version-matched objects
- Missing headers trigger INVALID_ARGUMENT errors

**SystemNodeRegistryTest** – Version-specific filtering:
- Filters catalog by engine kind and version
- Constructs ResourceIds correctly
- Caches results per version tuple

**SystemCatalogValidatorTest** – Structural validation:
- Duplicate names detected
- Type references exist
- Required fields present

### Running Plugin Tests

```bash
# Run plugin-side tests (validates .pbtxt files + parsing)
mvn -pl extensions/plugins/floedb test

# Run core engine tests
mvn -pl service test -Dtest=SystemObjectsServiceIT
mvn -pl core/catalog test -Dtest=SystemNodeRegistryTest
mvn -pl core/catalog test -Dtest=SystemCatalogValidator*

# All builtin/service catalogs
mvn -pl service,extensions/floedb,core/catalog test -Dtest="Builtin*,System*"
```

### Key Validation Points

When adding a new plugin or modifying .pbtxt files:

| What to Test | How | Tool |
|-------------|-----|------|
| `.pbtxt` parses | No TextFormat errors | FloeBuiltinExtensionTest.loads |
| No duplicate types | Catalog validator | SystemCatalogValidatorTest |
| Functions reference known types | Type resolution | SystemCatalogValidatorTest |
| Operators have valid types | Type resolution | SystemCatalogValidatorTest |
| Casts reference valid types | Type resolution | SystemCatalogValidatorTest |
| Engine-specific fields rewritten | Payload bytes present | FloeBuiltinExtensionTest.preservesRules |
| ServiceLoader discovers plugin | SystemObjectsServiceIT | Dynamic runtime discovery |
| Version filtering works | Version matching logic | SystemNodeRegistryTest |

## Future Enhancements

- **Dynamic Reload** – Hot-reload plugins without restarting service
- **Compression** – Compress payload bytes for large catalogs
- **Pre-warming** – Load all expected versions at startup for PG-scale catalogs
