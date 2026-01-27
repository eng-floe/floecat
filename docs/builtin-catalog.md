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
       │
       ▼
┌────────────────────────────────────────────────────────────┐
│ SystemGraph (service/metagraph)                             │
│ - reuses SystemNodeRegistry nodes to build _system graph     │
│ - snapshots per (engineKind, version) cached with LRU        │
│ - supplies CatalogOverlay/SystemObjectGraphView             │
└────────────────────────────────────────────────────────────┘
```

`ServiceLoaderSystemCatalogProvider` is the gatekeeper for engine plugins: it discovers every `EngineSystemCatalogExtension`, loads the normalized catalog snapshot for each engine kind, and fingerprints the raw data without applying any provider overlays. `SystemDefinitionRegistry` caches those snapshots keyed by `EngineContext.effectiveEngineKind()` (so blank headers collapse to `floecat_internal`) so the kind-level catalog only needs to parse once. The real layering happens in `SystemNodeRegistry`: on a cache miss it seeds the result with `FloecatInternalProvider` (the `floecat_internal` base that always brings `information_schema`), overlays the plugin catalog, and finally applies `SystemObjectScannerProvider.definitions(engineKind, engineVersion)` entries when overlays are enabled (i.e., headers are present and the plugin exists). Overrides happen deterministically because each step puts entries into a LinkedHashMap keyed by canonical names; we also log overrides at DEBUG to make the behavior visible during debugging. When headers are absent or the engine kind is unknown, `EngineContext.effectiveEngineKind()` resolves to `floecat_internal` and overlays are skipped, but the base definitions (and the shared `information_schema`) remain available. `SystemGraph` continues to reuse the merged `BuiltinNodes` to build `_system` snapshots (namespace buckets, relation map, `SystemTableNode`s) that `MetaGraph` exposes as `CatalogOverlay`/`SystemObjectGraphView`. That merged `_system` view (load + scan) is documented in [System objects](system-objects.md).

### Engine-specific Hint Resolution

Engine-specific metadata can come from two places: an overlay hint attached by the plugin (which covers namespaced, relation, type, function, etc. descriptors) and synthetic defaults computed by `FloeHintResolver`. The rule is simple: an overlay payload always wins; when no hint is present the `FloeHintResolver` helper synthesizes deterministic defaults (including type defaults when the per-type payload is missing) so that scanners and bundle decorators never throw and always emit the same values. In practice both scanners and the catalog bundle decorator rely on `FloeHintResolver` so the overlay-or-default contract is enforced consistently.

### Naming and Payload Intent

The Floe extension exposes canonical engine metadata contracts that are intentionally minimal. `FloeColumnSpecific` now carries the immutable column identity (attname/atttypid/atttypmod/attnum/attnotnull/attisdropped/attcollation/atthasdef) while the resolver synthesizes safe defaults for the remaining PG-only columns (attlen/attalign/attstorage/attndims). These contracts are not tied to the Postgres scanners—they are the shared column/relation metadata that every scanner and decorator should project into rows or bundles. When adjusting the plugin, think in terms of “Floe column metadata”/“Floe relation metadata” rather than raw `pg_attribute`/`pg_class` tables: the scanner projects the shared fields into the table schema for compatibility while the decorator reuses the same canonical payload when attaching `EngineSpecific` blobs. Future engines should follow the same pattern; you can rename the helper classes (e.g., `FloeEngineMetadata`, `FloeColumnHints`) as long as the contract—the shared Floe*Specific protos—remains the single source of truth.

### Plugin Implementations

Plugins inherit from the abstract `FloeCatalogExtension` base class and provide:

1. **Engine Kind** – A stable identifier (e.g., "floedb", "floe-demo")
2. **Catalog Data** – Loads from a `.pbtxt` resource file and processes Floe-specific fields
3. **Discovery** – Registered via ServiceLoader for automatic runtime discovery

## Core Components

### EngineSpecific (Proto)

The core envelope in `proto/src/main/proto/floecat/query/engine_specific.proto`:

```proto
message EngineSpecific {
  string engine_kind = 10;      // "floe-demo", "postgres", ...
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

### FloeCatalogExtension (Base Class)

Located in `extensions/plugins/floedb/src/main/java/`, provides shared logic:

- **PBtxt Loading** – Parses human-friendly proto text format
- **Extension Rewriting** – Extracts Floe-specific fields from `UnknownFieldSet` and converts to binary payloads
- **Inner Classes** – Two concrete implementations:
  - `FloeCatalogExtension.FloeDb` – "floedb" engine
  - `FloeCatalogExtension.FloeDemo` – "floe-demo" engine (test/demo)

These extensions now load from `builtins/{engineKind}/` directories. Each directory exposes
an `_index.txt` that lists the pbtxt fragments to merge; fragments are merged in the order listed in `_index.txt`
and typically focus on one repeated field (types, functions, operators, etc.).

### Resource Files

Located in `extensions/plugins/floedb/src/main/resources/builtins/`:

- **`floedb/`** – Production directory (`_index.txt` plus `00_registry.pbtxt`, `10_types.pbtxt`, …)
- **`floe-demo/`** – Fragmented demo catalog (same layout as above, but with a smaller subset)

```
resources/
  builtins/
    floedb/
      _index.txt             # lists fragments in merge order
      00_registry.pbtxt
      10_types.pbtxt
      20_functions.pbtxt
      30_operators.pbtxt
      40_casts.pbtxt
      50_collations.pbtxt
      60_aggregates.pbtxt
```

Each fragment independently defines just the repeated field it needs; the loader appends them to a
single `SystemObjectsRegistry.Builder` before running `rewriteFloeExtensions()`.

Format is human-readable protobuf text with embedded Floe-specific fields:

```protobuf
functions {
  name { name: "int4_add" path: "pg_catalog" }
  argument_types { name: "int4" path: "pg_catalog" }
  argument_types { name: "int4" path: "pg_catalog" }
  return_type { name: "int4" path: "pg_catalog" }
}

functions {
  name { name: "int4_abs" path: "pg_catalog" }
  argument_types { name: "int4" path: "pg_catalog" }
  return_type { name: "int4" path: "pg_catalog" }
  engine_specific {
    min_version: "9.5"
    floe_function {
      oid: 1250
      proname: "int4_abs"
      prolang: 12
      proisstrict: true
      prosrc: "int4_abs"
    }
  }
}
```

Floe-specific fields (like `floe_function`) are captured as unknown fields during parsing and later extracted and rewritten to binary payloads.

### ServiceLoader Registration

Each plugin registers itself in `META-INF/services/ai.floedb.floecat.extensions.spi.EngineBuiltinExtension`:

```
ai.floedb.floecat.extensions.floedb.FloeCatalogExtension$FloeDb
ai.floedb.floecat.extensions.floedb.FloeCatalogExtension$FloeDemo
```

## Data Flow

### Request Flow (Planner → Floecat)

1. **Planner** sends `GetSystemObjectsRequest` with headers:
   - `x-engine-kind: "floe-demo"`
   - `x-engine-version: "16.0"`
2. **SystemObjectsServiceImpl** validates the headers and calls `SystemNodeRegistry.nodesFor("floe-demo", "16.0")`.
3. **SystemNodeRegistry** normalizes the kind, looks up `(engineKind, engineVersion)` in its cache, and, on a miss, asks `SystemDefinitionRegistry` for the raw `SystemEngineCatalog`.
4. **SystemDefinitionRegistry** delegates to `ServiceLoaderSystemCatalogProvider` when it needs to load a raw catalog snapshot for the normalized engine kind.
5. **SystemNodeRegistry** takes that snapshot, seeds it with `floecat_internal` + `InformationSchema` definitions, overlays the plugin catalog and any `SystemObjectScannerProvider.definitions(engineKind, engineVersion)` entries, re-fingerprints the merged catalog, and caches the result per `(engineKind, engineVersion)`. Plugin/provider overlays only occur when engine headers are present (engine-plugin overlays).
6. **SystemNodeRegistry** filters the catalog by version (`EngineSpecificMatcher`), applies engine-specific rules, and materialises `BuiltinNodes` (graph nodes + filtered `SystemCatalogData`). The `BuiltinNodes` instance is cached for future requests for the same version.
7. **SystemObjectsServiceImpl** receives the cached `BuiltinNodes`, hands its embedded `SystemCatalogData` to `SystemCatalogProtoMapper.toProto()`, and streams the `GetSystemObjectsResponse` back to the planner.
8. **SystemGraph** reuses the same `BuiltinNodes` to build `_system` catalog snapshots (namespace buckets, relation map, `SystemTableNode`s) that `MetaGraph` exposes as `CatalogOverlay`/`SystemObjectGraphView` for system object scanning.

### SystemNodeRegistry Caching

All caches are case-normalized and thread-safe:
* `SystemDefinitionRegistry` keeps one `SystemEngineCatalog` per engine kind in a `ConcurrentHashMap`. Loading once per kind is enough because `SystemEngineCatalog` references a parsed `SystemCatalogData` snapshot that contains every supported version.
* `SystemNodeRegistry` caches `BuiltinNodes` per `VersionKey(engineKind, engineVersion)` via `ConcurrentHashMap.computeIfAbsent`. The result stores stable `ResourceId`s (via `SystemNodeRegistry.resourceId`) and a copy of the filtered `SystemCatalogData`.
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

FloeDB (which mirrors PG) inherits this scale. The system is **designed to handle this**:

### Layer 1 (Engine-Kind Cache) – PG-Scale Performance

| Metric | Small Catalog (Floe-demo) | PG-Scale (Postgres) |
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
  Floe-demo:     2MB (raw) + 2MB×5 (versions)      = 12MB
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

**Recommendation**: For PG-scale (FloeDB inherits this), pre-warm cache by loading all expected versions at service startup rather than lazy-loading on first planner request.

## Proto Extensions (Advanced)

Plugins can define proto extensions on `EngineSpecific` to support rich PBtxt syntax. The Floe plugin defines:

```proto
// In extensions/plugins/floedb/src/main/proto/floecat/engine_floe.proto
extend ai.floedb.floecat.query.EngineSpecific {
  FloeFunctionSpecific floe_function = 1001;
  FloeOperatorSpecific floe_operator = 1002;
  FloeCastSpecific floe_cast = 1003;
  FloeTypeSpecific floe_type = 1004;
  FloeAggregateSpecific floe_aggregate = 1005;
  FloeCollationSpecific floe_collation = 1006;
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

**Example: FloeBuiltinExtensionTest**

```java
class FloeBuiltinExtensionTest {
  @Test
  void floeDbLoadsAndValidates() {
    var extension = new FloeCatalogExtension.FloeDb();
    
    // Load the floedb catalog directory (`_index.txt` + fragments)
    SystemCatalogData catalog = extension.loadSystemCatalog();
    
    // Validate structural integrity
    var errors = SystemCatalogValidator.validate(catalog);
    assert errors.isEmpty() : "floedb catalog must pass validation, got: " + errors;
  }

  @Test
  void catalogDataPreservesEngineSpecificRules() {
    var extension = new FloeCatalogExtension.FloeDb();
    SystemCatalogData catalog = extension.loadSystemCatalog();

    // Ensure Floe-specific fields were rewritten to payload bytes
    var functionsWithRules = catalog.functions().stream()
        .filter(f -> !f.engineSpecific().isEmpty())
        .toList();

    for (var func : functionsWithRules) {
      for (var rule : func.engineSpecific()) {
        assert rule.hasExtensionPayload() : 
            "Rewritten rules must have payload bytes";
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
- Engine-specific rules (e.g., `floe_function`, `floe_operator`) are correctly rewritten to binary payloads
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
