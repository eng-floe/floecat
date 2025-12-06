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
┌─────────────────────────────────────────────────────────┐
│           Planner (Client)                              │
│  Requests: (engine_kind="floe-demo", version="16.0")   │
└──────────────────┬──────────────────────────────────────┘
                   │ gRPC GetBuiltinCatalogRequest
                   ▼
┌─────────────────────────────────────────────────────────┐
│      BuiltinCatalogServiceImpl (service/)                │
│  • Validates engine_kind & version headers              │
│  • Delegates to BuiltinDefinitionRegistry               │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│   BuiltinDefinitionRegistry (service/)                  │
│  • Caches catalogs by engine_kind                       │
│  • Delegates to BuiltinCatalogProvider                  │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│  ServiceLoaderBuiltinCatalogProvider (service/)         │
│  • Uses Java ServiceLoader to discover plugins          │
│  • Returns EngineBuiltinExtension implementations       │
└──────────────────┬──────────────────────────────────────┘
                   │
        ┌──────────┴──────────┐
        ▼                     ▼
   ┌─────────────┐   ┌──────────────────┐
   │ FloeDb      │   │ FloeDemo         │
   │ ("floedb")  │   │ ("floe-demo")    │
   └─────────────┘   └──────────────────┘
   (Plugin)          (Plugin - Test)
```

### Plugin Implementations

Plugins inherit from the abstract `FloeBuiltinExtension` base class and provide:

1. **Engine Kind** – A stable identifier (e.g., "floedb", "floe-demo")
2. **Catalog Data** – Loads from a `.pbtxt` resource file and processes Floe-specific fields
3. **Discovery** – Registered via ServiceLoader for automatic runtime discovery

## Core Components

### EngineSpecific (Proto)

The core envelope in `proto/src/main/proto/query/engine_specific.proto`:

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

### BuiltinCatalogProvider (SPI)

Interface for loading catalogs:

```java
public interface BuiltinCatalogProvider {
  BuiltinEngineCatalog load(String engineKind);
}
```

**Implementations**:
- **ServiceLoaderBuiltinCatalogProvider** – Discovers plugins via ServiceLoader
- **StaticBuiltinCatalogProvider** – For tests; allows programmatic registration

### Caching Architecture

The system uses **two-level caching** for efficiency:

#### Layer 1: BuiltinDefinitionRegistry (Engine-Kind Cache)

Caches raw catalogs by engine kind:

```java
@ApplicationScoped
public final class BuiltinDefinitionRegistry {
  private final BuiltinCatalogProvider provider;
  private final ConcurrentMap<String, BuiltinEngineCatalog> cache;  // Key: engine_kind

  public BuiltinEngineCatalog catalog(String engineKind) { ... }
}
```

- **Key**: Engine kind (case-normalized), e.g., "floe-demo"
- **Value**: `BuiltinEngineCatalog` – Raw catalog with all versions, all objects (~500-2000 objects per engine)
- **Scope**: Application-scoped singleton, persists for service lifetime
- **Thread-safety**: `ConcurrentHashMap` with atomic `computeIfAbsent()`
- **Load cost**: One-time per engine kind; plugin loads `.pbtxt` from JAR, parses with TextFormat parser, rewrites unknown fields to binary payloads
- **Hit rate**: Very high; all version-specific requests for same engine reuse this cache

#### Layer 2: BuiltinNodeRegistry (Version-Specific Cache)

Filters and caches catalogs per version:

```java
@ApplicationScoped
public class BuiltinNodeRegistry {
  private final BuiltinDefinitionRegistry definitionRegistry;
  private final ConcurrentMap<VersionKey, BuiltinNodes> cache;  // Key: (engine_kind, version)

  public BuiltinNodes nodesFor(String engineKind, String engineVersion) { ... }
  // where VersionKey = record(engineKind, engineVersion)
}
```

- **Key**: `VersionKey(engineKind, engineVersion)` tuple
- **Value**: `BuiltinNodes` – Filtered & constructed graph nodes for specific version (~30-50% of raw catalog)
- **Scope**: Application-scoped singleton
- **Thread-safety**: `ConcurrentHashMap`
- **Load cost**: Filters raw catalog by version range, constructs `BuiltinFunctionNode`, `BuiltinOperatorNode`, etc. with ResourceIds; zero if Layer 1 hit succeeds
- **Hit rate**: High after planner first requests a version; subsequent requests for same version hit cache

#### Layer 3: BuiltinCatalogServiceImpl (No Caching)

gRPC endpoint for planners:

```java
@GrpcService
public class BuiltinCatalogServiceImpl implements BuiltinCatalogService {
  public Uni<GetBuiltinCatalogResponse> getBuiltinCatalog(...) { ... }
}
```

- **Cache**: None at this layer
- **Action**: Retrieves cached `BuiltinNodes` from Layer 2, converts to proto wire format via `BuiltinCatalogProtoMapper.toProto()`, returns response
- **Load cost**: ~1ms proto conversion per request

**Request flow with caching:**

```
Request 1 for (floe-demo, 16.0):
  Layer 3: No cache
  Layer 2: MISS → Layer 1: MISS → Plugin loads floe-demo.pbtxt
           Cache result in Layer 1
           Filter by version 16.0, build nodes, cache in Layer 2
  Proto conversion → response

Request 2 for (floe-demo, 16.0) [identical]:
  Layer 3: No cache
  Layer 2: HIT → Return cached BuiltinNodes
  Proto conversion → response (~100x faster than request 1)

Request 3 for (floe-demo, 17.0) [different version]:
  Layer 3: No cache
  Layer 2: MISS → Layer 1: HIT (reuse cached floe-demo.pbtxt)
           Filter by version 17.0, build nodes, cache in Layer 2
  Proto conversion → response
```

Planner provides headers:
- `x-engine-kind` – engine identifier (e.g., "floe-demo")
- `x-engine-version` – requested version (e.g., "16.0")

## Plugin Architecture

### EngineBuiltinExtension (SPI)

Plugins implement this interface (defined in `extensions/builtin/spi/`):

```java
public interface EngineBuiltinExtension {
  String engineKind();
  BuiltinCatalogData loadBuiltinCatalog();
  default void onLoadError(Exception e) { }
}
```

### FloeBuiltinExtension (Base Class)

Located in `extensions/plugins/floedb/src/main/java/`, provides shared logic:

- **PBtxt Loading** – Parses human-friendly proto text format
- **Extension Rewriting** – Extracts Floe-specific fields from `UnknownFieldSet` and converts to binary payloads
- **Inner Classes** – Two concrete implementations:
  - `FloeBuiltinExtension.FloeDb` – "floedb" engine
  - `FloeBuiltinExtension.FloeDemo` – "floe-demo" engine (test/demo)

Both load from `builtins/{engineKind}.pbtxt` resources by default.

### Resource Files

Located in `extensions/plugins/floedb/src/main/resources/builtins/`:

- **`floedb.pbtxt`** – Full production catalog
- **`floe-demo.pbtxt`** – Filtered/demo catalog for testing

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

Each plugin registers itself in `META-INF/services/ai.floedb.floecat.extensions.builtin.spi.EngineBuiltinExtension`:

```
ai.floedb.floecat.extensions.floedb.FloeBuiltinExtension$FloeDb
ai.floedb.floecat.extensions.floedb.FloeBuiltinExtension$FloeDemo
```

## Data Flow

### Request Flow (Planner → Floecat)

1. **Planner** sends `GetBuiltinCatalogRequest` with headers:
   - `x-engine-kind: "floe-demo"`
   - `x-engine-version: "16.0"`
2. **BuiltinCatalogServiceImpl** validates headers, calls `BuiltinDefinitionRegistry.catalog("floe-demo")`
3. **BuiltinDefinitionRegistry** checks cache, calls provider if miss
4. **ServiceLoaderBuiltinCatalogProvider** finds `FloeBuiltinExtension.FloeDemo` via ServiceLoader
5. **FloeBuiltinExtension.FloeDemo** loads `floe-demo.pbtxt`, processes unknown fields, returns `BuiltinCatalogData`
6. **BuiltinCatalogProtoMapper** converts to gRPC protobuf; response sent to planner

### Caching

The registry caches by engine kind (case-insensitive), so repeated requests for the same engine avoid reloading.

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
// In extensions/plugins/floedb/src/main/proto/engine_floe.proto
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

The `BuiltinCatalogValidator` validates loaded catalogs:

```java
public static List<String> validate(BuiltinCatalogData catalog) { ... }
```

Checks include:
- Types defined before use
- Function/operator argument types exist
- Cast source/target types exist
- No duplicate names
- Required fields present

Validation errors are logged; invalid catalogs are still returned (planner must handle gracefully).

## Creating a New Plugin

### Step 1: Implement EngineBuiltinExtension

```java
public class MyEngineBuiltinExtension implements EngineBuiltinExtension {
  @Override
  public String engineKind() {
    return "my-engine";
  }

  @Override
  public BuiltinCatalogData loadBuiltinCatalog() {
    // Load and parse your catalog
    return new BuiltinCatalogData(...);
  }
}
```

### Step 2: Register with ServiceLoader

Create `resources/META-INF/services/ai.floedb.floecat.extensions.builtin.spi.EngineBuiltinExtension`:

```
com.example.MyEngineBuiltinExtension
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

Each plugin should validate its `.pbtxt` files using the `BuiltinCatalogValidator`:

**Example: FloeBuiltinExtensionTest**

```java
class FloeBuiltinExtensionTest {
  @Test
  void floeDbLoadsAndValidates() {
    var extension = new FloeBuiltinExtension.FloeDb();
    
    // Load the floedb.pbtxt file
    BuiltinCatalogData catalog = extension.loadBuiltinCatalog();
    
    // Validate structural integrity
    var errors = BuiltinCatalogValidator.validate(catalog);
    assert errors.isEmpty() : "floedb.pbtxt must pass validation, got: " + errors;
  }

  @Test
  void catalogDataPreservesEngineSpecificRules() {
    var extension = new FloeBuiltinExtension.FloeDb();
    BuiltinCatalogData catalog = extension.loadBuiltinCatalog();

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
      extension.loadBuiltinCatalog();
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

**BuiltinCatalogServiceIT** – Full gRPC flow:
- Valid engine headers return full catalog
- Version filtering returns only version-matched objects
- Missing headers trigger INVALID_ARGUMENT errors

**BuiltinNodeRegistryTest** – Version-specific filtering:
- Filters catalog by engine kind and version
- Constructs ResourceIds correctly
- Caches results per version tuple

**BuiltinCatalogValidatorTest** – Structural validation:
- Duplicate names detected
- Type references exist
- Required fields present

### Running Plugin Tests

```bash
# Run plugin-side tests (validates .pbtxt files + parsing)
mvn -pl extensions/plugins/floedb test

# Run core engine tests
mvn -pl service test -Dtest=BuiltinCatalogServiceIT
mvn -pl service test -Dtest=BuiltinNodeRegistryTest
mvn -pl catalog/builtin test -Dtest=BuiltinCatalogValidator*

# All builtin tests
mvn -pl service,extensions/plugins/floedb,catalog/builtin test -Dtest=Builtin*
```

### Key Validation Points

When adding a new plugin or modifying .pbtxt files:

| What to Test | How | Tool |
|-------------|-----|------|
| `.pbtxt` parses | No TextFormat errors | FloeBuiltinExtensionTest.loads |
| No duplicate types | Catalog validator | BuiltinCatalogValidatorTest |
| Functions reference known types | Type resolution | BuiltinCatalogValidatorTest |
| Operators have valid types | Type resolution | BuiltinCatalogValidatorTest |
| Casts reference valid types | Type resolution | BuiltinCatalogValidatorTest |
| Engine-specific fields rewritten | Payload bytes present | FloeBuiltinExtensionTest.preservesRules |
| ServiceLoader discovers plugin | BuiltinCatalogServiceIT | Dynamic runtime discovery |
| Version filtering works | Version matching logic | BuiltinNodeRegistryTest |

## Future Enhancements

- **Dynamic Reload** – Hot-reload plugins without restarting service
- **Compression** – Compress payload bytes for large catalogs
- **Pre-warming** – Load all expected versions at startup for PG-scale catalogs
