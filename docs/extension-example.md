# Example Catalog Extension

`floecat-extension-example` is a zero-code, file-based
[`EngineSystemCatalogExtension`](../extensions/example/src/main/java/ai/floedb/floecat/extensions/example/ExampleCatalogExtension.java)
that serves any engine's catalog from user-authored `.pbtxt` files — no Java required.

## Overview

The extension reads one or more `.pbtxt` files that describe your engine's built-in types,
functions, operators, casts, collations, and aggregates in protobuf text format.  Floecat parses
and merges those files at startup, then serves the resulting catalog to planners that send a
matching `x-engine-kind` header.

The extension is discovered automatically via Java's `ServiceLoader` mechanism.  One extension
instance handles exactly one engine kind per deployment.  Both the engine kind identifier and
the source directory of `.pbtxt` files are configurable at runtime without recompiling.

## Quick start

**1. Configure the extension.**

```bash
# Engine identifier sent by planners in x-engine-kind (default: "example")
export FLOECAT_EXTENSION_ENGINE_KIND=my-engine

# Directory that contains your *.pbtxt catalog files
export FLOECAT_EXTENSION_BUILTINS_DIR=/path/to/my-engine/catalog
```

**2. Write your `.pbtxt` files.**

Use the bundled format-reference files as templates:

```
extensions/example/src/main/resources/builtins/example/
  00_namespaces.pbtxt   ← namespace / schema declaration
  10_types.pbtxt        ← scalar and array types
  20_functions.pbtxt    ← scalar, aggregate state/finalizer, and window functions
  30_operators.pbtxt    ← infix operators
  40_casts.pbtxt        ← explicit, assignment, and implicit casts
  50_collations.pbtxt   ← text collations
  60_aggregates.pbtxt   ← aggregate functions
```

Each file is heavily commented with field-by-field documentation.  Copy, rename, and replace the
placeholder objects with your engine's actual catalog.

**3. Start Floecat.**

No code changes are required.  On startup Floecat loads `*.pbtxt` files from
`FLOECAT_EXTENSION_BUILTINS_DIR`, builds the catalog, and registers it under `my-engine`.

## Configuration

| Config key | Env var | Default | Description |
|---|---|---|---|
| `floecat.extension.engine-kind` | `FLOECAT_EXTENSION_ENGINE_KIND` | `example` | Engine identifier served by this extension.  Must match the `x-engine-kind` gRPC header that planners send. |
| `floecat.extension.builtins-dir` | `FLOECAT_EXTENSION_BUILTINS_DIR` | _(bundled classpath files)_ | Path to a directory of `*.pbtxt` catalog files.  When absent, the bundled format-reference catalog is loaded from the classpath. |

Config is resolved via MicroProfile Config at startup, so any supported config source (env vars,
system properties, `application.properties`) works.

## Loading behaviour

### Filesystem loading (`FLOECAT_EXTENSION_BUILTINS_DIR` set)

- Globs `*.pbtxt` in the configured directory (non-recursive, single level).
- Files are merged in **alphabetical filename order** — use numeric prefixes (`00_`, `10_`, `20_`,
  …) to control merge order across files.
- Only `*.pbtxt` files are read; any other extension (`.txt`, `.json`, …) is silently ignored.
- An empty directory is valid: Floecat starts with an empty catalog for this engine kind, which
  is useful during development before any files are written.

### Classpath loading (default, no directory configured)

- Reads `_index.txt` from `/builtins/example/` on the classpath.
- `_index.txt` lists filenames one per line in the desired merge order.
  Lines that are blank or start with `#` are ignored; absolute paths are rejected with a warning.
- Files are loaded in the order they appear in `_index.txt`.
- The bundled files are **format-reference examples only** — they define placeholder objects
  (`my_type`, `my_function`, etc.) for documentation purposes.
  Replace them entirely via `FLOECAT_EXTENSION_BUILTINS_DIR`.

### Error handling

All loading paths degrade gracefully; the service never fails to start due to catalog errors.

| Situation | Behaviour |
|---|---|
| Configured dir does not exist or is not a directory | `WARN` log; empty catalog served |
| Dir exists but contains no `*.pbtxt` files | `DEBUG` log; empty catalog served |
| `_index.txt` not found on classpath | `WARN` log; empty catalog served |
| File cannot be read (I/O error) | `WARN` log; file skipped; other files still load |
| File fails to parse (invalid proto text) | `WARN` log; file skipped; other files still load |
| `engine_kind` inside an `engine_specific` block | Silently stripped (see [engine_specific blocks](#engine_specific-blocks)) |

## PBtxt format reference

Files must be valid `SystemObjectsRegistry` proto text format.  Each file may define one or more
entries of any repeated field (`types`, `functions`, `operators`, `casts`, `collations`,
`aggregates`, `system_namespaces`); entries across files are accumulated in load order.

### Namespaces

Declares the SQL schema (namespace) that groups built-in objects.  At least one namespace entry
is required; every `name.path` value used in other files must reference a declared namespace name.

| Field | Type | Required | Description |
|---|---|---|---|
| `name.name` | string | yes | Namespace identifier.  Used as the `path` in all other object `name` blocks. |
| `display_name` | string | no | Human-readable label shown in UI or error messages. |

```protobuf
system_namespaces {
  name { name: "my_schema" }
  display_name: "my_schema"
}
```

### Types

Defines the scalar and collection types your engine exposes.

| Field | Type | Required | Description |
|---|---|---|---|
| `name.name` | string | yes | Unqualified type identifier. |
| `name.path` | string | yes | Namespace (must match a declared `system_namespaces` entry). |
| `category` | string | yes | One of: `"N"` numeric, `"S"` string/character, `"B"` boolean, `"D"` date/time, `"A"` array/collection, `"U"` user-defined. |
| `is_array` | bool | no | `true` for array / collection types.  Set `element_type` when `true`. |
| `element_type` | NameRef | when `is_array` | Element type reference: `name` + `path`. |
| `engine_specific.properties` | map&lt;string,string&gt; | no | Free-form metadata for your runtime (type IDs, byte widths, storage layouts, etc.). |

```protobuf
# Scalar type
types {
  name { name: "my_integer" path: "my_schema" }
  category: "N"
  engine_specific {
    properties { key: "description" value: "32-bit signed integer" }
    properties { key: "size_bytes"  value: "4" }
  }
}

# Array type — demonstrates is_array and element_type
types {
  name { name: "my_integer_array" path: "my_schema" }
  category: "A"
  is_array: true
  element_type { name: "my_integer" path: "my_schema" }
}
```

### Functions

Defines scalar functions, aggregate state-transition and finalizer functions, and window functions.

| Field | Type | Required | Description |
|---|---|---|---|
| `name.name` | string | yes | Function identifier. |
| `name.path` | string | yes | Namespace. |
| `argument_types` | repeated NameRef | no | Ordered parameter types.  Repeat the block once per argument. |
| `return_type` | NameRef | yes | Return type. |
| `is_aggregate` | bool | no | `true` for aggregate state-transition and finalizer functions. |
| `is_window` | bool | no | `true` for window functions. |
| `engine_specific.min_version` | string | no | Earliest engine version that supports this function (inclusive). |
| `engine_specific.max_version` | string | no | Latest engine version that supports this function (inclusive). |
| `engine_specific.properties` | map&lt;string,string&gt; | no | Free-form metadata (description, volatility, internal IDs, …). |

```protobuf
# Scalar function — single argument
functions {
  name { name: "my_abs" path: "my_schema" }
  argument_types { name: "my_integer" path: "my_schema" }
  return_type    { name: "my_integer" path: "my_schema" }
  engine_specific {
    properties { key: "volatility" value: "immutable" }
  }
}

# Function available only from a minimum engine version
functions {
  name { name: "my_json_extract" path: "my_schema" }
  argument_types { name: "my_text"    path: "my_schema" }
  argument_types { name: "my_text"    path: "my_schema" }
  return_type    { name: "my_text"    path: "my_schema" }
  engine_specific {
    min_version: "2.0"
    properties { key: "volatility" value: "immutable" }
  }
}
```

### Operators

Defines infix operators.

| Field | Type | Required | Description |
|---|---|---|---|
| `name.name` | string | yes | Operator symbol (e.g., `"+"`, `"-"`, `"\|\|"`). |
| `left_type` | NameRef | yes | Left operand type. |
| `right_type` | NameRef | yes | Right operand type. |
| `return_type` | NameRef | yes | Result type. |
| `is_commutative` | bool | no | `true` when `a OP b = b OP a`. |
| `is_associative` | bool | no | `true` when `(a OP b) OP c = a OP (b OP c)`. |

```protobuf
# Commutative, associative addition
operators {
  name { name: "+" }
  left_type   { name: "my_integer" path: "my_schema" }
  right_type  { name: "my_integer" path: "my_schema" }
  return_type { name: "my_integer" path: "my_schema" }
  is_commutative: true
  is_associative: true
}
```

### Casts

Defines type conversion rules.

| Field | Type | Required | Description |
|---|---|---|---|
| `name.name` | string | yes | Cast identifier. |
| `name.path` | string | yes | Namespace. |
| `source_type` | NameRef | yes | Input type. |
| `target_type` | NameRef | yes | Output type. |
| `method` | string | yes | One of: `"implicit"` (automatic, no CAST expression needed), `"assignment"` (automatic on column assignment), `"explicit"` (requires `CAST(x AS target_type)`). |

```protobuf
# Explicit cast: requires CAST(x AS my_integer)
casts {
  name { name: "my_text_to_integer" path: "my_schema" }
  source_type { name: "my_text"    path: "my_schema" }
  target_type { name: "my_integer" path: "my_schema" }
  method: "explicit"
}
```

### Collations

Defines text sort orders.

| Field | Type | Required | Description |
|---|---|---|---|
| `name.name` | string | yes | Collation identifier. |
| `name.path` | string | yes | Namespace. |
| `locale` | string | yes | BCP 47 locale tag (e.g., `"en_US"`, `"de_DE"`, `"C"`, `"POSIX"`). |

```protobuf
collations {
  name { name: "default" path: "my_schema" }
  locale: "en_US"
}
```

### Aggregates

Defines aggregate functions (the public-facing aggregate, not the internal state/finalizer
functions — those are defined as `functions` with `is_aggregate: true`).

| Field | Type | Required | Description |
|---|---|---|---|
| `name.name` | string | yes | Aggregate identifier. |
| `name.path` | string | yes | Namespace. |
| `argument_types` | repeated NameRef | no | Input column types.  Repeat once per argument. |
| `state_type` | NameRef | yes | Type of the running accumulator state. |
| `return_type` | NameRef | yes | Type returned after finalization. |
| `engine_specific.properties` | map&lt;string,string&gt; | no | Free-form metadata.  Conventionally used to reference state-transition and finalizer functions (`state_function`, `final_function`). |

```protobuf
aggregates {
  name { name: "my_sum" path: "my_schema" }
  argument_types { name: "my_integer" path: "my_schema" }
  state_type     { name: "my_bigint"  path: "my_schema" }
  return_type    { name: "my_bigint"  path: "my_schema" }
  engine_specific {
    properties { key: "state_function" value: "my_sum_state" }
    properties { key: "final_function" value: "my_sum_final" }
  }
}
```

## `engine_specific` blocks

Every catalog object type accepts an `engine_specific` block for version constraints and
free-form metadata:

```protobuf
engine_specific {
  min_version: "2.0"          # inclusive lower bound; omit if no minimum
  max_version: "5.9"          # inclusive upper bound; omit if no maximum
  properties { key: "volatility" value: "immutable" }
  properties { key: "description" value: "any string metadata" }
}
```

**Version filtering.**  `min_version` and `max_version` are compared against the engine version
sent in the `x-engine-version` gRPC header.  `EngineSpecificMatcher` evaluates the comparison
using lexicographic semver-style ordering.  Planners that send a version outside the declared
range will not see the object.  Objects with no `engine_specific` block (or no version fields)
are always included.

**`properties`.**  A free-form `map<string,string>` for any metadata your engine needs at
runtime — internal type or function IDs, byte widths, volatility, storage layout hints, etc.
Floecat does not interpret property keys or values; they are passed through to the planner as-is.

**`engine_kind` is not supported.**  If an `engine_kind` field appears inside an
`engine_specific` block it is silently stripped before the catalog is served.  The catalog's
configured engine kind (`FLOECAT_EXTENSION_ENGINE_KIND`) always applies uniformly — one
extension instance serves exactly one engine kind, so a per-rule filter is unnecessary and would
cause subtle scoping bugs if the engine kind is ever renamed.

## Namespace vs engine kind

These two concepts are independent and are frequently confused:

| Concept | Configured by | Purpose |
|---|---|---|
| **Engine kind** | `FLOECAT_EXTENSION_ENGINE_KIND` | Identifies the extension to Floecat.  Planners send this value in the `x-engine-kind` gRPC header to request this catalog. |
| **Namespace** | `name.path` in each PBtxt object | The SQL schema that groups objects within the catalog (e.g., `pg_catalog`, `information_schema`, `my_schema`).  Purely a catalog-level grouping concept. |

You can keep `path: "example"` in your PBtxt files even when `FLOECAT_EXTENSION_ENGINE_KIND` is
set to `my-db`.  Conversely, you can change the namespace to match your engine's schema without
changing the engine kind.  The only requirement is consistency: every `name.path` value must
reference a namespace declared in a `system_namespaces` block within the same catalog.

## Further reading

- [Builtin catalog architecture](builtin-catalog.md) — SPI contracts, `ServiceLoader` discovery,
  caching layers, version matching, and the full request flow from planner to gRPC response
- [`ExampleCatalogExtension.java`](../extensions/example/src/main/java/ai/floedb/floecat/extensions/example/ExampleCatalogExtension.java) — config key constants and loading implementation
- [Bundled format-reference files](../extensions/example/src/main/resources/builtins/example/) — annotated PBtxt templates for every object type
