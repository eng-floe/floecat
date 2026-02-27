# Tools

This directory holds standalone utilities that are built alongside the main
Floecat modules. Each tool is designed to run without the service stack so that
planners, CI pipelines, or operators can invoke them directly.

## builtin-validator

Validates builtin catalog protobufs (either binary `.pb` or text `.pbtxt`) to
ensure they contain the metadata Floecat’s planner expects.

### Build

```bash
mvn -pl tools/builtin-validator package
```

### Usage

```bash
java -jar tools/builtin-validator/target/floecat-builtin-validator.jar \
    /path/to/<engine_kind>.pb[txt] \
    [--json] [--strict]
```

```bash
java -jar tools/builtin-validator/target/floecat-builtin-validator.jar \
    --engine <engine_kind> \
    [--json] [--strict]
```

The CLI also accepts catalog directories that expose `_index.txt` under
`service/src/main/resources/builtins/<engine_kind>/_index.txt`. When using
`--engine`, the tool loads the matching `EngineSystemCatalogExtension` via
`ServiceLoader` (the extension JAR must be on the classpath) and runs
extension-specific validation in addition to the builtin catalog checks.

- `--json` emits machine-readable results (`valid`, `stats`, `errors`,
  `warnings`, `engine_issues`, `engine_error_count`, `engine_warning_count`).
- `--strict` makes warnings (core or engine) fail the run.
- Output is colorized automatically when running in a terminal; set `NO_COLOR=1`
  to disable ANSI colors.

Sample protobufs live under [`tools/builtin-validator/examples/`](./builtin-validator/examples).

### Examples

Validate the bundled good catalog:

```bash
java -jar tools/builtin-validator/target/builtin-validator.jar \
  tools/builtin-validator/examples/demo_catalog.pbtxt
```

Output:

```
✔ Loaded builtin catalog: demo
✔ Types: 1 OK
✔ Functions: 1 OK
✔ Operators: 1 OK
✔ Casts: 1 OK
✔ Collations: 1 OK
✔ Aggregates: 1 OK

ALL CHECKS PASSED.
```

Validate a broken catalog:

```bash
java -jar tools/builtin-validator/target/builtin-validator.jar \
  tools/builtin-validator/examples/broken_catalog.pbtxt
```

Output:

```
✖ ERROR: Function return type references unknown type 'pg_catalog.int5'
✖ ERROR: Duplicate collation 'pg_catalog.default'

VALIDATION FAILED (2 core errors + 0 engine errors + 0 engine warnings)
```

### Checks Performed

| Section | Rules Enforced |
|---------|----------------|
| **Catalog-Level** | File must parse as protobuf (binary or text). |
| **Types** | `type.name` required; names and OIDs must be unique; `category` required; if `is_array=true` the `element_type` must exist and cannot form a self-loop or circular array chain; `element_type` references must resolve. |
| **Functions** | `name` required; signature (`name + argument_types`) unique; every argument and return type must exist; aggregate + window combos are rejected/warned. |
| **Operators** | `name` required; signature (`name + left + right`) unique; operand types (unless intentionally blank for unary) must exist; referenced function must exist. |
| **Casts** | Source/target types must exist; `(source, target)` pair unique; `method` must be one of `implicit`, `assignment`, or `explicit`. |
| **Collations** | Name unique and locale populated. |
| **Aggregates** | Signature unique; argument/state/return types must exist; `state_fn` and `final_fn` must reference existing functions. |
| **Cross-Reference** | Ensures no object references missing types/functions; catches circular array definitions; verifies casts/operators/aggregates all point to valid entities. |
| **Output & Behavior** | Prints all errors before exiting; exit code 0 on success, 1 on failure; `--strict` treats warnings as errors; `--json` emits machine-friendly output. |

Failures are reported with human-friendly descriptions, and the process exits
with status 1. The footer now reports core vs engine counts (for example:
`VALIDATION FAILED (0 core errors + 11 engine errors + 0 engine warnings)`),
and the JSON payload contains `engine_issues`, `engine_error_count`, and
`engine_warning_count` when the engine extension path runs. All bundled catalogs
(under `service/src/main/resources/builtins/`) are validated automatically during
`mvn test`.

### Engine-specific validation

Passing `--engine <engine_kind>` (with or without a catalog path) loads the
corresponding `EngineSystemCatalogExtension` via `ServiceLoader`. After the
builtin catalog checks complete, the tool invokes the extension’s `validate`
hook and returns any issues it emits alongside the core errors/warnings. Those
issues appear in the console as `engine_issues` (with severity-aware coloring)
and in JSON fields `engine_error_count`/`engine_warning_count`, letting you gate
release pipelines on both core and engine-specific expectations from one CLI
command.
