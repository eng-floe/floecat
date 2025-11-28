# Tools

This directory holds standalone utilities that are built alongside the main
Metacat modules. Each tool is designed to run without the service stack so that
planners, CI pipelines, or operators can invoke them directly.

## builtin-validator

Validates builtin catalog protobufs (either binary `.pb` or text `.pbtxt`) to
ensure they contain the metadata FloeCat’s planner expects.

### Build

```bash
mvn -pl tools/builtin-validator package
```

### Usage

```bash
java -jar tools/builtin-validator/target/builtin-validator.jar \
    /path/to/builtin_catalog_<engine>.pb[txt] \
    [--json] [--strict]
```

- `--json` emits machine-readable results (`valid`, `stats`, `errors`,
  `warnings`).
- `--strict` makes warnings fail the run (currently reserved for future checks).
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

VALIDATION FAILED (2 errors)
```

### Checks Performed

| Section | Rules Enforced |
|---------|----------------|
| **Catalog-Level** | File must parse as protobuf (binary or text); `catalog.version` must not be empty and must match `^[A-Za-z0-9_.-]+$`. |
| **Types** | `type.name` required; names and OIDs must be unique; `category` required; if `is_array=true` the `element_type` must exist and cannot form a self-loop or circular array chain; `element_type` references must resolve. |
| **Functions** | `name` required; signature (`name + argument_types`) unique; every argument and return type must exist; aggregate + window combos are rejected/warned. |
| **Operators** | `name` required; signature (`name + left + right`) unique; operand types (unless intentionally blank for unary) must exist; referenced function must exist. |
| **Casts** | Source/target types must exist; `(source, target)` pair unique; `method` must be one of `implicit`, `assignment`, or `explicit`. |
| **Collations** | Name unique and locale populated. |
| **Aggregates** | Signature unique; argument/state/return types must exist; `state_fn` and `final_fn` must reference existing functions. |
| **Cross-Reference** | Ensures no object references missing types/functions; catches circular array definitions; verifies casts/operators/aggregates all point to valid entities. |
| **Output & Behavior** | Prints all errors before exiting; exit code 0 on success, 1 on failure; `--strict` treats warnings as errors; `--json` emits machine-friendly output. |

Failures are reported with human-friendly descriptions, and the process exits
with status 1. All bundled catalogs (under
`service/src/main/resources/builtins/`) are validated automatically during
`mvn test`.
