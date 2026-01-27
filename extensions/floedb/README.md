# FloeDB extension

This module groups everything FloeDB-specific: the generated catalog fragments (`*.pbtxt`), the validation
logic that checks those fragments against the proto contracts, and the build hooks that gate Maven runs on
“good” catalog inputs.

## What lives here

- **pbtxt resources**: `src/test/resources/builtins/floedb/*.pbtxt` (types, functions, operators, etc.).
- **Validators**: `src/main/java/ai/floedb/floecat/extensions/floedb/validation`, which enforces typed OIDs,
  registry completeness, and consistent signatures for Floe-specific builtin data.
- **Build integration**: `pom.xml` wires `process-resources` to rebuild and validate the catalog as part of the
  FloeDB extension Maven lifecycle.

## FloeDB builtin catalog workflow

This is the repeatable path that regenerates the builtin registry and keeps it valid:

### 1. Regenerate pbtxt from PostgreSQL CSVs

```bash
python3 tools/floedb/csv-importer/main.py \
  --csv-dir tools/floedb/csv-importer/csv \
  --out-dir tools/floedb/csv-importer/out \
  [--stats] [--debug-drops]
```

- `--stats`: prints per-module rows read/kept/dropped/emitted.
- `--debug-drops`: logs the first dropped rows with a reason (missing dependencies,
  namespace filtering, unsupported OID, etc.).
- The importer emits files like `10_types.pbtxt`, `20_functions.pbtxt`, and
  `00_registry.pbtxt` under `tools/floedb/csv-importer/out/`.

### 2. Review and publish

1. Copy the generated pbtxts from `tools/floedb/csv-importer/out/` into
   `extensions/floedb/src/test/resources/builtins/floedb/` (replace or merge).
2. Preserve the numbering scheme (00 registry, 10 types, 20 functions, …) so the build
   knows the ordering.

### 3. Validate the catalog

```bash
# Build+install the module and its reactor dependencies so the tool classpath is available
mvn -q -pl :floecat-extension-floedb -am -DskipTests install

# Run the validator from the module POM (so exec:java@validate-builtin is defined)
mvn -q -f extensions/floedb/pom.xml test-compile exec:java@validate-builtin \
  -Dexec.args="--engine=floedb"
```

- The validator loads the pbtxt artifacts and the `Floe*Specific` payloads, then checks:
  - Every type/function/cast/operator has consistent OIDs and argument lists.
  - Registry entries reference existing families/classes/support procedures.
  - No duplicate signatures exist at the same scope.
- Failure codes like `floe.type.array.*`, `floe.function.signature.duplicate`, or
  `floe.registry.*` indicate either importer bugs or missing CSV data—fix the source,
  regenerate, and rerun validation.

### 4. Format the pbtxt files


```bash
# Build+install the module and its reactor dependencies so the tool classpath is available
mvn -q -pl :floecat-extension-floedb -am -DskipTests install

# Run the formatter from the module POM (so exec:java@format-pbtxt is defined)
mvn -q -f extensions/floedb/pom.xml test-compile exec:java@format-pbtxt \
  -Dexec.args="--engine=floedb --apply"
```

- This runs `BuiltinPbtxtFormatter` to canonicalize the generated fragments: it rewrites each file with the protobuf `TextFormat` printer,
  preserves the comment/header preamble, and inserts blank lines between top-level blocks for readability.
- Use `--apply` when you want the formatter to rewrite the files, and `--check` when you just want to verify the current formatting without changes:
  ```bash
  mvn -q -pl :floecat-extension-floedb -am -DskipTests install

  mvn -q -f extensions/floedb/pom.xml test-compile exec:java@format-pbtxt \
    -Dexec.args="--engine=floedb --check"
  ```
- Formatter failures explain the offending files and point to the command above so you can fix or reformat before committing.

### 5. Ship

After validation passes, the updated pbtxt bundle is ready for CI and release. Future builds will rerun
`validate-builtin` to prevent regressions.
