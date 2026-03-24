# Example catalog extension

A zero-code, file-based `EngineSystemCatalogExtension` for Floecat.  Point it at a directory of
`.pbtxt` files and Floecat will serve your engine's catalog without any Java.

Full user documentation: [`docs/extension-example.md`](../../docs/extension-example.md).

## What lives here

- **`ExampleCatalogExtension.java`** — `EngineSystemCatalogExtension` implementation.  Reads
  config via MicroProfile Config (`FLOECAT_EXTENSION_ENGINE_KIND`,
  `FLOECAT_EXTENSION_BUILTINS_DIR`), loads `*.pbtxt` files from either the filesystem or the
  classpath, and strips any `engine_kind` fields from `engine_specific` blocks before serving.
- **`builtins/example/*.pbtxt`** — Bundled format-reference files.  These are annotated templates
  that show every available field with field-by-field comments.  They define placeholder objects
  (`my_type`, `my_function`, etc.) for documentation purposes only.  Replace them with your
  engine's actual catalog by setting `FLOECAT_EXTENSION_BUILTINS_DIR`.
- **`META-INF/services/...`** — `ServiceLoader` registration for `EngineSystemCatalogExtension`.

## PBtxt catalog format and tooling

All `.pbtxt` fragment files in `src/main/resources/builtins/` must satisfy two automated checks:

| Check | What it catches |
|---|---|
| **Semantic validation** (`validate-pbtxt`) | Parse errors, missing required fields, invalid type references |
| **Format check** (`check-pbtxt-format`) | Whitespace drift, non-canonical field ordering |

### File layout convention

```
src/main/resources/builtins/<engine>/
  _index.txt          # ordered list of fragment files
  00_namespaces.pbtxt
  10_types.pbtxt
  20_functions.pbtxt
  30_operators.pbtxt
  ...
```

Use 2-digit numeric prefixes to control merge order.  The `_index.txt` must list each file on its
own line (relative paths, no leading `/`, blank lines and `#` comments are ignored).

### Running the tools

```bash
# Validate semantics (all catalog dirs across the repo):
make validate-pbtxt

# Reformat to canonical proto text format (before committing):
make fmt-pbtxt

# Check formatting without rewriting (used by CI):
make check-pbtxt-format
```

### Adding pbtxt tooling to your own extension

Copy the `exec-maven-plugin` block from this module's `pom.xml` into your extension's `pom.xml`,
updating the directory paths to match your engine kind.  The executions shell out to the
`floecat-builtin-validator` fat JAR (built by `make build`) via `java -cp`; no extra Maven
dependency is required in your extension.

Then run your extension's catalog through the same tools:

```bash
# Validate semantics
mvn -pl extensions/my-extension exec:exec@validate-pbtxt

# Check formatting (default mode)
mvn -pl extensions/my-extension exec:exec@fmt-pbtxt

# Rewrite files to canonical format
mvn -pl extensions/my-extension exec:exec@fmt-pbtxt -Dfmt.mode=apply
```

Your catalog directory will also be **auto-discovered** by `make validate-pbtxt` and
`make check-pbtxt-format` as long as it contains an `_index.txt` file under
`src/main/resources/builtins/`.

## Testing your catalog files

Run the extension's unit tests against your own catalog directory to catch parse errors before
starting the service:

```bash
FLOECAT_EXTENSION_BUILTINS_DIR=/path/to/your/catalog \
  mvn -pl extensions/example test
```

Unparseable files are logged as `WARN` and skipped; the 45 unit tests must still pass because
the filesystem tests write their own temporary files in `@TempDir`.  Inspect
`target/surefire-reports/` for any failures and `target/surefire-reports/*.txt` for the full
log output including skipped-file warnings.
