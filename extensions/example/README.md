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
