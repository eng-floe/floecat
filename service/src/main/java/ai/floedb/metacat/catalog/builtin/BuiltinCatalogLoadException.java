package ai.floedb.metacat.catalog.builtin;

/** Wraps IO/protobuf parsing errors encountered while reading builtin catalog files. */
final class BuiltinCatalogLoadException extends RuntimeException {
  BuiltinCatalogLoadException(String engineVersion, Throwable cause) {
    super("Failed to load builtin catalog for engine version: " + engineVersion, cause);
  }
}
