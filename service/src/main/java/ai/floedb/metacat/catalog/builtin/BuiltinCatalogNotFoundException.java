package ai.floedb.metacat.catalog.builtin;

/** Thrown when no builtin catalog file exists for the requested engine version. */
public final class BuiltinCatalogNotFoundException extends RuntimeException {
  public BuiltinCatalogNotFoundException(String engineVersion) {
    super("Builtin catalog not found for engine version: " + engineVersion);
  }
}
