package ai.floedb.metacat.catalog.builtin;

/** Thrown when no builtin catalog file exists for the requested engine kind. */
public final class BuiltinCatalogNotFoundException extends RuntimeException {
  public BuiltinCatalogNotFoundException(String engineKind) {
    super("Builtin catalog not found for engine kind: " + engineKind);
  }
}
