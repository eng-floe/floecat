package ai.floedb.floecat.catalog.builtin.spi;

import ai.floedb.floecat.catalog.builtin.registry.BuiltinCatalogData;

/**
 * SPI for engine-specific builtin catalogs.
 *
 * <p>Implemented by plugin JARs discovered via Java ServiceLoader.
 */
public interface EngineBuiltinExtension {

  /** Globally unique engine identifier (e.g. "floe", "postgres", "trino"). */
  String engineKind();

  /** Returns builtin catalog data for current engine kind. */
  BuiltinCatalogData loadBuiltinCatalog();

  /**
   * Optional hook: plugin can validate itself or emit diagnostics. Floecat will ignore exceptions
   * by default.
   */
  default void onLoadError(Exception e) {
    // default: no-op
  }
}
