package ai.floedb.floecat.systemcatalog.spi;

import ai.floedb.floecat.systemcatalog.provider.SystemObjectScannerProvider;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;

/**
 * SPI for engine-specific builtin catalogs.
 *
 * <p>Implemented by plugin JARs discovered via Java ServiceLoader.
 */
public interface EngineSystemCatalogExtension extends SystemObjectScannerProvider {

  /** Globally unique engine identifier (e.g. "floe", "postgres", "trino"). */
  String engineKind();

  /** Returns builtin catalog data for current engine kind. */
  SystemCatalogData loadSystemCatalog();

  /**
   * Optional hook: plugin can validate itself or emit diagnostics. Floecat will ignore exceptions
   * by default.
   */
  default void onLoadError(Exception e) {
    // default: no-op
  }
}
