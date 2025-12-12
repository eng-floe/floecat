package ai.floedb.floecat.systemcatalog.provider;

import ai.floedb.floecat.systemcatalog.registry.SystemEngineCatalog;

/**
 * Provides system objects metadata catalogs per engine.
 *
 * <p>Used by SystemDefinitionRegistry. Production uses the ServiceLoader-based implementation,
 * while tests can supply their own static catalogs.
 */
public interface SystemCatalogProvider {

  /** Loads the catalog for the given engine kind. */
  SystemEngineCatalog load(String engineKind);
}
