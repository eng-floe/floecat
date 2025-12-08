package ai.floedb.floecat.catalog.builtin.provider;

import ai.floedb.floecat.catalog.builtin.registry.BuiltinEngineCatalog;

/**
 * Provides builtin engine catalogs.
 *
 * <p>Used by BuiltinDefinitionRegistry. Production uses the ServiceLoader-based implementation,
 * while tests can supply their own static catalogs.
 */
public interface BuiltinCatalogProvider {

  /** Loads the catalog for the given engine kind. */
  BuiltinEngineCatalog load(String engineKind);
}
