package ai.floedb.metacat.catalog.builtin;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Loads builtin catalog bundles per engine version and exposes indexed {@link BuiltinEngineCatalog}
 * snapshots. The data is cached in memory because builtin objects are immutable for a given engine
 * release.
 */
@ApplicationScoped
public class BuiltinDefinitionRegistry {

  private final BuiltinCatalogLoader loader;
  private final ConcurrentMap<String, BuiltinEngineCatalog> cache = new ConcurrentHashMap<>();

  @Inject
  public BuiltinDefinitionRegistry(BuiltinCatalogLoader loader) {
    this.loader = Objects.requireNonNull(loader, "loader");
  }

  /** Returns the builtin catalog for the provided engine version, loading it once if necessary. */
  public BuiltinEngineCatalog catalog(String engineVersion) {
    if (engineVersion == null || engineVersion.isBlank()) {
      throw new IllegalArgumentException("engine_version must be provided");
    }
    return cache.computeIfAbsent(engineVersion, this::loadCatalog);
  }

  /** Test-only hook to drop cached catalogs so subsequent calls reload from disk. */
  void clear() {
    cache.clear();
  }

  private BuiltinEngineCatalog loadCatalog(String engineVersion) {
    var data = loader.getCatalog(engineVersion);
    return BuiltinEngineCatalog.from(engineVersion, data);
  }
}
