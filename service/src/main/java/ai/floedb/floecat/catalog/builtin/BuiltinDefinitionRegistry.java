package ai.floedb.floecat.catalog.builtin;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Loads builtin catalog bundles per engine kind and exposes indexed {@link BuiltinEngineCatalog}
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

  /** Returns the builtin catalog for the provided engine kind, loading it once if necessary. */
  public BuiltinEngineCatalog catalog(String engineKind) {
    if (engineKind == null || engineKind.isBlank()) {
      throw new IllegalArgumentException("engine_kind must be provided");
    }
    return cache.computeIfAbsent(engineKind, this::loadCatalog);
  }

  /** Test-only hook to drop cached catalogs so subsequent calls reload from disk. */
  void clear() {
    cache.clear();
  }

  private BuiltinEngineCatalog loadCatalog(String engineKind) {
    var data = loader.getCatalog(engineKind);
    return BuiltinEngineCatalog.from(engineKind, data);
  }
}
