package ai.floedb.floecat.systemcatalog.registry;

import ai.floedb.floecat.systemcatalog.provider.SystemCatalogProvider;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Provides cached access to builtin catalogs for each engine kind. Delegates loading to a
 * SystemCatalogProvider.
 */
public final class SystemDefinitionRegistry {

  private final SystemCatalogProvider provider;
  private final ConcurrentMap<String, SystemEngineCatalog> cache = new ConcurrentHashMap<>();

  public SystemDefinitionRegistry(SystemCatalogProvider provider) {
    this.provider = Objects.requireNonNull(provider);
  }

  public SystemEngineCatalog catalog(String engineKind) {
    String key = engineKind.toLowerCase(Locale.ROOT);
    return cache.computeIfAbsent(key, provider::load);
  }

  /** Test-only: clears catalog cache. */
  public void clear() {
    cache.clear();
  }

  public List<String> engineKinds() {
    return provider.engineKinds();
  }
}
