package ai.floedb.floecat.catalog.builtin;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Provides cached access to builtin catalogs for each engine kind. Delegates loading to a
 * BuiltinCatalogProvider.
 */
@ApplicationScoped
public final class BuiltinDefinitionRegistry {

  private final BuiltinCatalogProvider provider;
  private final ConcurrentMap<String, BuiltinEngineCatalog> cache = new ConcurrentHashMap<>();

  @Inject
  public BuiltinDefinitionRegistry(BuiltinCatalogProvider provider) {
    this.provider = Objects.requireNonNull(provider);
  }

  public BuiltinEngineCatalog catalog(String engineKind) {
    if (engineKind == null || engineKind.isBlank()) {
      throw new IllegalArgumentException("engine_kind must be provided");
    }
    String key = engineKind.toLowerCase(Locale.ROOT);
    return cache.computeIfAbsent(key, provider::load);
  }

  /** Test-only: clears catalog cache. */
  public void clear() {
    cache.clear();
  }
}
