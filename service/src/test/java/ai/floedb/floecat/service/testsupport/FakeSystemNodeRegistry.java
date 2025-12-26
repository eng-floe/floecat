package ai.floedb.floecat.service.testsupport;

import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.provider.SystemCatalogProvider;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemDefinitionRegistry;
import ai.floedb.floecat.systemcatalog.registry.SystemEngineCatalog;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Test-only SystemNodeRegistry backed by an in-memory SystemCatalogProvider.
 *
 * <p>Uses the real SystemNodeRegistry + SystemDefinitionRegistry logic.
 */
public final class FakeSystemNodeRegistry extends SystemNodeRegistry {

  private final FakeSystemCatalogProvider provider;

  public FakeSystemNodeRegistry() {
    this(new FakeSystemCatalogProvider());
  }

  private FakeSystemNodeRegistry(FakeSystemCatalogProvider provider) {
    super(new SystemDefinitionRegistry(provider));
    this.provider = provider;
  }

  /**
   * Registers a synthetic system catalog for an engine kind.
   *
   * <p>Engine versions are still filtered later by SystemNodeRegistry.
   */
  public FakeSystemNodeRegistry register(String engineKind, SystemCatalogData catalogData) {

    provider.register(engineKind, catalogData);
    return this;
  }

  /* ----------------------------------------------------------------------
   * Fake provider
   * ---------------------------------------------------------------------- */

  private static final class FakeSystemCatalogProvider implements SystemCatalogProvider {

    private final Map<String, SystemEngineCatalog> catalogs = new HashMap<>();

    void register(String engineKind, SystemCatalogData data) {
      String key = engineKind.toLowerCase(Locale.ROOT);
      catalogs.put(key, SystemEngineCatalog.from(key, data));
    }

    @Override
    public SystemEngineCatalog load(String engineKind) {
      SystemEngineCatalog catalog = catalogs.get(engineKind.toLowerCase(Locale.ROOT));
      if (catalog == null) {
        throw new IllegalStateException(
            "No fake system catalog registered for engine: " + engineKind);
      }
      return catalog;
    }

    @Override
    public List<String> engineKinds() {
      return List.copyOf(catalogs.keySet());
    }
  }
}
