package ai.floedb.floecat.catalog.system_objects.provider;

import ai.floedb.floecat.catalog.system_objects.registry.SystemObjectRegistry;
import ai.floedb.floecat.catalog.system_objects.spi.SystemObjectProvider;
import java.util.List;

/**
 * Test-only static provider registry.
 *
 * <p>Allows tests to supply a closed set of SystemObjectProvider instances without relying on
 * ServiceLoader or CDI discovery.
 */
public final class StaticSystemObjectProviderRegistry {

  private final List<SystemObjectProvider> providers;

  public StaticSystemObjectProviderRegistry(List<SystemObjectProvider> providers) {
    this.providers = List.copyOf(providers);
  }

  public List<SystemObjectProvider> providers() {
    return providers;
  }

  public SystemObjectRegistry toRegistry() {
    return new SystemObjectRegistry(providers);
  }
}
