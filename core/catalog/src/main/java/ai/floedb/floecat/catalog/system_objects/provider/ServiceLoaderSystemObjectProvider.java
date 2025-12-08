package ai.floedb.floecat.catalog.system_objects.provider;

import ai.floedb.floecat.catalog.system_objects.spi.EngineSystemTablesExtension;
import ai.floedb.floecat.catalog.system_objects.spi.SystemObjectProvider;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Loads SystemObjectProvider implementations using Java's ServiceLoader mechanism.
 *
 * <p>This discovers: - builtin SystemObjectProvider - plugin EngineSystemTablesExtension (which
 * extends SystemObjectProvider)
 */
public final class ServiceLoaderSystemObjectProvider {

  private final List<SystemObjectProvider> providers;

  public ServiceLoaderSystemObjectProvider() {

    // Load builtin providers
    var baseProviders =
        ServiceLoader.load(SystemObjectProvider.class).stream().map(ServiceLoader.Provider::get);

    // Load engine-specific plugin providers
    var pluginProviders =
        ServiceLoader.load(EngineSystemTablesExtension.class).stream()
            .map(ServiceLoader.Provider::get);

    // Merge them into a single unmodifiable list
    this.providers =
        Stream.concat(baseProviders, pluginProviders).collect(Collectors.toUnmodifiableList());
  }

  public List<SystemObjectProvider> providers() {
    return providers;
  }
}
