package ai.floedb.floecat.catalog.builtin;

import ai.floedb.floecat.extensions.builtin.spi.EngineBuiltinExtension;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Default;
import java.util.*;
import org.jboss.logging.Logger;

/**
 * Production implementation of BuiltinCatalogProvider. Discovers EngineBuiltinExtension
 * implementations using ServiceLoader.
 */
@Default
@ApplicationScoped
public final class ServiceLoaderBuiltinCatalogProvider implements BuiltinCatalogProvider {

  private static final Logger LOG = Logger.getLogger(ServiceLoaderBuiltinCatalogProvider.class);

  private Map<String, EngineBuiltinExtension> plugins;

  public ServiceLoaderBuiltinCatalogProvider() {}

  @PostConstruct
  void init() {
    Map<String, EngineBuiltinExtension> tmp = new HashMap<>();
    ServiceLoader.load(EngineBuiltinExtension.class)
        .forEach(ext -> tmp.put(ext.engineKind().toLowerCase(Locale.ROOT), ext));
    this.plugins = Map.copyOf(tmp);
  }

  @Override
  public BuiltinEngineCatalog load(String engineKind) {
    if (engineKind == null || engineKind.isBlank()) {
      throw new IllegalArgumentException("engine_kind must be provided");
    }

    EngineBuiltinExtension ext = plugins.get(engineKind.toLowerCase(Locale.ROOT));

    if (ext == null) {
      LOG.warn("No builtin plugin found for engine_kind=" + engineKind);
      return BuiltinEngineCatalog.from(engineKind, BuiltinCatalogData.empty());
    }

    BuiltinCatalogData data = ext.loadBuiltinCatalog();
    return BuiltinEngineCatalog.from(engineKind, data);
  }
}
