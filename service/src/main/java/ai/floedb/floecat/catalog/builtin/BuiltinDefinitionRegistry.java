package ai.floedb.floecat.catalog.builtin;

import ai.floedb.floecat.extensions.builtin.spi.EngineBuiltinExtension;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.jboss.logging.Logger;

/**
 * Loads builtin catalog bundles per engine kind and exposes indexed {@link BuiltinEngineCatalog}
 * snapshots. The data is cached in memory because builtin objects are immutable for a given engine
 * release.
 */
@ApplicationScoped
public final class BuiltinDefinitionRegistry {
  private static final Logger LOG = Logger.getLogger(BuiltinDefinitionRegistry.class);

  private Map<String, EngineBuiltinExtension> plugins;
  private final ConcurrentMap<String, BuiltinEngineCatalog> cache = new ConcurrentHashMap<>();

  public BuiltinDefinitionRegistry() {
    // must stay empty — Quarkus CDI calls this via reflection
  }

  @PostConstruct
  void init() {
    Map<String, EngineBuiltinExtension> tmp = new HashMap<>();
    ServiceLoader.load(EngineBuiltinExtension.class)
        .forEach(ext -> tmp.put(ext.engineKind().toLowerCase(Locale.ROOT), ext));
    this.plugins = Map.copyOf(tmp);
  }

  /** Returns the builtin catalog for the provided engine kind, loading it once if necessary. */
  public BuiltinEngineCatalog catalog(String engineKind) {
    if (engineKind == null || engineKind.isBlank())
      throw new IllegalArgumentException("engine_kind must be provided");
    String key = engineKind.toLowerCase(Locale.ROOT);
    return cache.computeIfAbsent(key, this::loadCatalog);
  }

  private BuiltinEngineCatalog loadCatalog(String engineKind) {
    EngineBuiltinExtension ext = plugins.get(engineKind.toLowerCase(Locale.ROOT));
    if (ext == null) {
      LOG.warn("No builtin plugin for engine_kind=" + engineKind + " → empty catalog");
      return BuiltinEngineCatalog.from(
          engineKind,
          new BuiltinCatalogData(List.of(), List.of(), List.of(), List.of(), List.of(), List.of()));
    }
    BuiltinCatalogData data = ext.loadBuiltinCatalog();
    return BuiltinEngineCatalog.from(engineKind, data);
  }

  /** Test-only hook to drop cached catalogs so subsequent calls reload from disk. */
  public void clear() {
    cache.clear();
  }
}
