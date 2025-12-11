package ai.floedb.floecat.catalog.builtin.provider;

import ai.floedb.floecat.catalog.builtin.spi.EngineBuiltinExtension;
import java.util.*;
import org.jboss.logging.Logger;

/**
 * Discovers EngineBuiltinExtension providers using Java ServiceLoader. All plugin JARs register
 * themselves under META-INF/services.
 */
public final class EngineBuiltinExtensionLoader {

  private static final Logger LOG = Logger.getLogger(EngineBuiltinExtensionLoader.class);

  private final Map<String, EngineBuiltinExtension> byEngine;

  public EngineBuiltinExtensionLoader() {
    Map<String, EngineBuiltinExtension> tmp = new HashMap<>();

    ServiceLoader<EngineBuiltinExtension> loader = ServiceLoader.load(EngineBuiltinExtension.class);

    for (EngineBuiltinExtension ext : loader) {
      String kind = ext.engineKind().trim().toLowerCase(Locale.ROOT);

      if (kind.isEmpty()) {
        LOG.warnf("Ignoring plugin with empty engineKind: %s", ext.getClass().getName());
        continue;
      }
      if (tmp.containsKey(kind)) {
        LOG.warnf(
            "Duplicate plugin for engine kind '%s': using %s, ignoring %s",
            kind, tmp.get(kind).getClass().getName(), ext.getClass().getName());
        continue;
      }

      LOG.infof("Discovered builtin plugin for engine '%s': %s", kind, ext.getClass().getName());
      tmp.put(kind, ext);
    }

    this.byEngine = Map.copyOf(tmp);
  }

  public Optional<EngineBuiltinExtension> find(String engineKind) {
    if (engineKind == null) return Optional.empty();
    return Optional.ofNullable(byEngine.get(engineKind.toLowerCase(Locale.ROOT)));
  }

  public Set<String> knownEngines() {
    return byEngine.keySet();
  }
}
