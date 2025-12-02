package ai.floedb.metacat.service.query.graph;

import java.util.Objects;

/**
 * Cache key for engine-specific hints.
 *
 * <p>Separating the key from {@link EngineHint} lets implementations keep immutable hint maps while
 * still performing constant-time lookups.
 */
public record EngineKey(String engineKind, String engineVersion) {

  public EngineKey {
    Objects.requireNonNull(engineKind, "engineKind");
    Objects.requireNonNull(engineVersion, "engineVersion");
  }
}
