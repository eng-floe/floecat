package ai.floedb.metacat.service.query.graph.model;

import java.util.Objects;

/**
 * Engine-specific payload attached to a {@link RelationNode}.
 *
 * <p>Hints are versioned per engine kind/version pair so planners/executors can request extra
 * metadata (coercion rules, layout pointers, connector hints) without polluting the core node
 * schema.
 */
public record EngineHint(String engineKind, String engineVersion, byte[] payload) {

  public EngineHint {
    Objects.requireNonNull(engineKind, "engineKind");
    Objects.requireNonNull(engineVersion, "engineVersion");
    Objects.requireNonNull(payload, "payload");
  }
}
