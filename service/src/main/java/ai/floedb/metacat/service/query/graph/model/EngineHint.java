package ai.floedb.metacat.service.query.graph.model;

import java.util.Map;
import java.util.Objects;

/**
 * Engine-specific payload attached to a {@link RelationNode}.
 *
 * <p>Hints carry opaque binary blobs plus optional metadata describing the payload. Engine kind /
 * version scoping is handled outside the hint object (via {@link EngineKey}).
 */
public record EngineHint(
    String contentType, byte[] payload, long sizeBytes, Map<String, String> metadata) {

  public EngineHint(String contentType, byte[] payload) {
    this(contentType, payload, payload == null ? 0 : payload.length, Map.of());
  }

  public EngineHint {
    Objects.requireNonNull(payload, "payload");
    contentType =
        contentType == null || contentType.isBlank() ? "application/octet-stream" : contentType;
    sizeBytes = sizeBytes <= 0 ? payload.length : sizeBytes;
    metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
  }
}
