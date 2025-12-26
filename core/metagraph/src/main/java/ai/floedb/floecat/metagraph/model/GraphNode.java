package ai.floedb.floecat.metagraph.model;

import ai.floedb.floecat.common.rpc.ResourceId;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * Base type for every immutable metadata node tracked by the query graph.
 *
 * <p>Nodes mirror the logical catalog objects described in {@code FloeCat_Architecture_Book.md}
 * (catalogs, namespaces, tables, views, system relations). Each node captures the pointer version
 * so cache invalidation is deterministic, while {@link EngineHint} payloads allow planner-specific
 * extensions without mutating the core structure.
 */
public interface GraphNode {

  /** Stable identifier (account + kind + UUID) for the node. */
  ResourceId id();

  /** Pointer version/Etag used for cache invalidation. */
  long version();

  /** Human-readable name for display purposes. */
  String displayName();

  /**
   * Timestamp of the last metadata mutation for this node.
   *
   * <p>The timestamp reflects repository-level changes (schema updates, view edits) but is not tied
   * to snapshot pointer updates, which are tracked separately.
   */
  Instant metadataUpdatedAt();

  /** Logical kind returned to planners. */
  GraphNodeKind kind();

  /** Origin of the node */
  GraphNodeOrigin origin();

  /**
   * Engine-specific hint map keyed by kind/version.
   *
   * <p>Implementations should return immutable maps. Use {@link #engineHint(String, String)} for
   * convenience lookups.
   */
  Map<EngineKey, EngineHint> engineHints();

  /**
   * Lookup helper for an engine/version pair.
   *
   * @param engineKind planner/executor kind (e.g. TRINO)
   * @param engineVersion semantic version string
   * @return the hint payload if present
   */
  default Optional<EngineHint> engineHint(String engineKind, String engineVersion) {
    return Optional.ofNullable(engineHints().get(new EngineKey(engineKind, engineVersion)));
  }
}
