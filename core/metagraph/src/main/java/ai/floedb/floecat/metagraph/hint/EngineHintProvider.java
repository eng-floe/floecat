package ai.floedb.floecat.metagraph.hint;

import ai.floedb.floecat.metagraph.model.*;

/** Provider of engine-specific hint payloads for relation nodes. */
public interface EngineHintProvider {

  /** Returns true when this provider can compute the requested hint type for the node kind. */
  boolean supports(RelationNodeKind kind, String hintType);

  /** Returns true when the provider can serve the engine/version represented by the key. */
  boolean isAvailable(EngineKey engineKey);

  /**
   * Stable fingerprint describing the hint inputs for caching.
   *
   * <p>Implementations may combine pointer versions, schema hashes, provider versions, etc. to
   * ensure recomputation occurs only when relevant inputs change.
   */
  String fingerprint(RelationNode node, EngineKey engineKey, String hintType);

  /** Computes the actual hint payload. */
  EngineHint compute(RelationNode node, EngineKey engineKey, String hintType, String correlationId);
}
