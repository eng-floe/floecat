package ai.floedb.floecat.metagraph.model;

/**
 * Execution backend for a system object.
 *
 * <p>The enum intentionally mirrors the terminology in {@code FloeCat_Architecture_Book.md}; the
 * concrete RPC/proto definition can replace it once the contract is finalized.
 */
public enum GraphNodeOrigin {
  SYSTEM, // system object nodes (built-in or plugin)
  USER // user objects nodes
}
