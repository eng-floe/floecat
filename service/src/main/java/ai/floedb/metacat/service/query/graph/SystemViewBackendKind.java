package ai.floedb.metacat.service.query.graph;

/**
 * Execution backend for a system view.
 *
 * <p>The enum intentionally mirrors the terminology in {@code FloeCat_Architecture_Book.md}; the
 * concrete RPC/proto definition can replace it once the contract is finalized.
 */
public enum SystemViewBackendKind {
  FLOECAT,
  ENGINE
}
