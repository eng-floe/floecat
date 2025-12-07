package ai.floedb.floecat.service.query;

import ai.floedb.floecat.service.query.impl.QueryContext;
import java.util.Optional;

/**
 * Storage abstraction for server-side QueryContext.
 *
 * <p>Implementations must: - preserve immutability of QueryContext - ensure updates are monotonic
 * and consistent - never silently drop or overwrite contexts without explicit calls
 */
public interface QueryContextStore extends AutoCloseable {

  /** Retrieve an existing context and perform expiration checks. */
  Optional<QueryContext> get(String queryId);

  /** Insert a new context only if one does not already exist. */
  void put(QueryContext ctx);

  /** Extend TTL of an existing active context. */
  Optional<QueryContext> extendLease(String queryId, long requestedExpiresAtMs);

  /** Move context into END_COMMIT or END_ABORT state. */
  Optional<QueryContext> end(String queryId, boolean commit);

  /** Remove the context entirely. */
  boolean delete(String queryId);

  /** Return approximate cache size. */
  long size();

  /**
   * Replace an existing QueryContext with an updated version.
   *
   * <p>This is required for DescribeInputs(), GetCatalogBundle(), and other RPCs to populate: -
   * expansionMap - obligations - additional metadata
   *
   * <p>Unlike put(), this MUST overwrite the existing context.
   */
  void replace(QueryContext ctx);

  @Override
  void close();
}
