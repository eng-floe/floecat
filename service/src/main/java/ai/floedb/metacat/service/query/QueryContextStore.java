package ai.floedb.metacat.service.query;

import ai.floedb.metacat.service.query.impl.QueryContext;
import java.util.Optional;

public interface QueryContextStore extends AutoCloseable {
  Optional<QueryContext> get(String queryId);

  void put(QueryContext ctx);

  Optional<QueryContext> extendLease(String queryId, long requestedExpiresAtMs);

  Optional<QueryContext> end(String queryId, boolean commit);

  boolean delete(String queryId);

  long size();

  @Override
  void close();
}
