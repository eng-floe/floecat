package ai.floedb.metacat.service.planning;

import java.util.Optional;

import ai.floedb.metacat.service.planning.impl.PlanContext;

public interface PlanContextStore extends AutoCloseable {
  Optional<PlanContext> get(String planId);

  void put(PlanContext ctx);

  Optional<PlanContext> extendLease(String planId, long requestedExpiresAtMs);

  Optional<PlanContext> end(String planId, boolean commit);

  boolean delete(String planId);

  long size();

  @Override void close();
}