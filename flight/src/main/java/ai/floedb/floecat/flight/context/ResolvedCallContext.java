package ai.floedb.floecat.flight.context;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.scanner.utils.EngineContext;

/** Fully resolved per-call context for Flight producers and middleware. */
public record ResolvedCallContext(
    PrincipalContext principalContext,
    String queryId,
    String correlationId,
    EngineContext engineContext,
    String sessionHeaderValue,
    String authorizationHeaderValue) {

  /** Returns a default context that fails authorization when auth info is missing. */
  public static ResolvedCallContext unauthenticated() {
    return new ResolvedCallContext(
        PrincipalContext.getDefaultInstance(), "", "", EngineContext.empty(), null, null);
  }
}
