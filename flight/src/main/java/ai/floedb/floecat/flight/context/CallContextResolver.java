package ai.floedb.floecat.flight.context;

import java.util.function.Function;

/** Resolves the per-call context for an incoming Flight RPC. */
public interface CallContextResolver {

  /**
   * Resolves the context for a Flight call.
   *
   * @param headerReader reader for raw header values
   * @param allowUnauthenticated when {@code true} the resolver may return a default context without
   *     enforcing auth (used by health/reflection handlers)
   */
  ResolvedCallContext resolve(Function<String, String> headerReader, boolean allowUnauthenticated);
}
