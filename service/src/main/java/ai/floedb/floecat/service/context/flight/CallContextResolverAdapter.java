package ai.floedb.floecat.service.context.flight;

import ai.floedb.floecat.flight.context.CallContextResolver;
import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.service.context.impl.InboundCallContextHelper;
import java.util.function.Function;

/** Adapter that lets Flight middleware reuse the service's {@link InboundCallContextHelper}. */
public final class CallContextResolverAdapter implements CallContextResolver {

  private final InboundCallContextHelper helper;

  public CallContextResolverAdapter(InboundCallContextHelper helper) {
    this.helper = helper;
  }

  @Override
  public ResolvedCallContext resolve(
      Function<String, String> headerReader, boolean allowUnauthenticated) {
    return helper.resolve(headerReader, allowUnauthenticated);
  }
}
