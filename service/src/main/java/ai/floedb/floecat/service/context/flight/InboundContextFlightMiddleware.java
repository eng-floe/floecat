/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.context.flight;

import ai.floedb.floecat.flight.ContextHeaders;
import ai.floedb.floecat.flight.context.CallContextResolver;
import ai.floedb.floecat.flight.context.ResolvedCallContext;
import io.grpc.StatusRuntimeException;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.RequestContext;
import org.jboss.logging.MDC;

/**
 * Arrow Flight server middleware that mirrors the gRPC {@link
 * ai.floedb.floecat.service.context.impl.InboundContextInterceptor}.
 *
 * <p>The middleware resolves the per-call context, populates MDC, and echoes the correlation header
 * back to the client.
 */
public final class InboundContextFlightMiddleware implements FlightServerMiddleware {

  public static final Key<InboundContextFlightMiddleware> KEY = Key.of("floecat-inbound-context");

  private final ResolvedCallContext callContext;

  private InboundContextFlightMiddleware(ResolvedCallContext callContext) {
    this.callContext = callContext;
  }

  /** Returns the resolved context for the active call. */
  public ResolvedCallContext callContext() {
    return callContext;
  }

  @Override
  public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
    outgoingHeaders.insert(ContextHeaders.HEADER_CORRELATION_ID, callContext.correlationId());
  }

  @Override
  public void onCallCompleted(CallStatus status) {
    clearMdc();
  }

  @Override
  public void onCallErrored(Throwable err) {
    clearMdc();
  }

  private void clearMdc() {
    MDC.remove("query_id");
    MDC.remove("correlation_id");
    MDC.remove("floecat_account_id");
    MDC.remove("floecat_subject");
    MDC.remove("floecat_engine_kind");
    MDC.remove("floecat_engine_version");
  }

  /** --------------------------------------------------------------------- */
  public static final class Factory
      implements FlightServerMiddleware.Factory<InboundContextFlightMiddleware> {

    private final CallContextResolver resolver;

    public Factory(CallContextResolver resolver) {
      this.resolver = resolver;
    }

    @Override
    public InboundContextFlightMiddleware onCallStarted(
        CallInfo info, CallHeaders incomingHeaders, RequestContext requestContext) {
      ResolvedCallContext resolved;
      try {
        resolved = resolver.resolve(incomingHeaders::get, /* allowUnauthenticated */ false);
      } catch (StatusRuntimeException e) {
        throw toFlightStatus(e).toRuntimeException();
      }

      MDC.put("query_id", resolved.queryId());
      MDC.put("correlation_id", resolved.correlationId());
      MDC.put("floecat_account_id", resolved.principalContext().getAccountId());
      MDC.put("floecat_subject", resolved.principalContext().getSubject());
      // These MDC keys mirror the gRPC path so logs from Flight and gRPC share the same metadata.
      MDC.put("floecat_engine_kind", resolved.engineContext().engineKind());
      MDC.put("floecat_engine_version", resolved.engineContext().engineVersion());
      return new InboundContextFlightMiddleware(resolved);
    }

    private static CallStatus toFlightStatus(StatusRuntimeException e) {
      return switch (e.getStatus().getCode()) {
        case NOT_FOUND -> CallStatus.NOT_FOUND.withDescription(e.getMessage());
        case INVALID_ARGUMENT -> CallStatus.INVALID_ARGUMENT.withDescription(e.getMessage());
        case UNAUTHENTICATED -> CallStatus.UNAUTHENTICATED.withDescription(e.getMessage());
        case PERMISSION_DENIED -> CallStatus.UNAUTHORIZED.withDescription(e.getMessage());
        case UNAVAILABLE -> CallStatus.UNAVAILABLE.withDescription(e.getMessage());
        default -> CallStatus.INTERNAL.withDescription(e.getMessage()).withCause(e);
      };
    }
  }
}
