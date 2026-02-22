/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.query.flight;

import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.service.context.impl.InboundCallContextHelper;
import io.grpc.StatusRuntimeException;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.RequestContext;
import org.jboss.logging.MDC;

/**
 * Arrow Flight server middleware that provides full call-context parity with the gRPC path's {@code
 * InboundContextInterceptor}.
 *
 * <p>For every inbound Flight RPC, this middleware:
 *
 * <ol>
 *   <li>Resolves auth / {@link ai.floedb.floecat.common.rpc.PrincipalContext} from OIDC tokens (or
 *       {@code devContext()} in dev mode) via the shared {@link InboundCallContextHelper}.
 *   <li>Captures {@code x-engine-kind} and {@code x-engine-version} into an {@link
 *       ai.floedb.floecat.scanner.utils.EngineContext}.
 *   <li>Reads {@code x-query-id} and {@code x-correlation-id} (generating a random correlation ID
 *       when absent).
 *   <li>Populates MDC logging entries for the duration of the call.
 *   <li>Echoes {@code x-correlation-id} back in response headers.
 * </ol>
 *
 * <p>Producer methods retrieve the resolved context via:
 *
 * <pre>
 *   InboundContextFlightMiddleware mw =
 *       context.getMiddleware(InboundContextFlightMiddleware.KEY);
 * </pre>
 *
 * <p>Auth failures throw {@link io.grpc.StatusRuntimeException} inside {@link
 * InboundCallContextHelper#resolve}, which the {@link Factory} catches and converts to the
 * appropriate Arrow Flight {@link CallStatus} before rejecting the call.
 */
final class InboundContextFlightMiddleware implements FlightServerMiddleware {

  static final Key<InboundContextFlightMiddleware> KEY = Key.of("floecat-inbound-context");

  private final ResolvedCallContext callContext;

  private InboundContextFlightMiddleware(ResolvedCallContext callContext) {
    this.callContext = callContext;
  }

  /** Returns the fully resolved per-call context. */
  ResolvedCallContext callContext() {
    return callContext;
  }

  @Override
  public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
    // Echo correlation-id back to the caller (mirrors gRPC path).
    outgoingHeaders.insert(
        InboundCallContextHelper.HEADER_CORRELATION_ID, callContext.correlationId());
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

  // -------------------------------------------------------------------------
  //  Factory — registered with FlightServer.Builder
  // -------------------------------------------------------------------------

  /**
   * Factory that resolves the full inbound call context on each RPC and constructs the per-call
   * middleware instance.
   *
   * <p>Register via:
   *
   * <pre>
   *   FlightServer.builder(...)
   *       .middleware(InboundContextFlightMiddleware.KEY,
   *                  new InboundContextFlightMiddleware.Factory(contextHelper))
   *       .build();
   * </pre>
   */
  static final class Factory
      implements FlightServerMiddleware.Factory<InboundContextFlightMiddleware> {

    private final InboundCallContextHelper helper;

    Factory(InboundCallContextHelper helper) {
      this.helper = helper;
    }

    @Override
    public InboundContextFlightMiddleware onCallStarted(
        CallInfo info, CallHeaders incomingHeaders, RequestContext requestContext) {
      ResolvedCallContext resolved;
      try {
        // Flight has no health/reflection bypass — all calls require auth.
        resolved = helper.resolve(incomingHeaders::get, /* allowUnauthenticated */ false);
      } catch (StatusRuntimeException e) {
        // Convert gRPC Status → Arrow Flight CallStatus so the client receives the right error.
        throw toFlightStatus(e).toRuntimeException();
      }

      // Populate MDC for the duration of this call (cleared in onCallCompleted/onCallErrored).
      MDC.put("query_id", resolved.queryId());
      MDC.put("correlation_id", resolved.correlationId());
      MDC.put("floecat_account_id", resolved.principalContext().getAccountId());
      MDC.put("floecat_subject", resolved.principalContext().getSubject());
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
