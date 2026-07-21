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

package ai.floedb.floecat.service.telemetry;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.service.common.GrpcInterceptorPriorities;
import ai.floedb.floecat.service.context.impl.InboundContextInterceptor;
import ai.floedb.floecat.service.context.impl.ResolvedCallContexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.opentelemetry.api.trace.Span;
import io.quarkus.grpc.GlobalInterceptor;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.Prioritized;
import jakarta.inject.Inject;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

/**
 * Captures the call's gRPC server span — from the ONE place in the whole chain where it is
 * reachable — and makes it available to every other decoration site via the per-call
 * duplicated-context carrier.
 *
 * <p>Quarkus's {@code GrpcTracingServerInterceptor} creates the server span and makes it current
 * only inside its own {@code interceptCall} window (its listener never re-attaches around events;
 * it only ends the span on terminal ones). Because that interceptor carries no {@code Prioritized}
 * priority it sorts to 0 — INSIDE every floecat interceptor — so all of floecat's {@code
 * Span.current()} decoration reads (interceptor tags, handler attributes, per-phase diagnostic
 * events) resolve the invalid root span and silently no-op. This interceptor's {@link
 * GrpcInterceptorPriorities#SPAN_CAPTURE below-zero priority} places its chain build inside the
 * tracing window, where {@code Span.current()} provably resolves the real span.
 *
 * <p>Two actions, both inside the window:
 *
 * <ul>
 *   <li>stow the span on the duplicated-context carrier ({@link
 *       ResolvedCallContexts#storeSpanOnDuplicatedContext}) so {@code BaseServiceImpl} can graft it
 *       into service-method bodies and Mutiny pipelines;
 *   <li>apply the per-request identity attributes (query id, correlation id, principal, engine)
 *       directly onto that span.
 * </ul>
 *
 * <p>When tracing is effectively enabled but no valid span is current here on an application RPC,
 * that is an interceptor-ordering regression (this interceptor fell outside the window), and it is
 * logged at WARN — never silently — because it collapses every per-request decoration back to a
 * no-op. A missing span stays a quiet DEBUG when tracing is off (any of the three OTel switches) or
 * on infrastructure methods Quarkus may legitimately leave untraced.
 *
 * <p>The identity values come from the {@code io.grpc.Context} keys the inbound context interceptor
 * populated: it is far outer to this one, and {@code Contexts.interceptCall} keeps its context
 * attached through the inner chain build.
 */
@ApplicationScoped
@GlobalInterceptor
public final class SpanCaptureInterceptor implements ServerInterceptor, Prioritized {

  private static final Logger LOG = Logger.getLogger(SpanCaptureInterceptor.class);

  /**
   * Whether OpenTelemetry tracing is effectively active — the OTel master switch AND traces AND the
   * SDK not runtime-disabled. When it is, Quarkus's default {@code parentbased_always_on} sampler
   * yields a valid span context for EVERY call at an in-window capture, so a missing span here is
   * never a sampling artifact — it is an ordering regression, and is logged loudly. When any of the
   * three switches turns tracing off the SDK no-ops and a missing span is expected, so it stays
   * quiet: gating on {@code traces.enabled} alone would spam the regression WARN on every RPC when
   * an operator disables telemetry via the master switch while leaving {@code traces.enabled} at
   * its default.
   */
  private final boolean tracingEnabled;

  /**
   * Process-wide count of application RPCs that hit the no-valid-span regression branch. Under a
   * real ordering regression this branch is taken on EVERY call, so the WARN is throttled to the
   * first occurrence and each decade milestone ({@link #isFirstOrDecadeMilestone}) — the count
   * rides the log line so the regression stays countable without one WARN per request flooding the
   * log at production QPS.
   */
  private final AtomicLong missingSpanRegressions = new AtomicLong();

  @Inject
  public SpanCaptureInterceptor(
      @ConfigProperty(name = "quarkus.otel.enabled", defaultValue = "true") boolean otelEnabled,
      @ConfigProperty(name = "quarkus.otel.traces.enabled", defaultValue = "true")
          boolean tracesEnabled,
      @ConfigProperty(name = "quarkus.otel.sdk.disabled", defaultValue = "false")
          boolean sdkDisabled) {
    this.tracingEnabled = otelEnabled && tracesEnabled && !sdkDisabled;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    Span span = Span.current();
    if (span.getSpanContext().isValid()) {
      ResolvedCallContexts.storeSpanOnDuplicatedContext(span);
      applyCallAttributes(span);
    } else if (tracingEnabled
        && !InboundContextInterceptor.isInfrastructureMethod(
            call.getMethodDescriptor().getFullMethodName())) {
      // Tracing is on and this is an application RPC, so an in-window capture must see a valid span
      // (always_on gives one to every such call). Its absence means this interceptor no longer
      // sorts inside the tracing interceptor's makeCurrent() window — an ordering regression (e.g.
      // a
      // Quarkus/OTel bump changing that interceptor's priority). Loud and countable, because every
      // per-request span decoration silently no-ops when it happens; a DEBUG line (off in prod)
      // would hide the regression. Health probes and reflection are excluded: Quarkus may not open
      // a span for them, and they are polled on a tight interval, so a WARN there is pure noise
      // pointing at a regression that isn't real. Throttled to first occurrence + decade
      // milestones: this branch is taken on EVERY call under a real regression, so an unthrottled
      // per-call WARN would itself become a production log flood.
      long n = missingSpanRegressions.incrementAndGet();
      if (isFirstOrDecadeMilestone(n)) {
        LOG.warnf(
            "SpanCaptureInterceptor found no valid server span while tracing is enabled"
                + " (occurrence #%d, correlation_id=%s): it is no longer inside the tracing"
                + " interceptor's window, so every per-request span decoration will no-op. Check"
                + " GrpcInterceptorPriorities against Quarkus's gRPC interceptor ordering.",
            n, MDC.get("correlation_id"));
      }
    } else {
      LOG.debugf(
          "no server span to capture (correlation_id=%s): tracing disabled or an untraced"
              + " infrastructure method",
          MDC.get("correlation_id"));
    }
    return next.startCall(call, headers);
  }

  /**
   * Per-request identity attributes, from the inbound interceptor's {@code io.grpc.Context}. The
   * floecat-specific keys use the dotted {@code floecat.*} span-attribute namespace shared by every
   * other decoration on this span (the telemetry delegate's {@code floecat.component} /{@code
   * floecat.rpc.status}, the handler bodies' {@code floecat.get_user_objects.*}) so per-account and
   * per-engine trace queries group with the rest. {@code query_id}/{@code correlation_id} keep
   * their flat names — they mirror the MDC log keys and any dashboards already keyed on them. (The
   * MDC log fields, a separate namespace, stay underscore-cased.)
   */
  private static void applyCallAttributes(Span span) {
    String queryId = InboundContextInterceptor.QUERY_KEY.get();
    if (queryId != null && !queryId.isBlank()) {
      span.setAttribute("query_id", queryId);
    }
    String correlationId = InboundContextInterceptor.CORR_KEY.get();
    if (correlationId != null && !correlationId.isBlank()) {
      span.setAttribute("correlation_id", correlationId);
    }
    PrincipalContext principalContext = InboundContextInterceptor.PC_KEY.get();
    if (principalContext != null) {
      span.setAttribute("floecat.account_id", principalContext.getAccountId());
      span.setAttribute("floecat.subject", principalContext.getSubject());
    }
    String engineVersion = InboundContextInterceptor.ENGINE_VERSION_KEY.get();
    if (engineVersion != null && !engineVersion.isBlank()) {
      span.setAttribute("floecat.engine.version", engineVersion);
    }
    String engineKind = InboundContextInterceptor.ENGINE_KIND_KEY.get();
    if (engineKind != null && !engineKind.isBlank()) {
      span.setAttribute("floecat.engine.kind", engineKind);
    }
  }

  /**
   * Whether the {@code n}-th regression occurrence should be logged: the first, then each power of
   * ten (10, 100, 1000, …). This taps the WARN to at most ~one line per decade of volume — loud on
   * the first hit, still periodically visible under a sustained regression, never a per-call flood.
   */
  static boolean isFirstOrDecadeMilestone(long n) {
    if (n < 1) {
      return false;
    }
    while (n % 10 == 0) {
      n /= 10;
    }
    return n == 1;
  }

  /** Below zero — must stay inner to the priority-0 tracing interceptor; see the class doc. */
  @Override
  public int getPriority() {
    return GrpcInterceptorPriorities.SPAN_CAPTURE;
  }
}
