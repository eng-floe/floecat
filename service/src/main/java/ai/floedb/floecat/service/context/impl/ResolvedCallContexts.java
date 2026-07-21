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

package ai.floedb.floecat.service.context.impl;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import io.opentelemetry.api.trace.Span;
import io.smallrye.common.vertx.VertxContext;
import io.vertx.core.Vertx;
import java.util.Objects;
import java.util.concurrent.Callable;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

/**
 * Write-once carrier for the fully resolved inbound call context (principal, correlation id, engine
 * context, query id, session/authorization header values).
 *
 * <p>The resolved context is carried on three channels, consulted in order of reliability:
 *
 * <ol>
 *   <li><b>Explicit scope</b> ({@link #callWith}/{@link #runWith}) — a thread-confined override for
 *       code that was handed the context by reference across an executor hop. This is the only
 *       channel that works on arbitrary executor threads.
 *   <li><b>Vert.x duplicated-context local</b> ({@link #storeOnDuplicatedContext}) — written once
 *       by the inbound interceptor on the same per-call duplicated context that carries MDC. It
 *       survives Quarkus's gRPC dispatch worker hops and Mutiny's contextualized callbacks for
 *       exactly the same reason MDC does, and cannot bleed between concurrent calls.
 *   <li><b>{@code io.grpc.Context} keys</b> — the legacy channel. Under Quarkus, attach/detach on a
 *       duplicated context is a read/write of one shared last-writer-wins slot that several threads
 *       of the same call race; keys can read back empty (or stale, via the thread-local fallback on
 *       a reused worker) on a subset of calls. Kept as a fallback for callers that drive {@code
 *       io.grpc.Context} directly (unit tests, non-Vert.x threads).
 * </ol>
 *
 * <p>History: mirroring only the principal onto the duplicated context moved the context-loss
 * failure from a loud {@code PERMISSION_DENIED} into a silent engine-gated {@code NOT_FOUND}
 * (eng-floe/floecat#361). Carrying the whole {@link ResolvedCallContext} on one channel keeps the
 * principal, correlation id, and engine context from ever diverging again.
 */
public final class ResolvedCallContexts {

  private static final Logger LOG = Logger.getLogger(ResolvedCallContexts.class);

  /** Vert.x duplicated-context local key under which the resolved call context is stored. */
  private static final String CONTEXT_LOCAL = "floecat.resolved-call-context";

  /**
   * Vert.x duplicated-context local key under which the call's gRPC server span is stored. The span
   * is created by Quarkus's {@code GrpcTracingServerInterceptor} — which carries no {@code
   * Prioritized} priority and therefore sorts to 0, the INNERMOST slot of the chain — and is only
   * current inside that interceptor's {@code makeCurrent()} window. No floecat interceptor or
   * handler thread ever has it current, so every {@code Span.current()} decoration silently no-ops.
   * {@code SpanCaptureInterceptor} (priority below 0, inner to the tracing interceptor) captures
   * the span inside that window and stows it here — the same per-call channel MDC and the resolved
   * call context already ride — so decoration code anywhere in the call can reach it.
   */
  private static final String SPAN_LOCAL = "floecat.call-span";

  private static final ThreadLocal<ResolvedCallContext> SCOPE = new ThreadLocal<>();

  private ResolvedCallContexts() {}

  /**
   * Stores the resolved call context on the current Vert.x duplicated context so it survives the
   * gRPC dispatch worker thread-hop into the service-method body. Called once per call from the
   * inbound interceptor, alongside MDC population, on the same context MDC is written to.
   *
   * <p>No-op when the current call is not on a Vert.x duplicated context (the {@code
   * io.grpc.Context} keys remain the carrier in that case), so it is safe to call unconditionally.
   * The local is write-once per call: overwriting a different previously stored context is a
   * plumbing bug and is logged loudly rather than silently masked.
   */
  public static void storeOnDuplicatedContext(ResolvedCallContext resolved) {
    Objects.requireNonNull(resolved, "resolved");
    io.vertx.core.Context ctx = Vertx.currentContext();
    if (ctx == null || !VertxContext.isDuplicatedContext(ctx)) {
      return;
    }
    Object existing = ctx.getLocal(CONTEXT_LOCAL);
    if (existing != null && !existing.equals(resolved)) {
      LOG.warnf(
          "Overwriting a different resolved call context on the same duplicated context"
              + " (existing correlation_id=%s, new correlation_id=%s) — duplicated contexts are"
              + " per-call and this indicates a context-propagation bug",
          existing instanceof ResolvedCallContext previous ? previous.correlationId() : "?",
          resolved.correlationId());
    }
    ctx.putLocal(CONTEXT_LOCAL, resolved);
  }

  /**
   * Stores the call's gRPC server span on the current Vert.x duplicated context so decoration code
   * outside the tracing interceptor's {@code makeCurrent()} window (every floecat interceptor,
   * service handlers, Mutiny bodies) can reach it. Called once per call by {@code
   * SpanCaptureInterceptor}, the only floecat code that runs inside that window.
   *
   * <p>A pure store: no-op when off a duplicated context or when no valid span is current. The
   * loud-vs-quiet judgement for a missing span (ordering regression vs tracing disabled) belongs to
   * {@code SpanCaptureInterceptor}, which knows whether tracing is enabled; this carrier stays a
   * dumb sink so both callers and tests can invoke it without a config dependency.
   */
  public static void storeSpanOnDuplicatedContext(Span span) {
    io.vertx.core.Context ctx = Vertx.currentContext();
    if (ctx == null || !VertxContext.isDuplicatedContext(ctx)) {
      return;
    }
    if (span == null || !span.getSpanContext().isValid()) {
      return;
    }
    ctx.putLocal(SPAN_LOCAL, span);
  }

  /**
   * The gRPC server span carried on the current duplicated context, or {@link Span#getInvalid()}
   * when none is present (off a duplicated context, or no span was captured for this call).
   */
  public static Span currentCallSpanOrInvalid() {
    io.vertx.core.Context ctx = Vertx.currentContext();
    if (ctx != null && VertxContext.isDuplicatedContext(ctx)) {
      Object value = ctx.getLocal(SPAN_LOCAL);
      if (value instanceof Span span) {
        return span;
      }
    }
    return Span.getInvalid();
  }

  /**
   * Returns the resolved call context for the current thread, or {@code null} when no channel
   * carries one. Channels are consulted in reliability order: explicit scope, duplicated-context
   * local, {@code io.grpc.Context} keys.
   */
  public static ResolvedCallContext currentOrNull() {
    ResolvedCallContext scoped = SCOPE.get();
    if (scoped != null) {
      return scoped;
    }
    ResolvedCallContext fromDuplicatedContext = fromDuplicatedContext();
    if (fromDuplicatedContext != null) {
      return fromDuplicatedContext;
    }
    ResolvedCallContext fromKeys = fromGrpcContextKeys();
    if (fromKeys == null) {
      warnOnChannelDisagreement(
          "correlation_id",
          "no channel (scope, duplicated-context local, io.grpc.Context) carries a resolved call"
              + " context");
    }
    return fromKeys;
  }

  /**
   * Channel-disagreement detector shared by every carrier-backed reader: a populated MDC key proves
   * the inbound interceptor resolved this request on this very call path (MDC and the carrier are
   * written together), so a miss on the actual context channels is a propagation loss — the failure
   * mode of eng-floe/floecat#361, turned from a silent wrong answer into a countable signal. This
   * should never fire; any occurrence is a regression in context propagation.
   *
   * @param mdcKey the MDC key whose presence proves the request declared the lost value
   * @param missingWhat what the context channels failed to deliver, for the log message
   */
  public static void warnOnChannelDisagreement(String mdcKey, String missingWhat) {
    Object mdcValue = MDC.get(mdcKey);
    if (mdcValue instanceof String value && !value.isBlank()) {
      LOG.warnf(
          "call-context channels disagree: MDC %s=%s is populated but %s — the call context was"
              + " lost on the way to this thread (correlation_id=%s)",
          mdcKey, value, missingWhat, MDC.get("correlation_id"));
    }
  }

  /** Like {@link #currentOrNull()} but degrades to {@link ResolvedCallContext#unauthenticated}. */
  public static ResolvedCallContext currentOrUnauthenticated() {
    ResolvedCallContext current = currentOrNull();
    return current != null ? current : ResolvedCallContext.unauthenticated();
  }

  /**
   * Runs {@code body} with {@code resolved} as the current thread's call context. Use to carry the
   * context by reference across an executor hop: read the context once before the hop, then wrap
   * the hopped body. Passing {@code null} is allowed and runs the body unscoped.
   */
  public static void runWith(ResolvedCallContext resolved, Runnable body) {
    Objects.requireNonNull(body, "body");
    ResolvedCallContext previous = SCOPE.get();
    SCOPE.set(resolved);
    try {
      body.run();
    } finally {
      restore(previous);
    }
  }

  /**
   * Calls {@code body} with {@code resolved} as the current thread's call context and returns the
   * result. Checked exceptions are wrapped in a {@link RuntimeException}.
   */
  public static <T> T callWith(ResolvedCallContext resolved, Callable<T> body) {
    Objects.requireNonNull(body, "body");
    ResolvedCallContext previous = SCOPE.get();
    SCOPE.set(resolved);
    try {
      return body.call();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      restore(previous);
    }
  }

  private static void restore(ResolvedCallContext previous) {
    if (previous == null) {
      SCOPE.remove();
    } else {
      SCOPE.set(previous);
    }
  }

  private static ResolvedCallContext fromDuplicatedContext() {
    io.vertx.core.Context ctx = Vertx.currentContext();
    if (ctx != null && VertxContext.isDuplicatedContext(ctx)) {
      Object value = ctx.getLocal(CONTEXT_LOCAL);
      if (value instanceof ResolvedCallContext resolved) {
        return resolved;
      }
    }
    return null;
  }

  private static ResolvedCallContext fromGrpcContextKeys() {
    PrincipalContext principal = InboundContextInterceptor.PC_KEY.get();
    if (principal == null) {
      return null;
    }
    String queryId = InboundContextInterceptor.QUERY_KEY.get();
    String correlationId = InboundContextInterceptor.CORR_KEY.get();
    EngineContext engineContext = InboundContextInterceptor.ENGINE_CONTEXT_KEY.get();
    String sessionHeaderValue = InboundContextInterceptor.SESSION_HEADER_VALUE_KEY.get();
    String authorizationHeaderValue =
        InboundContextInterceptor.AUTHORIZATION_HEADER_VALUE_KEY.get();
    return new ResolvedCallContext(
        principal,
        queryId != null ? queryId : "",
        correlationId != null ? correlationId : "",
        engineContext != null ? engineContext : EngineContext.empty(),
        sessionHeaderValue,
        authorizationHeaderValue);
  }
}
