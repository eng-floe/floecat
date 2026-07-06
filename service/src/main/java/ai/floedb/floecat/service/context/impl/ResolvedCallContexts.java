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
      warnIfMdcDisagrees();
    }
    return fromKeys;
  }

  /**
   * Channel-disagreement detector: MDC carrying a correlation id proves this thread is inside a
   * resolved request, so a miss on every context channel is a propagation loss — the failure mode
   * of eng-floe/floecat#361, turned from a silent wrong answer into a countable signal. This should
   * never fire; any occurrence is a regression in context propagation.
   */
  private static void warnIfMdcDisagrees() {
    Object mdcCorrelationId = MDC.get("correlation_id");
    if (mdcCorrelationId instanceof String correlationId && !correlationId.isBlank()) {
      LOG.warnf(
          "call-context channels disagree: MDC correlation_id=%s is populated but no channel"
              + " (scope, duplicated-context local, io.grpc.Context) carries a resolved call"
              + " context — the call context was lost on the way to this thread",
          correlationId);
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
