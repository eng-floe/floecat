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
package ai.floedb.floecat.service.context;

import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.service.context.impl.InboundContextInterceptor;
import ai.floedb.floecat.service.context.impl.ResolvedCallContexts;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.smallrye.common.vertx.VertxContext;
import io.vertx.core.Vertx;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.jboss.logging.MDC;

/**
 * An immutable snapshot of a thread's ambient request context, for re-establishing it on another
 * thread. Request context does not follow a task across a thread hop on its own — it lives in
 * thread-locals (OpenTelemetry's {@link Context}, the resolved call context's scope) — so work
 * dispatched to an executor loses it, and ambient reads ({@code engineContext()}, principal,
 * correlation, log MDC) silently read empty off-thread. That is how engine-gated system objects
 * become unresolvable across a fan-out (eng-floe/floecat#361).
 *
 * <p>{@link #capture()} on the originating thread, {@link #supply} on the worker thread. Four
 * carriers travel together: the OpenTelemetry context (with the call's server span grafted on), the
 * caller's live {@code io.grpc.Context}, the {@link ResolvedCallContext}, and the log MDC. The
 * {@code ResolvedCallContext} is the authoritative channel for engine, principal and correlation;
 * the gRPC context still rides along because several readers consult only its keys
 * (session/authorization tokens, and the principal/engine fallbacks), and because the inbound
 * deadline lives there. Centralizing carriers here keeps every dispatch point that uses this
 * snapshot on the same context set.
 *
 * <p>"Snapshot" describes the captured VALUES, not isolation from the request's fate. The gRPC
 * carrier is a live reference on purpose: a later client cancellation or deadline expiry is visible
 * to a body already running, which is what lets abandoned fan-out work stop instead of running on
 * after the request is gone. Do not read this class as detaching the body from its request.
 *
 * <p><b>Relationship to {@code flight}'s {@code ContextSnapshot}:</b> the two do the same job for
 * different dispatch points — {@code ContextSnapshot} serves the Flight producers, this serves
 * service-layer executor dispatch — and both carry the gRPC context rather than detaching it. They
 * differ deliberately on one point: {@code ContextSnapshot} calls {@code fork()}, which drops the
 * cancellable ancestor, so its bodies have no deadline and never observe cancellation; this class
 * carries the live context so a fan-out unit — work that belongs to the request and should die with
 * it — still sees the inbound deadline and client cancellation. This one also adds the {@link
 * ResolvedCallContext} scope (the channel that survives Quarkus's gRPC worker hops, see
 * eng-floe/floecat#361) and the shared-carrier guard below. Prefer this class for service-layer
 * dispatch, and change them together rather than letting a third behavior appear.
 */
public final class PropagatedContext {

  // The request's cancellation signal, carried here so it follows work across a thread hop the same
  // way OTel/MDC do. Auto-admitted store reads (MetadataIoAdmissionInterceptor) read it to abort an
  // admission wait when the request cancels, even on a fan-out worker off the request thread.
  private static final ThreadLocal<BooleanSupplier> REQUEST_CANCELLATION = new ThreadLocal<>();

  private final Context otel;
  private final io.grpc.Context grpc;
  private final ResolvedCallContext call; // null off any request (e.g. unit tests, startup)
  private final Map<String, Object> sourceMdc;
  private final BooleanSupplier cancellation; // null off any cancellable request

  private PropagatedContext(
      Context otel,
      io.grpc.Context grpc,
      ResolvedCallContext call,
      Map<String, Object> sourceMdc,
      BooleanSupplier cancellation) {
    this.otel = otel;
    this.grpc = grpc;
    this.call = call;
    // Copy defensively: this snapshot is read concurrently by every worker running supply(), so the
    // immutability the class promises has to hold at the boundary, not by convention. A later line
    // stamping a per-task key would otherwise mutate a map being iterated on N threads. Null values
    // are tolerated rather than fatal — Quarkus/SmallRye and OTel instrumentation write to the MDC
    // too and can leave nulls, and Map.copyOf would turn that into an NPE on the request thread,
    // failing the whole request over a logging concern (ContextSnapshot guards the same way).
    this.sourceMdc = Collections.unmodifiableMap(new HashMap<>(sourceMdc));
    this.cancellation = cancellation;
  }

  /** Snapshot the calling thread's ambient context. Call on the request/driver thread. */
  public static PropagatedContext capture() {
    return new PropagatedContext(
        ResolvedCallContexts.withCurrentCallSpan(Context.current()),
        // The caller's LIVE gRPC context, deliberately not fork(): fork() builds a context with no
        // cancellableAncestor, so getDeadline() returns null and isCancelled() is permanently false
        // — the body would silently lose the inbound deadline and never observe client
        // cancellation.
        // Carrying the live context (rather than leaving the worker's own) is also what isolates
        // the
        // body: readers that consult only gRPC keys — AuthResolutionContexts' session/authorization
        // tokens, PrincipalProvider/EngineContextProvider's fallbacks — see THIS request's values,
        // and a body captured off-request carries an empty context instead of a foreign one.
        io.grpc.Context.current(),
        ResolvedCallContexts.currentOrNull(),
        snapshotMdc(),
        REQUEST_CANCELLATION.get());
  }

  /**
   * Bind {@code cancelled} as the current thread's request cancellation signal until the returned
   * scope is closed. {@link #capture()} carries it to worker threads, so a store read auto-admitted
   * off the request thread can still abort its admission wait when the request cancels. Bind at the
   * request/stream boundary and close it (try-with-resources) when production ends.
   */
  public static CancellationScope bindCancellation(BooleanSupplier cancelled) {
    BooleanSupplier prior = REQUEST_CANCELLATION.get();
    REQUEST_CANCELLATION.set(cancelled);
    return () -> restoreCancellation(prior);
  }

  /** The calling thread's request cancellation signal, or {@code null} when none is bound. */
  public static BooleanSupplier currentCancellation() {
    return REQUEST_CANCELLATION.get();
  }

  private static void restoreCancellation(BooleanSupplier prior) {
    if (prior == null) {
      REQUEST_CANCELLATION.remove();
    } else {
      REQUEST_CANCELLATION.set(prior);
    }
  }

  /** A thread-bound cancellation binding; closing it restores the prior signal. Never throws. */
  public interface CancellationScope extends AutoCloseable {
    @Override
    void close();
  }

  /**
   * Run {@code body} on the current thread with the captured context re-established for its
   * duration and torn down afterward, so its ambient reads behave as they did on the capturing
   * thread.
   */
  public <T> T supply(Supplier<T> body) {
    // Refuse to run where the carriers are not ours: see requireThreadConfinedCarriers.
    requireThreadConfinedCarriers();
    try (Scope ignored = otel.makeCurrent()) {
      io.grpc.Context priorGrpc = null;
      Map<String, Object> priorMdc = null;
      boolean attached = false;
      BooleanSupplier priorCancellation = REQUEST_CANCELLATION.get();
      try {
        // Install the captured gRPC context for the body's duration, replacing whatever the worker
        // carries, so the readers that consult only its keys see this request. Inside the try: an
        // exception between attach and the body would otherwise skip the detach in the finally and
        // strand this request's context on a pooled worker for every later task it runs.
        priorGrpc = grpc.attach();
        attached = true;
        priorMdc = snapshotMdc();
        replaceMdc(sourceMdc);
        // What an auto-admitted read on this worker polls to abort its admission wait when the
        // request cancels.
        restoreCancellation(cancellation);
        if (call == null) {
          // Captured off-request: isolate the body from any foreign request the worker still
          // carries
          // through a fallback carrier (duplicated-context local, io.grpc.Context keys), rather
          // than
          // callWith(null, ...) which would let those carriers show through.
          return ResolvedCallContexts.callWithoutRequestScope(body::get);
        }
        return ResolvedCallContexts.callWith(
            call,
            () -> {
              InboundContextInterceptor.populateMdc(call);
              return body.get();
            });
      } finally {
        // Nested so each restore runs even if an earlier one throws: otherwise this request's
        // gRPC context or cancellation signal stays on a pooled worker and every later task on it
        // reads our identity.
        try {
          if (priorMdc != null) {
            replaceMdc(priorMdc);
          }
        } finally {
          try {
            restoreCancellation(priorCancellation);
          } finally {
            if (attached) {
              grpc.detach(priorGrpc);
            }
          }
        }
      }
    }
  }

  /**
   * Refuse to run where this thread's carriers are shared rather than ours. Under Quarkus a
   * duplicated Vert.x context backs both the MDC map and the {@code io.grpc.Context} attach slot,
   * so on such a context every concurrent task on it shares them.
   *
   * <p>This throws rather than degrading, because the degradation is not a logging concern.
   * Skipping the MDC write alone would only mis-attribute log lines; but the gRPC context cannot be
   * attached there either (its slot is last-writer-wins across overlapping tasks), and then the
   * body runs with the captured {@link ResolvedCallContext}'s principal and engine while the
   * readers that consult only gRPC keys — {@code AuthResolutionContexts}' session/authorization
   * tokens — see whatever ambient request that thread carries. A body running under two requests'
   * identities at once is a security failure, and a failed task is the better outcome.
   *
   * <p>This fires only for a context-propagating executor (a Quarkus {@code ManagedExecutor}, say)
   * — a wiring mistake, and one whose message names the fix. Dispatching to a plain virtual- or
   * platform-thread executor never trips it.
   */
  private static void requireThreadConfinedCarriers() {
    io.vertx.core.Context ctx = Vertx.currentContext();
    if (ctx == null || !VertxContext.isDuplicatedContext(ctx)) {
      return;
    }
    throw new IllegalStateException(
        "PropagatedContext.supply must run on a thread that owns its MDC and gRPC context, but a"
            + " Vert.x duplicated context is current: both carriers are shared by every task on it,"
            + " so propagating would corrupt their MDC and could run this body under a mix of two"
            + " requests' identities. Dispatch this work to a virtual-/platform-thread executor that"
            + " does not propagate the Vert.x context.");
  }

  /** Copy the complete MDC so extension-defined keys participate in isolation and restoration. */
  private static Map<String, Object> snapshotMdc() {
    Map<String, Object> values = MDC.getMap();
    return values == null ? new HashMap<>() : new HashMap<>(values);
  }

  /**
   * Replace, rather than merge, MDC state so pooled workers cannot leak foreign request keys.
   *
   * <p>Requires a thread-confined MDC store. Under Quarkus the MDC keys off the current Vert.x
   * context and only falls back to a thread-local when there is none — so the executor must NOT be
   * a context-propagating one (e.g. a Quarkus {@code ManagedExecutor} sharing a Vert.x context),
   * where {@link MDC#clear()} would wipe state visible to every concurrent task on that context. A
   * plain virtual- or platform-thread executor carries no Vert.x context, so the store is
   * thread-local there; callers must dispatch to one of those.
   */
  private static void replaceMdc(Map<String, Object> values) {
    MDC.clear();
    for (Map.Entry<String, Object> entry : values.entrySet()) {
      // Skip nulls: Quarkus's VertxMDC.putObject does requireNonNull on the value, so a null
      // written
      // by other instrumentation would throw here — at restore time — defeating the null-tolerant
      // copy taken at capture.
      if (entry.getValue() != null) {
        MDC.put(entry.getKey(), entry.getValue());
      }
    }
  }
}
