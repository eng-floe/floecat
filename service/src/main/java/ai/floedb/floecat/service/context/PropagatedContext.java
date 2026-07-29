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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * <p>{@link #capture()} on the originating thread, {@link #supply}/{@link #run} on the worker
 * thread. The {@link ResolvedCallContext} is the single source of truth: engine, principal,
 * correlation and the log MDC all derive from it. Add a new carrier here once, and every dispatch
 * point that already propagates through this snapshot gets it for free.
 */
public final class PropagatedContext {

  private static final List<String> MDC_KEYS =
      List.of(
          "floecat_component",
          "floecat_operation",
          "query_id",
          "correlation_id",
          "floecat_account_id",
          "floecat_subject",
          "floecat_engine_kind",
          "floecat_engine_version");

  private final Context otel;
  private final ResolvedCallContext call; // null off any request (e.g. unit tests, startup)

  private PropagatedContext(Context otel, ResolvedCallContext call) {
    this.otel = otel;
    this.call = call;
  }

  /** Snapshot the calling thread's ambient context. Call on the request/driver thread. */
  public static PropagatedContext capture() {
    return new PropagatedContext(Context.current(), ResolvedCallContexts.currentOrNull());
  }

  /**
   * Run {@code body} on the current thread with the captured context re-established for its
   * duration and torn down afterward, so its ambient reads behave as they did on the capturing
   * thread. Checked exceptions from {@code body} propagate as {@link RuntimeException} (via {@link
   * ResolvedCallContexts#callWith}).
   */
  public <T> T supply(Supplier<T> body) {
    try (Scope ignored = otel.makeCurrent()) {
      if (call == null) {
        return body.get();
      }
      return ResolvedCallContexts.callWith(
          call,
          () -> {
            // MDC is a projection of the call context; re-derive it so off-thread log lines carry
            // the request's ids, then restore the worker's prior values for nested scopes and
            // executors that already propagate MDC.
            Map<String, Object> priorMdc = snapshotMdc();
            InboundContextInterceptor.populateMdc(call);
            try {
              return body.get();
            } finally {
              restoreMdc(priorMdc);
            }
          });
    }
  }

  /** {@link #supply} for a body with no return value. */
  public void run(Runnable body) {
    supply(
        () -> {
          body.run();
          return null;
        });
  }

  private static Map<String, Object> snapshotMdc() {
    Map<String, Object> values = new HashMap<>();
    for (String key : MDC_KEYS) {
      Object value = MDC.get(key);
      if (value != null) {
        values.put(key, value);
      }
    }
    return values;
  }

  private static void restoreMdc(Map<String, Object> priorMdc) {
    for (String key : MDC_KEYS) {
      Object value = priorMdc.get(key);
      if (value == null) {
        MDC.remove(key);
      } else {
        MDC.put(key, value);
      }
    }
  }
}
