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

package ai.floedb.floecat.service.common;

/**
 * Priorities for floecat's global gRPC server interceptors.
 *
 * <p>Quarkus sorts global interceptors ascending by the CDI {@link
 * jakarta.enterprise.inject.spi.Prioritized} <em>interface</em> and hands the sorted list to {@code
 * ServerInterceptors.intercept}, which makes the <b>last</b> element the <b>outermost</b>
 * interceptor — so a higher priority value means running earlier on call entry. {@code
 * jakarta.annotation.Priority} annotations are silently ignored by that comparator; before these
 * constants existed our three interceptors all tied at 0 with nondeterministic relative order.
 *
 * <p>Quarkus's built-in interceptors sit near {@code Integer.MAX_VALUE} ({@code
 * io.quarkus.grpc.runtime.Interceptors}) and deterministically stay outermost of all; these values
 * only order floecat's own interceptors below them.
 */
public final class GrpcInterceptorPriorities {

  /**
   * Outermost floecat interceptor: resolves the call context and populates {@code io.grpc.Context},
   * MDC, and the duplicated-context carrier before telemetry and logging read them — and clears MDC
   * after they are done (outer close runs last).
   */
  public static final int INBOUND_CONTEXT = 1_000_000;

  public static final int RPC_LOGGING = 200;

  /** Localizes error payloads before the outer logging interceptor observes them. */
  public static final int LOCALIZE_ERRORS = 100;

  /**
   * Telemetry runs INNER of Quarkus's tracing interceptor — deliberately below 0. Quarkus's {@code
   * GrpcTracingServerInterceptor} implements no {@code Prioritized}, so the comparator gives it 0;
   * it creates the call's server span and makes it current ONLY inside its own {@code
   * interceptCall} window, never re-attaching around listener events. Telemetry's delegate ({@code
   * GrpcTelemetryServerInterceptor}) captures {@code Span.current()} AND builds its {@code
   * PhaseDiagnostics} at {@code interceptCall}, so anything above 0 captures the invalid root span
   * and a {@code NOOP} diagnostics — and then emits neither {@code floecat.rpc.status} nor the
   * {@code floecat.rpc.summary} event. Sorting below 0 runs its chain build inside the window; it
   * captures the span synchronously there, so its later {@code finish()} (on call close, outside
   * the window) still holds a valid captured span.
   */
  public static final int TELEMETRY = -500;

  /**
   * Innermost of ALL interceptors — below {@link #TELEMETRY}. Captures the server span (valid only
   * here, inside the tracing window; see {@link #TELEMETRY}) onto the per-call carrier and applies
   * the per-request identity attributes. Ordering relative to {@link #TELEMETRY} is immaterial —
   * both sit inside the window and touch the span independently.
   */
  public static final int SPAN_CAPTURE = -1_000;

  private GrpcInterceptorPriorities() {}
}
