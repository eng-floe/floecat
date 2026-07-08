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

  public static final int TELEMETRY = 300;

  public static final int RPC_LOGGING = 200;

  /** Innermost: localizes error payloads before the outer logging interceptor observes them. */
  public static final int LOCALIZE_ERRORS = 100;

  private GrpcInterceptorPriorities() {}
}
