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

package ai.floedb.floecat.service.credentials;

import ai.floedb.floecat.connector.spi.AuthResolutionContext;
import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.service.context.impl.InboundContextInterceptor;
import ai.floedb.floecat.service.context.impl.ResolvedCallContexts;

public final class AuthResolutionContexts {
  private AuthResolutionContexts() {}

  /**
   * The current call's connector-auth tokens.
   *
   * <p>Prefers the resolved call context over the {@code io.grpc.Context} keys. Both carry the same
   * two header values — the inbound interceptor reads the headers once and populates both — but the
   * gRPC channel is the unreliable one: under Quarkus its attach slot is a shared last-writer-wins
   * slot that threads of the same call race, so it can read back empty, or stale from a reused
   * worker, while the resolved context still answers correctly (eng-floe/floecat#361). Reading the
   * gRPC keys first is therefore how a body carried across an executor hop ends up resolving
   * connector credentials with blank or another request's tokens while running as its own request.
   *
   * <p>A present resolved context is authoritative, including when a token is {@code null}: every
   * production construction populates these fields from the headers, so {@code null} means the
   * request did not carry that header rather than "not filled in". The gRPC keys are consulted only
   * when no resolved context exists at all — non-Vert.x threads and callers that drive {@code
   * io.grpc.Context} directly.
   */
  public static AuthResolutionContext fromInboundContext() {
    ResolvedCallContext resolved = ResolvedCallContexts.currentOrNull();
    if (resolved != null) {
      return new AuthResolutionContext(
          blankIfAbsent(resolved.sessionHeaderValue()),
          blankIfAbsent(resolved.authorizationHeaderValue()));
    }
    return new AuthResolutionContext(
        blankIfAbsent(InboundContextInterceptor.SESSION_HEADER_VALUE_KEY.get()),
        blankIfAbsent(InboundContextInterceptor.AUTHORIZATION_HEADER_VALUE_KEY.get()));
  }

  private static String blankIfAbsent(String headerValue) {
    return headerValue == null ? "" : headerValue;
  }
}
