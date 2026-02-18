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
package ai.floedb.floecat.reconciler.spi;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable context passed to reconciler backends. Keeps metadata explicit instead of hidden in
 * gRPC headers.
 */
public final class ReconcileContext {
  private final String correlationId;
  private final PrincipalContext principal;
  private final String source;
  private final Instant now;
  private final Optional<String> authorizationToken;

  public ReconcileContext(
      String correlationId,
      PrincipalContext principal,
      String source,
      Instant now,
      Optional<String> authorizationToken) {
    this.correlationId = Objects.requireNonNull(correlationId, "correlationId");
    this.principal = Objects.requireNonNull(principal, "principal");
    this.source = Objects.requireNonNull(source, "source");
    this.now = Objects.requireNonNull(now, "now");
    this.authorizationToken =
        Objects.requireNonNull(authorizationToken, "authorizationToken")
            .map(String::trim)
            .filter(v -> !v.isBlank());
  }

  public String correlationId() {
    return correlationId;
  }

  public PrincipalContext principal() {
    return principal;
  }

  public String source() {
    return source;
  }

  public Instant now() {
    return now;
  }

  public Optional<String> authorizationToken() {
    return authorizationToken;
  }
}
