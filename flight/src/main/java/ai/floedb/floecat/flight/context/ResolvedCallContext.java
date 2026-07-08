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

package ai.floedb.floecat.flight.context;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.scanner.utils.EngineContext;

/** Fully resolved per-call context shared by Flight producers and middleware. */
public record ResolvedCallContext(
    PrincipalContext principalContext,
    String queryId,
    String correlationId,
    EngineContext engineContext,
    String sessionHeaderValue,
    String authorizationHeaderValue) {

  public static ResolvedCallContext unauthenticated() {
    return new ResolvedCallContext(
        PrincipalContext.getDefaultInstance(), "", "", EngineContext.empty(), null, null);
  }

  /**
   * The correlation id service bodies should report: the resolved one, falling back to the id
   * stamped on the principal. One definition so the fallback cannot drift between call sites again
   * (it did once, pre-#361: one site fell back to a grpc-context key instead).
   */
  public String effectiveCorrelationId() {
    if (correlationId != null && !correlationId.isBlank()) {
      return correlationId;
    }
    return principalContext.getCorrelationId();
  }
}
