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

package ai.floedb.floecat.service.query.flight;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.context.impl.InboundContextInterceptor;

/** Resolves Floecat Flight call context from the current shared gRPC request context. */
final class GrpcResolvedCallContexts {

  private GrpcResolvedCallContexts() {}

  static ResolvedCallContext currentOrUnauthenticated() {
    PrincipalContext principal = InboundContextInterceptor.PC_KEY.get();
    String queryId = InboundContextInterceptor.QUERY_KEY.get();
    String correlationId = InboundContextInterceptor.CORR_KEY.get();
    EngineContext engineContext = InboundContextInterceptor.ENGINE_CONTEXT_KEY.get();
    String sessionHeaderValue = InboundContextInterceptor.SESSION_HEADER_VALUE_KEY.get();
    String authorizationHeaderValue =
        InboundContextInterceptor.AUTHORIZATION_HEADER_VALUE_KEY.get();

    if (principal == null
        && queryId == null
        && correlationId == null
        && engineContext == null
        && sessionHeaderValue == null
        && authorizationHeaderValue == null) {
      return ResolvedCallContext.unauthenticated();
    }

    return new ResolvedCallContext(
        principal != null ? principal : PrincipalContext.getDefaultInstance(),
        queryId != null ? queryId : "",
        correlationId != null ? correlationId : "",
        engineContext != null ? engineContext : EngineContext.empty(),
        sessionHeaderValue,
        authorizationHeaderValue);
  }
}
