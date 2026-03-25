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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.context.impl.InboundContextInterceptor;
import io.grpc.Context;
import org.junit.jupiter.api.Test;

class GrpcResolvedCallContextsTest {

  @Test
  void currentOrUnauthenticatedReturnsUnauthenticatedWhenPrincipalMissing() {
    Context callContext =
        Context.current()
            .withValue(InboundContextInterceptor.QUERY_KEY, "query-1")
            .withValue(InboundContextInterceptor.CORR_KEY, "corr-1");

    Context previous = callContext.attach();
    try {
      var resolved = GrpcResolvedCallContexts.currentOrUnauthenticated();

      assertEquals(PrincipalContext.getDefaultInstance(), resolved.principalContext());
      assertEquals("", resolved.queryId());
      assertEquals("", resolved.correlationId());
      assertEquals(EngineContext.empty(), resolved.engineContext());
      assertNull(resolved.sessionHeaderValue());
      assertNull(resolved.authorizationHeaderValue());
    } finally {
      callContext.detach(previous);
    }
  }

  @Test
  void currentOrUnauthenticatedUsesDefaultsForMissingOptionalValues() {
    PrincipalContext principal =
        PrincipalContext.newBuilder().setAccountId("acct-1").setSubject("subject-1").build();
    Context callContext = Context.current().withValue(InboundContextInterceptor.PC_KEY, principal);

    Context previous = callContext.attach();
    try {
      var resolved = GrpcResolvedCallContexts.currentOrUnauthenticated();

      assertEquals(principal, resolved.principalContext());
      assertEquals("", resolved.queryId());
      assertEquals("", resolved.correlationId());
      assertEquals(EngineContext.empty(), resolved.engineContext());
      assertNull(resolved.sessionHeaderValue());
      assertNull(resolved.authorizationHeaderValue());
    } finally {
      callContext.detach(previous);
    }
  }

  @Test
  void currentOrUnauthenticatedReturnsFullyPopulatedContext() {
    PrincipalContext principal =
        PrincipalContext.newBuilder().setAccountId("acct-1").setSubject("subject-1").build();
    EngineContext engineContext = EngineContext.of("floedb", "9.9.9");
    Context callContext =
        Context.current()
            .withValue(InboundContextInterceptor.PC_KEY, principal)
            .withValue(InboundContextInterceptor.QUERY_KEY, "query-1")
            .withValue(InboundContextInterceptor.CORR_KEY, "corr-1")
            .withValue(InboundContextInterceptor.ENGINE_CONTEXT_KEY, engineContext)
            .withValue(InboundContextInterceptor.SESSION_HEADER_VALUE_KEY, "session-token")
            .withValue(
                InboundContextInterceptor.AUTHORIZATION_HEADER_VALUE_KEY, "Bearer access-token");

    Context previous = callContext.attach();
    try {
      var resolved = GrpcResolvedCallContexts.currentOrUnauthenticated();

      assertEquals(principal, resolved.principalContext());
      assertEquals("query-1", resolved.queryId());
      assertEquals("corr-1", resolved.correlationId());
      assertEquals(engineContext, resolved.engineContext());
      assertEquals("session-token", resolved.sessionHeaderValue());
      assertEquals("Bearer access-token", resolved.authorizationHeaderValue());
    } finally {
      callContext.detach(previous);
    }
  }
}
