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

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import io.grpc.Context;
import org.jboss.logging.MDC;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class InboundContextInterceptorContextTest {

  @AfterEach
  void tearDown() {
    InboundContextInterceptor.clearMdc();
  }

  @Test
  void contextWithResolvedCallContextPopulatesGrpcKeys() {
    ResolvedCallContext resolved = sampleContext();
    Context callContext =
        InboundContextInterceptor.contextWithResolvedCallContext(Context.current(), resolved);

    Context previous = callContext.attach();
    try {
      assertEquals("acct-1", ContextStore.get(InboundContextInterceptor.PC_KEY).getAccountId());
      assertEquals("query-1", InboundContextInterceptor.QUERY_KEY.get());
      assertEquals("corr-1", InboundContextInterceptor.CORR_KEY.get());
      assertEquals("floedb", InboundContextInterceptor.ENGINE_KIND_KEY.get());
      assertEquals("9.9.9", InboundContextInterceptor.ENGINE_VERSION_KEY.get());
      assertEquals(
          "x-floe-session-token", InboundContextInterceptor.SESSION_HEADER_VALUE_KEY.get());
      assertEquals("Bearer token", InboundContextInterceptor.AUTHORIZATION_HEADER_VALUE_KEY.get());
      assertEquals("floedb", InboundContextInterceptor.ENGINE_CONTEXT_KEY.get().engineKind());
    } finally {
      callContext.detach(previous);
    }
  }

  @Test
  void populateAndClearMdcUsesResolvedContextValues() {
    ResolvedCallContext resolved = sampleContext();

    InboundContextInterceptor.populateMdc(resolved);
    assertEquals("query-1", MDC.get("query_id"));
    assertEquals("corr-1", MDC.get("correlation_id"));
    assertEquals("acct-1", MDC.get("floecat_account_id"));
    assertEquals("subject-1", MDC.get("floecat_subject"));
    assertEquals("floedb", MDC.get("floecat_engine_kind"));
    assertEquals("9.9.9", MDC.get("floecat_engine_version"));

    InboundContextInterceptor.clearMdc();
    assertEquals(null, MDC.get("query_id"));
    assertEquals(null, MDC.get("correlation_id"));
  }

  private static ResolvedCallContext sampleContext() {
    return new ResolvedCallContext(
        PrincipalContext.newBuilder().setAccountId("acct-1").setSubject("subject-1").build(),
        "query-1",
        "corr-1",
        EngineContext.of("floedb", "9.9.9"),
        "x-floe-session-token",
        "Bearer token");
  }
}
