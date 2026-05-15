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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import io.grpc.Context;
import io.vertx.core.Vertx;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ContextStoreTest {

  @Test
  void grpcPrincipalTakesPrecedence() {
    PrincipalContext expected =
        PrincipalContext.newBuilder()
            .setSubject("grpc-user")
            .addPermissions("account.read")
            .build();
    AtomicReference<PrincipalContext> observed = new AtomicReference<>();

    ContextStore.set(Context.current(), InboundContextInterceptor.PC_KEY, expected)
        .run(() -> observed.set(ContextStore.get(InboundContextInterceptor.PC_KEY)));

    assertSame(expected, observed.get());
  }

  @Test
  void fallsBackToVertxPrincipalWhenGrpcPrincipalMissing() throws Exception {
    Vertx vertx = Vertx.vertx();
    try {
      PrincipalContext expected =
          PrincipalContext.newBuilder()
              .setSubject("vertx-user")
              .setAccountId("account-1")
              .addPermissions("account.write")
              .build();
      AtomicReference<PrincipalContext> observed = new AtomicReference<>();

      CompletableFuture<Void> completion = new CompletableFuture<>();
      vertx.runOnContext(
          ignored -> {
            ContextStore.set(Context.current(), InboundContextInterceptor.PC_KEY, expected);
            observed.set(ContextStore.get(InboundContextInterceptor.PC_KEY));
            ContextStore.clear();
            completion.complete(null);
          });

      completion.get();
      assertEquals(expected, observed.get());
    } finally {
      vertx.close().toCompletionStage().toCompletableFuture().get();
    }
  }

  @Test
  void ignoresEmptyGrpcPrincipalAndFallsBackToVertxPrincipal() throws Exception {
    Vertx vertx = Vertx.vertx();
    try {
      PrincipalContext vertxPrincipal =
          PrincipalContext.newBuilder()
              .setSubject("vertx-user")
              .setAccountId("account-1")
              .addPermissions("account.write")
              .build();
      AtomicReference<PrincipalContext> observed = new AtomicReference<>();

      CompletableFuture<Void> completion = new CompletableFuture<>();
      vertx.runOnContext(
          ignored ->
              ContextStore.set(
                      Context.current(),
                      InboundContextInterceptor.PC_KEY,
                      PrincipalContext.getDefaultInstance())
                  .run(
                      () -> {
                        ContextStore.set(
                            Context.current(), InboundContextInterceptor.PC_KEY, vertxPrincipal);
                        observed.set(ContextStore.get(InboundContextInterceptor.PC_KEY));
                        ContextStore.clear();
                        completion.complete(null);
                      }));

      completion.get();
      assertEquals(vertxPrincipal, observed.get());
    } finally {
      vertx.close().toCompletionStage().toCompletableFuture().get();
    }
  }

  @Test
  void fallsBackToVertxEngineContextWhenGrpcContextMissing() throws Exception {
    Vertx vertx = Vertx.vertx();
    try {
      EngineContext expected = EngineContext.of("floedb", "16.0");
      AtomicReference<EngineContext> observed = new AtomicReference<>();

      CompletableFuture<Void> completion = new CompletableFuture<>();
      vertx.runOnContext(
          ignored -> {
            ContextStore.set(
                Context.current(), InboundContextInterceptor.ENGINE_CONTEXT_KEY, expected);
            observed.set(ContextStore.get(InboundContextInterceptor.ENGINE_CONTEXT_KEY));
            ContextStore.clear();
            completion.complete(null);
          });

      completion.get();
      assertEquals(expected, observed.get());
    } finally {
      vertx.close().toCompletionStage().toCompletableFuture().get();
    }
  }

  @Test
  void blankGrpcStringFallsBackToVertxLocalValue() throws Exception {
    Vertx vertx = Vertx.vertx();
    try {
      AtomicReference<String> observed = new AtomicReference<>();

      CompletableFuture<Void> completion = new CompletableFuture<>();
      vertx.runOnContext(
          ignored ->
              ContextStore.set(Context.current(), InboundContextInterceptor.QUERY_KEY, "")
                  .run(
                      () -> {
                        ContextStore.set(
                            Context.current(), InboundContextInterceptor.QUERY_KEY, "query-1");
                        observed.set(ContextStore.get(InboundContextInterceptor.QUERY_KEY));
                        ContextStore.clear();
                        completion.complete(null);
                      }));

      completion.get();
      assertEquals("query-1", observed.get());
    } finally {
      vertx.close().toCompletionStage().toCompletableFuture().get();
    }
  }

  @Test
  void clearRemovesVertxLocalValues() throws Exception {
    Vertx vertx = Vertx.vertx();
    try {
      AtomicReference<String> observed = new AtomicReference<>();

      CompletableFuture<Void> completion = new CompletableFuture<>();
      vertx.runOnContext(
          ignored -> {
            ContextStore.set(Context.current(), InboundContextInterceptor.CORR_KEY, "corr-1");
            ContextStore.clear();
            observed.set(ContextStore.get(InboundContextInterceptor.CORR_KEY));
            completion.complete(null);
          });

      completion.get();
      assertNull(observed.get());
    } finally {
      vertx.close().toCompletionStage().toCompletableFuture().get();
    }
  }

  @Test
  void clearRemovesStoredPrincipal() throws Exception {
    Vertx vertx = Vertx.vertx();
    try {
      PrincipalContext vertxPrincipal =
          PrincipalContext.newBuilder().setSubject("vertx-user").build();
      AtomicReference<PrincipalContext> observed = new AtomicReference<>();

      CompletableFuture<Void> completion = new CompletableFuture<>();
      vertx.runOnContext(
          ignored -> {
            ContextStore.set(Context.current(), InboundContextInterceptor.PC_KEY, vertxPrincipal);
            ContextStore.clear();
            observed.set(ContextStore.get(InboundContextInterceptor.PC_KEY));
            completion.complete(null);
          });

      completion.get();
      assertNull(observed.get());
    } finally {
      vertx.close().toCompletionStage().toCompletableFuture().get();
    }
  }
}
