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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.smallrye.common.vertx.VertxContext;
import io.vertx.core.Vertx;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * The span carrier that lets streaming RPCs re-activate the gRPC server span across the dispatch/
 * subscription hop. The carrier rides the same per-call Vert.x duplicated-context channel as the
 * resolved call context, so these run the store/read on an actual duplicated context.
 */
class ResolvedCallContextsSpanTest {

  private static final Span SAMPLED_SPAN =
      Span.wrap(
          SpanContext.createFromRemoteParent(
              "0af7651916cd43dd8448eb211c80319c",
              "b7ad6b7169203331",
              TraceFlags.getSampled(),
              TraceState.getDefault()));

  /** Runs {@code body} on a fresh Vert.x duplicated context and returns its result. */
  private static <T> T onDuplicatedContext(Vertx vertx, java.util.function.Supplier<T> body)
      throws Exception {
    CompletableFuture<T> result = new CompletableFuture<>();
    io.vertx.core.Context duplicated =
        VertxContext.createNewDuplicatedContext(vertx.getOrCreateContext());
    duplicated.runOnContext(
        ignored -> {
          try {
            result.complete(body.get());
          } catch (Throwable t) {
            result.completeExceptionally(t);
          }
        });
    return result.get(10, TimeUnit.SECONDS);
  }

  @Test
  void aValidSpanRoundTripsOnTheDuplicatedContext() throws Exception {
    Vertx vertx = Vertx.vertx();
    try {
      Span readBack =
          onDuplicatedContext(
              vertx,
              () -> {
                ResolvedCallContexts.storeSpanOnDuplicatedContext(SAMPLED_SPAN);
                return ResolvedCallContexts.currentCallSpanOrInvalid();
              });
      assertTrue(readBack.getSpanContext().isValid());
      assertEquals(
          SAMPLED_SPAN.getSpanContext().getTraceId(), readBack.getSpanContext().getTraceId());
      assertEquals(
          SAMPLED_SPAN.getSpanContext().getSpanId(), readBack.getSpanContext().getSpanId());
    } finally {
      vertx.close().toCompletionStage().toCompletableFuture().get();
    }
  }

  @Test
  void anInvalidSpanIsNotCarried() throws Exception {
    Vertx vertx = Vertx.vertx();
    try {
      // An invalid span can never prove which trace this call belongs to, so it must not be
      // stored: the reader returns the invalid span, and callers keep the no-op behaviour rather
      // than decorating a bogus span.
      Span readBack =
          onDuplicatedContext(
              vertx,
              () -> {
                ResolvedCallContexts.storeSpanOnDuplicatedContext(Span.getInvalid());
                return ResolvedCallContexts.currentCallSpanOrInvalid();
              });
      assertFalse(readBack.getSpanContext().isValid());
    } finally {
      vertx.close().toCompletionStage().toCompletableFuture().get();
    }
  }

  @Test
  void readingOffAnyDuplicatedContextWithoutAStoredSpanReturnsInvalid() throws Exception {
    Vertx vertx = Vertx.vertx();
    try {
      Span readBack = onDuplicatedContext(vertx, ResolvedCallContexts::currentCallSpanOrInvalid);
      assertFalse(readBack.getSpanContext().isValid());
    } finally {
      vertx.close().toCompletionStage().toCompletableFuture().get();
    }
  }

  @Test
  void storeOffAnyVertxContextIsANoOpNotACrash() {
    // No Vert.x context on a plain JUnit thread: store must silently no-op and the reader must
    // return the invalid span rather than throwing.
    ResolvedCallContexts.storeSpanOnDuplicatedContext(SAMPLED_SPAN);
    assertFalse(ResolvedCallContexts.currentCallSpanOrInvalid().getSpanContext().isValid());
  }
}
