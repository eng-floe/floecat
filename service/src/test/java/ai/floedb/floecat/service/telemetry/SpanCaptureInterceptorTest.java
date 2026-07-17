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

package ai.floedb.floecat.service.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.service.context.impl.InboundContextInterceptor;
import ai.floedb.floecat.service.context.impl.ResolvedCallContexts;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Scope;
import io.smallrye.common.vertx.VertxContext;
import io.vertx.core.Vertx;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/**
 * The capture interceptor's contract: inside the tracing window (a valid span current), it stows
 * the span on the per-call duplicated-context carrier and applies the per-request identity
 * attributes; outside it (invalid root span), it does neither and never disturbs the chain. Tests
 * run under OTel's default thread-local context storage — the production Quarkus storage cannot be
 * installed in a unit test. Where the span IS current in production (the interceptor-ordering
 * premise) is guarded by {@code SpanDecorationIT}, which boots a real Quarkus and asserts the
 * decorations on an exported span; what this covers is what the interceptor does once it is.
 */
class SpanCaptureInterceptorTest {

  private static final SpanContext VALID_SPAN_CONTEXT =
      SpanContext.createFromRemoteParent(
          "0af7651916cd43dd8448eb211c80319c",
          "b7ad6b7169203331",
          TraceFlags.getSampled(),
          TraceState.getDefault());

  /** Runs {@code body} on a fresh Vert.x duplicated context and returns its result. */
  private static <T> T onDuplicatedContext(Vertx vertx, Supplier<T> body) throws Exception {
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

  @SuppressWarnings("unchecked")
  private static ServerCall<Object, Object> stubCall() {
    return stubCall("floecat.catalog.v1.CatalogService/ListCatalogs");
  }

  @SuppressWarnings("unchecked")
  private static ServerCall<Object, Object> stubCall(String fullMethodName) {
    ServerCall<Object, Object> call = mock(ServerCall.class);
    io.grpc.MethodDescriptor<Object, Object> md = mock(io.grpc.MethodDescriptor.class);
    when(md.getFullMethodName()).thenReturn(fullMethodName);
    when(call.getMethodDescriptor()).thenReturn(md);
    return call;
  }

  /**
   * A minimal real {@link Span} whose attribute writes are observable. A Mockito mock cannot serve
   * here: {@code makeCurrent()}/{@code storeInContext()} are interface DEFAULT methods that a mock
   * stubs away, so the "span" never actually becomes current.
   */
  private static final class RecordingSpan implements Span {
    final java.util.Map<String, Object> attributes = new java.util.concurrent.ConcurrentHashMap<>();

    @Override
    public <T> Span setAttribute(AttributeKey<T> key, T value) {
      attributes.put(key.getKey(), value);
      return this;
    }

    @Override
    public Span addEvent(String name, io.opentelemetry.api.common.Attributes a) {
      return this;
    }

    @Override
    public Span addEvent(
        String name, io.opentelemetry.api.common.Attributes a, long ts, TimeUnit unit) {
      return this;
    }

    @Override
    public Span setStatus(StatusCode statusCode, String description) {
      return this;
    }

    @Override
    public Span recordException(Throwable exception, io.opentelemetry.api.common.Attributes a) {
      return this;
    }

    @Override
    public Span updateName(String name) {
      return this;
    }

    @Override
    public void end() {}

    @Override
    public void end(long timestamp, TimeUnit unit) {}

    @Override
    public SpanContext getSpanContext() {
      return VALID_SPAN_CONTEXT;
    }

    @Override
    public boolean isRecording() {
      return true;
    }
  }

  @Test
  void insideTheWindowStowsTheSpanAndAppliesIdentityAttributes() throws Exception {
    Vertx vertx = Vertx.vertx();
    try {
      RecordingSpan span = new RecordingSpan();
      SpanCaptureInterceptor interceptor = new SpanCaptureInterceptor(true, true, false);
      @SuppressWarnings("unchecked")
      ServerCallHandler<Object, Object> next = mock(ServerCallHandler.class);
      ServerCall.Listener<Object> chainListener = new ServerCall.Listener<>() {};
      when(next.startCall(any(), any())).thenReturn(chainListener);

      // The inbound interceptor's io.grpc.Context keys, attached as Contexts.interceptCall keeps
      // them through the inner chain build.
      Context grpcCtx =
          Context.current()
              .withValue(InboundContextInterceptor.QUERY_KEY, "query-1")
              .withValue(InboundContextInterceptor.CORR_KEY, "corr-1")
              .withValue(
                  InboundContextInterceptor.PC_KEY,
                  PrincipalContext.newBuilder().setAccountId("acct").setSubject("dev").build())
              .withValue(InboundContextInterceptor.ENGINE_VERSION_KEY, "0.1")
              .withValue(InboundContextInterceptor.ENGINE_KIND_KEY, "floedb");

      ServerCall.Listener<Object> returned =
          onDuplicatedContext(
              vertx,
              () -> {
                Context prev = grpcCtx.attach();
                try (Scope ignored = span.makeCurrent()) {
                  var listener = interceptor.interceptCall(stubCall(), new Metadata(), next);
                  // Captured on the SAME duplicated context, readable after the window closes.
                  assertTrue(
                      ResolvedCallContexts.currentCallSpanOrInvalid().getSpanContext().isValid());
                  return listener;
                } finally {
                  grpcCtx.detach(prev);
                }
              });

      assertSame(chainListener, returned, "the chain must continue undisturbed");
      assertEquals("query-1", span.attributes.get("query_id"));
      assertEquals("corr-1", span.attributes.get("correlation_id"));
      assertEquals("acct", span.attributes.get("floecat.account_id"));
      assertEquals("dev", span.attributes.get("floecat.subject"));
      assertEquals("0.1", span.attributes.get("floecat.engine.version"));
      assertEquals("floedb", span.attributes.get("floecat.engine.kind"));
    } finally {
      vertx.close().toCompletionStage().toCompletableFuture().get();
    }
  }

  @Test
  void outsideTheWindowStowsNothingAndNeverDisturbsTheChain() throws Exception {
    // The no-span path across every log-level case. Only the FIRST — an application RPC with
    // tracing effectively on — takes the loud regression WARN; all the others stay a quiet DEBUG:
    //   - application RPC, traces.enabled=false                → tracing off
    //   - infrastructure RPC (health), tracing on              → health/reflection may legitimately
    //                                                             be untraced; suppress per-probe
    // noise
    //   - application RPC, otel.enabled=false (master switch)  → SDK no-op though
    // traces.enabled=true
    //   - application RPC, sdk.disabled=true                   → SDK no-op though
    // traces.enabled=true
    // The master-switch and sdk-disabled cases are the ones that would spam the WARN if the gate
    // read traces.enabled alone. All behave identically here — nothing stowed, chain undisturbed —
    // only the level differs; this asserts the shared behaviour for every case.
    record Case(boolean otelEnabled, boolean tracesEnabled, boolean sdkDisabled, String method) {}
    String app = "floecat.catalog.v1.CatalogService/ListCatalogs";
    Case[] cases = {
      new Case(true, true, false, app), // loud
      new Case(true, false, false, app), // traces off → quiet
      new Case(true, true, false, "grpc.health.v1.Health/Check"), // infra → quiet
      new Case(false, true, false, app), // master switch off → quiet
      new Case(true, true, true, app), // sdk disabled → quiet
    };
    for (Case c : cases) {
      Vertx vertx = Vertx.vertx();
      try {
        SpanCaptureInterceptor interceptor =
            new SpanCaptureInterceptor(c.otelEnabled(), c.tracesEnabled(), c.sdkDisabled());
        @SuppressWarnings("unchecked")
        ServerCallHandler<Object, Object> next = mock(ServerCallHandler.class);
        ServerCall.Listener<Object> chainListener = new ServerCall.Listener<>() {};
        when(next.startCall(any(), any())).thenReturn(chainListener);

        ServerCall.Listener<Object> returned =
            onDuplicatedContext(
                vertx,
                () -> {
                  // No span current: the interceptor must not stow the invalid root span.
                  var listener =
                      interceptor.interceptCall(stubCall(c.method()), new Metadata(), next);
                  assertFalse(
                      ResolvedCallContexts.currentCallSpanOrInvalid().getSpanContext().isValid());
                  return listener;
                });

        assertSame(chainListener, returned);
      } finally {
        vertx.close().toCompletionStage().toCompletableFuture().get();
      }
    }
  }

  @Test
  void sortsBelowTheImplicitZeroOfTheQuarkusTracingInterceptor() {
    // The whole mechanism rests on this: Quarkus's GrpcTracingServerInterceptor carries no
    // Prioritized priority, so the comparator assigns it 0 — the capture interceptor must sort
    // strictly below to have its chain build run inside the tracing window.
    assertTrue(new SpanCaptureInterceptor(true, true, false).getPriority() < 0);
    assertEquals(
        ai.floedb.floecat.service.common.GrpcInterceptorPriorities.SPAN_CAPTURE,
        new SpanCaptureInterceptor(true, true, false).getPriority());
  }

  @Test
  void regressionWarnLogsOnFirstOccurrenceAndEachDecadeOnly() {
    // The regression WARN is taken on every call once the ordering breaks; it must taper to first
    // occurrence + powers of ten so it never floods the log at production QPS.
    for (long milestone : new long[] {1, 10, 100, 1000, 10_000, 1_000_000}) {
      assertTrue(
          SpanCaptureInterceptor.isFirstOrDecadeMilestone(milestone), "should log at " + milestone);
    }
    for (long between : new long[] {0, 2, 5, 9, 11, 99, 101, 999, 123_456}) {
      assertFalse(
          SpanCaptureInterceptor.isFirstOrDecadeMilestone(between),
          "should stay quiet at " + between);
    }
  }
}
