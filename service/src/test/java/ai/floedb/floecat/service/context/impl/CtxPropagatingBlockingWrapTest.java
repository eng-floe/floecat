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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import io.grpc.Attributes;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.vertx.core.Vertx;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Direct coverage of {@link CtxPropagatingBlockingWrap}. Tests build the wrap with a plain Vert.x
 * instance and a stub inner interceptor; no CDI / Quarkus stack required.
 *
 * <p>Each test runs against the wrap from a Vert.x event-loop context (as the real gRPC server
 * would call {@code interceptCall}), then drives listener events directly.
 */
class CtxPropagatingBlockingWrapTest {

  private static final Context.Key<String> KEY = Context.key("test-principal");

  private static ServerInterceptor makeWrap(Vertx vertx, ServerInterceptor inner) {
    return new CtxPropagatingBlockingWrap(vertx, inner)::interceptCall;
  }

  /**
   * Single-call propagation through the {@code Contexts.interceptCall} CSL chain — the production
   * shape. The inner attaches a value via {@code Contexts.interceptCall}; the handler reads {@code
   * KEY.get()} on its {@code onHalfClose}; the wrap must dispatch the event to a worker and the
   * inner CSL must re-attach the value.
   */
  @Test
  void grpcContextSurvivesSingleCall() throws Exception {
    Vertx vertx = Vertx.vertx();
    try {
      ServerInterceptor inner =
          new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call,
                Metadata headers,
                ServerCallHandler<ReqT, RespT> next) {
              Context ctx = Context.current().withValue(KEY, "set-by-inner");
              return Contexts.interceptCall(ctx, call, headers, next);
            }
          };
      ServerInterceptor wrap = makeWrap(vertx, inner);

      AtomicReference<String> observed = new AtomicReference<>("<unset>");
      CountDownLatch done = new CountDownLatch(1);
      ServerCallHandler<Object, Object> handler = capturingHandler(observed, done);

      drive(vertx, wrap, handler);
      assertTrue(done.await(10, TimeUnit.SECONDS), "handler.onHalfClose never ran");
      assertEquals("set-by-inner", observed.get());
    } finally {
      vertx.close().toCompletionStage().toCompletableFuture().get();
    }
  }

  /**
   * Buffer/replay path: events fire on the wrap's listener <i>before</i> the inner has finished
   * building its listener on the worker. The wrap must buffer the events and deliver them after the
   * delegate is set.
   */
  @Test
  void bufferedEventsReplayedAfterSetDelegate() throws Exception {
    Vertx vertx = Vertx.vertx();
    try {
      CountDownLatch holdInner = new CountDownLatch(1);
      ServerInterceptor inner =
          new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call,
                Metadata headers,
                ServerCallHandler<ReqT, RespT> next) {
              // Block the chain build until the test fires the event, forcing the buffer path.
              try {
                holdInner.await(5, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              Context ctx = Context.current().withValue(KEY, "set-by-inner");
              return Contexts.interceptCall(ctx, call, headers, next);
            }
          };
      ServerInterceptor wrap = makeWrap(vertx, inner);

      AtomicReference<String> observed = new AtomicReference<>("<unset>");
      CountDownLatch done = new CountDownLatch(1);
      ServerCallHandler<Object, Object> handler = capturingHandler(observed, done);

      CompletableFuture<ServerCall.Listener<Object>> outerListener = new CompletableFuture<>();
      vertx.runOnContext(
          ignored ->
              outerListener.complete(wrap.interceptCall(stubCall(), new Metadata(), handler)));
      ServerCall.Listener<Object> listener = outerListener.get(5, TimeUnit.SECONDS);

      // Fire BEFORE the inner is allowed to finish — must be buffered.
      listener.onHalfClose();
      // Give the wrap a moment to confirm delegate is still null.
      Thread.sleep(50);
      assertEquals("<unset>", observed.get(), "handler ran before delegate was set");

      holdInner.countDown();
      assertTrue(done.await(10, TimeUnit.SECONDS), "buffered event never replayed");
      assertEquals("set-by-inner", observed.get());
    } finally {
      vertx.close().toCompletionStage().toCompletableFuture().get();
    }
  }

  /**
   * Concurrent calls: each carries a unique principal in metadata and must observe its own value at
   * the handler entry. Any mismatch indicates cross-call state bleed.
   */
  @Test
  void concurrentCallsDoNotBleed() throws Exception {
    int n = 200;
    Vertx vertx = Vertx.vertx();
    try {
      Metadata.Key<String> principalHeader =
          Metadata.Key.of("x-test-principal", Metadata.ASCII_STRING_MARSHALLER);

      ServerInterceptor inner =
          new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call,
                Metadata headers,
                ServerCallHandler<ReqT, RespT> next) {
              String value = headers.get(principalHeader);
              Context ctx = Context.current().withValue(KEY, value);
              return Contexts.interceptCall(ctx, call, headers, next);
            }
          };
      ServerInterceptor wrap = makeWrap(vertx, inner);

      ConcurrentMap<Integer, String> mismatches = new ConcurrentHashMap<>();
      CountDownLatch done = new CountDownLatch(n);

      for (int i = 0; i < n; i++) {
        final int id = i;
        final String expected = "principal-" + id;
        ServerCallHandler<Object, Object> handler =
            (call, headers) ->
                new ServerCall.Listener<>() {
                  @Override
                  public void onHalfClose() {
                    String v = KEY.get();
                    if (!expected.equals(v)) {
                      mismatches.putIfAbsent(id, "expected=" + expected + " observed=" + v);
                    }
                    done.countDown();
                  }
                };
        Metadata md = new Metadata();
        md.put(principalHeader, expected);

        vertx.runOnContext(
            ignored -> {
              try {
                ServerCall.Listener<Object> listener = wrap.interceptCall(stubCall(), md, handler);
                listener.onHalfClose();
              } catch (Throwable t) {
                mismatches.putIfAbsent(id, "dispatch-failed=" + t);
                done.countDown();
              }
            });
      }

      assertTrue(
          done.await(30, TimeUnit.SECONDS),
          () -> "did not complete in 30s; remaining=" + done.getCount());
      if (!mismatches.isEmpty()) {
        StringBuilder sb = new StringBuilder();
        sb.append("cross-call bleed: ").append(mismatches.size()).append(" / ").append(n);
        int printed = 0;
        for (Map.Entry<Integer, String> e : mismatches.entrySet()) {
          if (printed++ >= 5) break;
          sb.append("\n  id=").append(e.getKey()).append(' ').append(e.getValue());
        }
        fail(sb.toString());
      }
    } finally {
      vertx.close().toCompletionStage().toCompletableFuture().get();
    }
  }

  /**
   * All five {@link ServerCall.Listener} callbacks are forwarded to the inner listener in arrival
   * order, including events fired before the inner chain has finished building (the buffer-replay
   * path). Matches the surface of the deprecated {@code io.vertx.grpc.BlockingServerInterceptor
   * .AsyncListener} that this wrap replaces.
   */
  @Test
  void allFiveCallbacksForwardedInOrderThroughBuffer() throws Exception {
    Vertx vertx = Vertx.vertx();
    try {
      List<String> order = new ArrayList<>();
      Object lock = new Object();
      CountDownLatch holdInner = new CountDownLatch(1);

      ServerInterceptor inner =
          new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call,
                Metadata headers,
                ServerCallHandler<ReqT, RespT> next) {
              try {
                // Block the chain build so every event below buffers before delegate is set.
                holdInner.await(5, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              return next.startCall(call, headers);
            }
          };
      ServerInterceptor wrap = makeWrap(vertx, inner);

      CountDownLatch done = new CountDownLatch(5);
      ServerCallHandler<Object, Object> handler =
          (call, headers) ->
              new ServerCall.Listener<>() {
                @Override
                public void onReady() {
                  record("ready");
                }

                @Override
                public void onMessage(Object message) {
                  record("message:" + message);
                }

                @Override
                public void onHalfClose() {
                  record("halfClose");
                }

                @Override
                public void onCancel() {
                  record("cancel");
                }

                @Override
                public void onComplete() {
                  record("complete");
                }

                private void record(String event) {
                  synchronized (lock) {
                    order.add(event);
                  }
                  done.countDown();
                }
              };

      CompletableFuture<ServerCall.Listener<Object>> outer = new CompletableFuture<>();
      vertx.runOnContext(
          ignored -> outer.complete(wrap.interceptCall(stubCall(), new Metadata(), handler)));
      ServerCall.Listener<Object> listener = outer.get(5, TimeUnit.SECONDS);

      // Fire ALL five events before unblocking the chain build — every one must buffer.
      listener.onReady();
      listener.onMessage("m1");
      listener.onHalfClose();
      listener.onCancel();
      listener.onComplete();

      // Brief pause to confirm nothing fired yet.
      Thread.sleep(50);
      synchronized (lock) {
        assertTrue(order.isEmpty(), "events ran before delegate was set: " + order);
      }

      holdInner.countDown();
      assertTrue(done.await(10, TimeUnit.SECONDS), "buffered events never fully replayed");
      synchronized (lock) {
        assertEquals(List.of("ready", "message:m1", "halfClose", "cancel", "complete"), order);
      }
    } finally {
      vertx.close().toCompletionStage().toCompletableFuture().get();
    }
  }

  /**
   * Error path: inner throws — the wrap must close the {@link ServerCall} with the translated
   * status rather than swallowing the error or hanging.
   */
  @Test
  void innerFailureClosesCallWithStatus() throws Exception {
    Vertx vertx = Vertx.vertx();
    try {
      ServerInterceptor failing =
          new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call,
                Metadata headers,
                ServerCallHandler<ReqT, RespT> next) {
              throw Status.PERMISSION_DENIED.withDescription("nope").asRuntimeException();
            }
          };
      ServerInterceptor wrap = makeWrap(vertx, failing);

      ServerCall<Object, Object> call = stubCall();
      CountDownLatch closed = new CountDownLatch(1);
      AtomicReference<Status> capturedStatus = new AtomicReference<>();
      org.mockito.Mockito.doAnswer(
              inv -> {
                capturedStatus.set(inv.getArgument(0));
                closed.countDown();
                return null;
              })
          .when(call)
          .close(any(Status.class), any(Metadata.class));

      vertx.runOnContext(ignored -> wrap.interceptCall(call, new Metadata(), noopHandler()));

      assertTrue(closed.await(5, TimeUnit.SECONDS), "call.close never invoked");
      assertNotNull(capturedStatus.get());
      assertEquals(Status.Code.PERMISSION_DENIED, capturedStatus.get().getCode());
      verify(call, atLeastOnce()).close(any(Status.class), any(Metadata.class));
    } finally {
      vertx.close().toCompletionStage().toCompletableFuture().get();
    }
  }

  /**
   * Proves the actual production symptom: a gRPC {@link Context} value attached on the caller's
   * thread when a listener event arrives must still be readable from the handler when the event is
   * finally delivered. The inner is a plain pass-through (no {@code Contexts.interceptCall} → no
   * inner CSL to re-attach) so the only thing keeping the {@link Context} alive across the
   * worker/buffer hop is the wrap itself.
   *
   * <p>This is the test that <i>fails</i> against the deprecated {@code BlockingServerInterceptor
   * .wrap}: that wrap doesn't capture the caller's gRPC {@link Context} when an event is buffered,
   * so by the time {@code setDelegate} replays the event on the {@code executeBlocking} onComplete
   * thread (which has no {@link Context} attached), {@code KEY.get()} returns {@code null} — i.e.,
   * {@link io.grpc.Context.Key#get()} loses the value. The new wrap captures the {@link Context} at
   * event arrival in {@code scheduleOrEnqueue} and re-attaches it on the worker around the delegate
   * call.
   */
  @Test
  void grpcContextAttachedAtArrivalSurvivesBufferAndReplay() throws Exception {
    Vertx vertx = Vertx.vertx();
    try {
      // Block the chain build so onHalfClose hits the BUFFER path.
      CountDownLatch holdInner = new CountDownLatch(1);
      ServerInterceptor inner =
          new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call,
                Metadata headers,
                ServerCallHandler<ReqT, RespT> next) {
              try {
                holdInner.await(5, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              // Pass-through: no Contexts.interceptCall, no inner CSL. The wrap is solely
              // responsible for io.grpc.Context propagation across the buffer.
              return next.startCall(call, headers);
            }
          };
      ServerInterceptor wrap = makeWrap(vertx, inner);

      AtomicReference<String> observed = new AtomicReference<>("<unset>");
      CountDownLatch done = new CountDownLatch(1);
      ServerCallHandler<Object, Object> handler =
          (call, headers) ->
              new ServerCall.Listener<>() {
                @Override
                public void onHalfClose() {
                  String v = KEY.get();
                  observed.set(v == null ? "<null>" : v);
                  done.countDown();
                }
              };

      CompletableFuture<ServerCall.Listener<Object>> outer = new CompletableFuture<>();
      vertx.runOnContext(
          ignored -> outer.complete(wrap.interceptCall(stubCall(), new Metadata(), handler)));
      ServerCall.Listener<Object> listener = outer.get(5, TimeUnit.SECONDS);

      // Fire onHalfClose from a thread that has the gRPC Context attached — this is the only
      // window where the wrap can capture the value. Detach immediately so the value is gone
      // from every ThreadLocal by the time the worker runs the replay.
      Thread driver =
          new Thread(
              () -> {
                Context ctx = Context.current().withValue(KEY, "set-at-arrival");
                Context prev = ctx.attach();
                try {
                  listener.onHalfClose();
                } finally {
                  ctx.detach(prev);
                }
              },
              "test-event-driver");
      driver.start();
      driver.join(5_000);

      // Let the wrap settle (event sitting in the buffer), then release the chain build so
      // setDelegate fires and the buffered event gets replayed.
      Thread.sleep(50);
      assertEquals("<unset>", observed.get(), "handler ran before delegate was set");
      holdInner.countDown();

      assertTrue(done.await(10, TimeUnit.SECONDS), "buffered event never replayed");
      assertEquals(
          "set-at-arrival",
          observed.get(),
          "io.grpc.Context attached at event arrival was lost across the buffer/replay");
    } finally {
      vertx.close().toCompletionStage().toCompletableFuture().get();
    }
  }

  /**
   * Models the gRPC service implementation shape: the listener callback observes the request
   * context, captures it, returns, and only then runs the actual service work on another executor.
   * This covers the extra async hop used by Mutiny service methods.
   */
  @Test
  void capturedGrpcContextSurvivesAsyncWorkAfterListenerCallbackReturns() throws Exception {
    Vertx vertx = Vertx.vertx();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      ServerInterceptor inner =
          new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call,
                Metadata headers,
                ServerCallHandler<ReqT, RespT> next) {
              Context ctx = Context.current().withValue(KEY, "set-by-inner");
              return Contexts.interceptCall(ctx, call, headers, next);
            }
          };
      ServerInterceptor wrap = makeWrap(vertx, inner);

      CountDownLatch callbackReturned = new CountDownLatch(1);
      CountDownLatch releaseAsyncWork = new CountDownLatch(1);
      CountDownLatch asyncDone = new CountDownLatch(1);
      AtomicReference<String> observed = new AtomicReference<>("<unset>");

      ServerCallHandler<Object, Object> handler =
          (call, headers) ->
              new ServerCall.Listener<>() {
                @Override
                public void onHalfClose() {
                  Context captured = Context.current();
                  executor.execute(
                      () -> {
                        try {
                          releaseAsyncWork.await(10, TimeUnit.SECONDS);
                          observed.set(captured.call(KEY::get));
                        } catch (Exception e) {
                          observed.set("error=" + e.getClass().getSimpleName());
                        } finally {
                          asyncDone.countDown();
                        }
                      });
                  callbackReturned.countDown();
                }
              };

      drive(vertx, wrap, handler);
      assertTrue(callbackReturned.await(10, TimeUnit.SECONDS), "listener callback never returned");
      releaseAsyncWork.countDown();

      assertTrue(asyncDone.await(10, TimeUnit.SECONDS), "async service work never ran");
      assertEquals("set-by-inner", observed.get());
    } finally {
      executor.shutdownNow();
      vertx.close().toCompletionStage().toCompletableFuture().get();
    }
  }

  /**
   * The full streaming-relay shape from eng-floe/floecat#361: inbound interceptor (production key
   * wiring + duplicated-context carrier) → blocking wrap → a service body that reads the resolved
   * call context at entry and re-emits on a bare executor ({@code runSubscriptionOn} shape) — while
   * extra listener events fire concurrently, flipping the shared io.grpc.Context slot the way the
   * call's own trailing events do in production.
   *
   * <p>Asserts principal, correlation id, and engine context integrity at both layers: the service
   * entry (listener callback on a worker) and the emitter (bare executor thread where no
   * io.grpc.Context and no duplicated context exist — only the explicit {@code runWith} scope).
   */
  @Test
  void resolvedCallContextSurvivesStreamingRelayUnderConcurrentEvents() throws Exception {
    int n = 50;
    Vertx vertx = Vertx.vertx();
    ExecutorService subscriptionPool = Executors.newFixedThreadPool(4);
    try {
      Metadata.Key<String> subjectHeader =
          Metadata.Key.of("x-test-subject", Metadata.ASCII_STRING_MARSHALLER);
      Metadata.Key<String> correlationHeader =
          Metadata.Key.of("x-test-correlation", Metadata.ASCII_STRING_MARSHALLER);
      Metadata.Key<String> engineHeader =
          Metadata.Key.of("x-test-engine", Metadata.ASCII_STRING_MARSHALLER);

      // Models InboundContextInterceptor: resolve the call context from headers, store it on the
      // per-call duplicated context, and populate the production io.grpc.Context keys.
      ServerInterceptor inner =
          new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call,
                Metadata headers,
                ServerCallHandler<ReqT, RespT> next) {
              PrincipalContext principal =
                  PrincipalContext.newBuilder()
                      .setSubject(headers.get(subjectHeader))
                      .setAccountId("acct")
                      .addPermissions("catalog.read")
                      .build();
              ResolvedCallContext resolved =
                  new ResolvedCallContext(
                      principal,
                      "q-1",
                      headers.get(correlationHeader),
                      EngineContext.of(headers.get(engineHeader), "16.0"),
                      null,
                      null);
              ResolvedCallContexts.storeOnDuplicatedContext(resolved);
              Context ctx =
                  InboundContextInterceptor.contextWithResolvedCallContext(
                      Context.current(), resolved);
              return Contexts.interceptCall(ctx, call, headers, next);
            }
          };
      ServerInterceptor wrap = makeWrap(vertx, inner);

      ConcurrentMap<Integer, String> violations = new ConcurrentHashMap<>();
      CountDownLatch done = new CountDownLatch(n);

      for (int i = 0; i < n; i++) {
        final int id = i;
        final String expectedSubject = "subject-" + id;
        final String expectedCorrelation = "corr-" + id;
        final String expectedEngine = "engine-" + id;

        ServerCallHandler<Object, Object> handler =
            (call, headers) ->
                new ServerCall.Listener<>() {
                  @Override
                  public void onHalfClose() {
                    // Service entry: read once, before any executor hop.
                    ResolvedCallContext callCtx = ResolvedCallContexts.currentOrUnauthenticated();
                    if (!expectedSubject.equals(callCtx.principalContext().getSubject())
                        || !expectedCorrelation.equals(callCtx.correlationId())
                        || !expectedEngine.equals(callCtx.engineContext().engineKind())) {
                      violations.putIfAbsent(
                          id,
                          "entry: subject="
                              + callCtx.principalContext().getSubject()
                              + " corr="
                              + callCtx.correlationId()
                              + " engine="
                              + callCtx.engineContext().engineKind());
                      done.countDown();
                      return;
                    }
                    // Emitter layer: a bare pool thread with no io.grpc.Context and no duplicated
                    // context. Only the explicit runWith scope can carry the call context here,
                    // and the providers must read through it (as the bundle iterator does).
                    subscriptionPool.execute(
                        () ->
                            ResolvedCallContexts.runWith(
                                callCtx,
                                () -> {
                                  try {
                                    String subject =
                                        new ai.floedb.floecat.service.security.impl
                                                .PrincipalProvider()
                                            .get()
                                            .getSubject();
                                    String engine =
                                        new ai.floedb.floecat.service.context
                                                .EngineContextProvider()
                                            .engineContext()
                                            .engineKind();
                                    String correlation =
                                        ResolvedCallContexts.currentOrUnauthenticated()
                                            .correlationId();
                                    if (!expectedSubject.equals(subject)
                                        || !expectedCorrelation.equals(correlation)
                                        || !expectedEngine.equals(engine)) {
                                      violations.putIfAbsent(
                                          id,
                                          "emitter: subject="
                                              + subject
                                              + " corr="
                                              + correlation
                                              + " engine="
                                              + engine);
                                    }
                                  } finally {
                                    done.countDown();
                                  }
                                }));
                  }
                };

        Metadata md = new Metadata();
        md.put(subjectHeader, expectedSubject);
        md.put(correlationHeader, expectedCorrelation);
        md.put(engineHeader, expectedEngine);

        // Each call gets its own per-call duplicated context, as Quarkus's outermost
        // duplicated-context interceptor provides in production.
        io.vertx.core.Context dup =
            io.smallrye.common.vertx.VertxContext.createNewDuplicatedContext(
                vertx.getOrCreateContext());
        dup.runOnContext(
            ignored -> {
              try {
                ServerCall.Listener<Object> listener = wrap.interceptCall(stubCall(), md, handler);
                // Stir the shared io.grpc.Context slot with trailing events around the real work,
                // like the call's own listener-event replay does in production.
                listener.onReady();
                listener.onHalfClose();
                listener.onReady();
                listener.onComplete();
              } catch (Throwable t) {
                violations.putIfAbsent(id, "dispatch-failed=" + t);
                done.countDown();
              }
            });
      }

      assertTrue(
          done.await(30, TimeUnit.SECONDS),
          () -> "did not complete in 30s; remaining=" + done.getCount());
      if (!violations.isEmpty()) {
        StringBuilder sb = new StringBuilder();
        sb.append("call-context integrity violations: ")
            .append(violations.size())
            .append(" / ")
            .append(n);
        int printed = 0;
        for (Map.Entry<Integer, String> e : violations.entrySet()) {
          if (printed++ >= 5) break;
          sb.append("\n  id=").append(e.getKey()).append(' ').append(e.getValue());
        }
        fail(sb.toString());
      }
    } finally {
      subscriptionPool.shutdownNow();
      vertx.close().toCompletionStage().toCompletableFuture().get();
    }
  }

  // ---------- helpers ----------

  private static void drive(
      Vertx vertx, ServerInterceptor wrap, ServerCallHandler<Object, Object> handler)
      throws Exception {
    CompletableFuture<Void> driven = new CompletableFuture<>();
    vertx.runOnContext(
        ignored -> {
          try {
            ServerCall.Listener<Object> listener =
                wrap.interceptCall(stubCall(), new Metadata(), handler);
            listener.onHalfClose();
            driven.complete(null);
          } catch (Throwable t) {
            driven.completeExceptionally(t);
          }
        });
    driven.get(5, TimeUnit.SECONDS);
  }

  private static ServerCallHandler<Object, Object> capturingHandler(
      AtomicReference<String> sink, CountDownLatch done) {
    return (call, headers) ->
        new ServerCall.Listener<>() {
          @Override
          public void onHalfClose() {
            String v = KEY.get();
            sink.set(v == null ? "<null>" : v);
            done.countDown();
          }
        };
  }

  private static ServerCallHandler<Object, Object> noopHandler() {
    return (call, headers) -> new ServerCall.Listener<>() {};
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static ServerCall<Object, Object> stubCall() {
    ServerCall mocked = mock(ServerCall.class);
    MethodDescriptor<Object, Object> method =
        MethodDescriptor.newBuilder()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName("test/Method")
            .setRequestMarshaller(new NoopMarshaller())
            .setResponseMarshaller(new NoopMarshaller())
            .build();
    when(mocked.getMethodDescriptor()).thenReturn(method);
    when(mocked.getAttributes()).thenReturn(Attributes.EMPTY);
    return mocked;
  }

  private static final class NoopMarshaller implements MethodDescriptor.Marshaller<Object> {
    @Override
    public InputStream stream(Object value) {
      return new ByteArrayInputStream(new byte[0]);
    }

    @Override
    public Object parse(InputStream stream) {
      return new Object();
    }
  }
}
