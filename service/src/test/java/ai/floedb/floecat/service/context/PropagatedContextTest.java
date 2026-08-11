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
package ai.floedb.floecat.service.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.connector.spi.AuthResolutionContext;
import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.context.impl.InboundContextInterceptor;
import ai.floedb.floecat.service.context.impl.ResolvedCallContexts;
import ai.floedb.floecat.service.credentials.AuthResolutionContexts;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.smallrye.common.vertx.VertxContext;
import io.vertx.core.Vertx;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.jboss.logging.MDC;
import org.junit.jupiter.api.Test;

/**
 * Off-thread propagation is the whole point of {@link PropagatedContext}: a fan-out worker never
 * inherits the request thread's thread-locals, so without re-establishing them the ambient reads
 * ({@code engineContext()}, the log MDC) read empty. These tests capture on a context-bearing
 * thread and assert the body sees that context on a plain executor thread that carries none of its
 * own.
 */
class PropagatedContextTest {

  private static final java.util.concurrent.ScheduledExecutorService deadlineScheduler =
      java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
          r -> {
            Thread t = new Thread(r, "test-deadline");
            t.setDaemon(true);
            return t;
          });

  @Test
  void aThrowingBodyStillRestoresEveryCarrier() throws Exception {
    // supply()'s try/finally nesting is justified only in comments — that an exception between the
    // gRPC attach and the body must not skip the detach, and that the detach must run even if the
    // MDC restore throws. Nothing asserted it. Run on a pooled thread deliberately left carrying a
    // foreign gRPC context and a stale MDC key, so a missed restore is visible.
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      ResolvedCallContext resolved = resolvedWithEngine("spark");
      PropagatedContext captured =
          ResolvedCallContexts.callWith(resolved, PropagatedContext::capture);
      io.grpc.Context foreign =
          io.grpc.Context.current()
              .withValue(
                  InboundContextInterceptor.PC_KEY,
                  PrincipalContext.newBuilder().setSubject("foreign").build());

      Object[] after =
          worker
              .submit(
                  () -> {
                    MDC.put("leftover", "stale");
                    io.grpc.Context priorGrpc = foreign.attach();
                    try {
                      assertThatThrownBy(
                              () ->
                                  captured.supply(
                                      () -> {
                                        throw new IllegalStateException("body failed");
                                      }))
                          .isInstanceOf(IllegalStateException.class)
                          .hasMessage("body failed");
                      return new Object[] {
                        InboundContextInterceptor.PC_KEY.get().getSubject(),
                        MDC.get("leftover"),
                        MDC.get("floecat_engine_kind")
                      };
                    } finally {
                      foreign.detach(priorGrpc);
                      MDC.clear();
                    }
                  })
              .get(5, TimeUnit.SECONDS);

      assertThat(after[0])
          .as("the worker's own gRPC context must be reattached after a throwing body")
          .isEqualTo("foreign");
      assertThat(after[1]).as("the worker's own MDC must be restored").isEqualTo("stale");
      assertThat(after[2]).as("no captured MDC key may leak past the body").isNull();
    } finally {
      worker.shutdownNow();
    }
  }

  @Test
  void propagatesTheOtelSpanToTheWorkerSoOffThreadTracesStayAttached() throws Exception {
    // The class promises to re-establish OpenTelemetry context across the hop; assert span identity
    // on the worker, which no other test did. (capture()'s other half — grafting the call span when
    // the captured context has none — needs a Vert.x duplicated context. The graft helper itself is
    // pinned by ResolvedCallContextsSpanTest; capture()'s CALL of it is not covered here.)
    SpanContext spanContext =
        SpanContext.create(
            "00000000000000000000000000000abc",
            "0000000000000abc",
            TraceFlags.getSampled(),
            TraceState.getDefault());
    Span span = Span.wrap(spanContext);
    ExecutorService foreign = Executors.newSingleThreadExecutor();
    try (Scope ignored = Context.current().with(span).makeCurrent()) {
      PropagatedContext captured = PropagatedContext.capture();
      String seenSpanId =
          foreign
              .submit(() -> captured.supply(() -> Span.current().getSpanContext().getSpanId()))
              .get();
      assertThat(seenSpanId).isEqualTo("0000000000000abc");
    } finally {
      foreign.shutdownNow();
    }
  }

  @Test
  void anEmptyCaptureIsolatesFromAForeignScopeOnTheExecutionThread() throws Exception {
    // Captured off any request, so the captured call context is null. When that body later runs on
    // a thread already bearing a foreign request scope, it must not inherit that scope's principal
    // or engine — supply() installs an explicitly empty scope for the body and restores the foreign
    // one afterward.
    PropagatedContext captured = PropagatedContext.capture();
    ResolvedCallContext foreign = resolvedWithEngine("spark");
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      worker
          .submit(
              () ->
                  ResolvedCallContexts.callWith(
                      foreign,
                      () -> {
                        ResolvedCallContext seenInBody =
                            captured.supply(ResolvedCallContexts::currentOrNull);
                        assertThat(seenInBody)
                            .as("off-request body must not inherit the worker's foreign scope")
                            .isNull();
                        assertThat(ResolvedCallContexts.currentOrNull())
                            .as("the foreign scope is restored after supply")
                            .isSameAs(foreign);
                        return null;
                      }))
          .get();
    } finally {
      worker.shutdownNow();
    }
  }

  @Test
  void anEmptyCaptureIsolatesFromAFallbackCarrierNotJustAnExplicitScope() throws Exception {
    // The worker bears a foreign request only through a fallback carrier (io.grpc.Context keys),
    // not
    // an explicit scope. An off-request capture must still isolate: the explicitly empty scope that
    // supply() installs wins over the fallback, so the body observes no foreign principal or
    // engine.
    PropagatedContext captured = PropagatedContext.capture(); // off-request: call == null
    io.grpc.Context grpcWithForeignRequest =
        io.grpc.Context.current()
            .withValue(
                InboundContextInterceptor.PC_KEY,
                PrincipalContext.newBuilder().setSubject("foreign-subject").build());
    grpcWithForeignRequest.call(
        () -> {
          // Premise of the test: the fallback carrier really does resolve, and to the foreign
          // request. A bare isNotNull() here would also pass if it resolved to something else,
          // leaving the isolation assertion below testing nothing in particular.
          assertThat(ResolvedCallContexts.currentOrNull())
              .isNotNull()
              .extracting(c -> c.principalContext().getSubject())
              .isEqualTo("foreign-subject");
          ResolvedCallContext seenInBody = captured.supply(ResolvedCallContexts::currentOrNull);
          assertThat(seenInBody)
              .as("an off-request body must not inherit a foreign request from a fallback carrier")
              .isNull();
          return null;
        });
  }

  @Test
  void anOnRequestCaptureCarriesTheGrpcContextKeysIntoTheBody() throws Exception {
    // The counterpart to the isolation test below: several readers consult ONLY gRPC keys — the
    // session/authorization tokens AuthResolutionContexts reads, and the principal/engine fallbacks
    // —
    // so the captured context has to travel, not be detached. Detaching it would hand fan-out tasks
    // empty credentials, breaking connector credential resolution while every ResolvedCallContext
    // assertion still passed.
    PrincipalContext principal =
        PrincipalContext.newBuilder().setAccountId("acct-1").setSubject("subject-1").build();
    io.grpc.Context inbound =
        io.grpc.Context.current()
            .withValue(InboundContextInterceptor.PC_KEY, principal)
            .withValue(
                InboundContextInterceptor.ENGINE_CONTEXT_KEY, EngineContext.of("duckdb", "1"))
            .withValue(InboundContextInterceptor.SESSION_HEADER_VALUE_KEY, "session-token")
            .withValue(InboundContextInterceptor.AUTHORIZATION_HEADER_VALUE_KEY, "Bearer real");
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      PropagatedContext captured = inbound.call(PropagatedContext::capture);
      worker
          .submit(
              () ->
                  captured.supply(
                      () -> {
                        assertThat(InboundContextInterceptor.SESSION_HEADER_VALUE_KEY.get())
                            .as("the request's session token must reach the worker")
                            .isEqualTo("session-token");
                        assertThat(InboundContextInterceptor.AUTHORIZATION_HEADER_VALUE_KEY.get())
                            .as("and its authorization token")
                            .isEqualTo("Bearer real");
                        assertThat(InboundContextInterceptor.PC_KEY.get()).isEqualTo(principal);
                        return null;
                      }))
          .get();
    } finally {
      worker.shutdownNow();
    }
  }

  @Test
  void theInboundDeadlineAndCancellationReachTheBody() throws Exception {
    // The captured gRPC context must keep its cancellable ancestor. Context.fork() drops it, and
    // then getDeadline() returns null and isCancelled() is permanently false — the body silently
    // loses the request's deadline and can never observe client cancellation, which for
    // non-interruptible store work means it keeps running after the request is gone.
    io.grpc.Context.CancellableContext cancellable =
        io.grpc.Context.current().withDeadlineAfter(30, TimeUnit.SECONDS, deadlineScheduler);
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      PropagatedContext captured = cancellable.call(PropagatedContext::capture);
      io.grpc.Deadline inboundDeadline = cancellable.getDeadline();
      assertThat(inboundDeadline).isNotNull();

      worker
          .submit(
              () ->
                  captured.supply(
                      () -> {
                        assertThat(io.grpc.Context.current().getDeadline())
                            .as("the worker must see the inbound deadline, not a fresh one")
                            .isSameAs(inboundDeadline);
                        assertThat(io.grpc.Context.current().isCancelled()).isFalse();
                        return null;
                      }))
          .get();

      cancellable.cancel(new RuntimeException("client went away"));
      worker
          .submit(
              () ->
                  captured.supply(
                      () -> {
                        assertThat(io.grpc.Context.current().isCancelled())
                            .as("client cancellation must be observable in the body")
                            .isTrue();
                        return null;
                      }))
          .get();
    } finally {
      cancellable.close();
      worker.shutdownNow();
    }
  }

  @Test
  void anEmptyCaptureIsolatesTheGrpcContextFallbackReadersToo() throws Exception {
    // The empty call scope only makes ResolvedCallContexts.currentOrNull() null. Several readers
    // bypass it and fall back straight to io.grpc.Context keys — PrincipalProvider.get(),
    // EngineContextProvider.engineContext(), the inbound session/authorization keys — so without
    // detaching that context an off-request body would read the FOREIGN request's principal and
    // engine through its own supposedly-isolated scope. This is the cross-request leak the scope
    // alone does not close.
    PropagatedContext captured = PropagatedContext.capture(); // off-request: call == null
    PrincipalContext foreignPrincipal =
        PrincipalContext.newBuilder()
            .setAccountId("other-acct")
            .setSubject("other-subject")
            .build();
    io.grpc.Context foreignRequest =
        io.grpc.Context.current()
            .withValue(InboundContextInterceptor.PC_KEY, foreignPrincipal)
            .withValue(InboundContextInterceptor.ENGINE_CONTEXT_KEY, EngineContext.of("spark", "3"))
            .withValue(InboundContextInterceptor.AUTHORIZATION_HEADER_VALUE_KEY, "Bearer foreign");
    var principals = new PrincipalProvider();
    var engines = new EngineContextProvider();

    foreignRequest.call(
        () -> {
          // Sanity: on this thread the foreign request really is visible through the fallbacks.
          assertThat(principals.get().getSubject()).isEqualTo("other-subject");

          captured.supply(
              () -> {
                assertThat(principals.get().getSubject())
                    .as("an off-request body must not inherit the foreign principal")
                    .isEmpty();
                assertThat(engines.engineContext().engineKind())
                    .as("nor the foreign engine — an isolated body resolves to no engine at all")
                    .isEmpty();
                assertThat(InboundContextInterceptor.AUTHORIZATION_HEADER_VALUE_KEY.get())
                    .as("nor the foreign authorization token")
                    .isNull();
                return null;
              });

          // The foreign context is restored for the caller afterward.
          assertThat(principals.get().getSubject()).isEqualTo("other-subject");
          return null;
        });
  }

  @Test
  void supplyRefusesToRunWhereTheCarriersAreSharedRatherThanOwned() throws Exception {
    // On a Quarkus duplicated Vert.x context both the MDC map and the gRPC attach slot are shared
    // by
    // every task on it. Propagating there would corrupt their MDC AND — because the gRPC context
    // cannot be attached either — run the body under the captured principal while the key-only
    // readers (session/authorization tokens) see whatever ambient request the thread carries. That
    // mixed identity is worse than a failed task, so this fails fast. No production path reaches
    // it:
    // supply runs on the fan-out's virtual threads or the admission pool's platform threads.
    MDC.put("floecat_component", "ambient-component");
    PropagatedContext captured = PropagatedContext.capture();
    Vertx vertx = Vertx.vertx();
    try {
      assertThatThrownBy(
              () ->
                  onDuplicatedContext(
                      vertx,
                      () -> {
                        MDC.put("floecat_component", "shared-context-owner");
                        return captured.supply(() -> "unreachable");
                      }))
          .cause()
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("owns its MDC and gRPC context");

      // And it refuses BEFORE touching either carrier: the shared context's MDC is left intact.
      String ambient =
          onDuplicatedContext(
              vertx,
              () -> {
                MDC.put("floecat_component", "shared-context-owner");
                try {
                  captured.supply(() -> "unreachable");
                } catch (IllegalStateException expected) {
                  // fall through to read the MDC back
                }
                return String.valueOf(MDC.get("floecat_component"));
              });
      assertThat(ambient)
          .as("the shared context's MDC must not be cleared or replaced")
          .isEqualTo("shared-context-owner");
    } finally {
      vertx.close();
      MDC.remove("floecat_component");
    }
  }

  @Test
  void captureGraftsTheCallSpanSoAWorkerStaysOnTheRequestTrace() throws Exception {
    // capture()'s second half: the gRPC server span is not current at the service-method layer, it
    // lives on the duplicated-context carrier. Without the graft a captured context has no valid
    // span, the worker re-roots its trace, and off-thread spans silently detach from the request.
    // ResolvedCallContextsSpanTest pins withCurrentCallSpan itself; this pins capture()'s use of
    // it,
    // which nothing did — removing the call left all 20 context tests green.
    Span carried =
        Span.wrap(
            SpanContext.createFromRemoteParent(
                "0af7651916cd43dd8448eb211c80319c",
                "b7ad6b7169203331",
                TraceFlags.getSampled(),
                TraceState.getDefault()));
    Vertx vertx = Vertx.vertx();
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      PropagatedContext captured =
          onDuplicatedContext(
              vertx,
              () -> {
                ResolvedCallContexts.storeSpanOnDuplicatedContext(carried);
                assertThat(Span.current().getSpanContext().isValid())
                    .as("premise: no span is current at the capture point")
                    .isFalse();
                return PropagatedContext.capture();
              });

      String seenSpanId =
          worker
              .submit(() -> captured.supply(() -> Span.current().getSpanContext().getSpanId()))
              .get(5, TimeUnit.SECONDS);

      assertThat(seenSpanId)
          .as("the worker must run under the request's server span, not a fresh trace root")
          .isEqualTo("b7ad6b7169203331");
    } finally {
      worker.shutdownNow();
      vertx.close();
    }
  }

  @Test
  void connectorCredentialsFollowTheResolvedContextNotTheGrpcCarrier() throws Exception {
    // The #361 shape, end to end. Request A is captured with its tokens on the RESOLVED context
    // while the raw io.grpc.Context carries none — the state the duplicated-context race produces.
    // The body then runs on a pooled worker where foreign request B's gRPC keys are ambient.
    //
    // AuthResolutionContexts is the last reader of those tokens, and it used to read the gRPC keys
    // only. That made a fan-out task resolve connector credentials with blank tokens (or, on a
    // reused worker whose captured slot went stale, request B's) while every other part of the task
    // ran as request A — the cross-request leak this class exists to prevent.
    ResolvedCallContext requestA =
        new ResolvedCallContext(
            PrincipalContext.newBuilder().setSubject("subject-a").build(),
            "query-a",
            "corr-a",
            EngineContext.of("duckdb", "1.0"),
            "session-a",
            "authorization-a");

    PropagatedContext captured =
        ResolvedCallContexts.callWith(requestA, PropagatedContext::capture);

    io.grpc.Context foreignRequestB =
        io.grpc.Context.current()
            .withValue(
                InboundContextInterceptor.PC_KEY,
                PrincipalContext.newBuilder().setSubject("subject-b").build())
            .withValue(InboundContextInterceptor.SESSION_HEADER_VALUE_KEY, "session-b")
            .withValue(InboundContextInterceptor.AUTHORIZATION_HEADER_VALUE_KEY, "authorization-b");

    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      AuthResolutionContext seen =
          worker
              .submit(
                  () -> {
                    io.grpc.Context prior = foreignRequestB.attach();
                    try {
                      return captured.supply(AuthResolutionContexts::fromInboundContext);
                    } finally {
                      foreignRequestB.detach(prior);
                    }
                  })
              .get(5, TimeUnit.SECONDS);

      assertThat(seen.sessionToken())
          .as(
              "the worker must resolve credentials as request A, whose tokens only the resolved"
                  + " context carries")
          .isEqualTo("session-a");
      assertThat(seen.authorizationToken()).isEqualTo("authorization-a");
    } finally {
      worker.shutdownNow();
    }
  }

  @Test
  void connectorCredentialsStillFallBackToTheGrpcKeysOffAnyResolvedContext() {
    // The fallback branch: no resolved context at all (a non-Vert.x thread, or a caller driving
    // io.grpc.Context directly). The gRPC keys remain the source there, so the preference above
    // cannot blank out credentials for those callers.
    io.grpc.Context onlyGrpcKeys =
        io.grpc.Context.current()
            .withValue(InboundContextInterceptor.SESSION_HEADER_VALUE_KEY, "session-legacy")
            .withValue(InboundContextInterceptor.AUTHORIZATION_HEADER_VALUE_KEY, "auth-legacy");

    AuthResolutionContext seen =
        ResolvedCallContexts.callWithoutRequestScope(
            () -> onlyGrpcKeys.call(AuthResolutionContexts::fromInboundContext));

    assertThat(seen.sessionToken()).isEqualTo("session-legacy");
    assertThat(seen.authorizationToken()).isEqualTo("auth-legacy");
  }

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

  private static ResolvedCallContext resolvedWithEngine(String engineKind) {
    return new ResolvedCallContext(
        PrincipalContext.getDefaultInstance(),
        "query-1",
        "corr-1",
        EngineContext.of(engineKind, "16.0"),
        null,
        null);
  }

  @Test
  void reEstablishesAmbientContextOnAForeignThread() throws Exception {
    ResolvedCallContext resolved = resolvedWithEngine("duckdb");
    // A single-thread pool that never saw the request: its thread-locals are empty until supply()
    // re-establishes them, so a wrong read here would be the real regression.
    ExecutorService foreign = Executors.newSingleThreadExecutor();
    MDC.put("floecat_component", "query-resolver");
    MDC.put("floecat_operation", "resolve-inputs");
    try {
      String engineKind =
          ResolvedCallContexts.callWith(
              resolved,
              () -> {
                PropagatedContext captured = PropagatedContext.capture();
                return foreign
                    .submit(
                        () ->
                            captured.supply(
                                () -> {
                                  ResolvedCallContext seen = ResolvedCallContexts.currentOrNull();
                                  assertThat(seen)
                                      .as("the captured call context must cross the thread hop")
                                      .isSameAs(resolved);
                                  // MDC is derived from the call context, so off-thread log lines
                                  // carry the request's ids too.
                                  assertThat(MDC.get("floecat_engine_kind")).isEqualTo("duckdb");
                                  assertThat(MDC.get("correlation_id")).isEqualTo("corr-1");
                                  assertThat(MDC.get("floecat_component"))
                                      .isEqualTo("query-resolver");
                                  assertThat(MDC.get("floecat_operation"))
                                      .isEqualTo("resolve-inputs");
                                  return seen.engineContext().engineKind();
                                }))
                    .get();
              });
      assertThat(engineKind).isEqualTo("duckdb");
    } finally {
      foreign.shutdownNow();
      MDC.remove("floecat_component");
      MDC.remove("floecat_operation");
    }
  }

  @Test
  void clearsMdcAfterTheBodySoAPooledThreadDoesNotLeakIt() throws Exception {
    ResolvedCallContext resolved = resolvedWithEngine("spark");
    ExecutorService foreign = Executors.newSingleThreadExecutor();
    try {
      // MDC.get is typed Object, so keep the carrier Object too rather than casting.
      Object mdcAfterBody =
          ResolvedCallContexts.callWith(
              resolved,
              () -> {
                PropagatedContext captured = PropagatedContext.capture();
                return foreign
                    .submit(
                        () -> {
                          captured.supply(
                              () -> {
                                assertThat(MDC.get("floecat_engine_kind")).isEqualTo("spark");
                                return null;
                              });
                          // Same pooled thread, outside the body: the request's ids must be gone so
                          // the next unrelated task cannot inherit them.
                          return MDC.get("floecat_engine_kind");
                        })
                    .get();
              });
      assertThat(mdcAfterBody).isNull();
    } finally {
      foreign.shutdownNow();
    }
  }

  @Test
  void clearsExtensionMdcKeysAddedByTheBody() throws Exception {
    ExecutorService foreign = Executors.newSingleThreadExecutor();
    try {
      Object mdcAfterBody =
          foreign
              .submit(
                  () -> {
                    PropagatedContext.capture()
                        .supply(
                            () -> {
                              MDC.put("extension_tenant", "tenant-a");
                              return null;
                            });
                    return MDC.get("extension_tenant");
                  })
              .get();

      assertThat(mdcAfterBody).isNull();
    } finally {
      foreign.shutdownNow();
    }
  }

  @Test
  void replacesForeignWorkerMdcWithTheCompleteCapturedMap() throws Exception {
    ExecutorService foreign = Executors.newSingleThreadExecutor();
    MDC.put("extension_tenant", "request-tenant");
    try {
      PropagatedContext captured = PropagatedContext.capture();
      foreign.submit(() -> MDC.put("extension_tenant", "stale-worker-tenant")).get();

      Object tenantSeenByBody =
          foreign.submit(() -> captured.supply(() -> MDC.get("extension_tenant"))).get();
      Object tenantRestoredAfterBody = foreign.submit(() -> MDC.get("extension_tenant")).get();

      assertThat(tenantSeenByBody).isEqualTo("request-tenant");
      assertThat(tenantRestoredAfterBody).isEqualTo("stale-worker-tenant");
    } finally {
      MDC.remove("extension_tenant");
      foreign.shutdownNow();
    }
  }

  @Test
  void restoresExistingMdcAfterAnInlineNestedScope() {
    MDC.put("floecat_component", "outer-component");
    MDC.put("floecat_operation", "outer-operation");
    MDC.put("correlation_id", "outer-correlation");
    try {
      ResolvedCallContexts.callWith(
          resolvedWithEngine("spark"),
          () -> {
            PropagatedContext.capture()
                .supply(
                    () -> {
                      assertThat(MDC.get("floecat_engine_kind")).isEqualTo("spark");
                      return null;
                    });
            return null;
          });

      assertThat(MDC.get("floecat_component")).isEqualTo("outer-component");
      assertThat(MDC.get("floecat_operation")).isEqualTo("outer-operation");
      assertThat(MDC.get("correlation_id")).isEqualTo("outer-correlation");
    } finally {
      MDC.remove("floecat_component");
      MDC.remove("floecat_operation");
      MDC.remove("correlation_id");
    }
  }

  @Test
  void offAnyRequestTheBodyStillRuns() {
    // Captured with no ambient request (startup, unit tests): supply must run the body rather than
    // fail, and no context is fabricated.
    PropagatedContext captured = PropagatedContext.capture();
    assertThat(captured.supply(() -> "ran")).isEqualTo("ran");
    assertThat(ResolvedCallContexts.currentOrNull()).isNull();
  }

  @Test
  void propagatesTheRequestCancellationSignalToTheWorkerAndClearsItAfter() throws Exception {
    // An auto-admitted store read on a fan-out worker must see the request's live cancellation
    // signal
    // so it can abort an admission wait; the worker carries none of its own, so supply() must
    // re-establish it (still reflecting later flips of the same signal) and clear it afterward.
    ExecutorService foreign = Executors.newSingleThreadExecutor();
    AtomicBoolean cancelled = new AtomicBoolean(false);
    try (PropagatedContext.CancellationScope ignored =
        PropagatedContext.bindCancellation(cancelled::get)) {
      PropagatedContext captured = PropagatedContext.capture();
      Object cancellationAfterBody =
          foreign
              .submit(
                  () -> {
                    assertThat(PropagatedContext.currentCancellation()).isNull();
                    captured.supply(
                        () -> {
                          BooleanSupplier seen = PropagatedContext.currentCancellation();
                          assertThat(seen).isNotNull();
                          assertThat(seen.getAsBoolean()).isFalse();
                          cancelled.set(true);
                          assertThat(seen.getAsBoolean()).isTrue();
                          return null;
                        });
                    return PropagatedContext.currentCancellation();
                  })
              .get();
      assertThat(cancellationAfterBody).isNull();
    } finally {
      foreign.shutdownNow();
    }
  }

  @Test
  void bindCancellationRestoresThePriorSignalWhenClosed() {
    assertThat(PropagatedContext.currentCancellation()).isNull();
    BooleanSupplier outer = () -> false;
    try (PropagatedContext.CancellationScope outerScope =
        PropagatedContext.bindCancellation(outer)) {
      assertThat(PropagatedContext.currentCancellation()).isSameAs(outer);
      BooleanSupplier inner = () -> true;
      try (PropagatedContext.CancellationScope innerScope =
          PropagatedContext.bindCancellation(inner)) {
        assertThat(PropagatedContext.currentCancellation()).isSameAs(inner);
      }
      assertThat(PropagatedContext.currentCancellation()).isSameAs(outer);
    }
    assertThat(PropagatedContext.currentCancellation()).isNull();
  }
}
