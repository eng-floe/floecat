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

package ai.floedb.floecat.service.security.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.context.impl.ResolvedCallContexts;
import io.grpc.Context;
import io.smallrye.common.vertx.VertxContext;
import io.vertx.core.Vertx;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for the resolved-call-context carrier as read through {@link PrincipalProvider}.
 *
 * <p>These tests model the production failure deterministically: the whole {@link
 * ResolvedCallContext} is stored on a Vert.x duplicated context by the inbound interceptor, then
 * read back from the service-method body, which runs on a worker after Quarkus's gRPC dispatch
 * thread-hop — a point where the {@code io.grpc.Context} keys are absent (or, on a reused worker,
 * stale). The duplicated-context local must carry principal, correlation id, and engine context
 * across that hop the same way MDC does.
 */
class PrincipalProviderTest {

  private static final PrincipalContext PRINCIPAL =
      PrincipalContext.newBuilder()
          .setSubject("dev-user")
          .setAccountId("5eaa9cd5")
          .addPermissions("catalog.read")
          .build();

  private static final ResolvedCallContext RESOLVED =
      new ResolvedCallContext(
          PRINCIPAL, "q-1", "corr-1", EngineContext.of("test-engine", "16.0"), null, null);

  @FunctionalInterface
  private interface Body {
    PrincipalContext run();
  }

  /**
   * The core fix: a call context stored on the duplicated context survives the worker thread-hop
   * into the service body and is read back even though {@code io.grpc.Context} carries no principal
   * there. Before the fix this returned {@link PrincipalContext#getDefaultInstance()} → missing
   * {@code catalog.read}.
   */
  @Test
  void principalSurvivesWorkerHopWhenGrpcKeyAbsent() throws Exception {
    Vertx vertx = Vertx.vertx();
    try {
      PrincipalContext observed = storeThenRunOnWorker(vertx, () -> new PrincipalProvider().get());
      assertEquals("dev-user", observed.getSubject());
      assertEquals(List.of("catalog.read"), observed.getPermissionsList());
    } finally {
      close(vertx);
    }
  }

  /**
   * The whole context — not just the principal — survives the hop: correlation id and engine
   * context read back intact. Guards against the piecemeal-mirroring regression where the principal
   * survived but engine/correlation silently degraded (eng-floe/floecat#361).
   */
  @Test
  void correlationAndEngineSurviveWorkerHopWithPrincipal() throws Exception {
    Vertx vertx = Vertx.vertx();
    try {
      CompletableFuture<ResolvedCallContext> result = new CompletableFuture<>();
      io.vertx.core.Context dup =
          VertxContext.createNewDuplicatedContext(vertx.getOrCreateContext());
      dup.runOnContext(
          ignored -> {
            try {
              ResolvedCallContexts.storeOnDuplicatedContext(RESOLVED);
              Vertx.currentContext()
                  .<ResolvedCallContext>executeBlocking(
                      ResolvedCallContexts::currentOrUnauthenticated, false)
                  .onComplete(
                      ar -> {
                        if (ar.succeeded()) {
                          result.complete(ar.result());
                        } else {
                          result.completeExceptionally(ar.cause());
                        }
                      });
            } catch (Throwable t) {
              result.completeExceptionally(t);
            }
          });
      ResolvedCallContext observed = result.get(10, TimeUnit.SECONDS);
      assertEquals("corr-1", observed.correlationId());
      assertEquals("test-engine", observed.engineContext().engineKind());
      assertEquals("dev-user", observed.principalContext().getSubject());
    } finally {
      close(vertx);
    }
  }

  /**
   * The duplicated-context local (per-call, correct) is preferred over a non-null {@code
   * io.grpc.Context} key — which on a reused worker can hold a previous call's principal via the
   * thread-local fallback. Guards against cross-call principal bleed.
   */
  @Test
  void duplicatedContextLocalPreferredOverStaleGrpcKey() throws Exception {
    Vertx vertx = Vertx.vertx();
    try {
      PrincipalContext stale =
          PrincipalContext.newBuilder().setSubject("other-tenant").setAccountId("deadbeef").build();
      PrincipalContext observed =
          storeThenRunOnWorker(
              vertx,
              () -> {
                // A stale principal lingering on io.grpc.Context, as on a reused worker thread.
                Context grpc = Context.current().withValue(PrincipalProvider.KEY, stale);
                Context prev = grpc.attach();
                try {
                  return new PrincipalProvider().get();
                } finally {
                  grpc.detach(prev);
                }
              });
      assertEquals("dev-user", observed.getSubject(), "should prefer the per-call context local");
    } finally {
      close(vertx);
    }
  }

  /**
   * An explicit scope ({@code callWith}) outranks every other channel: it is the by-reference
   * handoff service bodies use across executor hops, where even the duplicated context may be
   * absent.
   */
  @Test
  void explicitScopeOutranksGrpcKey() throws Exception {
    PrincipalContext scoped =
        Context.ROOT.call(
            () -> ResolvedCallContexts.callWith(RESOLVED, () -> new PrincipalProvider().get()));
    assertEquals("dev-user", scoped.getSubject());
    // Outside the scope the channel is cleared again.
    PrincipalContext outside = Context.ROOT.call(() -> new PrincipalProvider().get());
    assertEquals(PrincipalContext.getDefaultInstance(), outside);
  }

  /** Off any Vert.x duplicated context, the {@code io.grpc.Context} key remains the carrier. */
  @Test
  void fallsBackToGrpcKeyOffDuplicatedContext() {
    Context grpc = Context.current().withValue(PrincipalProvider.KEY, PRINCIPAL);
    Context prev = grpc.attach();
    try {
      assertEquals("dev-user", new PrincipalProvider().get().getSubject());
    } finally {
      grpc.detach(prev);
    }
  }

  /**
   * With no principal on either channel, the empty default is returned (contract preserved). Runs
   * under a clean root gRPC context: the shared surefire JVM can leave an unrelated principal
   * attached on this thread's {@code io.grpc.Context}, which must not mask the default.
   */
  @Test
  void returnsDefaultWhenNoPrincipalAnywhere() throws Exception {
    PrincipalContext got = Context.ROOT.call(() -> new PrincipalProvider().get());
    assertEquals(PrincipalContext.getDefaultInstance(), got);
  }

  /** Storing off a duplicated context is a safe no-op, not an exception. */
  @Test
  void storeIsNoOpOffDuplicatedContext() throws Exception {
    PrincipalContext got =
        Context.ROOT.call(
            () -> {
              ResolvedCallContexts.storeOnDuplicatedContext(RESOLVED);
              return new PrincipalProvider().get();
            });
    assertEquals(PrincipalContext.getDefaultInstance(), got);
  }

  // ---------- helpers ----------

  /**
   * Models the real call shape: store the resolved call context on a fresh per-call duplicated
   * context (the inbound interceptor), then run {@code body} on a worker hop that stays on that
   * same duplicated context (the service body, dispatched off the event loop). The completion is
   * chained back asynchronously so the event loop is never blocked.
   */
  private static PrincipalContext storeThenRunOnWorker(Vertx vertx, Body body) throws Exception {
    CompletableFuture<PrincipalContext> result = new CompletableFuture<>();
    io.vertx.core.Context dup = VertxContext.createNewDuplicatedContext(vertx.getOrCreateContext());
    dup.runOnContext(
        ignored -> {
          try {
            ResolvedCallContexts.storeOnDuplicatedContext(RESOLVED);
            Vertx.currentContext()
                .<PrincipalContext>executeBlocking(body::run, false)
                .onComplete(
                    ar -> {
                      if (ar.succeeded()) {
                        result.complete(ar.result());
                      } else {
                        result.completeExceptionally(ar.cause());
                      }
                    });
          } catch (Throwable t) {
            result.completeExceptionally(t);
          }
        });
    return result.get(10, TimeUnit.SECONDS);
  }

  private static void close(Vertx vertx) throws Exception {
    vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
  }
}
