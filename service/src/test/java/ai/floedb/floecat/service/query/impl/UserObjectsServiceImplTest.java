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

package ai.floedb.floecat.service.query.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.GetUserObjectsRequest;
import ai.floedb.floecat.query.rpc.TableReferenceCandidate;
import ai.floedb.floecat.query.rpc.UserObjectsBundleChunk;
import ai.floedb.floecat.service.context.impl.ContextStore;
import ai.floedb.floecat.service.context.impl.InboundContextInterceptor;
import ai.floedb.floecat.service.query.catalog.UserObjectBundleService;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import io.grpc.Context;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Verifies that the gRPC principal context is propagated to the worker thread during streaming item
 * emission in {@link UserObjectsServiceImpl#getUserObjects}. Without the {@code grpcCtx.run()}
 * wrapping the entire subscription, {@code principal.get()} on the executor thread returns the
 * default (empty) {@link PrincipalContext}, causing {@code requireAccountId()} to fail with "key
 * arg 'account_id' is null/blank".
 */
class UserObjectsServiceImplTest {

  private static final String ACCOUNT_ID = "test-account-id";

  @Test
  void principalContextIsAvailableDuringItemEmission() {
    // Set up a mock UserObjectBundleService whose stream() reads principal.get() on the
    // calling thread — simulating what UserGraph.requireAccountId() does during name resolution.
    AtomicReference<String> observedAccountId = new AtomicReference<>();
    PrincipalProvider principalProvider = new PrincipalProvider();

    UserObjectBundleService mockBundles = Mockito.mock(UserObjectBundleService.class);
    Mockito.when(
            mockBundles.stream(
                Mockito.anyString(), Mockito.any(QueryContext.class), Mockito.anyList()))
        .thenAnswer(
            _ -> {
              // This runs inside grpcCtx.call/run — context should be set.
              // Return a Multi that reads principal.get() during item emission (on the
              // executor thread). This is the critical path that fails without the fix.
              return Multi.createFrom()
                  .items(UserObjectsBundleChunk.getDefaultInstance())
                  .onItem()
                  .invoke(
                      _ -> {
                        // This lambda runs during item emission on the executor thread.
                        // Without the fix, the gRPC context has already been detached so
                        // principal.get() returns the default instance with empty accountId.
                        PrincipalContext ctx = principalProvider.get();
                        observedAccountId.set(ctx.getAccountId());
                      });
            });

    // Set up query context store with a test context
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    QueryContext queryContext =
        QueryContext.builder()
            .queryId("q-1")
            .principal(
                PrincipalContext.newBuilder()
                    .setAccountId(ACCOUNT_ID)
                    .setSubject("tester")
                    .addPermissions("catalog.read")
                    .build())
            .snapshotSet(new byte[0])
            .createdAtMs(1)
            .expiresAtMs(Long.MAX_VALUE)
            .state(QueryContext.State.ACTIVE)
            .version(1)
            .queryDefaultCatalogId(
                ResourceId.newBuilder()
                    .setAccountId(ACCOUNT_ID)
                    .setId("cat")
                    .setKind(ResourceKind.RK_CATALOG)
                    .build())
            .build();
    store.seed(queryContext);

    // Wire up the service
    UserObjectsServiceImpl service = new UserObjectsServiceImpl();
    service.principal = principalProvider;
    service.authz = new Authorizer();
    service.queryStore = store;
    service.bundles = mockBundles;

    // Build request
    GetUserObjectsRequest request =
        GetUserObjectsRequest.newBuilder()
            .setQueryId("q-1")
            .addTables(TableReferenceCandidate.getDefaultInstance())
            .build();

    // Set the gRPC principal context (simulating what floecat's interceptor does) and invoke.
    // The context is set on the TEST thread; getUserObjects() runs the Multi on an executor
    // thread. The fix ensures the context is propagated to that executor thread.
    PrincipalContext principal =
        PrincipalContext.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setSubject("tester")
            .addPermissions("catalog.read")
            .build();
    Context grpcCtx =
        ContextStore.set(Context.current(), InboundContextInterceptor.PC_KEY, principal);
    Context previous = grpcCtx.attach();
    try {
      List<UserObjectsBundleChunk> chunks =
          service.getUserObjects(request).collect().asList().await().indefinitely();
      assertFalse(chunks.isEmpty(), "Expected at least one chunk");
    } finally {
      grpcCtx.detach(previous);
    }

    // The critical assertion: principal.get().getAccountId() during item emission
    // must return the account from the gRPC context, not empty string.
    assertEquals(
        ACCOUNT_ID,
        observedAccountId.get(),
        "principal.get() on the executor thread should see the gRPC context's accountId");
  }

  @Test
  void cancellingOuterStreamCancelsInnerBundleSubscription() throws Exception {
    // Set up a bundle service that emits items slowly, so we can cancel mid-stream.
    // We track whether the inner subscription was cancelled.
    CountDownLatch innerCancelled = new CountDownLatch(1);
    CountDownLatch firstItemEmitted = new CountDownLatch(1);
    PrincipalProvider principalProvider = new PrincipalProvider();

    UserObjectBundleService mockBundles = Mockito.mock(UserObjectBundleService.class);
    Mockito.when(
            mockBundles.stream(
                Mockito.anyString(), Mockito.any(QueryContext.class), Mockito.anyList()))
        .thenReturn(
            Multi.createFrom()
                .<UserObjectsBundleChunk>emitter(
                    innerEmitter -> {
                      // Emit one item immediately, then wait — simulating a long-running stream.
                      innerEmitter.emit(UserObjectsBundleChunk.getDefaultInstance());
                      firstItemEmitted.countDown();
                      // Register cancellation callback so we can assert it was called.
                      innerEmitter.onTermination(innerCancelled::countDown);
                    }));

    // Set up query context store with an active context
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    QueryContext queryContext =
        QueryContext.builder()
            .queryId("q-cancel")
            .principal(
                PrincipalContext.newBuilder()
                    .setAccountId(ACCOUNT_ID)
                    .setSubject("tester")
                    .addPermissions("catalog.read")
                    .build())
            .snapshotSet(new byte[0])
            .createdAtMs(1)
            .expiresAtMs(Long.MAX_VALUE)
            .state(QueryContext.State.ACTIVE)
            .version(1)
            .queryDefaultCatalogId(
                ResourceId.newBuilder()
                    .setAccountId(ACCOUNT_ID)
                    .setId("cat")
                    .setKind(ResourceKind.RK_CATALOG)
                    .build())
            .build();
    store.seed(queryContext);

    // Wire up the service
    UserObjectsServiceImpl service = new UserObjectsServiceImpl();
    service.principal = principalProvider;
    service.authz = new Authorizer();
    service.queryStore = store;
    service.bundles = mockBundles;

    // Build request
    GetUserObjectsRequest request =
        GetUserObjectsRequest.newBuilder()
            .setQueryId("q-cancel")
            .addTables(TableReferenceCandidate.getDefaultInstance())
            .build();

    // Subscribe within a gRPC context, then cancel after the first item arrives.
    PrincipalContext principal =
        PrincipalContext.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setSubject("tester")
            .addPermissions("catalog.read")
            .build();
    Context grpcCtx =
        ContextStore.set(Context.current(), InboundContextInterceptor.PC_KEY, principal);
    Context previous = grpcCtx.attach();
    try {
      AtomicReference<Subscription> subscriptionRef = new AtomicReference<>();
      CountDownLatch itemReceived = new CountDownLatch(1);

      service
          .getUserObjects(request)
          .subscribe()
          .withSubscriber(
              new MultiSubscriber<UserObjectsBundleChunk>() {
                @Override
                public void onItem(UserObjectsBundleChunk item) {
                  itemReceived.countDown();
                }

                @Override
                public void onFailure(Throwable failure) {}

                @Override
                public void onCompletion() {}

                @Override
                public void onSubscribe(Subscription s) {
                  subscriptionRef.set(s);
                  s.request(Long.MAX_VALUE);
                }
              });

      // Wait for the first item to be emitted, then cancel.
      assertTrue(
          itemReceived.await(5, TimeUnit.SECONDS), "Timed out waiting for first item emission");
      subscriptionRef.get().cancel();

      // Give cancellation a moment to propagate through the reactive pipeline.
      assertTrue(
          innerCancelled.await(5, TimeUnit.SECONDS),
          "Inner bundle subscription should have been cancelled when the outer stream was cancelled");
    } finally {
      grpcCtx.detach(previous);
    }
  }
}
