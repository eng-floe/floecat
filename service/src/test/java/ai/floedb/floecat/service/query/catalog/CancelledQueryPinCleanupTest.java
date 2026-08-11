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

package ai.floedb.floecat.service.query.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.RelationPinSet;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.testsupport.SnapshotTestSupport;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CancelledQueryPinCleanupTest {

  @Test
  void emptyPinSetDoesNotDispatchCleanup() {
    QueryContextStore queryStore = mock(QueryContextStore.class);
    AtomicInteger dispatches = new AtomicInteger();
    CancelledQueryPinCleanup cleanup =
        new CancelledQueryPinCleanup(
            queryStore,
            task -> {
              dispatches.incrementAndGet();
              task.run();
            });

    cleanup.release("query", RelationPinSet.getDefaultInstance());

    assertThat(dispatches).hasValue(0);
    verifyNoInteractions(queryStore);
  }

  @Test
  void rejectedDispatchRetainsRootsForNonBlockingRetry() {
    QueryContextStore queryStore = mock(QueryContextStore.class);
    RelationPinSet pins = pins();
    AtomicInteger attempts = new AtomicInteger();
    AtomicBoolean accepting = new AtomicBoolean();
    CancelledQueryPinCleanup cleanup =
        new CancelledQueryPinCleanup(
            queryStore,
            task -> {
              attempts.incrementAndGet();
              if (!accepting.get()) {
                throw new RejectedExecutionException("saturated");
              }
              task.run();
            });

    cleanup.release("query", pins);

    verifyNoInteractions(queryStore);
    assertThat(attempts).hasValue(1);

    accepting.set(true);
    cleanup.retryPending();

    verify(queryStore).releaseResolvingPinBlobs("query", roots());
    assertThat(attempts).hasValue(2);
  }

  @Test
  void cancellationBurstBoundsDrainersAndUsesSynchronousOverflow() {
    QueryContextStore queryStore = mock(QueryContextStore.class);
    RelationPinSet pins = pins();
    List<Runnable> drainers = new ArrayList<>();
    CancelledQueryPinCleanup cleanup = new CancelledQueryPinCleanup(queryStore, drainers::add);

    int cancellations = CancelledQueryPinCleanup.MAX_RETAINED_RELEASES + 44;
    for (int cancellation = 0; cancellation < cancellations; cancellation++) {
      cleanup.release("query", pins);
    }

    List<String> roots = roots();
    assertThat(drainers).hasSize(4);
    verify(queryStore, times(44)).releaseResolvingPinBlobs("query", roots);

    drainers.forEach(Runnable::run);
    verify(queryStore, times(cancellations)).releaseResolvingPinBlobs("query", roots);
  }

  @Test
  void shutdownDrainsRetainedRootsAndRejectsUseAfterClose() {
    QueryContextStore queryStore = mock(QueryContextStore.class);
    RelationPinSet pins = pins();
    CancelledQueryPinCleanup cleanup =
        new CancelledQueryPinCleanup(
            queryStore,
            ignored -> {
              throw new RejectedExecutionException("stopping");
            });

    cleanup.release("before-shutdown", pins);
    cleanup.drainOnShutdown();

    List<String> roots = roots();
    verify(queryStore).releaseResolvingPinBlobs("before-shutdown", roots);
    assertThatThrownBy(() -> cleanup.release("after-shutdown", pins))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void shutdownWaitsForClaimedDrainerAndRacingInlineRelease() throws Exception {
    QueryContextStore queryStore = mock(QueryContextStore.class);
    RelationPinSet pins = pins();
    CountDownLatch claimedStarted = new CountDownLatch(1);
    CountDownLatch allowClaimed = new CountDownLatch(1);
    CountDownLatch inlineStarted = new CountDownLatch(1);
    CountDownLatch allowInline = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              String queryId = invocation.getArgument(0);
              if (queryId.equals("claimed")) {
                claimedStarted.countDown();
                assertThat(allowClaimed.await(5, TimeUnit.SECONDS)).isTrue();
              } else if (queryId.equals("inline")) {
                inlineStarted.countDown();
                assertThat(allowInline.await(5, TimeUnit.SECONDS)).isTrue();
              }
              return null;
            })
        .when(queryStore)
        .releaseResolvingPinBlobs(any(), any());

    try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
      CancelledQueryPinCleanup cleanup = new CancelledQueryPinCleanup(queryStore, executor);
      cleanup.release("claimed", pins);
      assertThat(claimedStarted.await(5, TimeUnit.SECONDS)).isTrue();

      CompletableFuture<Void> shutdown = CompletableFuture.runAsync(cleanup::drainOnShutdown);
      assertFutureIncomplete(shutdown);

      CompletableFuture<Void> inline =
          CompletableFuture.runAsync(() -> cleanup.release("inline", pins));
      assertThat(inlineStarted.await(5, TimeUnit.SECONDS)).isTrue();

      allowClaimed.countDown();
      assertFutureIncomplete(shutdown);
      allowInline.countDown();
      inline.get(5, TimeUnit.SECONDS);
      shutdown.get(5, TimeUnit.SECONDS);
    }
  }

  /** Return one stable blob-backed pin used by cleanup ownership tests. */
  private static RelationPinSet pins() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    return SnapshotTestSupport.relationPins(SnapshotTestSupport.blobBackedPin(tableId, 7L));
  }

  /** Return the resolving roots represented by {@link #pins()}. */
  private static List<String> roots() {
    return List.of("s3://table/table.pb", "s3://table/snap-7.pb");
  }

  /** Assert that cleanup does not complete while a release remains blocked. */
  private static void assertFutureIncomplete(CompletableFuture<Void> future) {
    assertThatThrownBy(() -> future.get(200, TimeUnit.MILLISECONDS))
        .isInstanceOf(TimeoutException.class);
  }
}
