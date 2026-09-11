/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.repo.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class PlanningPointerIndexTest {
  @Test
  void completeAccountIndexMakesMissingPlannerPointerAuthoritative() {
    CountingStore durable = new CountingStore();
    String present = Keys.tablePointerById("acct", "table");
    durable.compareAndSet(present, 0L, pointer(present, "s3://table"));
    IndexedPointerStore store = new IndexedPointerStore(durable, synchronousIndex(durable));

    assertThat(store.get(present)).isPresent();
    int readsAfterLoad = durable.pointReads.get();
    assertThat(store.get(Keys.tablePointerById("acct", "missing"))).isEmpty();
    assertThat(durable.pointReads.get()).isEqualTo(readsAfterLoad);
  }

  @Test
  void successfulMutationPublishesWhileTheAccountPartitionIsLocked() {
    CountingStore durable = new CountingStore();
    PlanningPointerIndex index = synchronousIndex(durable);
    IndexedPointerStore store = new IndexedPointerStore(durable, index);
    String key = Keys.tablePointerById("acct", "table");

    assertThat(store.get(key)).isEmpty();
    assertThat(store.compareAndSet(key, 0L, pointer(key, "s3://new"))).isTrue();
    assertThat(store.get(key).map(Pointer::getBlobUri)).contains("s3://new");
  }

  @Test
  void operationalPointersStayOnDurablePath() {
    CountingStore durable = new CountingStore();
    String key = Keys.transactionPointerById("acct", "tx");
    durable.compareAndSet(key, 0L, pointer(key, "s3://transaction"));
    IndexedPointerStore store = new IndexedPointerStore(durable, synchronousIndex(durable));

    assertThat(store.get(key)).isPresent();
    assertThat(durable.pointReads).hasValue(1);
  }

  @Test
  void nonOwnedAccountUsesDurablePathForReads() {
    CountingStore durable = new CountingStore();
    String key = Keys.tablePointerById("acct", "table");
    durable.compareAndSet(key, 0L, pointer(key, "s3://table"));
    PlanningPointerIndex index =
        new PlanningPointerIndex(
            durable,
            (account, access) ->
                account.equals("acct")
                    ? Optional.empty()
                    : Optional.of(PlanningPointerIndex.Ownership.Permit.NOOP));
    IndexedPointerStore store = new IndexedPointerStore(durable, index);

    assertThat(store.get(key)).isPresent();
    assertThat(durable.pointReads).hasValue(1);
  }

  @Test
  void batchFallsBackAsAWholeWhenOnePlannerPartitionIsNotOwned() {
    CountingStore durable = new CountingStore();
    String ownedKey = Keys.tablePointerById("owned", "table");
    String otherKey = Keys.tablePointerById("other", "table");
    durable.compareAndSet(ownedKey, 0L, pointer(ownedKey, "s3://owned"));
    durable.compareAndSet(otherKey, 0L, pointer(otherKey, "s3://other"));
    PlanningPointerIndex index =
        new PlanningPointerIndex(
            durable,
            (account, access) ->
                account.equals("owned")
                    ? Optional.of(PlanningPointerIndex.Ownership.Permit.NOOP)
                    : Optional.empty());
    IndexedPointerStore store = new IndexedPointerStore(durable, index);

    assertThat(store.get(ownedKey)).isPresent(); // Load the owned partition first.
    durable.batchReads.set(0);
    Map<String, Pointer> result = store.getBatch(List.of(ownedKey, otherKey));

    assertThat(result).containsKeys(ownedKey, otherKey);
    assertThat(durable.batchReads).hasValue(1);
  }

  @Test
  void completeBatchUsesOnlyTheIndex() {
    CountingStore durable = new CountingStore();
    String first = Keys.tablePointerById("acct", "first");
    String second = Keys.tablePointerById("acct", "second");
    durable.compareAndSet(first, 0L, pointer(first, "s3://first"));
    durable.compareAndSet(second, 0L, pointer(second, "s3://second"));
    PlanningPointerIndex index = synchronousIndex(durable);
    IndexedPointerStore store = new IndexedPointerStore(durable, index);

    assertThat(store.get(first)).isPresent();
    durable.batchReads.set(0);
    assertThat(store.getBatch(List.of(first, second))).containsKeys(first, second);
    assertThat(durable.batchReads).hasValue(0);
  }

  @Test
  void globalDirectoryUsesDurablePath() {
    CountingStore durable = new CountingStore();
    String key = Keys.accountPointerById("acct");
    durable.compareAndSet(key, 0L, pointer(key, "s3://account"));
    PlanningPointerIndex index = synchronousIndex(durable);
    IndexedPointerStore store = new IndexedPointerStore(durable, index);

    assertThat(store.get(key)).isPresent();
    assertThat(durable.pointReads).hasValue(1);
    assertThat(index.completePartitionCount()).isZero();
  }

  @Test
  void batchContainingOperationalKeyUsesDurablePathForEveryKey() {
    CountingStore durable = new CountingStore();
    String planningKey = Keys.tablePointerById("acct", "table");
    String operationalKey = Keys.transactionPointerById("acct", "tx");
    durable.compareAndSet(planningKey, 0L, pointer(planningKey, "s3://table"));
    durable.compareAndSet(operationalKey, 0L, pointer(operationalKey, "s3://tx"));
    PlanningPointerIndex index = synchronousIndex(durable);
    IndexedPointerStore store = new IndexedPointerStore(durable, index);

    Map<String, Pointer> result = store.getBatch(List.of(planningKey, operationalKey));

    assertThat(result).containsKeys(planningKey, operationalKey);
    assertThat(durable.batchReads).hasValue(1);
    assertThat(index.completePartitionCount()).isZero();
  }

  @Test
  void nonOwnedAccountCannotMutateDurableStoreThroughTheIndex() {
    CountingStore durable = new CountingStore();
    String key = Keys.tablePointerById("acct", "table");
    PlanningPointerIndex index =
        new PlanningPointerIndex(durable, (account, access) -> Optional.empty());
    IndexedPointerStore store = new IndexedPointerStore(durable, index);

    assertThatThrownBy(() -> store.compareAndSet(key, 0L, pointer(key, "s3://table")))
        .isInstanceOf(StorageAbortRetryableException.class);
    assertThat(durable.writes).hasValue(0);
  }

  @Test
  void firstReadUsesDurablePathAndSchedulesBackgroundWarm() {
    CountingStore durable = new CountingStore();
    String key = Keys.tablePointerById("acct", "table");
    durable.compareAndSet(key, 0L, pointer(key, "s3://table"));
    Deque<Runnable> tasks = new ArrayDeque<>();
    PlanningPointerIndex index =
        new PlanningPointerIndex(durable, PlanningPointerIndex.Ownership.ALWAYS_OWNED, tasks::add);
    IndexedPointerStore store = new IndexedPointerStore(durable, index);

    assertThat(store.get(key)).isPresent();
    assertThat(index.completePartitionCount()).isZero();
    assertThat(tasks).hasSize(1);

    tasks.removeFirst().run();
    assertThat(index.completePartitionCount()).isEqualTo(1);
    durable.pointReads.set(0);
    assertThat(store.get(key)).isPresent();
    assertThat(durable.pointReads).hasValue(0);
  }

  @Test
  void backgroundWarmReportsItsOutcome() {
    CountingStore durable = new CountingStore();
    String key = Keys.tablePointerById("acct", "table");
    durable.compareAndSet(key, 0L, pointer(key, "s3://table"));
    AtomicInteger starts = new AtomicInteger();
    AtomicReference<Duration> completed = new AtomicReference<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    PlanningPointerIndex index =
        new PlanningPointerIndex(
            durable,
            PlanningPointerIndex.Ownership.ALWAYS_OWNED,
            Runnable::run,
            new PlanningPointerIndex.WarmObserver() {
              @Override
              public void started(String accountId) {
                starts.incrementAndGet();
              }

              @Override
              public void completed(String accountId, Duration duration) {
                completed.set(duration);
              }

              @Override
              public void failed(String accountId, Duration duration, Throwable error) {
                failure.set(error);
              }
            });
    IndexedPointerStore store = new IndexedPointerStore(durable, index);

    assertThat(store.get(key)).isPresent();
    assertThat(starts).hasValue(1);
    assertThat(completed).isNotNull();
    assertThat(failure.get()).isNull();
    assertThat(index.entryCount()).isEqualTo(1);
  }

  @Test
  void ownershipReceivesLogicalAccountIdForEncodedAccountKeys() {
    CountingStore durable = new CountingStore();
    String accountId = "account with space";
    String key = Keys.tablePointerById(accountId, "table");
    durable.compareAndSet(key, 0L, pointer(key, "s3://table"));
    AtomicReference<String> requestedAccount = new AtomicReference<>();
    PlanningPointerIndex index =
        new PlanningPointerIndex(
            durable,
            (account, access) -> {
              requestedAccount.set(account);
              return account.equals(accountId)
                  ? Optional.of(PlanningPointerIndex.Ownership.Permit.NOOP)
                  : Optional.empty();
            },
            Runnable::run);
    IndexedPointerStore store = new IndexedPointerStore(durable, index);

    assertThat(store.get(key)).isPresent();
    assertThat(requestedAccount).hasValue(accountId);
    assertThat(index.completePartitionCount()).isEqualTo(1);
  }

  @Test
  void ownershipReacquisitionDropsThePreviousCompleteImageBeforeWarming() {
    CountingStore durable = new CountingStore();
    String key = Keys.tablePointerById("acct", "table");
    durable.compareAndSet(key, 0L, pointer(key, "s3://first"));
    Deque<Runnable> tasks = new ArrayDeque<>();
    PlanningPointerIndex index =
        new PlanningPointerIndex(durable, PlanningPointerIndex.Ownership.ALWAYS_OWNED, tasks::add);
    IndexedPointerStore store = new IndexedPointerStore(durable, index);

    assertThat(store.get(key)).isPresent();
    tasks.removeFirst().run();
    assertThat(index.completePartitionCount()).isEqualTo(1);
    durable.compareAndSet(key, 1L, pointer(key, "s3://second"));

    index.ownershipLost("acct");
    assertThat(index.completePartitionCount()).isZero();
    index.ownershipGained("acct");
    assertThat(tasks).hasSize(1);
    tasks.removeFirst().run();

    assertThat(store.get(key).map(Pointer::getBlobUri)).contains("s3://second");
  }

  @Test
  void clearDropsTheCompleteImageAfterAnOutOfBandFixtureReset() {
    CountingStore durable = new CountingStore();
    String key = Keys.tablePointerById("acct", "table");
    durable.compareAndSet(key, 0L, pointer(key, "s3://table"));
    PlanningPointerIndex index = synchronousIndex(durable);
    IndexedPointerStore store = new IndexedPointerStore(durable, index);

    assertThat(store.get(key)).isPresent();
    assertThat(index.completePartitionCount()).isEqualTo(1);

    durable.delete(key);
    index.clear();

    assertThat(index.entryCount()).isZero();
    assertThat(index.completePartitionCount()).isZero();
    assertThat(store.get(key)).isEmpty();
  }

  @Test
  void differentPlannerKeysCanMutateInParallel() throws Exception {
    BlockingStore durable = new BlockingStore();
    String first = Keys.tablePointerById("acct", "first");
    String second = Keys.tablePointerById("acct", "second");
    durable.compareAndSet(first, 0L, pointer(first, "s3://first"));
    durable.compareAndSet(second, 0L, pointer(second, "s3://second"));
    PlanningPointerIndex index = synchronousIndex(durable);
    IndexedPointerStore store = new IndexedPointerStore(durable, index);
    assertThat(store.get(first)).isPresent();

    durable.blockNextMutation();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      var firstMutation =
          executor.submit(() -> store.compareAndSet(first, 1L, pointer(first, "s3://first-new")));
      assertThat(durable.blockedMutationStarted.await(1, TimeUnit.SECONDS)).isTrue();
      assertThat(store.get(second).map(Pointer::getBlobUri)).contains("s3://second");

      var secondMutation =
          executor.submit(
              () -> store.compareAndSet(second, 1L, pointer(second, "s3://second-new")));
      assertThat(durable.otherMutationEntered.await(1, TimeUnit.SECONDS)).isTrue();
      assertThat(secondMutation.get(1, TimeUnit.SECONDS)).isTrue();

      durable.releaseBlockedMutation.countDown();
      assertThat(firstMutation.get(1, TimeUnit.SECONDS)).isTrue();
    } finally {
      durable.releaseBlockedMutation.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void samePlannerKeyMutationsAreSerialized() throws Exception {
    BlockingStore durable = new BlockingStore();
    String key = Keys.tablePointerById("acct", "table");
    durable.compareAndSet(key, 0L, pointer(key, "s3://initial"));
    PlanningPointerIndex index = synchronousIndex(durable);
    IndexedPointerStore store = new IndexedPointerStore(durable, index);
    assertThat(store.get(key)).isPresent();

    durable.blockNextMutation();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      var firstMutation =
          executor.submit(() -> store.compareAndSet(key, 1L, pointer(key, "s3://first-new")));
      assertThat(durable.blockedMutationStarted.await(1, TimeUnit.SECONDS)).isTrue();

      CountDownLatch secondTaskStarted = new CountDownLatch(1);
      var secondMutation =
          executor.submit(
              () -> {
                secondTaskStarted.countDown();
                return store.compareAndSet(key, 1L, pointer(key, "s3://second-new"));
              });
      assertThat(secondTaskStarted.await(1, TimeUnit.SECONDS)).isTrue();
      // The task has started while the first durable call is held. A broken key lock would let it
      // reach the durable boundary and signal here; the bounded wait is only to give that started
      // task a scheduling point before the first call is released.
      assertThat(durable.otherMutationEntered.await(1, TimeUnit.SECONDS)).isFalse();

      durable.releaseBlockedMutation.countDown();
      assertThat(firstMutation.get(1, TimeUnit.SECONDS)).isTrue();
      assertThat(secondMutation.get(1, TimeUnit.SECONDS)).isFalse();
      assertThat(store.get(key).map(Pointer::getBlobUri)).contains("s3://first-new");
    } finally {
      durable.releaseBlockedMutation.countDown();
      executor.shutdownNow();
    }
  }

  private static PlanningPointerIndex synchronousIndex(InMemoryPointerStore durable) {
    return new PlanningPointerIndex(
        durable, PlanningPointerIndex.Ownership.ALWAYS_OWNED, Runnable::run);
  }

  private static Pointer pointer(String key, String uri) {
    return Pointer.newBuilder().setKey(key).setBlobUri(uri).build();
  }

  private static final class CountingStore extends InMemoryPointerStore {
    private final AtomicInteger pointReads = new AtomicInteger();
    private final AtomicInteger batchReads = new AtomicInteger();
    private final AtomicInteger writes = new AtomicInteger();

    @Override
    public synchronized Optional<Pointer> get(String key) {
      pointReads.incrementAndGet();
      return super.get(key);
    }

    @Override
    public synchronized Optional<Pointer> getConsistent(String key) {
      pointReads.incrementAndGet();
      return super.getConsistent(key);
    }

    @Override
    public synchronized Map<String, Pointer> getBatch(List<String> keys) {
      batchReads.incrementAndGet();
      return super.getBatch(keys);
    }

    @Override
    public synchronized boolean compareAndSet(String key, long expectedVersion, Pointer next) {
      writes.incrementAndGet();
      return super.compareAndSet(key, expectedVersion, next);
    }
  }

  private static final class BlockingStore extends InMemoryPointerStore {
    private final AtomicInteger calls = new AtomicInteger();
    private final AtomicInteger blockedCalls = new AtomicInteger();
    private volatile boolean blockNext;
    private volatile CountDownLatch blockedMutationStarted = new CountDownLatch(1);
    private volatile CountDownLatch releaseBlockedMutation = new CountDownLatch(1);
    private volatile CountDownLatch otherMutationEntered = new CountDownLatch(1);

    void blockNextMutation() {
      calls.set(0);
      blockedCalls.set(0);
      blockedMutationStarted = new CountDownLatch(1);
      releaseBlockedMutation = new CountDownLatch(1);
      otherMutationEntered = new CountDownLatch(1);
      blockNext = true;
    }

    @Override
    public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
      int call = calls.incrementAndGet();
      if (blockNext && call == 1) {
        blockNext = false;
        blockedMutationStarted.countDown();
        await(releaseBlockedMutation);
      } else if (blockedCalls.incrementAndGet() > 0) {
        otherMutationEntered.countDown();
      }
      return super.compareAndSet(key, expectedVersion, next);
    }

    private static void await(CountDownLatch latch) {
      try {
        latch.await();
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new AssertionError(interrupted);
      }
    }
  }
}
