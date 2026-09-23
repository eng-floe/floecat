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

package ai.floedb.floecat.service.repo.cache;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class PointerIndexGateAndBudgetTest {
  private static final String ACCOUNT = "acct";
  private static final PlanningPointerIndex.Policy DISABLED =
      new PlanningPointerIndex.Policy(false, Long.MAX_VALUE, Long.MAX_VALUE);

  @Test
  void aDisabledIndexAnswersEveryReadFromDurableStorage() {
    InMemoryPointerStore durable = new InMemoryPointerStore();
    PlanningPointerIndex index = index(durable, DISABLED);
    IndexedPointerStore store = new IndexedPointerStore(durable, index);
    String key = Keys.tablePointerById(ACCOUNT, "t");
    store.compareAndSet(key, 0, pointer(key, "s3://t/1"));

    assertThat(store.get(key)).as("durable KV still answers").isPresent();
    assertThat(
            store.listPointersByPrefix(
                Keys.tablePointerByIdPrefix(ACCOUNT), 10, "", new StringBuilder()))
        .hasSize(1);
    assertThat(index.entryCount()).as("a disabled index holds nothing").isZero();
  }

  /**
   * The invariant that lets the switch flip live. Loads are the only thing that marks a partition
   * complete and the only thing the gate stops, so while it is off nothing can become the authority
   * -- which is why re-enabling has no stale image to trust.
   */
  @Test
  void whileDisabledNoPartitionEverBecomesComplete() {
    InMemoryPointerStore durable = new InMemoryPointerStore();
    PlanningPointerIndex index = index(durable, DISABLED);
    IndexedPointerStore store = new IndexedPointerStore(durable, index);

    for (int i = 0; i < 5; i++) {
      String key = Keys.tablePointerById(ACCOUNT, "t" + i);
      store.compareAndSet(key, 0, pointer(key, "s3://t/" + i));
      store.get(key);
      store.listPointersByPrefix(Keys.tablePointerByIdPrefix(ACCOUNT), 10, "", new StringBuilder());
    }

    assertThat(index.completePartitionCount()).isZero();
    assertThat(index.entryCount()).isZero();
  }

  @Test
  void anAccountOverTheBudgetIsRefusedAndStaysCorrect() {
    InMemoryPointerStore durable = new InMemoryPointerStore();
    List<String> keys = seed(durable, 20);
    PlanningPointerIndex index = index(durable, perAccount(400));
    IndexedPointerStore store = new IndexedPointerStore(durable, index);

    for (String key : keys) {
      assertThat(store.get(key)).as(key).isPresent();
    }

    assertThat(index.refusedPartitionCount()).isEqualTo(1);
    assertThat(index.completePartitionCount()).isZero();
    assertThat(index.loadingPartitionCount()).as("a refusal is not a warm in progress").isZero();
    assertThat(index.entryCount()).as("a refused load retains nothing").isZero();
  }

  /**
   * A per-account cap alone is unbounded in the number of accounts, which is the defect this whole
   * change exists to remove. The second account must be refused even though it fits its own cap.
   */
  @Test
  void anAccountThatFitsAloneIsRefusedWhenTheTotalIsAlreadySpent() {
    InMemoryPointerStore durable = new InMemoryPointerStore();
    seedAccount(durable, "first", 12);
    seedAccount(durable, "second", 12);

    // Size the budget from a measured account rather than a guessed byte count: the point is that
    // the second account is refused by the total, not by its own cap, which it comfortably fits.
    PlanningPointerIndex probe = index(durable, PlanningPointerIndex.Policy.UNLIMITED);
    new IndexedPointerStore(durable, probe).get(Keys.tablePointerById("first", "t0"));
    long oneAccount = probe.residentBytes();

    // Room for one account and half of another: the total trips around the midpoint of the second
    // load, far from the per-account cap, so the assertion cannot hinge on exact entry weights.
    long budget = oneAccount + oneAccount / 2;
    PlanningPointerIndex index =
        index(durable, new PlanningPointerIndex.Policy(true, budget, budget));
    IndexedPointerStore store = new IndexedPointerStore(durable, index);

    store.get(Keys.tablePointerById("first", "t0"));
    long afterFirst = index.residentBytes();
    store.get(Keys.tablePointerById("second", "t0"));

    assertThat(oneAccount).as("the probe loaded one account").isPositive();
    assertThat(afterFirst).as("the first account fit").isEqualTo(oneAccount);
    assertThat(index.completePartitionCount()).isEqualTo(1);
    assertThat(index.refusedPartitionCount()).isEqualTo(1);
    assertThat(index.residentBytes()).as("a refusal admits nothing").isEqualTo(afterFirst);
    assertThat(store.get(Keys.tablePointerById("second", "t5")))
        .as("the refused account is still answered")
        .isPresent();
  }

  @Test
  void aRefusedAccountIsNotRescannedOnEveryRead() {
    ScanCountingStore counting = new ScanCountingStore();
    List<String> keys = seed(counting, 20);
    PlanningPointerIndex index = index(counting, perAccount(400));
    IndexedPointerStore store = new IndexedPointerStore(counting, index);

    store.get(keys.get(0));
    int afterFirstLoad = counting.prefixScans.size();
    for (String key : keys) {
      store.get(key);
    }

    assertThat(counting.prefixScans).hasSize(afterFirstLoad);
  }

  @Test
  void theLoadReadsOnlyPlannerFamilyPrefixes() {
    ScanCountingStore counting = new ScanCountingStore();
    seed(counting, 2);
    PlanningPointerIndex index = index(counting, PlanningPointerIndex.Policy.UNLIMITED);
    IndexedPointerStore store = new IndexedPointerStore(counting, index);

    store.get(Keys.tablePointerById(ACCOUNT, "t0"));

    assertThat(index.completePartitionCount()).isEqualTo(1);
    assertThat(counting.prefixScans)
        .as("the account root would read every operational row too")
        .doesNotContain(Keys.accountRootPrefix(ACCOUNT))
        .isSubsetOf(PlannerPointerShape.loadPrefixes(ACCOUNT));
  }

  @Test
  void totalBudgetIsRecheckedWhenTheLoadedImagePublishes() {
    InMemoryPointerStore durable = new InMemoryPointerStore();
    seedAccount(durable, "first", 12);
    seedAccount(durable, "second", 12);

    PlanningPointerIndex probe = index(durable, PlanningPointerIndex.Policy.UNLIMITED);
    new IndexedPointerStore(durable, probe).get(Keys.tablePointerById("first", "t0"));
    long oneAccount = probe.residentBytes();
    long budget = oneAccount + oneAccount / 2;
    PlanningPointerIndex index =
        index(durable, new PlanningPointerIndex.Policy(true, budget, budget));
    IndexedPointerStore store = new IndexedPointerStore(durable, index);

    store.get(Keys.tablePointerById("first", "t0"));
    store.get(Keys.tablePointerById("second", "t0"));

    assertThat(index.residentBytes()).isEqualTo(oneAccount);
    assertThat(index.completePartitionCount()).isEqualTo(1);
    assertThat(index.refusedPartitionCount()).isEqualTo(1);
  }

  @Test
  void concurrentAccountWarmsCannotBothPublishPastTheTotalBudget() throws Exception {
    BarrierStore durable = new BarrierStore("first", "second");
    seedAccount(durable, "first", 12);
    seedAccount(durable, "second", 12);

    PlanningPointerIndex probe = index(durable, PlanningPointerIndex.Policy.UNLIMITED);
    new IndexedPointerStore(durable, probe).get(Keys.tablePointerById("first", "t0"));
    long oneAccount = probe.residentBytes();
    long budget = oneAccount + oneAccount / 2;
    durable.blockTableIdentityScans();

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      PlanningPointerIndex index =
          index(durable, new PlanningPointerIndex.Policy(true, budget, budget), executor);
      IndexedPointerStore store = new IndexedPointerStore(durable, index);

      store.get(Keys.tablePointerById("first", "t0"));
      store.get(Keys.tablePointerById("second", "t0"));
      assertThat(durable.tableScansStarted.await(1, TimeUnit.SECONDS)).isTrue();
      durable.releaseTableScans.countDown();

      waitFor(() -> index.completePartitionCount() + index.refusedPartitionCount() == 2);
      assertThat(index.residentBytes()).isLessThanOrEqualTo(budget);
      assertThat(index.completePartitionCount()).isEqualTo(1);
      assertThat(index.refusedPartitionCount()).isEqualTo(1);
    } finally {
      durable.releaseTableScans.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void residentMutationThatExceedsTheBudgetDemotesTheAccount() {
    InMemoryPointerStore durable = new InMemoryPointerStore();
    seedAccount(durable, ACCOUNT, 1);

    PlanningPointerIndex probe = index(durable, PlanningPointerIndex.Policy.UNLIMITED);
    new IndexedPointerStore(durable, probe).get(Keys.tablePointerById(ACCOUNT, "t0"));
    long loadedBytes = probe.residentBytes();
    PlanningPointerIndex index =
        index(durable, new PlanningPointerIndex.Policy(true, loadedBytes + 1, loadedBytes + 1));
    IndexedPointerStore store = new IndexedPointerStore(durable, index);
    assertThat(store.get(Keys.tablePointerById(ACCOUNT, "t0"))).isPresent();

    String newTable = Keys.tablePointerById(ACCOUNT, "new-table");
    assertThat(store.compareAndSet(newTable, 0, pointer(newTable, "s3://new-table"))).isTrue();

    assertThat(index.residentBytes()).isZero();
    assertThat(index.completePartitionCount()).isZero();
    assertThat(index.refusedPartitionCount()).isEqualTo(1);
    assertThat(store.get(newTable)).as("the durable store remains authoritative").isPresent();
  }

  /**
   * A demotion that lands while a mutation holds its key locks leaves the image alone and retires
   * the partition once the holder lets go. Emptying it in place would strand those locks: the
   * holder's release finds no entry to decrement, and the next acquirer builds a second lock for
   * the same key.
   */
  @Test
  void aDemotionUnderAHeldKeyLockRetiresThePartitionRatherThanEmptyingIt() throws Exception {
    InMemoryPointerStore durable = new InMemoryPointerStore();
    seedAccount(durable, ACCOUNT, 1);
    String key = Keys.tablePointerById(ACCOUNT, "t0");

    PlanningPointerIndex probe = index(durable, PlanningPointerIndex.Policy.UNLIMITED);
    new IndexedPointerStore(durable, probe).get(key);
    long loadedBytes = probe.residentBytes();
    PlanningPointerIndex index =
        index(durable, new PlanningPointerIndex.Policy(true, loadedBytes + 1, loadedBytes + 1));
    new IndexedPointerStore(durable, index).get(key);
    long loadedEntries = index.entryCount();
    assertThat(loadedEntries).isPositive();

    CountDownLatch demoted = new CountDownLatch(1);
    CountDownLatch releaseFirstMutation = new CountDownLatch(1);
    CountDownLatch secondMutationEntered = new CountDownLatch(1);
    Pointer oversized = pointer(key, "s3://" + "x".repeat((int) loadedBytes + 100));

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      var first =
          executor.submit(
              () ->
                  index.mutateKeys(
                      List.of(key),
                      () -> true,
                      ignored -> {
                        index.publish(key, oversized);
                        demoted.countDown();
                        await(releaseFirstMutation);
                      }));
      assertThat(demoted.await(1, TimeUnit.SECONDS)).isTrue();
      assertThat(index.completePartitionCount()).as("the account is demoted").isZero();
      assertThat(index.entryCount())
          .as("the image survives the demotion while a key lock is held")
          .isEqualTo(loadedEntries);

      var second =
          executor.submit(
              () ->
                  index.mutateKeys(
                      List.of(key),
                      () -> {
                        secondMutationEntered.countDown();
                        return true;
                      },
                      ignored -> {}));

      assertThat(secondMutationEntered.await(100, TimeUnit.MILLISECONDS))
          .as("the second mutation waits for the first, which the demotion did not release")
          .isFalse();
      releaseFirstMutation.countDown();
      first.get(1, TimeUnit.SECONDS);
      second.get(1, TimeUnit.SECONDS);
      assertThat(secondMutationEntered.await(1, TimeUnit.SECONDS)).isTrue();
      assertThat(index.entryCount())
          .as("and is retired with the partition once the holder lets go")
          .isZero();
    } finally {
      releaseFirstMutation.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void deletingDurableOnlyTableSubtreesDoesNotWarmTheResidentIndex() {
    ScanCountingStore counting = new ScanCountingStore();
    String stats = Keys.snapshotTargetStatsGenerationPointer(ACCOUNT, "t0", 7L, "gen", "target");
    counting.compareAndSet(stats, 0, pointer(stats, "s3://stats"));
    PlanningPointerIndex index = index(counting, PlanningPointerIndex.Policy.UNLIMITED);
    IndexedPointerStore store = new IndexedPointerStore(counting, index);

    store.deleteByPrefix(Keys.snapshotTargetStatsGenerationPrefix(ACCOUNT, "t0", 7L, "gen"));

    assertThat(counting.prefixScans)
        .as("operational cleanup must not trigger a planner account load")
        .isEmpty();
    assertThat(index.completePartitionCount()).isZero();
  }

  @Test
  void writingDurableOnlySnapshotRowsDoesNotWarmTheResidentIndex() {
    ScanCountingStore counting = new ScanCountingStore();
    PlanningPointerIndex index = index(counting, PlanningPointerIndex.Policy.UNLIMITED);
    IndexedPointerStore store = new IndexedPointerStore(counting, index);
    String stats = Keys.snapshotTargetStatsGenerationPointer(ACCOUNT, "t0", 7L, "gen", "target");

    assertThat(store.compareAndSet(stats, 0, pointer(stats, "s3://stats"))).isTrue();

    assertThat(counting.prefixScans)
        .as("snapshot-scoped writes must stay out of the resident planner image")
        .isEmpty();
    assertThat(index.completePartitionCount()).isZero();
    assertThat(store.get(stats)).as("durable KV still answers the row").isPresent();
  }

  private static PlanningPointerIndex index(
      InMemoryPointerStore durable, PlanningPointerIndex.Policy policy) {
    return index(durable, policy, Runnable::run);
  }

  private static PlanningPointerIndex index(
      InMemoryPointerStore durable, PlanningPointerIndex.Policy policy, Executor executor) {
    return new PlanningPointerIndex(
        durable,
        PlanningPointerIndex.Ownership.ALWAYS_OWNED,
        executor,
        PlanningPointerIndex.WarmObserver.NONE,
        policy);
  }

  private static PlanningPointerIndex.Policy perAccount(long maxBytesPerAccount) {
    return new PlanningPointerIndex.Policy(true, maxBytesPerAccount, Long.MAX_VALUE);
  }

  private static List<String> seed(InMemoryPointerStore durable, int rows) {
    return seedAccount(durable, ACCOUNT, rows);
  }

  private static List<String> seedAccount(InMemoryPointerStore durable, String account, int rows) {
    List<String> keys = new ArrayList<>();
    for (int i = 0; i < rows; i++) {
      String key = Keys.tablePointerById(account, "t" + i);
      durable.compareAndSet(key, 0, pointer(key, "s3://bucket/table/" + i));
      keys.add(key);
    }
    return List.copyOf(keys);
  }

  private static Pointer pointer(String key, String uri) {
    return Pointer.newBuilder().setKey(key).setBlobUri(uri).build();
  }

  private static void waitFor(java.util.function.BooleanSupplier condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (System.nanoTime() - deadline < 0) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(10);
    }
    assertThat(condition.getAsBoolean()).isTrue();
  }

  private static void await(CountDownLatch latch) {
    try {
      assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  @Test
  void repeatedRefusalsBackOffAndThenStopGrowing() {
    // Each refusal costs a scan up to the budget and nothing the index does shrinks the account,
    // so a fixed interval would pay that forever. The ceiling keeps it from becoming never.
    long first = PlanningPointerIndex.refusalPauseNanos(0);
    assertThat(first).isPositive();
    assertThat(PlanningPointerIndex.refusalPauseNanos(1)).isEqualTo(first * 2);
    assertThat(PlanningPointerIndex.refusalPauseNanos(3)).isEqualTo(first * 8);

    long ceiling = PlanningPointerIndex.refusalPauseNanos(5);
    assertThat(PlanningPointerIndex.refusalPauseNanos(50)).isEqualTo(ceiling);
    assertThat(PlanningPointerIndex.refusalPauseNanos(Integer.MAX_VALUE)).isEqualTo(ceiling);
  }

  /**
   * The shape the whole change rests on: a table's identity and its two current pointers are
   * resident, and everything keyed by snapshot is not. At a five-second commit cadence the
   * per-snapshot rows outrun any heap, so they have to be answered from durable KV.
   */
  @Test
  void aTablesCurrentPointersAreResidentAndItsPerSnapshotRowsAreNot() {
    ScanCountingStore counting = new ScanCountingStore();
    String table = "t0";
    String identity = Keys.tablePointerById(ACCOUNT, table);
    String root = Keys.tableRootByTable(ACCOUNT, table);
    String currentSnapshot = Keys.currentSnapshotPointerByTable(ACCOUNT, table);
    String perSnapshot = Keys.snapshotPointerById(ACCOUNT, table, 1L);
    String perTarget = Keys.snapshotTargetStatsGenerationPointer(ACCOUNT, table, 1L, "gen", "col");
    for (String key : List.of(identity, root, currentSnapshot, perSnapshot, perTarget)) {
      counting.compareAndSet(key, 0, pointer(key, "s3://blob/" + key.hashCode()));
    }

    PlanningPointerIndex index = index(counting, PlanningPointerIndex.Policy.UNLIMITED);
    IndexedPointerStore store = new IndexedPointerStore(counting, index);
    store.get(identity);

    assertThat(index.completePartitionCount()).isEqualTo(1);
    assertThat(index.entryCount())
        .as("identity plus the two current pointers, and nothing keyed by snapshot")
        .isEqualTo(3);
    assertThat(store.get(root)).as("fetched by the per-table phase").isPresent();
    assertThat(store.get(currentSnapshot)).isPresent();
    assertThat(store.get(perSnapshot)).as("still answered, from durable KV").isPresent();
    assertThat(store.get(perTarget)).as("still answered, from durable KV").isPresent();
    assertThat(counting.prefixScans)
        .as("the table subtree is never scanned wholesale")
        .doesNotContain(Keys.tableRootPrefix(ACCOUNT))
        .contains(Keys.tablePointerByIdPrefix(ACCOUNT));
  }

  @Test
  void aMalformedTableIdentityRowDoesNotWedgeTheAccount() {
    // Keys rejects a blank table id, and a load that throws retries into the same throw forever.
    InMemoryPointerStore durable = new InMemoryPointerStore();
    String stray = Keys.tablePointerByIdPrefix(ACCOUNT);
    durable.compareAndSet(stray, 0, pointer(stray, "s3://stray"));
    String good = Keys.tablePointerById(ACCOUNT, "t0");
    durable.compareAndSet(good, 0, pointer(good, "s3://good"));

    PlanningPointerIndex index = index(durable, PlanningPointerIndex.Policy.UNLIMITED);
    IndexedPointerStore store = new IndexedPointerStore(durable, index);

    assertThat(store.get(good)).isPresent();
    assertThat(index.completePartitionCount()).as("the load still completes").isEqualTo(1);
  }

  /**
   * The image covers the tables the identity listing named. A table with no by-id row but a live
   * subtree is not one of them, so the index answers absent -- correctly, since the catalog holds
   * no identity for it. Any caller for whom that absence is load-bearing, such as the blob sweep
   * deciding what to delete, reads the store instead.
   */
  @Test
  void aTableWithNoIdentityRowIsOutsideTheImage() {
    InMemoryPointerStore durable = new InMemoryPointerStore();
    String orphanRoot = Keys.tableRootByTable(ACCOUNT, "orphan");
    durable.compareAndSet(orphanRoot, 0, pointer(orphanRoot, "s3://root/orphan"));
    String named = Keys.tablePointerById(ACCOUNT, "named");
    durable.compareAndSet(named, 0, pointer(named, "s3://table/named"));

    PlanningPointerIndex index = index(durable, PlanningPointerIndex.Policy.UNLIMITED);
    IndexedPointerStore store = new IndexedPointerStore(durable, index);
    store.get(named);

    assertThat(index.completePartitionCount()).isEqualTo(1);
    assertThat(index.entryCount()).as("only rows the identity listing reached").isEqualTo(1);
    assertThat(durable.getConsistent(orphanRoot)).as("the row is in the store").isPresent();
    assertThat(store.get(orphanRoot)).as("and outside the image").isEmpty();
  }

  /** Records which prefixes a load actually asks the store for. */
  private static final class ScanCountingStore extends InMemoryPointerStore {
    private final List<String> prefixScans = new ArrayList<>();

    @Override
    public synchronized List<Pointer> listPointersByPrefixConsistent(
        String prefix, int limit, String token, StringBuilder next) {
      prefixScans.add(prefix);
      return super.listPointersByPrefixConsistent(prefix, limit, token, next);
    }
  }

  private static final class BarrierStore extends InMemoryPointerStore {
    private final Set<String> tablePrefixes;
    private volatile CountDownLatch tableScansStarted = new CountDownLatch(0);
    private volatile CountDownLatch releaseTableScans = new CountDownLatch(0);

    BarrierStore(String... accounts) {
      List<String> prefixes = new ArrayList<>();
      for (String account : accounts) {
        prefixes.add(Keys.tablePointerByIdPrefix(account));
      }
      tablePrefixes = Set.copyOf(prefixes);
    }

    void blockTableIdentityScans() {
      tableScansStarted = new CountDownLatch(tablePrefixes.size());
      releaseTableScans = new CountDownLatch(1);
    }

    @Override
    public List<Pointer> listPointersByPrefixConsistent(
        String prefix, int limit, String token, StringBuilder next) {
      if (tablePrefixes.contains(prefix)) {
        tableScansStarted.countDown();
        try {
          assertThat(releaseTableScans.await(1, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new AssertionError(interrupted);
        }
      }
      return super.listPointersByPrefixConsistent(prefix, limit, token, next);
    }
  }
}
