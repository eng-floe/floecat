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

package ai.floedb.floecat.service.account;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.account.AccountAssignment.AccountMode;
import ai.floedb.floecat.service.account.AccountAssignment.AssignmentPhase;
import ai.floedb.floecat.service.account.AccountAssignment.Mode;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.util.AccountDeletionFence;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore.CasCheck;
import ai.floedb.floecat.storage.spi.PointerStore.CasCheckAbsent;
import ai.floedb.floecat.storage.spi.PointerStore.CasDelete;
import ai.floedb.floecat.storage.spi.PointerStore.CasOp;
import ai.floedb.floecat.storage.spi.PointerStore.CasUpsert;
import ai.floedb.floecat.telemetry.TestObservability;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AssignmentFenceTest {
  private static final String MEMBER = "floecat-0";
  private static final String INCARNATION = "floecat-0/test";
  private static final String A = "acct-a";

  private RecordingStore raw;
  private AccountAssignmentTest.RecordingHooks hooks;
  private TestObservability observability;
  private AccountAssignment assignment;
  private AssignmentFence fence;
  private String key;
  private String shard;
  private String fenceKey;

  @BeforeEach
  void setUp() {
    raw = new RecordingStore();
    hooks = new AccountAssignmentTest.RecordingHooks();
    observability = new TestObservability();
    assignment =
        AccountAssignment.forTesting(
            Mode.MANAGED, MEMBER, INCARNATION, raw, hooks, Runnable::run, observability);
    fence = new AssignmentFence(raw, assignment);
    key = Keys.tablePointerById(A, "table");
    shard = Keys.accountDeletionFenceShard(A, key);
    fenceKey = Keys.accountAssignmentFence(A);
  }

  private void serve() {
    assignment.apply(1L, AssignmentPhase.SERVING, List.of(A), List.of(A), INCARNATION);
    raw.batches.clear();
    raw.singleCas.clear();
  }

  private long rememberedVersion() {
    return assignment.fenceVersion(A).orElseThrow();
  }

  @Test
  void addsTheOwnersCheckAndLeavesTheDeletionCheckAlone() {
    serve();

    boolean committed =
        fence.compareAndSetBatch(
            List.of(new CasCheckAbsent(shard), new CasUpsert(key, 0L, pointer("s3://t"))));

    assertThat(committed).isTrue();
    assertThat(raw.batches).hasSize(1);
    assertThat(raw.batches.get(0).get(0)).isEqualTo(new CasCheck(fenceKey, rememberedVersion()));
    assertThat(raw.batches.get(0))
        .as("the repositories' deletion check is untouched")
        .contains(new CasCheckAbsent(shard));
    assertThat(raw.get(key).map(Pointer::getBlobUri)).contains("s3://t");
  }

  @Test
  void aWriteCarryingAnOlderFenceVersionFailsAndRevokesOwnership() {
    serve();
    long remembered = rememberedVersion();
    // A successor entered SERVING: the fence moved on.
    raw.compareAndSet(
        fenceKey,
        remembered,
        PointerReferences.opaqueMarkerPointer(fenceKey, "owned/2/floecat-1", 1L));

    boolean committed =
        fence.compareAndSetBatch(
            List.of(new CasCheckAbsent(shard), new CasUpsert(key, 0L, pointer("s3://stale"))));

    assertThat(committed).isFalse();
    assertThat(raw.get(key)).isEmpty();
    assertThat(raw.batches.get(0).get(0)).isEqualTo(new CasCheck(fenceKey, remembered));
    assertThat(assignment.status(A).mode()).isEqualTo(AccountMode.UNASSIGNED);
    assertThat(hooks.lost).containsExactly(A);
    assertThat(observability.counterValue(ServiceMetrics.Assignment.FENCE_REJECTIONS))
        .isEqualTo(1.0d);
  }

  @Test
  void anOrdinaryConflictOnThePayloadKeyDoesNotRevoke() {
    serve();
    raw.compareAndSet(key, 0L, pointer("s3://first"));

    boolean committed =
        fence.compareAndSetBatch(
            List.of(new CasCheckAbsent(shard), new CasUpsert(key, 0L, pointer("s3://second"))));

    assertThat(committed).isFalse();
    assertThat(assignment.status(A).mode()).isEqualTo(AccountMode.SERVING);
    assertThat(hooks.lost).isEmpty();
  }

  @Test
  void singleKeyOperationsBecomeTwoItemTransactions() {
    serve();

    assertThat(fence.compareAndSet(key, 0L, pointer("s3://t"))).isTrue();
    assertThat(raw.singleCas).isEmpty();
    assertThat(raw.batches.get(0))
        .containsExactly(
            new CasCheck(fenceKey, rememberedVersion()), new CasUpsert(key, 0L, pointer("s3://t")));

    assertThat(fence.compareAndDelete(key, 1L)).isTrue();
    assertThat(raw.batches.get(1))
        .containsExactly(new CasCheck(fenceKey, rememberedVersion()), new CasDelete(key, 1L));

    assertThat(fence.compareAndSet(key, 0L, pointer("s3://again"))).isTrue();
    assertThat(fence.delete(key)).isTrue();
    assertThat(raw.batches.get(3))
        .containsExactly(new CasCheck(fenceKey, rememberedVersion()), new CasDelete(key, 1L));
    assertThat(raw.get(key)).isEmpty();
  }

  @Test
  void aBatchSpanningTwoOwnedAccountsCarriesBothChecks() {
    String other = "acct-b";
    assignment.apply(1L, AssignmentPhase.SERVING, List.of(A, other), List.of(A), INCARNATION);
    raw.batches.clear();

    boolean committed =
        fence.compareAndSetBatch(
            List.of(
                new CasUpsert(key, 0L, pointer("s3://a")),
                new CasUpsert(Keys.tablePointerById(other, "table"), 0L, pointer("s3://b"))));

    assertThat(committed).isTrue();
    assertThat(raw.batches.get(0).subList(0, 2))
        .containsExactlyInAnyOrder(
            new CasCheck(fenceKey, assignment.fenceVersion(A).orElseThrow()),
            new CasCheck(
                Keys.accountAssignmentFence(other), assignment.fenceVersion(other).orElseThrow()));
  }

  @Test
  void deletionInProgressStopsWritesWithoutTouchingOwnership() {
    serve();
    String marker = Keys.accountDeletionMarker(A);
    List<CasOp> deletion = new ArrayList<>();
    deletion.add(new CasUpsert(marker, 0L, PointerReferences.opaqueMarkerPointer(marker, "m", 1L)));
    for (String each : Keys.accountDeletionFenceShards(A)) {
      deletion.add(
          new CasUpsert(each, 0L, PointerReferences.opaqueMarkerPointer(each, "deleting", 1L)));
    }
    assertThat(fence.compareAndSetBatch(deletion)).isTrue();

    assertThatThrownBy(
            () ->
                AccountDeletionFence.compareAndSetBatch(
                    fence, A, List.of(new CasUpsert(key, 0L, pointer("s3://late")))))
        .isInstanceOf(BaseResourceRepository.AccountDeletionInProgressException.class);
    assertThat(raw.get(key)).isEmpty();
    assertThat(assignment.status(A).mode())
        .as("deletion is not a revocation")
        .isEqualTo(AccountMode.SERVING);
  }

  @Test
  void aWriteAfterTheFencePointerIsDeletedRevokesOwnership() {
    serve();
    raw.compareAndDelete(fenceKey, rememberedVersion());

    assertThat(fence.compareAndSet(key, 0L, pointer("s3://gone"))).isFalse();

    assertThat(assignment.status(A).mode()).isEqualTo(AccountMode.UNASSIGNED);
  }

  @Test
  void aRevokedAccountKeepsFencingWritesThatWereAlreadyAdmitted() {
    serve();
    long held = rememberedVersion();
    var permit = assignment.admitMutation(A);

    // Another process takes the fence; this one notices and gives the account up.
    raw.compareAndSet(
        fenceKey,
        held,
        PointerReferences.opaqueMarkerPointer(fenceKey, "owned/2/floecat-1", held + 1L));
    assignment.selfCheck();

    assertThat(assignment.fenceVersion(A))
        .as("the admitted write must still carry a check, not lose it")
        .hasValue(held);
    assertThat(fence.compareAndSet(key, 0L, pointer("s3://after-revocation")))
        .as("and that check must fail against the moved fence")
        .isFalse();
    assertThat(raw.get(key)).isEmpty();

    assertThat(assignment.status().drained())
        .as("Core must not see this pod as idle while the write is in flight")
        .isFalse();

    permit.close();
    assertThat(assignment.status(A).mode()).isEqualTo(AccountMode.UNASSIGNED);
  }

  @Test
  void prefixDeleteRequiresTheRememberedFenceVersion() {
    serve();
    String prefix = Keys.snapshotRootPrefix(A, "table");
    String pointerKey = prefix + "one";
    raw.compareAndSet(pointerKey, 0L, pointer("s3://one"));
    assertThat(fence.deleteByPrefix(prefix)).isEqualTo(1);

    raw.compareAndSet(pointerKey, 0L, pointer("s3://two"));
    raw.compareAndSet(
        fenceKey,
        rememberedVersion(),
        PointerReferences.opaqueMarkerPointer(fenceKey, "owned/2/y", 1L));

    assertThatThrownBy(() -> fence.deleteByPrefix(prefix))
        .isInstanceOf(StorageAbortRetryableException.class);
    assertThat(raw.get(pointerKey)).isPresent();
    assertThat(assignment.status(A).mode()).isEqualTo(AccountMode.UNASSIGNED);
  }

  @Test
  void directoryAndMemberIndexPointersAreNotFenced() {
    serve();
    String directory = Keys.accountPointerById(A);
    String index = Keys.memberAssignmentIndex(MEMBER);

    assertThat(fence.compareAndSet(directory, 0L, pointer("s3://dir"))).isTrue();
    assertThat(fence.compareAndSet(index, 1L, pointer("1;"))).isTrue();
    assertThat(
            fence.compareAndSetBatch(
                List.of(new CasUpsert(Keys.accountPointerByName("name"), 0L, pointer("s3://n")))))
        .isTrue();

    assertThat(raw.singleCas).containsExactly(directory, index);
    assertThat(raw.batches).hasSize(1);
    assertThat(raw.batches.get(0)).noneMatch(op -> op instanceof CasCheck);
  }

  @Test
  void nonOwnedAndStandaloneWritesPassThroughUnchanged() {
    List<CasOp> ops = List.of(new CasCheckAbsent(shard), new CasUpsert(key, 0L, pointer("s3://t")));

    assertThat(fence.compareAndSetBatch(ops)).isTrue();
    assertThat(raw.batches.get(0)).isEqualTo(ops);

    raw.batches.clear();
    AccountAssignment standalone =
        AccountAssignment.forTesting(
            Mode.STANDALONE, "", "local", raw, hooks, Runnable::run, observability);
    AssignmentFence inert = new AssignmentFence(raw, standalone);
    assertThat(
            inert.compareAndSetBatch(
                List.of(new CasCheckAbsent(shard), new CasUpsert(key, 1L, pointer("s3://u")))))
        .isTrue();
    assertThat(raw.batches.get(0).get(0)).isEqualTo(new CasCheckAbsent(shard));
    assertThat(inert.compareAndSet(key, 2L, pointer("s3://v"))).isTrue();
    assertThat(raw.singleCas).containsExactly(key);
  }

  @Test
  void accountScopeCoversPlannerAndOperationalKeysOnly() {
    assertThat(AssignmentFence.accountScope(Keys.tablePointerById(A, "t"))).contains(A);
    assertThat(AssignmentFence.accountScope(Keys.transactionPointerById(A, "tx"))).contains(A);
    assertThat(AssignmentFence.accountScope(Keys.accountPointerById(A))).isEmpty();
    assertThat(AssignmentFence.accountScope(Keys.memberAssignmentIndex(MEMBER))).isEmpty();
    assertThat(AssignmentFence.accountScope(shard)).isEmpty();
    assertThat(AssignmentFence.accountScope(Keys.accountAssignmentFence(A))).isEmpty();
  }

  private static Pointer pointer(String uri) {
    return Pointer.newBuilder().setBlobUri(uri).build();
  }

  /** Records what reaches the durable store beneath the fence. */
  static final class RecordingStore extends InMemoryPointerStore {
    final List<List<CasOp>> batches = new ArrayList<>();
    final List<String> singleCas = new ArrayList<>();

    @Override
    public synchronized boolean compareAndSetBatch(List<CasOp> ops) {
      batches.add(List.copyOf(ops));
      return super.compareAndSetBatch(ops);
    }

    @Override
    public synchronized boolean compareAndSet(String key, long expectedVersion, Pointer next) {
      singleCas.add(key);
      return super.compareAndSet(key, expectedVersion, next);
    }
  }
}
