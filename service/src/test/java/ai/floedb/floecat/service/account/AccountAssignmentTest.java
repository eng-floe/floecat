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
import ai.floedb.floecat.service.account.AccountAssignment.Mode;
import ai.floedb.floecat.service.account.AssignmentControl.AccountMode;
import ai.floedb.floecat.service.account.AssignmentControl.AssignmentPhase;
import ai.floedb.floecat.service.repo.cache.IndexedPointerStore;
import ai.floedb.floecat.service.repo.cache.PlanningPointerIndex;
import ai.floedb.floecat.service.repo.cache.PlanningPointerIndex.Ownership.Access;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.telemetry.TestObservability;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AccountAssignmentTest {
  private static final String MEMBER = "floecat-0";
  private static final String INCARNATION = "floecat-0/test";
  private static final String A = "acct-a";
  private static final String B = "acct-b";

  private FailableStore raw;
  private RecordingHooks hooks;
  private TestObservability observability;
  private AccountAssignment assignment;

  @BeforeEach
  void setUp() {
    raw = new FailableStore();
    hooks = new RecordingHooks();
    observability = new TestObservability();
    assignment = managed();
  }

  private AccountAssignment managed() {
    return AccountAssignment.forTesting(
        Mode.MANAGED, MEMBER, INCARNATION, raw, hooks, Runnable::run, observability);
  }

  // ---------------------------------------------------------------------------------------------
  // Apply
  // ---------------------------------------------------------------------------------------------

  @Test
  void servingFencesJoiningAccountsAndOpensAdmission() {
    var status = assignment.apply(1L, AssignmentPhase.SERVING, List.of(A), List.of(A), INCARNATION);

    assertThat(status.epoch()).isEqualTo(1L);
    assertThat(status.phase()).isEqualTo(AssignmentPhase.SERVING);
    assertThat(status.account(A).orElseThrow().mode()).isEqualTo(AccountMode.SERVING);
    assertThat(status.account(A).orElseThrow().gcAllowed()).isTrue();
    assertThat(fencePayload(A)).contains(assignment.ownedPayload(1L));
    assertThat(hooks.gained).containsExactly(A);
    assertThat(assignment.acquire(A, Access.WRITE)).isPresent();
    assignment.admitResolution(A).close();
    assertThat(assignment.tryAcquireGc(A)).isPresent();
    assertThat(AccountAssignment.MemberIndex.parse(memberIndexPayload()).orElseThrow())
        .isEqualTo(new AccountAssignment.MemberIndex(1L, List.of(A)));
  }

  @Test
  void anOlderServingTaskDoesNotOverwriteANewerMemberIndex() {
    String memberIndex = Keys.memberAssignmentIndex(MEMBER);
    raw.compareAndSet(
        memberIndex,
        0L,
        PointerReferences.opaqueMarkerPointer(memberIndex, "2;" + Keys.encodeSegment(B), 1L));

    assignment.apply(1L, AssignmentPhase.SERVING, List.of(A), List.of(A), INCARNATION);

    assertThat(AccountAssignment.MemberIndex.parse(memberIndexPayload()).orElseThrow())
        .isEqualTo(new AccountAssignment.MemberIndex(2L, List.of(B)));
  }

  @Test
  void drainingFencesLeavingAccountsUntilInFlightMutationsFinish() {
    assignment.apply(1L, AssignmentPhase.SERVING, List.of(A, B), List.of(A, B), INCARNATION);
    var mutation = assignment.acquire(A, Access.WRITE).orElseThrow();

    var status =
        assignment.apply(2L, AssignmentPhase.DRAINING, List.of(B), List.of(B), INCARNATION);

    var leaving = status.account(A).orElseThrow();
    assertThat(leaving.mode()).isEqualTo(AccountMode.DRAINING);
    assertThat(leaving.drained()).isFalse();
    assertThat(leaving.gcAllowed()).isFalse();
    assertThat(assignment.acquire(A, Access.WRITE)).isEmpty();
    assertThat(assignment.acquire(A, Access.READ)).as("reads continue on a loser").isPresent();
    assertThatThrownBy(() -> assignment.admitResolution(A))
        .isInstanceOf(PlanningPointerIndex.Ownership.NotOwnedException.class);
    assertThat(assignment.tryAcquireGc(A)).isEmpty();
    assertThat(hooks.lost).isEmpty();
    assertThat(status.account(B).orElseThrow().mode()).isEqualTo(AccountMode.SERVING);

    mutation.close();

    assertThat(assignment.status(A).mode()).isEqualTo(AccountMode.UNASSIGNED);
    assertThat(assignment.fenceVersion(A)).isEmpty();
    assertThat(hooks.lost).containsExactly(A);
    assertThat(assignment.acquire(A, Access.READ)).as("non-owned reads fall through").isEmpty();
  }

  @Test
  void drainingDoesNotAdmitJoiningAccountsBeforeServing() {
    assignment.apply(1L, AssignmentPhase.SERVING, List.of(), List.of(), INCARNATION);

    var draining =
        assignment.apply(2L, AssignmentPhase.DRAINING, List.of(A), List.of(A), INCARNATION);
    assertThat(draining.account(A).orElseThrow().mode()).isEqualTo(AccountMode.UNASSIGNED);
    assertThat(fencePayload(A)).isEmpty();
    assertThat(assignment.acquire(A, Access.WRITE)).isEmpty();

    var serving =
        assignment.apply(2L, AssignmentPhase.SERVING, List.of(A), List.of(A), INCARNATION);
    assertThat(serving.account(A).orElseThrow().mode()).isEqualTo(AccountMode.SERVING);
    assertThat(fencePayload(A)).contains(assignment.ownedPayload(2L));
  }

  @Test
  void rejectsLowerEpochEqualEpochServingToDrainingAndWrongIncarnation() {
    assignment.apply(3L, AssignmentPhase.SERVING, List.of(A), List.of(), INCARNATION);

    assertThatThrownBy(
            () -> assignment.apply(2L, AssignmentPhase.SERVING, List.of(A), List.of(), INCARNATION))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("older");
    assertThatThrownBy(
            () ->
                assignment.apply(3L, AssignmentPhase.DRAINING, List.of(A), List.of(), INCARNATION))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("SERVING -> DRAINING");
    assertThatThrownBy(
            () ->
                assignment.apply(
                    3L, AssignmentPhase.SERVING, List.of(A, B), List.of(), INCARNATION))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("different account set");
    assertThatThrownBy(
            () -> assignment.apply(4L, AssignmentPhase.SERVING, List.of(A), List.of(), "other/inc"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("target_incarnation");
    assertThat(assignment.status().epoch()).isEqualTo(3L);

    // Same state is idempotent; the GC subset may change without an epoch advance.
    assignment.apply(3L, AssignmentPhase.SERVING, List.of(A), List.of(A), INCARNATION);
    assertThat(assignment.status(A).gcAllowed()).isTrue();

    // DRAINING -> SERVING at the same epoch is the handoff's second step.
    assignment.apply(4L, AssignmentPhase.DRAINING, List.of(A, B), List.of(), INCARNATION);
    assertThat(assignment.status(B).mode()).isEqualTo(AccountMode.UNASSIGNED);
    assignment.apply(4L, AssignmentPhase.SERVING, List.of(A, B), List.of(), INCARNATION);
    assertThat(assignment.status(B).mode()).isEqualTo(AccountMode.SERVING);
  }

  @Test
  void managedWithoutAssignmentRefusesPinsAndWritesAndServesReadsReadThrough() {
    String key = Keys.tablePointerById(A, "table");
    raw.compareAndSet(key, 0L, PointerReferences.blobPointer(key, "s3://table", 1L));
    var index = new PlanningPointerIndex(raw, assignment);
    var store = new IndexedPointerStore(new AssignmentFence(raw, assignment), index);

    assertThat(store.get(key).map(Pointer::getBlobUri)).contains("s3://table");
    assertThatThrownBy(
            () ->
                store.compareAndSet(
                    key, 1L, PointerReferences.blobPointer(key, "s3://table-2", 2L)))
        .isInstanceOf(StorageAbortRetryableException.class);
    assertThat(raw.get(key).map(Pointer::getBlobUri)).contains("s3://table");
    assertThatThrownBy(() -> assignment.admitResolution(A))
        .isInstanceOf(PlanningPointerIndex.Ownership.NotOwnedException.class)
        .hasMessageContaining("floecat.not_assigned");
    assertThat(assignment.tryAcquireGc(A)).isEmpty();
    assertThat(assignment.status(A).mode()).isEqualTo(AccountMode.UNASSIGNED);
  }

  @Test
  void servingTakesTheFenceSoAWriteCarryingTheOlderVersionFailsItsCondition() {
    // A previous owner held the fence and remembered its version.
    String fence = Keys.accountAssignmentFence(A);
    takeFence(A, "owned/0/floecat-9");
    String key = Keys.tablePointerById(A, "table");
    long previousOwnerVersion = raw.get(fence).orElseThrow().getVersion();

    assignment.apply(1L, AssignmentPhase.SERVING, List.of(A), List.of(), INCARNATION);

    assertThat(raw.get(fence).orElseThrow().getVersion()).isEqualTo(previousOwnerVersion + 1L);
    boolean staleWriter =
        raw.compareAndSetBatch(
            List.of(
                new PointerStore.CasCheck(fence, previousOwnerVersion),
                new PointerStore.CasUpsert(
                    key, 0L, PointerReferences.blobPointer(key, "s3://stale", 1L))));
    assertThat(staleWriter).isFalse();
    assertThat(raw.get(key)).isEmpty();

    var fenced = new AssignmentFence(raw, assignment);
    assertThat(fenced.compareAndSet(key, 0L, PointerReferences.blobPointer(key, "s3://owner", 1L)))
        .isTrue();
    assertThat(raw.get(key).map(Pointer::getBlobUri)).contains("s3://owner");
  }

  /** Deletion runs on the owner, so taking an account whose deletion started is not special. */
  @Test
  void servingTakesTheFenceEvenWhileTheAccountIsBeingDeleted() {
    String marker = Keys.accountDeletionMarker(A);
    raw.compareAndSet(marker, 0L, PointerReferences.opaqueMarkerPointer(marker, "meta", 1L));

    assignment.apply(1L, AssignmentPhase.SERVING, List.of(A), List.of(A), INCARNATION);

    assertThat(assignment.status(A).mode()).isEqualTo(AccountMode.SERVING);
    assertThat(fencePayload(A)).contains(assignment.ownedPayload(1L));
  }

  // ---------------------------------------------------------------------------------------------
  // GC permits and self-check
  // ---------------------------------------------------------------------------------------------

  @Test
  void gcPermitIsDeniedOutsideTheGcAllowedSetAndGrantedInside() {
    assignment.apply(1L, AssignmentPhase.SERVING, List.of(A, B), List.of(A), INCARNATION);

    assertThat(assignment.tryAcquireGc(B)).isEmpty();
    var permit = assignment.tryAcquireGc(A).orElseThrow();
    assertThat(permit.valid()).isTrue();
    assertThat(assignment.status(A).activeGc()).isEqualTo(1L);

    // The control plane withdraws GC for A at the same epoch: the held permit is revoked, not just
    // future ones.
    assignment.apply(1L, AssignmentPhase.SERVING, List.of(A, B), List.of(), INCARNATION);
    assertThat(permit.valid()).isFalse();
    assertThatThrownBy(permit::requireValid)
        .isInstanceOf(AccountScope.GcPermitRevokedException.class);
    permit.close();
    assertThat(assignment.status(A).activeGc()).isZero();
    assertThat(assignment.tryAcquireGc(A)).isEmpty();
  }

  @Test
  void aTakenFenceDeniesTheNextPermitAndDropsTheAccount() {
    assignment.apply(1L, AssignmentPhase.SERVING, List.of(A), List.of(A), INCARNATION);
    takeFence(A, "owned/2/floecat-1");

    assertThat(assignment.tryAcquireGc(A)).isEmpty();

    assertThat(assignment.status(A).mode()).isEqualTo(AccountMode.UNASSIGNED);
    assertThat(assignment.fenceVersion(A)).isEmpty();
    assertThat(hooks.lost).containsExactly(A);
    assertThat(assignment.acquire(A, Access.WRITE)).isEmpty();
  }

  @Test
  void sweepDropsAnAccountWhoseFenceChanged() {
    assignment.apply(1L, AssignmentPhase.SERVING, List.of(A, B), List.of(A, B), INCARNATION);
    takeFence(B, "owned/2/floecat-1");

    assignment.selfCheck();

    assertThat(assignment.status(A).mode()).isEqualTo(AccountMode.SERVING);
    assertThat(assignment.status(B).mode()).isEqualTo(AccountMode.UNASSIGNED);
    assertThat(hooks.lost).containsExactly(B);
    assertThat(counter(ServiceMetrics.Assignment.SELF_CHECKS)).isEqualTo(1.0d);
  }

  @Test
  void sweepThatCannotReachTheStoreFencesEveryOwnedAccountUntilOneSucceeds() {
    assignment.apply(1L, AssignmentPhase.SERVING, List.of(A, B), List.of(A, B), INCARNATION);
    raw.failConsistentReads = true;

    assignment.selfCheck();

    for (String account : List.of(A, B)) {
      assertThat(assignment.acquire(account, Access.WRITE)).isEmpty();
      assertThat(assignment.acquire(account, Access.READ)).isPresent();
      assertThatThrownBy(() -> assignment.admitResolution(account))
          .isInstanceOf(PlanningPointerIndex.Ownership.NotOwnedException.class);
      assertThat(assignment.tryAcquireGc(account)).isEmpty();
      assertThat(assignment.status(account).mode()).isEqualTo(AccountMode.SERVING);
    }
    assertThat(hooks.lost).as("fencing is not a revocation").isEmpty();

    raw.failConsistentReads = false;
    assignment.selfCheck();

    assertThat(assignment.acquire(A, Access.WRITE)).isPresent();
    assertThat(assignment.tryAcquireGc(B)).isPresent();
  }

  @Test
  void sweepRetriesAJoinWhoseFenceFailedEarlier() {
    raw.failWrites = true;
    assignment.apply(1L, AssignmentPhase.SERVING, List.of(A), List.of(A), INCARNATION);
    assertThat(assignment.status(A).mode()).isEqualTo(AccountMode.UNASSIGNED);

    raw.failWrites = false;
    assignment.selfCheck();

    assertThat(assignment.status(A).mode()).isEqualTo(AccountMode.SERVING);
    assertThat(fencePayload(A)).contains(assignment.ownedPayload(1L));
  }

  // ---------------------------------------------------------------------------------------------
  // Recovery
  // ---------------------------------------------------------------------------------------------

  @Test
  void recoveryRestoresAccountsWhoseFenceNamesThisMemberAndExcludesReownedOnes() {
    assignment.apply(5L, AssignmentPhase.SERVING, List.of(A, B), List.of(A, B), INCARNATION);
    takeFence(B, "owned/6/" + Keys.encodeSegment("floecat-1"));
    RecordingHooks restartedHooks = new RecordingHooks();
    AccountAssignment restarted =
        AccountAssignment.forTesting(
            Mode.MANAGED,
            MEMBER,
            "floecat-0/restart",
            raw,
            restartedHooks,
            Runnable::run,
            observability);

    var status = restarted.recoverFromStore();

    assertThat(status.recoveredFromStore()).isTrue();
    assertThat(status.epoch()).isEqualTo(5L);
    assertThat(status.account(A).orElseThrow().mode()).isEqualTo(AccountMode.SERVING);
    assertThat(status.account(A).orElseThrow().gcAllowed())
        .as("roots died with the process")
        .isFalse();
    assertThat(status.account(B)).isEmpty();
    assertThat(restartedHooks.gained).containsExactly(A);
    assertThat(restarted.acquire(A, Access.WRITE)).isPresent();
    assertThat(restarted.tryAcquireGc(A)).isEmpty();
    assertThat(restarted.acquire(B, Access.WRITE)).isEmpty();

    // The recovered fence is live: the remembered versions are the store's.
    var fenced = new AssignmentFence(raw, restarted);
    String key = Keys.tablePointerById(A, "table");
    assertThat(fenced.compareAndSet(key, 0L, PointerReferences.blobPointer(key, "s3://t", 1L)))
        .isTrue();

    // The control plane's next push at a later epoch reopens GC and clears the recovered flag.
    var pushed =
        restarted.apply(6L, AssignmentPhase.SERVING, List.of(A), List.of(A), "floecat-0/restart");
    assertThat(pushed.recoveredFromStore()).isFalse();
    assertThat(restarted.tryAcquireGc(A)).isPresent();
  }

  @Test
  void recoveryFencesOutAPredecessorHoldingTheSameMemberId() {
    // member-id is stable across restarts and the marker carries no incarnation, so a replacement
    // pod cannot tell "I wrote this" from "a still-running me wrote this". Adopting the version
    // would leave both processes passing the same CasCheck -- two writers on one account.
    assignment.apply(5L, AssignmentPhase.SERVING, List.of(A), List.of(A), INCARNATION);
    var predecessor = new AssignmentFence(raw, assignment);
    String key = Keys.tablePointerById(A, "table");
    assertThat(predecessor.compareAndSet(key, 0L, PointerReferences.blobPointer(key, "s3://t", 1L)))
        .as("the original owner writes before the restart")
        .isTrue();

    AccountAssignment restarted =
        AccountAssignment.forTesting(
            Mode.MANAGED,
            MEMBER,
            "floecat-0/restart",
            raw,
            new RecordingHooks(),
            Runnable::run,
            observability);
    assertThat(restarted.recoverFromStore().account(A).orElseThrow().mode())
        .isEqualTo(AccountMode.SERVING);

    assertThat(
            predecessor.compareAndSet(key, 1L, PointerReferences.blobPointer(key, "s3://t2", 2L)))
        .as("the predecessor still holds the superseded fence and must be refused")
        .isFalse();
  }

  @Test
  void recoveryWithNoMemberIndexOwnsNothing() {
    var status = assignment.recoverFromStore();

    assertThat(status.recoveredFromStore()).isFalse();
    assertThat(status.accounts()).isEmpty();
    assertThat(hooks.gained).isEmpty();
  }

  // ---------------------------------------------------------------------------------------------
  // Process drain and modes
  // ---------------------------------------------------------------------------------------------

  @Test
  void processDrainStopsAdmissionAndReportsDrainedWhenWorkFinishes() {
    assignment.apply(1L, AssignmentPhase.SERVING, List.of(A), List.of(A), INCARNATION);
    var resolution = assignment.admitResolution(A);

    var status = assignment.beginProcessDrain();

    assertThat(status.processDraining()).isTrue();
    assertThat(status.drained()).isFalse();
    assertThat(status.account(A).orElseThrow().mode()).isEqualTo(AccountMode.DRAINING);
    assertThatThrownBy(() -> assignment.admitResolution(A))
        .isInstanceOf(PlanningPointerIndex.Ownership.NotOwnedException.class);
    assertThat(assignment.acquire(A, Access.WRITE)).isEmpty();
    assertThat(assignment.tryAcquireGc(A)).isEmpty();
    assertThatThrownBy(
            () -> assignment.apply(2L, AssignmentPhase.SERVING, List.of(A), List.of(), INCARNATION))
        .isInstanceOf(IllegalStateException.class);
    assertThat(raw.get(Keys.memberAssignmentIndex(MEMBER)).map(Pointer::getVersion))
        .as("drain touches no KV")
        .contains(1L);

    resolution.close();

    assertThat(assignment.status().drained()).isTrue();
  }

  @Test
  void processDrainRejectsQueuedFenceTakesBeforeTheyTouchDurableState() {
    List<Runnable> queued = new ArrayList<>();
    assignment =
        AccountAssignment.forTesting(
            Mode.MANAGED, MEMBER, INCARNATION, raw, hooks, queued::add, observability);

    assignment.apply(1L, AssignmentPhase.SERVING, List.of(A), List.of(A), INCARNATION);
    assertThat(queued).hasSize(1);

    assertThat(assignment.beginProcessDrain().drained()).isTrue();
    queued.removeFirst().run();

    assertThat(raw.get(Keys.accountAssignmentFence(A))).isEmpty();
    assertThat(assignment.status(A).mode()).isEqualTo(AccountMode.UNASSIGNED);
    assertThat(hooks.gained).isEmpty();
  }

  @Test
  void processDrainCountsAnAdmittedFenceTakeAfterItsAccountLeavesTheAssignment() throws Exception {
    CountDownLatch fenceReadStarted = new CountDownLatch(1);
    CountDownLatch releaseFenceRead = new CountDownLatch(1);
    AtomicReference<Thread> worker = new AtomicReference<>();
    raw.blockFenceRead(fenceReadStarted, releaseFenceRead);
    assignment =
        AccountAssignment.forTesting(
            Mode.MANAGED,
            MEMBER,
            INCARNATION,
            raw,
            hooks,
            command -> {
              Thread thread = new Thread(command);
              worker.set(thread);
              thread.start();
            },
            observability);

    assignment.apply(1L, AssignmentPhase.SERVING, List.of(A), List.of(), INCARNATION);
    assertThat(fenceReadStarted.await(1, TimeUnit.SECONDS)).isTrue();
    Thread fenceWorker = worker.get();
    assertThat(fenceWorker).isNotNull();

    assignment.apply(2L, AssignmentPhase.SERVING, List.of(), List.of(), INCARNATION);
    assertThat(assignment.beginProcessDrain().drained())
        .as("the in-flight fence take remains visible after the account is removed")
        .isFalse();

    releaseFenceRead.countDown();
    fenceWorker.join(1_000L);
    assertThat(fenceWorker.isAlive()).isFalse();
    assertThat(assignment.status().drained()).isTrue();
  }

  @Test
  void standaloneServesEverythingAndTouchesNoStore() {
    AccountAssignment standalone =
        AccountAssignment.forTesting(
            Mode.STANDALONE, "", "local", raw, hooks, Runnable::run, observability);

    assertThat(standalone.acquire(A, Access.WRITE)).isPresent();
    assertThat(standalone.acquire(A, Access.READ)).isPresent();
    standalone.admitResolution(A).close();
    assertThat(standalone.tryAcquireGc(A)).isPresent();
    assertThat(standalone.fenceVersion(A)).isEmpty();
    assertThat(standalone.status(A).mode()).isEqualTo(AccountMode.SERVING);
    assertThat(standalone.status(A).gcAllowed()).isTrue();
    standalone.selfCheck();
    standalone.recoverFromStore();
    assertThatThrownBy(
            () -> standalone.apply(1L, AssignmentPhase.SERVING, List.of(A), List.of(), "local"))
        .isInstanceOf(IllegalStateException.class);
    assertThat(raw.isEmpty()).isTrue();
    assertThat(hooks.gained).isEmpty();
  }

  @Test
  void noneModeOwnsNothing() {
    AccountAssignment none =
        AccountAssignment.forTesting(
            Mode.NONE, "", "local", raw, hooks, Runnable::run, observability);

    assertThat(none.acquire(A, Access.WRITE)).isEmpty();
    assertThat(none.acquire(A, Access.READ)).isEmpty();
    assertThatThrownBy(() -> none.admitResolution(A))
        .isInstanceOf(PlanningPointerIndex.Ownership.NotOwnedException.class);
    assertThat(none.tryAcquireGc(A)).isEmpty();
    assertThat(none.status(A).mode()).isEqualTo(AccountMode.UNASSIGNED);
  }

  @Test
  void managedModeRequiresAMemberId() {
    assertThatThrownBy(
            () ->
                AccountAssignment.forTesting(
                    Mode.MANAGED, " ", INCARNATION, raw, hooks, Runnable::run, observability))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("member-id");
    assertThat(Mode.parse("Managed")).isEqualTo(Mode.MANAGED);
    assertThat(Mode.parse(null)).isEqualTo(Mode.STANDALONE);
    assertThatThrownBy(() -> Mode.parse("cluster")).isInstanceOf(IllegalArgumentException.class);
  }

  // ---------------------------------------------------------------------------------------------

  private Optional<String> fencePayload(String accountId) {
    return raw.get(Keys.accountAssignmentFence(accountId)).map(Pointer::getBlobUri);
  }

  @Test
  void anAccountHandedBackServesAgainWithoutWaitingOutItsDrain() {
    assignment.apply(5L, AssignmentPhase.SERVING, List.of(A), List.of(A), INCARNATION);
    // Hold a permit so the account cannot finish draining.
    var inFlight = assignment.acquire(A, Access.WRITE).orElseThrow();
    assignment.apply(6L, AssignmentPhase.DRAINING, List.of(), List.of(), INCARNATION);
    assertThat(assignment.status().account(A).orElseThrow().mode()).isEqualTo(AccountMode.DRAINING);

    assignment.apply(7L, AssignmentPhase.SERVING, List.of(A), List.of(A), INCARNATION);

    // Waiting for the drain would hold it out of service for the length of that query, and
    // forever if the permit leaked.
    assertThat(assignment.status().account(A).orElseThrow().mode()).isEqualTo(AccountMode.SERVING);
    assertThat(assignment.acquire(A, Access.WRITE)).isPresent();
    inFlight.close();
  }

  /** A successor takes the account: the fence pointer moves to its marker. */
  @Test
  void aReturningAccountIsRevokedWhenItsFenceMovedWhileDraining() {
    assignment.apply(5L, AssignmentPhase.SERVING, List.of(A), List.of(A), INCARNATION);
    var inFlight = assignment.acquire(A, Access.WRITE).orElseThrow();
    assignment.apply(6L, AssignmentPhase.DRAINING, List.of(), List.of(), INCARNATION);
    // Another process took it while this one was draining; nothing tells this pod so.
    takeFence(A, "owned/6/" + Keys.encodeSegment("floecat-9"));

    assignment.apply(7L, AssignmentPhase.SERVING, List.of(A), List.of(A), INCARNATION);

    // Resuming is optimistic, so the check against the store is what has to catch it.
    assertThat(assignment.acquire(A, Access.WRITE))
        .as("a resumed account whose fence moved must not keep serving")
        .isEmpty();
    inFlight.close();
  }

  private void takeFence(String accountId, String payload) {
    String key = Keys.accountAssignmentFence(accountId);
    long version = raw.get(key).map(Pointer::getVersion).orElse(0L);
    assertThat(
            raw.compareAndSet(
                key, version, PointerReferences.opaqueMarkerPointer(key, payload, version + 1L)))
        .isTrue();
  }

  private String memberIndexPayload() {
    return raw.get(Keys.memberAssignmentIndex(MEMBER)).orElseThrow().getBlobUri();
  }

  private double counter(ai.floedb.floecat.telemetry.MetricId metric) {
    return observability.counterValue(metric);
  }

  static final class RecordingHooks implements AccountAssignment.PartitionHooks {
    final List<String> gained = new ArrayList<>();
    final List<String> lost = new ArrayList<>();

    @Override
    public void ownershipGained(String accountId) {
      gained.add(accountId);
    }

    @Override
    public void ownershipLost(String accountId) {
      lost.add(accountId);
    }

    @Override
    public String partitionState(String accountId) {
      return gained.contains(accountId) && !lost.contains(accountId) ? "COMPLETE" : "ABSENT";
    }
  }

  /** In-memory store whose consistent reads or writes can be made to fail like a store outage. */
  static final class FailableStore extends InMemoryPointerStore {
    volatile boolean failConsistentReads;
    volatile boolean failWrites;
    private volatile CountDownLatch fenceReadStarted;
    private volatile CountDownLatch releaseFenceRead;

    void blockFenceRead(CountDownLatch started, CountDownLatch release) {
      fenceReadStarted = started;
      releaseFenceRead = release;
    }

    private void awaitFenceRead(String key) {
      CountDownLatch started = fenceReadStarted;
      CountDownLatch release = releaseFenceRead;
      if (Keys.accountAssignmentFence(A).equals(key) && started != null && release != null) {
        started.countDown();
        try {
          if (!release.await(1, TimeUnit.SECONDS)) {
            throw new AssertionError("timed out waiting to release the fence read");
          }
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new AssertionError(
              "interrupted while waiting to release the fence read", interrupted);
        }
      }
    }

    @Override
    public java.util.Optional<Pointer> getConsistent(String key) {
      awaitFenceRead(key);
      synchronized (this) {
        if (failConsistentReads) {
          throw new StorageAbortRetryableException("store unreachable");
        }
        return super.getConsistent(key);
      }
    }

    @Override
    public synchronized Map<String, Pointer> getBatchConsistent(List<String> keys) {
      if (failConsistentReads) {
        throw new StorageAbortRetryableException("store unreachable");
      }
      return super.getBatchConsistent(keys);
    }

    @Override
    public synchronized boolean compareAndSet(String key, long expectedVersion, Pointer next) {
      if (failWrites) {
        throw new StorageAbortRetryableException("store unreachable");
      }
      return super.compareAndSet(key, expectedVersion, next);
    }

    @Override
    public synchronized boolean compareAndSetBatch(List<CasOp> ops) {
      if (failWrites) {
        throw new StorageAbortRetryableException("store unreachable");
      }
      return super.compareAndSetBatch(ops);
    }
  }
}
