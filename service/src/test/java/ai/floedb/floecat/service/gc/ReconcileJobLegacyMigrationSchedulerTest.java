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

package ai.floedb.floecat.service.gc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexBackend;
import ai.floedb.floecat.telemetry.TestObservability;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ReconcileJobLegacyMigrationSchedulerTest {

  @Test
  void completedMigrationsRunOnlyOnce() {
    RecordingGc gc = new RecordingGc();
    ReconcileJobLegacyMigrationScheduler scheduler = scheduler(gc);

    scheduler.tick();
    scheduler.tick();

    assertEquals(1, gc.cleanupMigrationCalls);
    assertEquals(1, gc.lookupMigrationCalls);
  }

  @Test
  void lookupMigrationStopsAfterConflictOnlyPassMakesNoProgress() {
    RecordingGc gc = new RecordingGc();
    gc.lookupResults.add(new ReconcileJobGc.LookupMigrationResult(2, 1, 1, 0, ""));
    gc.lookupResults.add(new ReconcileJobGc.LookupMigrationResult(1, 0, 1, 0, ""));
    ReconcileJobLegacyMigrationScheduler scheduler = scheduler(gc);

    scheduler.tick();
    scheduler.tick();
    scheduler.tick();

    assertEquals(2, gc.lookupMigrationCalls);
  }

  @Test
  void lookupMigrationRetriesAfterRetryableFullPass() {
    RecordingGc gc = new RecordingGc();
    gc.lookupResults.add(new ReconcileJobGc.LookupMigrationResult(1, 0, 0, 1, ""));
    gc.lookupResults.add(new ReconcileJobGc.LookupMigrationResult(1, 1, 0, 0, ""));
    gc.lookupResults.add(new ReconcileJobGc.LookupMigrationResult(0, 0, 0, 0, ""));
    ReconcileJobLegacyMigrationScheduler scheduler = scheduler(gc);

    scheduler.tick();
    scheduler.tick();
    scheduler.tick();
    scheduler.tick();

    assertEquals(3, gc.lookupMigrationCalls);
  }

  @Test
  void cleanupMigrationRetriesBeforeWritingCompletionMarker() {
    RecordingGc gc = new RecordingGc();
    gc.cleanupResults.add(new ReconcileJobGc.CleanupMigrationResult(10, 2, 0, 0, 1, ""));
    gc.cleanupResults.add(new ReconcileJobGc.CleanupMigrationResult(10, 3, 0, 0, 0, ""));
    ReconcileJobLegacyMigrationScheduler scheduler = scheduler(gc);

    scheduler.tick();
    scheduler.tick();
    scheduler.tick();

    assertEquals(3, gc.cleanupMigrationCalls);
    assertEquals(1, gc.cleanupCompletionCalls);
  }

  @Test
  void cleanupMigrationCompletesAfterConflictOnlyVerificationPass() {
    RecordingGc gc = new RecordingGc();
    gc.cleanupResults.add(new ReconcileJobGc.CleanupMigrationResult(10, 2, 0, 3, 0, ""));
    gc.cleanupResults.add(new ReconcileJobGc.CleanupMigrationResult(10, 0, 0, 3, 0, ""));
    ReconcileJobLegacyMigrationScheduler scheduler = scheduler(gc);

    scheduler.tick();
    scheduler.tick();

    assertEquals(2, gc.cleanupMigrationCalls);
    assertEquals(1, gc.cleanupCompletionCalls);
  }

  @Test
  void cleanupMigrationCompletesAfterUnresolvableOnlyVerificationPass() {
    RecordingGc gc = new RecordingGc();
    gc.cleanupResults.add(new ReconcileJobGc.CleanupMigrationResult(10, 2, 3, 0, 0, ""));
    gc.cleanupResults.add(new ReconcileJobGc.CleanupMigrationResult(10, 0, 3, 0, 0, ""));
    ReconcileJobLegacyMigrationScheduler scheduler = scheduler(gc);

    scheduler.tick();
    scheduler.tick();

    assertEquals(2, gc.cleanupMigrationCalls);
    assertEquals(1, gc.cleanupCompletionCalls);
  }

  @Test
  void cleanupWaitsForDurableLookupCompletion() {
    RecordingGc gc = new RecordingGc();
    gc.lookupResults.add(new ReconcileJobGc.LookupMigrationResult(1, 0, 0, 1, ""));
    ReconcileJobLegacyMigrationScheduler scheduler = scheduler(gc);

    scheduler.tick();
    assertEquals(1, gc.lookupMigrationCalls);
    assertEquals(0, gc.cleanupMigrationCalls);

    gc.completed.add(ReconcileJobIndexBackend.LegacyMigration.LOOKUP);
    scheduler.tick();
    assertEquals(1, gc.cleanupMigrationCalls);
  }

  @Test
  void durableCompletionStopsEveryReplicaWithoutAcquiringLease() {
    RecordingGc gc = new RecordingGc();
    gc.completed.addAll(EnumSet.allOf(ReconcileJobIndexBackend.LegacyMigration.class));
    ReconcileJobLegacyMigrationScheduler scheduler = scheduler(gc);

    scheduler.tick();
    scheduler.tick();

    assertEquals(0, gc.lookupMigrationCalls);
    assertEquals(0, gc.cleanupMigrationCalls);
    assertEquals(2, gc.lookupCompletionChecks);
    assertEquals(1, gc.cleanupCompletionChecks);
  }

  @Test
  void lookupFailureDoesNotPreventCleanupFromRunning() {
    RecordingGc gc = new RecordingGc();
    gc.completed.add(ReconcileJobIndexBackend.LegacyMigration.LOOKUP);
    gc.failNextLookupCompletionCheck = true;
    ReconcileJobLegacyMigrationScheduler scheduler = scheduler(gc);

    scheduler.tick();

    assertEquals(1, gc.cleanupMigrationCalls);
  }

  @Test
  void replacementReplicaResumesDurablePassAndStillRequiresQuietVerification() {
    RecordingGc gc = new RecordingGc();
    gc.completed.add(ReconcileJobIndexBackend.LegacyMigration.LOOKUP);
    gc.cleanupResults.add(new ReconcileJobGc.CleanupMigrationResult(10, 1, 0, 0, 0, "page-2"));
    gc.cleanupResults.add(new ReconcileJobGc.CleanupMigrationResult(10, 0, 0, 0, 0, ""));
    gc.cleanupResults.add(new ReconcileJobGc.CleanupMigrationResult(10, 0, 0, 0, 0, ""));
    AtomicLong nowMs = new AtomicLong(1_000L);
    ReconcileJobLegacyMigrationScheduler first = scheduler(gc, "first", nowMs);
    ReconcileJobLegacyMigrationScheduler second = scheduler(gc, "second", nowMs);

    first.tick();
    second.tick();
    assertEquals(List.of(""), gc.cleanupPageTokens);
    assertEquals(0, gc.cleanupCompletionCalls);

    nowMs.addAndGet(101L);
    second.tick();
    assertEquals(List.of("", "page-2"), gc.cleanupPageTokens);
    assertEquals(0, gc.cleanupCompletionCalls);

    second.tick();
    assertEquals(List.of("", "page-2", ""), gc.cleanupPageTokens);
    assertEquals(1, gc.cleanupCompletionCalls);
  }

  @Test
  void replacementReplicaCompletesPersistedQuietPassWithoutRescanning() {
    RecordingGc gc = new RecordingGc();
    gc.completed.add(ReconcileJobIndexBackend.LegacyMigration.LOOKUP);
    gc.failNextCleanupCompletion = true;
    AtomicLong nowMs = new AtomicLong(1_000L);
    ReconcileJobLegacyMigrationScheduler first = scheduler(gc, "first", nowMs);
    ReconcileJobLegacyMigrationScheduler second = scheduler(gc, "second", nowMs);

    first.tick();
    assertEquals(1, gc.cleanupMigrationCalls);
    assertEquals(1, gc.cleanupCompletionCalls);

    second.tick();
    assertEquals(1, gc.cleanupMigrationCalls);
    assertEquals(1, gc.cleanupCompletionCalls);

    nowMs.addAndGet(101L);
    second.tick();
    assertEquals(1, gc.cleanupMigrationCalls);
    assertEquals(2, gc.cleanupCompletionCalls);
  }

  @Test
  void failedPostSliceCheckpointForcesAnotherFullQuietPass() {
    RecordingGc gc = new RecordingGc();
    gc.completed.add(ReconcileJobIndexBackend.LegacyMigration.LOOKUP);
    gc.failCheckpointAfterNextCleanupSlice = true;
    ReconcileJobLegacyMigrationScheduler scheduler = scheduler(gc);

    scheduler.tick();
    assertEquals(1, gc.cleanupMigrationCalls);
    assertEquals(0, gc.cleanupCompletionCalls);

    scheduler.tick();
    assertEquals(2, gc.cleanupMigrationCalls);
    assertEquals(0, gc.cleanupCompletionCalls);

    scheduler.tick();
    assertEquals(3, gc.cleanupMigrationCalls);
    assertEquals(1, gc.cleanupCompletionCalls);
  }

  private static ReconcileJobLegacyMigrationScheduler scheduler(RecordingGc gc) {
    return scheduler(gc, "scheduler", new AtomicLong(1_000L));
  }

  private static ReconcileJobLegacyMigrationScheduler scheduler(
      RecordingGc gc, String ownerId, AtomicLong nowMs) {
    ReconcileJobLegacyMigrationScheduler scheduler = new ReconcileJobLegacyMigrationScheduler();
    scheduler.reconcileJobGc = () -> gc;
    scheduler.observability = new TestObservability();
    scheduler.ownerId = ownerId;
    scheduler.currentTimeMillis = nowMs::get;
    scheduler.leaseDuration = Duration.ofMillis(100L);
    scheduler.initMeters();
    return scheduler;
  }

  private static final class RecordingGc extends ReconcileJobGc {
    private final ArrayDeque<CleanupMigrationResult> cleanupResults = new ArrayDeque<>();
    private final ArrayDeque<LookupMigrationResult> lookupResults = new ArrayDeque<>();
    private final List<String> cleanupPageTokens = new ArrayList<>();
    private final EnumMap<ReconcileJobIndexBackend.LegacyMigration, LeaseState> leases =
        new EnumMap<>(ReconcileJobIndexBackend.LegacyMigration.class);
    private final Set<ReconcileJobIndexBackend.LegacyMigration> completed =
        EnumSet.noneOf(ReconcileJobIndexBackend.LegacyMigration.class);
    private int cleanupMigrationCalls;
    private int cleanupCompletionCalls;
    private int lookupMigrationCalls;
    private int lookupCompletionChecks;
    private int cleanupCompletionChecks;
    private long nextFence;
    private boolean failNextCleanupCompletion;
    private boolean failCheckpointAfterNextCleanupSlice;
    private boolean failNextCleanupCheckpoint;
    private boolean failNextLookupCompletionCheck;

    @Override
    public CleanupMigrationResult runLegacyCleanupMigrationSlice(String pageToken) {
      cleanupMigrationCalls++;
      cleanupPageTokens.add(pageToken);
      if (failCheckpointAfterNextCleanupSlice) {
        failCheckpointAfterNextCleanupSlice = false;
        failNextCleanupCheckpoint = true;
      }
      return cleanupResults.isEmpty()
          ? new CleanupMigrationResult(0, 0, 0, 0, 0, "")
          : cleanupResults.removeFirst();
    }

    @Override
    public boolean completeLegacyCleanupMigration() {
      cleanupCompletionCalls++;
      return true;
    }

    @Override
    public LookupMigrationResult runLegacyLookupMigrationSlice(String pageToken) {
      lookupMigrationCalls++;
      return lookupResults.isEmpty()
          ? new LookupMigrationResult(0, 0, 0, 0, "")
          : lookupResults.removeFirst();
    }

    @Override
    public boolean completeLegacyLookupMigration() {
      return true;
    }

    @Override
    public synchronized Optional<ReconcileJobIndexBackend.LegacyMigrationLease>
        acquireLegacyMigrationLease(
            ReconcileJobIndexBackend.LegacyMigration migration,
            String ownerId,
            long nowMs,
            long leaseDurationMs) {
      if (completed.contains(migration)) {
        return Optional.empty();
      }
      LeaseState state = leases.computeIfAbsent(migration, ignored -> new LeaseState());
      if (state.ownerId != null
          && !state.ownerId.equals(ownerId)
          && state.leaseExpiresAtMs > nowMs) {
        return Optional.empty();
      }
      state.ownerId = ownerId;
      state.fence = ++nextFence;
      state.leaseExpiresAtMs = nowMs + leaseDurationMs;
      return Optional.of(
          new ReconcileJobIndexBackend.LegacyMigrationLease(state.fence, state.progress));
    }

    @Override
    public synchronized boolean checkpointLegacyMigration(
        ReconcileJobIndexBackend.LegacyMigration migration,
        String ownerId,
        long fence,
        ReconcileJobIndexBackend.LegacyMigrationProgress progress,
        long nowMs,
        long leaseDurationMs) {
      LeaseState state = leases.get(migration);
      if (state == null
          || !ownerId.equals(state.ownerId)
          || fence != state.fence
          || state.leaseExpiresAtMs < nowMs) {
        return false;
      }
      if (migration == ReconcileJobIndexBackend.LegacyMigration.CLEANUP
          && failNextCleanupCheckpoint) {
        failNextCleanupCheckpoint = false;
        return false;
      }
      state.progress = progress;
      state.leaseExpiresAtMs = nowMs + leaseDurationMs;
      return true;
    }

    @Override
    public synchronized boolean completeLegacyMigration(
        ReconcileJobIndexBackend.LegacyMigration migration,
        String ownerId,
        long fence,
        long nowMs) {
      LeaseState state = leases.get(migration);
      if (state == null
          || !ownerId.equals(state.ownerId)
          || fence != state.fence
          || state.leaseExpiresAtMs < nowMs
          || !state.progress.quietPassComplete()
          || state.progress.changed() != 0
          || state.progress.retryable() != 0) {
        return false;
      }
      if (migration == ReconcileJobIndexBackend.LegacyMigration.CLEANUP) {
        cleanupCompletionCalls++;
        if (failNextCleanupCompletion) {
          failNextCleanupCompletion = false;
          return false;
        }
      }
      completed.add(migration);
      return true;
    }

    @Override
    public synchronized boolean legacyMigrationComplete(
        ReconcileJobIndexBackend.LegacyMigration migration) {
      if (migration == ReconcileJobIndexBackend.LegacyMigration.LOOKUP) {
        lookupCompletionChecks++;
        if (failNextLookupCompletionCheck) {
          failNextLookupCompletionCheck = false;
          throw new IllegalStateException("lookup completion read failed");
        }
      } else {
        cleanupCompletionChecks++;
      }
      return completed.contains(migration);
    }

    private static final class LeaseState {
      private String ownerId;
      private long fence;
      private long leaseExpiresAtMs;
      private ReconcileJobIndexBackend.LegacyMigrationProgress progress =
          ReconcileJobIndexBackend.LegacyMigrationProgress.empty();
    }
  }
}
