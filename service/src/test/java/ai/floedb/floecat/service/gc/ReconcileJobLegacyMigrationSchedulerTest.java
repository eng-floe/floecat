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
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ReconcileJobLegacyMigrationSchedulerTest {

  @Test
  void completedLookupMigrationRunsOnlyOnce() {
    RecordingGc gc = new RecordingGc();
    ReconcileJobLegacyMigrationScheduler scheduler = scheduler(gc);

    scheduler.tick();
    scheduler.tick();

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
  void durableLookupCompletionStopsEveryReplicaWithoutAcquiringLease() {
    RecordingGc gc = new RecordingGc();
    gc.completed.add(ReconcileJobIndexBackend.LegacyMigration.LOOKUP);
    ReconcileJobLegacyMigrationScheduler scheduler = scheduler(gc);

    scheduler.tick();
    scheduler.tick();

    assertEquals(0, gc.lookupMigrationCalls);
    assertEquals(1, gc.lookupCompletionChecks);
  }

  private static ReconcileJobLegacyMigrationScheduler scheduler(RecordingGc gc) {
    ReconcileJobLegacyMigrationScheduler scheduler = new ReconcileJobLegacyMigrationScheduler();
    scheduler.reconcileJobGc = () -> gc;
    scheduler.observability = new TestObservability();
    scheduler.ownerId = "scheduler";
    scheduler.currentTimeMillis = new AtomicLong(1_000L)::get;
    scheduler.leaseDuration = Duration.ofMillis(100L);
    scheduler.initMeters();
    return scheduler;
  }

  private static final class RecordingGc extends ReconcileJobGc {
    private final ArrayDeque<LookupMigrationResult> lookupResults = new ArrayDeque<>();
    private final EnumMap<ReconcileJobIndexBackend.LegacyMigration, LeaseState> leases =
        new EnumMap<>(ReconcileJobIndexBackend.LegacyMigration.class);
    private final Set<ReconcileJobIndexBackend.LegacyMigration> completed =
        EnumSet.noneOf(ReconcileJobIndexBackend.LegacyMigration.class);
    private int lookupMigrationCalls;
    private int lookupCompletionChecks;
    private long nextFence;

    @Override
    public LookupMigrationResult runLegacyLookupMigrationSlice(String pageToken) {
      lookupMigrationCalls++;
      return lookupResults.isEmpty()
          ? new LookupMigrationResult(0, 0, 0, 0, "")
          : lookupResults.removeFirst();
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
      completed.add(migration);
      return true;
    }

    @Override
    public synchronized boolean legacyMigrationComplete(
        ReconcileJobIndexBackend.LegacyMigration migration) {
      lookupCompletionChecks++;
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
