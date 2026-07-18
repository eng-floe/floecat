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

import ai.floedb.floecat.telemetry.TestObservability;
import java.util.ArrayDeque;
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
    gc.cleanupResults.add(new ReconcileJobGc.CleanupMigrationResult(10, 2, 0, 1, ""));
    gc.cleanupResults.add(new ReconcileJobGc.CleanupMigrationResult(10, 3, 0, 0, ""));
    ReconcileJobLegacyMigrationScheduler scheduler = scheduler(gc);

    scheduler.tick();
    scheduler.tick();
    scheduler.tick();

    assertEquals(2, gc.cleanupMigrationCalls);
    assertEquals(1, gc.cleanupCompletionCalls);
  }

  @Test
  void cleanupMigrationDoesNotCompleteWhileConflictsRemain() {
    RecordingGc gc = new RecordingGc();
    gc.cleanupResults.add(new ReconcileJobGc.CleanupMigrationResult(10, 2, 3, 0, ""));
    gc.cleanupResults.add(new ReconcileJobGc.CleanupMigrationResult(10, 0, 3, 0, ""));
    ReconcileJobLegacyMigrationScheduler scheduler = scheduler(gc);

    scheduler.tick();
    scheduler.tick();

    assertEquals(2, gc.cleanupMigrationCalls);
    assertEquals(0, gc.cleanupCompletionCalls);
  }

  private static ReconcileJobLegacyMigrationScheduler scheduler(RecordingGc gc) {
    ReconcileJobLegacyMigrationScheduler scheduler = new ReconcileJobLegacyMigrationScheduler();
    scheduler.reconcileJobGc = () -> gc;
    scheduler.observability = new TestObservability();
    scheduler.initMeters();
    return scheduler;
  }

  private static final class RecordingGc extends ReconcileJobGc {
    private final ArrayDeque<CleanupMigrationResult> cleanupResults = new ArrayDeque<>();
    private final ArrayDeque<LookupMigrationResult> lookupResults = new ArrayDeque<>();
    private int cleanupMigrationCalls;
    private int cleanupCompletionCalls;
    private int lookupMigrationCalls;

    @Override
    public CleanupMigrationResult runLegacyCleanupMigrationSlice(String pageToken) {
      cleanupMigrationCalls++;
      return cleanupResults.isEmpty()
          ? new CleanupMigrationResult(0, 0, 0, 0, "")
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
  }
}
