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

package ai.floedb.floecat.service.statistics.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.reconciler.jobs.CoverageLevel;
import java.util.OptionalLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SchedulerSignalIndexTest {

  private static final String ACCOUNT = "acct-1";
  private static final String TABLE = "tbl-42";
  private static final long SNAP = 7L;
  private static final String TABLE_KEY = SchedulerSignalIndex.tableKey(ACCOUNT, TABLE);

  private SchedulerSignalIndex index;

  @BeforeEach
  void setUp() {
    // 1-hour demand window
    index = new SchedulerSignalIndex(3_600_000L);
  }

  // ── tableKey ─────────────────────────────────────────────────────────────

  @Test
  void tableKeyFormatIsAccountColonTable() {
    assertEquals("acct-1:tbl-42", SchedulerSignalIndex.tableKey("acct-1", "tbl-42"));
  }

  @Test
  void tableKeysDifferByAccount() {
    String k1 = SchedulerSignalIndex.tableKey("acct-A", "tbl-1");
    String k2 = SchedulerSignalIndex.tableKey("acct-B", "tbl-1");
    assertFalse(k1.equals(k2), "Same table under different accounts must produce different keys");
  }

  // ── lastSuccessfulCaptureMs ───────────────────────────────────────────────

  @Test
  void lastSuccessfulCaptureMsReturnsEmptyBeforeRecord() {
    assertFalse(index.lastSuccessfulCaptureMs(TABLE_KEY).isPresent());
  }

  @Test
  void recordFinalizedSnapshotPopulatesLastCapture() {
    long now = System.currentTimeMillis();
    index.recordFinalizedSnapshot(ACCOUNT, TABLE, SNAP, now);

    OptionalLong result = index.lastSuccessfulCaptureMs(TABLE_KEY);
    assertTrue(result.isPresent());
    assertEquals(now, result.getAsLong());
  }

  @Test
  void recordFinalizedSnapshotOverwritesOlderTimestamp() {
    long t1 = 1_000_000L;
    long t2 = 2_000_000L;
    index.recordFinalizedSnapshot(ACCOUNT, TABLE, SNAP, t1);
    index.recordFinalizedSnapshot(ACCOUNT, TABLE, SNAP + 1, t2);

    assertEquals(t2, index.lastSuccessfulCaptureMs(TABLE_KEY).getAsLong());
  }

  @Test
  void lastSuccessfulCaptureMsIsScopedToAccount() {
    index.recordFinalizedSnapshot("acct-A", TABLE, SNAP, 1_000L);
    assertFalse(
        index.lastSuccessfulCaptureMs(SchedulerSignalIndex.tableKey("acct-B", TABLE)).isPresent());
  }

  // ── coverageLevel ─────────────────────────────────────────────────────────

  @Test
  void coverageLevelDefaultsToNone() {
    assertEquals(CoverageLevel.NONE, index.coverageLevel(TABLE_KEY, SNAP));
  }

  @Test
  void recordPartialCoverageSetsCoveragePartial() {
    index.recordPartialCoverage(ACCOUNT, TABLE, SNAP);
    assertEquals(CoverageLevel.PARTIAL, index.coverageLevel(TABLE_KEY, SNAP));
  }

  @Test
  void recordFinalizedSnapshotSetsCoverageFull() {
    index.recordFinalizedSnapshot(ACCOUNT, TABLE, SNAP, System.currentTimeMillis());
    assertEquals(CoverageLevel.FULL, index.coverageLevel(TABLE_KEY, SNAP));
  }

  @Test
  void partialDoesNotDowngradeFull() {
    index.recordFinalizedSnapshot(ACCOUNT, TABLE, SNAP, System.currentTimeMillis());
    index.recordPartialCoverage(ACCOUNT, TABLE, SNAP); // late write — must be ignored
    assertEquals(
        CoverageLevel.FULL,
        index.coverageLevel(TABLE_KEY, SNAP),
        "A late PARTIAL write must not downgrade a closed FULL entry");
  }

  @Test
  void partialCanBeUpgradedToFull() {
    index.recordPartialCoverage(ACCOUNT, TABLE, SNAP);
    index.recordFinalizedSnapshot(ACCOUNT, TABLE, SNAP, System.currentTimeMillis());
    assertEquals(CoverageLevel.FULL, index.coverageLevel(TABLE_KEY, SNAP));
  }

  @Test
  void coverageLevelIsScopedToSnapshot() {
    index.recordFinalizedSnapshot(ACCOUNT, TABLE, SNAP, System.currentTimeMillis());
    assertEquals(
        CoverageLevel.NONE,
        index.coverageLevel(TABLE_KEY, SNAP + 1),
        "Coverage for a different snapshot must not bleed over");
  }

  // ── snapshotDeltaRows ─────────────────────────────────────────────────────

  @Test
  void snapshotDeltaRowsReturnsEmptyBeforeRecord() {
    assertFalse(index.snapshotDeltaRows(TABLE_KEY, SNAP).isPresent());
  }

  @Test
  void recordSnapshotDeltaWithPresentValueStores() {
    index.recordSnapshotDelta(ACCOUNT, TABLE, SNAP, OptionalLong.of(42_000L));
    OptionalLong result = index.snapshotDeltaRows(TABLE_KEY, SNAP);
    assertTrue(result.isPresent());
    assertEquals(42_000L, result.getAsLong());
  }

  @Test
  void recordSnapshotDeltaWithEmptyDoesNotStore() {
    index.recordSnapshotDelta(ACCOUNT, TABLE, SNAP, OptionalLong.empty());
    assertFalse(index.snapshotDeltaRows(TABLE_KEY, SNAP).isPresent());
  }

  @Test
  void snapshotDeltaRowsIsScopedToSnapshot() {
    index.recordSnapshotDelta(ACCOUNT, TABLE, SNAP, OptionalLong.of(100L));
    assertFalse(index.snapshotDeltaRows(TABLE_KEY, SNAP + 1).isPresent());
  }

  @Test
  void snapshotSignalCachesAreBoundedByConfiguredMaxEntries() {
    SchedulerSignalIndex bounded = new SchedulerSignalIndex(3_600_000L, 3L, 86_400_000L);
    for (int i = 0; i < 25; i++) {
      String table = "tbl-" + i;
      bounded.recordPartialCoverage(ACCOUNT, table, SNAP + i);
      bounded.recordSnapshotDelta(ACCOUNT, table, SNAP + i, OptionalLong.of(i));
    }

    assertTrue(
        bounded.coverageEntryCount() <= 3L,
        "coverage cache must stay within configured max entries");
    assertTrue(
        bounded.deltaEntryCount() <= 3L, "delta cache must stay within configured max entries");
  }

  // ── demand counters ───────────────────────────────────────────────────────

  @Test
  void recentTableDemandStartsAtZero() {
    assertEquals(0L, index.recentTableDemand(TABLE_KEY));
  }

  @Test
  void recordTableDemandIncrementsCounter() {
    index.recordTableDemand(ACCOUNT, TABLE);
    index.recordTableDemand(ACCOUNT, TABLE);
    assertEquals(2L, index.recentTableDemand(TABLE_KEY));
  }

  @Test
  void recentColumnDemandStartsAtZero() {
    assertEquals(0L, index.recentColumnDemand(TABLE_KEY, "col_a"));
  }

  @Test
  void recordColumnDemandIncrementsCounter() {
    index.recordColumnDemand(ACCOUNT, TABLE, "col_a");
    index.recordColumnDemand(ACCOUNT, TABLE, "col_a");
    index.recordColumnDemand(ACCOUNT, TABLE, "col_b");
    assertEquals(2L, index.recentColumnDemand(TABLE_KEY, "col_a"));
    assertEquals(1L, index.recentColumnDemand(TABLE_KEY, "col_b"));
  }

  @Test
  void recordColumnDemandNormalizesSelector() {
    // Write with mixed case / surrounding spaces
    index.recordColumnDemand(ACCOUNT, TABLE, "  COL_A  ");
    // Read with already-normalized form
    assertEquals(
        1L,
        index.recentColumnDemand(TABLE_KEY, "col_a"),
        "Selector must be trimmed + lowercased before storage");
  }

  @Test
  void recordColumnDemandIgnoresBlankSelector() {
    index.recordColumnDemand(ACCOUNT, TABLE, "   ");
    assertEquals(0L, index.recentColumnDemand(TABLE_KEY, ""), "Blank selector must be ignored");
  }

  @Test
  void recordColumnDemandIgnoresNullSelector() {
    // Must not throw
    index.recordColumnDemand(ACCOUNT, TABLE, null);
    assertEquals(0L, index.recentColumnDemand(TABLE_KEY, ""));
  }

  @Test
  void tableDemandIsScopedToAccount() {
    index.recordTableDemand("acct-A", TABLE);
    assertEquals(0L, index.recentTableDemand(SchedulerSignalIndex.tableKey("acct-B", TABLE)));
  }

  // ── demand window rotation ────────────────────────────────────────────────

  @Test
  void windowRotationPreservesCountInPreviousBucket() {
    // Use a very short window so we can simulate expiry with a fabricated index
    SchedulerSignalIndex shortWindow = new SchedulerSignalIndex(100L /* ms */);
    shortWindow.recordTableDemand(ACCOUNT, TABLE);

    // Sleep just past the window boundary
    try {
      Thread.sleep(110L);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // After rotation the old count should still be visible (previous bucket)
    long demand = shortWindow.recentTableDemand(TABLE_KEY);
    assertEquals(
        1L, demand, "Demand from just-expired window must remain visible via previous bucket");
  }

  @Test
  void longIdleGapClearsBothBuckets() {
    // Window = 50ms; gap = 110ms → elapsed >= 2 * windowMs → both buckets cleared
    SchedulerSignalIndex shortWindow = new SchedulerSignalIndex(50L);
    shortWindow.recordTableDemand(ACCOUNT, TABLE);

    try {
      Thread.sleep(110L);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Trigger rotation via a read
    long demand = shortWindow.recentTableDemand(TABLE_KEY);
    assertEquals(0L, demand, "After 2x window idle gap both buckets must be cleared");
  }

  // ── unknown-rate drain counters ───────────────────────────────────────────

  @Test
  void drainLastCaptureKnownCountsHitsAndResets() {
    index.recordFinalizedSnapshot(ACCOUNT, TABLE, SNAP, 1_000L);
    index.lastSuccessfulCaptureMs(TABLE_KEY); // hit
    index.lastSuccessfulCaptureMs(TABLE_KEY); // hit
    assertEquals(2L, index.drainLastCaptureKnown());
    assertEquals(0L, index.drainLastCaptureKnown()); // reset after drain
  }

  @Test
  void drainLastCaptureUnknownCountsMissesAndResets() {
    index.lastSuccessfulCaptureMs("no-such-key"); // miss
    assertEquals(1L, index.drainLastCaptureUnknown());
    assertEquals(0L, index.drainLastCaptureUnknown());
  }

  @Test
  void drainCoverageKnownCountsHits() {
    index.recordPartialCoverage(ACCOUNT, TABLE, SNAP);
    index.coverageLevel(TABLE_KEY, SNAP); // hit
    assertEquals(1L, index.drainCoverageKnown());
    assertEquals(0L, index.drainCoverageKnown());
  }

  @Test
  void drainCoverageUnknownCountsMisses() {
    index.coverageLevel(TABLE_KEY, 999L); // miss (NONE default)
    assertEquals(1L, index.drainCoverageUnknown());
    assertEquals(0L, index.drainCoverageUnknown());
  }

  @Test
  void drainDeltaKnownCountsHits() {
    index.recordSnapshotDelta(ACCOUNT, TABLE, SNAP, OptionalLong.of(5L));
    index.snapshotDeltaRows(TABLE_KEY, SNAP); // hit
    assertEquals(1L, index.drainDeltaKnown());
    assertEquals(0L, index.drainDeltaKnown());
  }

  @Test
  void drainDeltaUnknownCountsMisses() {
    index.snapshotDeltaRows(TABLE_KEY, 999L); // miss
    assertEquals(1L, index.drainDeltaUnknown());
    assertEquals(0L, index.drainDeltaUnknown());
  }
}
