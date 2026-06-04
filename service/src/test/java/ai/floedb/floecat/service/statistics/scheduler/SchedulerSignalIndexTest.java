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
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.stats.spi.CoverageLevel;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class SchedulerSignalIndexTest {

  private static SchedulerSignalIndex index(long windowMs) {
    return new SchedulerSignalIndex(windowMs, 100_000L);
  }

  private static SchedulerSignalIndex index() {
    return new SchedulerSignalIndex(3_600_000L, 100_000L);
  }

  // ---------------------------------------------------------------------------
  // tableKey
  // ---------------------------------------------------------------------------

  @Test
  void tableKeyFormatIsAccountColonTable() {
    String key = SchedulerSignalIndex.tableKey("acct1", "tbl1");
    assertEquals("acct1:tbl1", key);
  }

  @Test
  void tableKeysDifferByAccount() {
    assertNotEquals(
        SchedulerSignalIndex.tableKey("acct1", "tbl"),
        SchedulerSignalIndex.tableKey("acct2", "tbl"));
  }

  // ---------------------------------------------------------------------------
  // lastSuccessfulCaptureMs
  // ---------------------------------------------------------------------------

  @Test
  void lastSuccessfulCaptureMsEmptyBeforeRecording() {
    var idx = index();
    assertTrue(idx.lastSuccessfulCaptureMs("acct:tbl").isEmpty());
  }

  @Test
  void lastSuccessfulCaptureMsReturnedAfterFinalize() {
    var idx = index();
    long now = System.currentTimeMillis();
    idx.recordFinalizedSnapshot("acct", "tbl", 1L, now);
    OptionalLong result = idx.lastSuccessfulCaptureMs(SchedulerSignalIndex.tableKey("acct", "tbl"));
    assertTrue(result.isPresent());
    assertEquals(now, result.getAsLong());
  }

  // ---------------------------------------------------------------------------
  // coverageLevel
  // ---------------------------------------------------------------------------

  @Test
  void coverageLevelIsNoneBeforeRecording() {
    var idx = index();
    assertEquals(CoverageLevel.NONE, idx.coverageLevel("acct:tbl", 1L));
  }

  @Test
  void coverageLevelIsFullAfterFinalize() {
    var idx = index();
    idx.recordFinalizedSnapshot("acct", "tbl", 1L, System.currentTimeMillis());
    assertEquals(
        CoverageLevel.FULL, idx.coverageLevel(SchedulerSignalIndex.tableKey("acct", "tbl"), 1L));
  }

  @Test
  void coverageLevelIsPartialAfterPartialRecord() {
    var idx = index();
    idx.recordPartialCoverage("acct", "tbl", 1L);
    assertEquals(
        CoverageLevel.PARTIAL, idx.coverageLevel(SchedulerSignalIndex.tableKey("acct", "tbl"), 1L));
  }

  @Test
  void fullClosedInvariant_partialCannotDowngradeFromFull() {
    var idx = index();
    idx.recordFinalizedSnapshot("acct", "tbl", 1L, System.currentTimeMillis());
    idx.recordPartialCoverage("acct", "tbl", 1L); // should be ignored
    assertEquals(
        CoverageLevel.FULL,
        idx.coverageLevel(SchedulerSignalIndex.tableKey("acct", "tbl"), 1L),
        "FULL is a closed state and must not be downgraded to PARTIAL");
  }

  // ---------------------------------------------------------------------------
  // snapshotDeltaRows
  // ---------------------------------------------------------------------------

  @Test
  void snapshotDeltaRowsEmptyBeforeRecording() {
    var idx = index();
    assertTrue(idx.snapshotDeltaRows("acct:tbl", 1L).isEmpty());
  }

  @Test
  void snapshotDeltaRowsReturnedAfterRecord() {
    var idx = index();
    idx.recordSnapshotDelta("acct", "tbl", 1L, OptionalLong.of(42_000L));
    OptionalLong result = idx.snapshotDeltaRows(SchedulerSignalIndex.tableKey("acct", "tbl"), 1L);
    assertTrue(result.isPresent());
    assertEquals(42_000L, result.getAsLong());
  }

  @Test
  void recordEmptyDeltaDoesNotWriteEntry() {
    var idx = index();
    idx.recordSnapshotDelta("acct", "tbl", 1L, OptionalLong.empty());
    assertTrue(idx.snapshotDeltaRows(SchedulerSignalIndex.tableKey("acct", "tbl"), 1L).isEmpty());
  }

  // ---------------------------------------------------------------------------
  // Demand counters
  // ---------------------------------------------------------------------------

  @Test
  void tableDemandAccumulatesAcrossCalls() {
    var idx = index();
    String key = SchedulerSignalIndex.tableKey("acct", "tbl");
    idx.recordTableDemand("acct", "tbl");
    idx.recordTableDemand("acct", "tbl");
    assertEquals(2L, idx.recentTableDemand(key));
  }

  @Test
  void tableDemandIsScopedToAccount() {
    var idx = index();
    idx.recordTableDemand("acct1", "tbl");
    assertEquals(1L, idx.recentTableDemand(SchedulerSignalIndex.tableKey("acct1", "tbl")));
    assertEquals(0L, idx.recentTableDemand(SchedulerSignalIndex.tableKey("acct2", "tbl")));
  }

  // ---------------------------------------------------------------------------
  // Drain counters
  // ---------------------------------------------------------------------------

  @Test
  void drainResetsCounter() {
    var idx = index();
    idx.recordFinalizedSnapshot("acct", "tbl", 1L, System.currentTimeMillis());
    long first = idx.drainLastCaptureKnown();
    assertTrue(first > 0L);
    assertEquals(0L, idx.drainLastCaptureKnown(), "drain must reset to 0 after first call");
  }

  // ---------------------------------------------------------------------------
  // Demand decay on read path
  // ---------------------------------------------------------------------------

  @Test
  void demandDecaysToZeroAfterTwoWindowsOnRead() {
    // Use a very short window so we can simulate time passing by using a tiny window.
    // The index is constructed with windowMs=50ms; we record demand, then read after 2×50ms.
    var idx = new SchedulerSignalIndex(50L, 100_000L);
    String key = SchedulerSignalIndex.tableKey("acct", "tbl");

    // Record demand at "now"
    idx.recordTableDemand("acct", "tbl");
    assertEquals(1L, idx.recentTableDemand(key), "Demand must be visible immediately");

    // After 2×windowMs, the read path should rotate and demand should decay to zero.
    // We use Thread.sleep to advance real time past 2 windows.
    try {
      Thread.sleep(120L);
    } catch (InterruptedException ignored) {
    }

    assertEquals(
        0L,
        idx.recentTableDemand(key),
        "Demand must decay to 0 after 2 full windows when read path triggers rotation");
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static void assertNotEquals(String a, String b) {
    if (a.equals(b)) throw new AssertionError("Expected different but got: " + a);
  }
}
