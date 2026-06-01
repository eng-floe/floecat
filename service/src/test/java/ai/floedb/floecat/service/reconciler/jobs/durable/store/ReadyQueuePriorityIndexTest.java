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

package ai.floedb.floecat.service.reconciler.jobs.durable.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.stats.spi.StatsPriorityClass;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the BY_PRIORITY ready-queue index.
 *
 * <p>Validates key format, prefix generation, round-trip decode through {@link
 * ReadyQueueBackendSupport}, and the partition-key contract used by both the memory and Dynamo
 * backends. No infrastructure (KV store, blob store, etc.) is required.
 */
class ReadyQueuePriorityIndexTest {

  // ---------------------------------------------------------------------------
  // Key format
  // ---------------------------------------------------------------------------

  @Test
  void priorityKeyStartsWithByPriorityPrefix() {
    String key = Keys.reconcileReadyByPriorityPointerByDue(0, 1_000L, "acct", "job");
    assertTrue(
        key.startsWith(Keys.reconcileReadyByPriorityPointerPrefix()),
        "BY_PRIORITY key must start with the base prefix");
  }

  @Test
  void priorityKeyIncludesPriorityOrderAsFirstSegmentAfterBase() {
    for (StatsPriorityClass cls : StatsPriorityClass.values()) {
      String key = Keys.reconcileReadyByPriorityPointerByDue(cls.order, 5_000L, "acct", "job");
      String prefix = Keys.reconcileReadyByPriorityPointerPrefix();
      assertTrue(
          key.startsWith(prefix + cls.order + "/"),
          "Key for " + cls + " must include order " + cls.order + " as first segment after prefix");
    }
  }

  @Test
  void p0KeySortsBeforeP3KeyLexicographically() {
    long dueAtMs = 1_000_000L;
    String p0Key = Keys.reconcileReadyByPriorityPointerByDue(0, dueAtMs, "acct", "job");
    String p3Key = Keys.reconcileReadyByPriorityPointerByDue(3, dueAtMs, "acct", "job");
    // Lexicographic ordering ensures P0 (order=0) is scanned before P3 (order=3).
    assertTrue(
        p0Key.compareTo(p3Key) < 0,
        "P0 key must sort before P3 key: '" + p0Key + "' vs '" + p3Key + "'");
  }

  @Test
  void earlierDueSortsBeforeLaterDueWithinSamePriorityClass() {
    String earlyKey = Keys.reconcileReadyByPriorityPointerByDue(1, 1_000L, "acct", "job");
    String lateKey = Keys.reconcileReadyByPriorityPointerByDue(1, Long.MAX_VALUE, "acct", "job");
    assertTrue(
        earlyKey.compareTo(lateKey) < 0,
        "Earlier dueAtMs must sort before later dueAtMs within the same priority class");
  }

  // ---------------------------------------------------------------------------
  // ReadyQueueBackendSupport round-trip
  // ---------------------------------------------------------------------------

  @Test
  void sliceForReadyPointerKeyRecognizesByPriorityKey() {
    String key = Keys.reconcileReadyByPriorityPointerByDue(0, 1_000L, "acct", "job1");
    var slice = ReadyQueueBackendSupport.sliceForReadyPointerKey(key);
    assertNotNull(slice, "sliceForReadyPointerKey must recognise a BY_PRIORITY key");
    assertEquals(ReconcileReadyQueueStore.ReadyIndexType.BY_PRIORITY, slice.indexType());
    assertEquals("0", slice.filterValue(), "filterValue must be the priority order as a string");
  }

  @Test
  void sliceForReadyPointerKeyExtractsCorrectOrderForAllClasses() {
    for (StatsPriorityClass cls : StatsPriorityClass.values()) {
      String key = Keys.reconcileReadyByPriorityPointerByDue(cls.order, 1_000L, "acct", "job");
      var slice = ReadyQueueBackendSupport.sliceForReadyPointerKey(key);
      assertNotNull(slice, "Slice must not be null for " + cls);
      assertEquals(
          String.valueOf(cls.order),
          slice.filterValue(),
          "filterValue must match order for " + cls);
    }
  }

  @Test
  void partitionKeyContainsPriorityPartitionPrefix() {
    var slice =
        new ReconcileReadyQueueBackend.ReadyQueueSlice(
            ReconcileReadyQueueStore.ReadyIndexType.BY_PRIORITY, "0");
    String pk = ReadyQueueBackendSupport.partitionKey(slice);
    assertTrue(
        pk.contains("priority"), "Partition key for BY_PRIORITY must contain 'priority': " + pk);
    assertTrue(pk.endsWith("0"), "Partition key must include the priority order: " + pk);
  }

  @Test
  void readyIndexPrefixReturnsPrioritySpecificPrefix() {
    var slice =
        new ReconcileReadyQueueBackend.ReadyQueueSlice(
            ReconcileReadyQueueStore.ReadyIndexType.BY_PRIORITY, "2");
    String prefix = ReadyQueueBackendSupport.readyIndexPrefix(slice);
    assertEquals(Keys.reconcileReadyByPriorityPointerPrefix(2), prefix);
  }

  @Test
  void readyIndexPrefixReturnsEmptyForBlankFilterValue() {
    var slice =
        new ReconcileReadyQueueBackend.ReadyQueueSlice(
            ReconcileReadyQueueStore.ReadyIndexType.BY_PRIORITY, "");
    String prefix = ReadyQueueBackendSupport.readyIndexPrefix(slice);
    assertTrue(prefix.isBlank(), "Prefix must be blank when filterValue is blank");
  }
}
