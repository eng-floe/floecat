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

import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import java.util.function.Predicate;

public final class ReadyQueuePruneSupport {
  public enum ReadyEntryPruneReason {
    NONE,
    STALE,
    CANCELLATION_BLOCKED
  }

  private ReadyQueuePruneSupport() {}

  public static ReadyEntryPruneReason readyEntryPruneReason(
      ReconcileReadyQueueStore.ReadyQueueEntry readyEntry,
      ReconcileReadyQueueBackend readyQueueBackend,
      ReconcileReadyQueueStore readyQueueStore,
      ReconcileJobIndexStore jobIndexStore,
      Predicate<StoredReconcileJob> blockedByCancellation,
      ReconcileReadyQueueStore.LeaseScanStats scanStats) {
    if (readyEntry == null || blank(readyEntry.readyPointerKey())) {
      return ReadyEntryPruneReason.NONE;
    }
    CanonicalPointerSnapshot canonicalSnapshot =
        readyQueueBackend
            .loadCanonicalSnapshot(readyEntry.canonicalPointerKey(), scanStats)
            .orElse(null);
    if (canonicalSnapshot == null) {
      return ReadyEntryPruneReason.STALE;
    }
    var record = jobIndexStore.readRecord(canonicalSnapshot);
    if (record.isEmpty()) {
      return ReadyEntryPruneReason.STALE;
    }
    return readyEntryPruneReason(readyEntry, readyQueueStore, record.get(), blockedByCancellation);
  }

  public static ReadyEntryPruneReason readyEntryPruneReason(
      ReconcileReadyQueueStore.ReadyQueueEntry readyEntry,
      ReconcileReadyQueueStore readyQueueStore,
      StoredReconcileJob record,
      Predicate<StoredReconcileJob> blockedByCancellation) {
    if (readyEntry == null || blank(readyEntry.readyPointerKey()) || record == null) {
      return ReadyEntryPruneReason.NONE;
    }
    if (blockedByCancellation != null && Boolean.TRUE.equals(blockedByCancellation.test(record))) {
      return ReadyEntryPruneReason.CANCELLATION_BLOCKED;
    }
    if (!readyQueueStore.readyPointerMatchesRecord(readyEntry, record)) {
      return ReadyEntryPruneReason.STALE;
    }
    return ReadyEntryPruneReason.NONE;
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
