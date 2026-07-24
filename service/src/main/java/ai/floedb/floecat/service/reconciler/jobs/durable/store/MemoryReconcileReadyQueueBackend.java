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

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.PointerStore;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@Singleton
@IfBuildProperty(name = "floecat.kv", stringValue = "memory")
public class MemoryReconcileReadyQueueBackend implements ReconcileReadyQueueBackend {
  private PointerStore pointerStore;

  @Inject
  public MemoryReconcileReadyQueueBackend(PointerStore pointerStore) {
    this.pointerStore = pointerStore;
  }

  public MemoryReconcileReadyQueueBackend() {}

  public void bind(PointerStore pointerStore) {
    this.pointerStore = pointerStore;
  }

  @Override
  public ReconcileReadyQueueStore.ReadyQueueScanPage scanReadySlice(
      ReadyQueueSlice slice,
      int pageSize,
      String pageToken,
      ReconcileReadyQueueStore.LeaseScanStats scanStats) {
    String prefix = ReadyQueueBackendSupport.readyIndexPrefix(slice);
    if (prefix.isBlank()) {
      return new ReconcileReadyQueueStore.ReadyQueueScanPage(List.of(), "");
    }
    int limit = Math.max(1, pageSize);
    String token = blankToEmpty(pageToken);
    List<ReconcileReadyQueueStore.ReadyQueueEntry> entries = new ArrayList<>(limit);
    while (entries.size() < limit) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers =
          pointerStore.listPointersByPrefix(prefix, limit - entries.size(), token, next);
      for (Pointer pointer : pointers) {
        if (!ReadyQueueBackendSupport.pointerBelongsToSlice(pointer.getKey(), slice)) {
          continue;
        }
        var decoded =
            ReadyQueueBackendSupport.decodeReadyQueueEntry(
                pointer.getKey(), pointer.getBlobUri(), slice);
        if (decoded != null) {
          entries.add(decoded);
        }
      }
      token = next.toString();
      if (token.isBlank() || pointers.isEmpty()) {
        break;
      }
    }
    return new ReconcileReadyQueueStore.ReadyQueueScanPage(entries, token);
  }

  @Override
  public ReadyQueueScanPage scanAllReadyEntries(int pageSize, String pageToken) {
    List<ReconcileReadyQueueStore.ReadyQueueEntry> entries = new ArrayList<>();
    collectEntriesForPrefix(
        Keys.reconcileReadyPointerPrefix(),
        new ReadyQueueSlice(ReconcileReadyQueueStore.ReadyIndexType.GLOBAL, ""),
        entries);
    collectEntriesForPrefix(Keys.reconcileReadyByExecutionClassPointerPrefix(), null, entries);
    collectEntriesForPrefix(Keys.reconcileReadyByExecutionLanePointerPrefix(), null, entries);
    collectEntriesForPrefix(Keys.reconcileReadyByPinnedExecutorPointerPrefix(), null, entries);
    collectEntriesForPrefix(Keys.reconcileReadyByJobKindPointerPrefix(), null, entries);
    entries.sort(
        Comparator.comparingLong(ReconcileReadyQueueStore.ReadyQueueEntry::dueAtMs)
            .thenComparing(ReconcileReadyQueueStore.ReadyQueueEntry::readyPointerKey));

    int offset = 0;
    if (pageToken != null && !pageToken.isBlank()) {
      try {
        offset = Math.max(0, Integer.parseInt(pageToken));
      } catch (NumberFormatException ignored) {
        offset = 0;
      }
    }
    if (offset >= entries.size()) {
      return new ReadyQueueScanPage(List.of(), "");
    }
    int end = Math.min(entries.size(), offset + Math.max(1, pageSize));
    String next = end >= entries.size() ? "" : Integer.toString(end);
    return new ReadyQueueScanPage(entries.subList(offset, end), next);
  }

  @Override
  public boolean deleteReadyEntry(String readyPointerKey) {
    Pointer current = pointerStore.get(readyPointerKey).orElse(null);
    return current != null && pointerStore.compareAndDelete(readyPointerKey, current.getVersion());
  }

  @Override
  public Optional<CanonicalPointerSnapshot> loadCanonicalSnapshot(
      String canonicalPointerKey, ReconcileReadyQueueStore.LeaseScanStats scanStats) {
    return pointerStore
        .get(canonicalPointerKey)
        .map(
            pointer ->
                new CanonicalPointerSnapshot(
                    pointer.getKey(), pointer.getBlobUri(), pointer.getVersion()));
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value;
  }

  private void collectEntriesForPrefix(
      String prefix,
      ReadyQueueSlice knownSlice,
      List<ReconcileReadyQueueStore.ReadyQueueEntry> out) {
    String token = "";
    do {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers = pointerStore.listPointersByPrefix(prefix, 512, token, next);
      for (Pointer pointer : pointers) {
        ReadyQueueSlice slice =
            knownSlice != null
                ? knownSlice
                : ReadyQueueBackendSupport.sliceForReadyPointerKey(pointer.getKey());
        if (slice == null
            || !ReadyQueueBackendSupport.pointerBelongsToSlice(pointer.getKey(), slice)) {
          continue;
        }
        var decoded =
            ReadyQueueBackendSupport.decodeReadyQueueEntry(
                pointer.getKey(), pointer.getBlobUri(), slice);
        if (decoded != null) {
          out.add(decoded);
        }
      }
      token = next.toString();
    } while (!token.isBlank());
  }
}
