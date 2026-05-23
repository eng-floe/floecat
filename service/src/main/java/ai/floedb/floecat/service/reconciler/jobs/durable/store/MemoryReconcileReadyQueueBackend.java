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
import ai.floedb.floecat.storage.spi.PointerStore;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
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
      ReadyQueueSlice slice, int pageSize, String pageToken) {
    String prefix = ReadyQueueBackendSupport.readyIndexPrefix(slice);
    if (prefix.isBlank()) {
      return new ReconcileReadyQueueStore.ReadyQueueScanPage(List.of(), "");
    }
    StringBuilder next = new StringBuilder();
    List<Pointer> pointers =
        pointerStore.listPointersByPrefix(
            prefix, Math.max(1, pageSize), blankToEmpty(pageToken), next);
    List<ReconcileReadyQueueStore.ReadyQueueEntry> entries = new ArrayList<>(pointers.size());
    for (Pointer pointer : pointers) {
      var decoded =
          ReadyQueueBackendSupport.decodeReadyQueueEntry(
              pointer.getKey(), pointer.getBlobUri(), slice);
      if (decoded != null) {
        entries.add(decoded);
      }
    }
    return new ReconcileReadyQueueStore.ReadyQueueScanPage(entries, next.toString());
  }

  @Override
  public Optional<CanonicalPointerSnapshot> loadCanonicalSnapshot(String canonicalPointerKey) {
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
}
