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

import ai.floedb.floecat.storage.spi.PointerStore;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Optional;

@Singleton
@IfBuildProperty(name = "floecat.kv", stringValue = "memory")
public class MemoryReconcileJobIndexBackend implements ReconcileJobIndexBackend {
  private PointerStore pointerStore;

  @Inject
  public MemoryReconcileJobIndexBackend(PointerStore pointerStore) {
    this.pointerStore = pointerStore;
  }

  public MemoryReconcileJobIndexBackend() {}

  public void bind(PointerStore pointerStore) {
    this.pointerStore = pointerStore;
  }

  @Override
  public Optional<StoredPointerSnapshot> loadStoredPointer(String pointerKey) {
    return pointerStore
        .get(pointerKey)
        .map(
            pointer ->
                new StoredPointerSnapshot(
                    pointer.getKey(), pointer.getBlobUri(), pointer.getVersion()));
  }

  @Override
  public boolean compareAndSetBatch(ReconcileJobIndexStore.JobIndexWriteBatch batch) {
    return pointerStore.compareAndSetBatch(
        JobIndexWriteBatchSupport.toCasOps(
            batch,
            key ->
                pointerStore
                    .get(key)
                    .map(
                        pointer ->
                            new StoredPointerSnapshot(
                                pointer.getKey(), pointer.getBlobUri(), pointer.getVersion()))));
  }

  @Override
  public List<StoredPointerSnapshot> listStoredPointersByPrefix(
      String prefix, int limit, String pageToken, StringBuilder nextPageToken) {
    return pointerStore.listPointersByPrefix(prefix, limit, pageToken, nextPageToken).stream()
        .map(
            pointer ->
                new StoredPointerSnapshot(
                    pointer.getKey(), pointer.getBlobUri(), pointer.getVersion()))
        .toList();
  }
}
