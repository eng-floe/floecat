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
import java.util.List;
import java.util.Optional;

@Singleton
@IfBuildProperty(name = "floecat.kv", stringValue = "memory")
public class MemoryReconcileProjectionBackend implements ReconcileProjectionBackend {
  private PointerStore pointerStore;

  @Inject
  public MemoryReconcileProjectionBackend(PointerStore pointerStore) {
    this.pointerStore = pointerStore;
  }

  public MemoryReconcileProjectionBackend() {}

  public void bind(PointerStore pointerStore) {
    this.pointerStore = pointerStore;
  }

  @Override
  public Optional<Pointer> loadPointer(String key) {
    return pointerStore.get(key);
  }

  @Override
  public List<Pointer> listPointersByPrefix(
      String prefix, int limit, String pageToken, StringBuilder nextPageToken) {
    return pointerStore.listPointersByPrefix(prefix, limit, pageToken, nextPageToken);
  }

  @Override
  public boolean compareAndSetBatch(ProjectionWriteBatch batch) {
    return pointerStore.compareAndSetBatch(ProjectionWriteBatchSupport.toCasOps(batch));
  }
}
