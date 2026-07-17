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

import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.PointerStore;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
@IfBuildProperty(name = "floecat.kv", stringValue = "memory")
public class MemoryReconcileJobIndexBackend implements ReconcileJobIndexBackend {
  private PointerStore pointerStore;
  private final Map<String, ReconcileJobIndexCleanupManifest> cleanupManifests =
      new ConcurrentHashMap<>();

  @Inject
  public MemoryReconcileJobIndexBackend(PointerStore pointerStore) {
    this.pointerStore = pointerStore;
  }

  public MemoryReconcileJobIndexBackend() {}

  public void bind(PointerStore pointerStore) {
    this.pointerStore = pointerStore;
  }

  @Override
  public Optional<JobIndexEntrySnapshot> loadIndexEntry(String pointerKey) {
    return pointerStore
        .get(pointerKey)
        .map(
            pointer ->
                new JobIndexEntrySnapshot(
                    pointer.getKey(), pointer.getBlobUri(), pointer.getVersion()));
  }

  @Override
  public boolean compareAndSetBatch(ReconcileJobIndexStore.JobIndexWriteBatch batch) {
    return compareAndSetBatch(batch, List.of());
  }

  @Override
  public synchronized boolean compareAndSetBatch(
      ReconcileJobIndexStore.JobIndexWriteBatch batch, List<PointerStore.CasOp> extraPointerOps) {
    List<PointerStore.CasOp> ops =
        new ArrayList<>(
            JobIndexWriteBatchSupport.toCasOps(
                batch,
                key ->
                    pointerStore
                        .get(key)
                        .map(
                            pointer ->
                                new JobIndexEntrySnapshot(
                                    pointer.getKey(),
                                    pointer.getBlobUri(),
                                    pointer.getVersion()))));
    if (extraPointerOps != null) {
      ops.addAll(extraPointerOps);
    }
    boolean committed = pointerStore.compareAndSetBatch(ops);
    if (!committed) {
      return false;
    }
    if (batch != null) {
      for (ReconcileJobIndexStore.JobIndexWriteOp write : batch.writes()) {
        if (write instanceof ReconcileJobIndexStore.JobIndexUpsert upsert
            && JobIndexBackendSupport.parseCanonicalJobKey(upsert.pointerKey()) != null) {
          cleanupManifests.put(upsert.pointerKey(), upsert.cleanupManifest());
        } else if (write instanceof ReconcileJobIndexStore.JobIndexDelete delete
            && JobIndexBackendSupport.parseCanonicalJobKey(delete.pointerKey()) != null) {
          cleanupManifests.remove(delete.pointerKey());
        }
      }
    }
    return true;
  }

  @Override
  public synchronized ReconcileJobIndexCleanupManifest loadCleanupManifest(
      String canonicalPointerKey) {
    return cleanupManifests.getOrDefault(
        canonicalPointerKey, ReconcileJobIndexCleanupManifest.EMPTY);
  }

  @Override
  public JobIndexQueryPage listCanonicalEntries(String accountId, int limit, String pageToken) {
    return listPointers(Keys.reconcileJobPointerByIdPrefix(accountId), limit, pageToken);
  }

  @Override
  public JobIndexQueryPage listDedupeEntries(String accountId, int limit, String pageToken) {
    return listPointers(Keys.reconcileDedupePointerPrefix(accountId), limit, pageToken);
  }

  @Override
  public JobIndexQueryPage listParentEntries(
      String accountId, String parentJobId, int limit, String pageToken) {
    return listPointers(
        Keys.reconcileJobByParentPointerPrefix(accountId, parentJobId), limit, pageToken);
  }

  @Override
  public JobIndexQueryPage listConnectorEntries(
      String accountId, String connectorId, int limit, String pageToken) {
    return listPointers(
        Keys.reconcileJobByConnectorPointerPrefix(accountId, connectorId), limit, pageToken);
  }

  @Override
  public JobIndexQueryPage listGlobalStateEntries(String state, int limit, String pageToken) {
    return listPointers(Keys.reconcileJobByStatePointerPrefix(state), limit, pageToken);
  }

  @Override
  public JobIndexQueryPage listAccountStateEntries(
      String accountId, String state, int limit, String pageToken) {
    return listPointers(
        Keys.reconcileJobByAccountStatePointerPrefix(accountId, state), limit, pageToken);
  }

  @Override
  public JobIndexQueryPage listConnectorStateEntries(
      String accountId, String connectorId, String state, int limit, String pageToken) {
    return listPointers(
        Keys.reconcileJobByConnectorStatePointerPrefix(accountId, connectorId, state),
        limit,
        pageToken);
  }

  private JobIndexQueryPage listPointers(String prefix, int limit, String pageToken) {
    StringBuilder nextPageToken = new StringBuilder();
    List<JobIndexEntrySnapshot> entries =
        pointerStore.listPointersByPrefix(prefix, limit, pageToken, nextPageToken).stream()
            .map(
                pointer ->
                    new JobIndexEntrySnapshot(
                        pointer.getKey(), pointer.getBlobUri(), pointer.getVersion()))
            .toList();
    return new JobIndexQueryPage(entries, nextPageToken.toString());
  }
}
