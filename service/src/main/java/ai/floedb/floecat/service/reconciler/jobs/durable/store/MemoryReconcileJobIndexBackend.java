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
    boolean committed =
        pointerStore.compareAndSetBatch(
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
    if (!committed) {
      return false;
    }
    return true;
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

  @Override
  public boolean purgeEntriesByCanonicalReference(String canonicalPointerKey) {
    if (pointerStore == null || blank(canonicalPointerKey)) {
      return false;
    }
    List<JobIndexEntrySnapshot> matches = new ArrayList<>();
    collectMatches(matches, Keys.reconcileJobLookupPointerByIdPrefix(), canonicalPointerKey);
    collectMatches(matches, "/accounts/by-id/reconcile/jobs/by-state/", canonicalPointerKey);
    collectMatches(matches, "/accounts/", canonicalPointerKey);
    boolean deleted = false;
    for (JobIndexEntrySnapshot entry : matches) {
      deleted |=
          pointerStore.compareAndSetBatch(
              List.of(new PointerStore.CasDelete(entry.pointerKey(), entry.version())));
    }
    return deleted;
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

  private void collectMatches(
      List<JobIndexEntrySnapshot> matches, String prefix, String canonicalPointerKey) {
    String token = "";
    while (true) {
      StringBuilder nextPageToken = new StringBuilder();
      List<JobIndexEntrySnapshot> page =
          pointerStore.listPointersByPrefix(prefix, 256, token, nextPageToken).stream()
              .map(
                  pointer ->
                      new JobIndexEntrySnapshot(
                          pointer.getKey(), pointer.getBlobUri(), pointer.getVersion()))
              .filter(entry -> referencesCanonical(entry, canonicalPointerKey))
              .toList();
      matches.addAll(page);
      token = nextPageToken.toString();
      if (token.isBlank()) {
        return;
      }
    }
  }

  private boolean referencesCanonical(JobIndexEntrySnapshot entry, String canonicalPointerKey) {
    return entry != null
        && (canonicalPointerKey.equals(entry.pointerKey())
            || canonicalPointerKey.equals(entry.blobUri()));
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
