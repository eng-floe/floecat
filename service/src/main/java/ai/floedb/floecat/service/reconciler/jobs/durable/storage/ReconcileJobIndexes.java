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

package ai.floedb.floecat.service.reconciler.jobs.durable.storage;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

@ApplicationScoped
public class ReconcileJobIndexes {
  private PointerStore pointerStore;
  private Predicate<StoredReconcileJob> requiresReadyPointer;
  private Function<StoredReconcileJob, List<String>> readyPointerKeys;

  public void bind(
      PointerStore pointerStore,
      Predicate<StoredReconcileJob> requiresReadyPointer,
      Function<StoredReconcileJob, List<String>> readyPointerKeys) {
    this.pointerStore = pointerStore;
    this.requiresReadyPointer = requiresReadyPointer;
    this.readyPointerKeys = readyPointerKeys;
  }

  public String parentPointerKey(String accountId, String parentJobId, String jobId) {
    if (blank(accountId) || blank(parentJobId) || blank(jobId)) {
      return "";
    }
    return Keys.reconcileJobByParentPointer(accountId, parentJobId, jobId);
  }

  public String connectorIndexPointerKey(
      String accountId, String connectorId, long createdAtMs, String jobId) {
    if (blank(accountId) || blank(connectorId) || blank(jobId)) {
      return "";
    }
    return Keys.reconcileJobByConnectorPointer(
        accountId, connectorId, connectorSortableJobToken(createdAtMs, jobId));
  }

  public String dedupePointerKey(StoredReconcileJob record) {
    if (record == null || blank(record.accountId) || blank(record.dedupeKeyHash)) {
      return "";
    }
    return Keys.reconcileDedupePointer(record.accountId, record.dedupeKeyHash);
  }

  public boolean hasValidLookupPointer(String jobId, String expectedReference) {
    if (blank(jobId) || blank(expectedReference)) {
      return false;
    }
    Pointer existing = pointerStore.get(Keys.reconcileJobLookupPointerById(jobId)).orElse(null);
    return existing != null && expectedReference.equals(existing.getBlobUri());
  }

  public boolean hasStateIndex(StoredReconcileJob record) {
    return record != null
        && !blank(record.state)
        && !blank(record.accountId)
        && !blank(record.jobId);
  }

  public String statePointerPrefix(String state) {
    return Keys.reconcileJobByStatePointerPrefix(state);
  }

  public String statePointerKey(StoredReconcileJob record) {
    if (!hasStateIndex(record)) {
      return "";
    }
    return Keys.reconcileJobByStatePointer(
        record.state, Math.max(0L, record.createdAtMs), record.accountId, record.jobId);
  }

  public List<String> statePointerKeys(StoredReconcileJob record) {
    if (!hasStateIndex(record)) {
      return List.of();
    }
    List<String> keys = new ArrayList<>(3);
    String globalStateKey = statePointerKey(record);
    if (!globalStateKey.isBlank()) {
      keys.add(globalStateKey);
    }
    String accountStateKey = accountStatePointerKey(record);
    if (!accountStateKey.isBlank()) {
      keys.add(accountStateKey);
    }
    String connectorStateKey = connectorStatePointerKey(record);
    if (!connectorStateKey.isBlank()) {
      keys.add(connectorStateKey);
    }
    return keys;
  }

  public boolean hasValidStatePointer(StoredReconcileJob record, String canonicalPointerKey) {
    for (String pointerKey : statePointerKeys(record)) {
      Pointer existing = pointerStore.get(pointerKey).orElse(null);
      if (existing == null || !canonicalPointerKey.equals(existing.getBlobUri())) {
        return false;
      }
    }
    return true;
  }

  public boolean hasValidReadyPointers(StoredReconcileJob record, String expectedReference) {
    if (record == null || blank(expectedReference)) {
      return false;
    }
    List<String> currentReadyKeys = readyPointerKeys.apply(record);
    if (currentReadyKeys.isEmpty()) {
      return !requiresReadyPointer.test(record);
    }
    for (String readyKey : currentReadyKeys) {
      if (!hasValidReadyPointer(readyKey, expectedReference)) {
        return false;
      }
    }
    return true;
  }

  private boolean hasValidReadyPointer(String readyPointerKey, String expectedReference) {
    if (blank(readyPointerKey) || blank(expectedReference)) {
      return false;
    }
    Pointer existing = pointerStore.get(readyPointerKey).orElse(null);
    return existing != null && expectedReference.equals(existing.getBlobUri());
  }

  private String accountStatePointerKey(StoredReconcileJob record) {
    if (!hasStateIndex(record)) {
      return "";
    }
    return Keys.reconcileJobByAccountStatePointer(
        record.accountId, record.state, Math.max(0L, record.createdAtMs), record.jobId);
  }

  private String connectorStatePointerKey(StoredReconcileJob record) {
    if (!hasStateIndex(record) || blank(record.connectorId)) {
      return "";
    }
    return Keys.reconcileJobByConnectorStatePointer(
        record.accountId,
        record.connectorId,
        record.state,
        Math.max(0L, record.createdAtMs),
        record.jobId);
  }

  private static String connectorSortableJobToken(long createdAtMs, String jobId) {
    long created = Math.max(0L, createdAtMs);
    long reversedCreated = Long.MAX_VALUE - created;
    return String.format("%019d-%s", reversedCreated, jobId);
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
