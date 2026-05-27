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

package ai.floedb.floecat.service.reconciler.jobs.durable.projection;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileProjectionRefreshCursor;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Optional;

@ApplicationScoped
public class ReconcileProjectionRefreshCursorStore {
  private static final int CAS_MAX = 8;

  private PointerStore pointerStore;
  private ReconcilePayloadStore payloadStore;

  public void bind(PointerStore pointerStore, ReconcilePayloadStore payloadStore) {
    this.pointerStore = pointerStore;
    this.payloadStore = payloadStore;
  }

  public Optional<StoredReconcileProjectionRefreshCursor> load(
      String accountId, String parentJobId) {
    if (blank(accountId) || blank(parentJobId)) {
      return Optional.empty();
    }
    return pointerStore
        .get(Keys.reconcileProjectionRefreshPointer(accountId, parentJobId))
        .flatMap(pointer -> payloadStore.readInlineProjectionRefreshCursor(pointer.getBlobUri()));
  }

  public void upsert(StoredReconcileProjectionRefreshCursor cursor) {
    if (cursor == null || blank(cursor.accountId()) || blank(cursor.parentJobId())) {
      return;
    }
    String key = Keys.reconcileProjectionRefreshPointer(cursor.accountId(), cursor.parentJobId());
    String blobUri = payloadStore.encodeInlineProjectionRefreshCursor(cursor);
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer current = pointerStore.get(key).orElse(null);
      long expectedVersion = current == null ? 0L : current.getVersion();
      StoredReconcileProjectionRefreshCursor currentCursor =
          current == null
              ? null
              : payloadStore.readInlineProjectionRefreshCursor(current.getBlobUri()).orElse(null);
      if (currentCursor != null && currentCursor.targetGeneration() > cursor.targetGeneration()) {
        return;
      }
      if (currentCursor != null
          && currentCursor.targetGeneration() == cursor.targetGeneration()
          && isMoreAdvanced(currentCursor, cursor)) {
        return;
      }
      if (current != null && blobUri.equals(current.getBlobUri())) {
        return;
      }
      Pointer next =
          Pointer.newBuilder()
              .setKey(key)
              .setBlobUri(blobUri)
              .setVersion(expectedVersion + 1L)
              .build();
      if (pointerStore.compareAndSet(key, expectedVersion, next)) {
        return;
      }
    }
    throw new IllegalStateException(
        "Failed to upsert projection refresh cursor for reconcile job " + cursor.parentJobId());
  }

  public void delete(String accountId, String parentJobId) {
    if (!blank(accountId) && !blank(parentJobId)) {
      pointerStore.delete(Keys.reconcileProjectionRefreshPointer(accountId, parentJobId));
    }
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }

  private static boolean isMoreAdvanced(
      StoredReconcileProjectionRefreshCursor current,
      StoredReconcileProjectionRefreshCursor candidate) {
    if (current == null || candidate == null) {
      return false;
    }
    if (current.pageCount() != candidate.pageCount()) {
      return current.pageCount() > candidate.pageCount();
    }
    return current.childrenScanned() > candidate.childrenScanned();
  }
}
