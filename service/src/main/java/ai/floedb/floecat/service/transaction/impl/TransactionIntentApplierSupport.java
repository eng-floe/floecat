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

package ai.floedb.floecat.service.transaction.impl;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.impl.TransactionIntentRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.transaction.rpc.TransactionIntent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TransactionIntentApplierSupport {

  private static final Logger LOG = Logger.getLogger(TransactionIntentApplierSupport.class);

  @Inject PointerStore pointerStore;
  @Inject BlobStore blobStore;

  public boolean isTableByIdPointer(String pointerKey) {
    return pointerKey != null && pointerKey.contains("/tables/by-id/");
  }

  public Table readTable(String blobUri) {
    try {
      byte[] bytes = blobStore.get(blobUri);
      if (bytes == null) {
        LOG.debugf("table blob missing: %s", blobUri);
        return null;
      }
      return Table.parseFrom(bytes);
    } catch (Exception e) {
      LOG.debugf("table blob parse failed: %s", blobUri, e);
      return null;
    }
  }

  public void updateTableNamePointers(Pointer currentPtr, Table nextTable, String nextBlobUri) {
    String accountId = nextTable.getResourceId().getAccountId();
    String newKey =
        Keys.tablePointerByName(
            accountId,
            nextTable.getCatalogId().getId(),
            nextTable.getNamespaceId().getId(),
            nextTable.getDisplayName());
    upsertPointerBestEffort(newKey, nextBlobUri);

    if (currentPtr == null) {
      return;
    }
    Table oldTable = readTable(currentPtr.getBlobUri());
    if (oldTable == null) {
      return;
    }
    String oldKey =
        Keys.tablePointerByName(
            oldTable.getResourceId().getAccountId(),
            oldTable.getCatalogId().getId(),
            oldTable.getNamespaceId().getId(),
            oldTable.getDisplayName());
    if (!oldKey.equals(newKey)) {
      pointerStore
          .get(oldKey)
          .ifPresent(ptr -> pointerStore.compareAndDelete(oldKey, ptr.getVersion()));
    }
  }

  public void upsertPointerBestEffort(String key, String blobUri) {
    var ptr = pointerStore.get(key).orElse(null);
    if (ptr == null) {
      Pointer created = Pointer.newBuilder().setKey(key).setBlobUri(blobUri).setVersion(1L).build();
      if (pointerStore.compareAndSet(key, 0L, created)) {
        return;
      }
      ptr = pointerStore.get(key).orElse(null);
      if (ptr == null) {
        LOG.warnf("pointer missing for %s", key);
        return;
      }
    }
    Pointer next =
        Pointer.newBuilder()
            .setKey(key)
            .setBlobUri(blobUri)
            .setVersion(ptr.getVersion() + 1)
            .build();
    if (!pointerStore.compareAndSet(key, ptr.getVersion(), next)) {
      LOG.warnf("pointer update conflict for %s", key);
    }
  }

  public boolean applyIntentBestEffort(
      TransactionIntent intent, TransactionIntentRepository intentRepo) {
    String pointerKey = intent.getTargetPointerKey();
    var current = pointerStore.get(pointerKey).orElse(null);
    long expected = current == null ? 0L : current.getVersion();
    if (current != null && intent.getBlobUri().equals(current.getBlobUri())) {
      intentRepo.deleteBothIndices(intent);
      return true;
    }
    if (intent.hasExpectedVersion() && expected != intent.getExpectedVersion()) {
      LOG.warnf(
          "intent apply skipped (version mismatch) key=%s expected=%d actual=%d",
          pointerKey, intent.getExpectedVersion(), expected);
      return false;
    }

    long nextVersion = expected + 1;
    Pointer next =
        Pointer.newBuilder()
            .setKey(pointerKey)
            .setBlobUri(intent.getBlobUri())
            .setVersion(nextVersion)
            .build();
    if (!pointerStore.compareAndSet(pointerKey, expected, next)) {
      LOG.warnf("intent apply CAS failed key=%s", pointerKey);
      return false;
    }

    if (isTableByIdPointer(pointerKey)) {
      Table nextTable = readTable(intent.getBlobUri());
      if (nextTable != null) {
        updateTableNamePointers(current, nextTable, intent.getBlobUri());
      }
    }
    intentRepo.deleteBothIndices(intent);
    return true;
  }
}
