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
import ai.floedb.floecat.service.repo.impl.TransactionRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.transaction.rpc.Transaction;
import ai.floedb.floecat.transaction.rpc.TransactionIntent;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TransactionIntentApplier {

  private static final Logger LOG = Logger.getLogger(TransactionIntentApplier.class);

  @Inject PointerStore pointerStore;
  @Inject BlobStore blobStore;
  @Inject TransactionRepository txRepo;
  @Inject TransactionIntentRepository intentRepo;

  public record Result(int scanned, int applied, int skipped) {}

  public Result runForAccount(String accountId, long deadlineMs) {
    int pageSize =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.transaction.apply.page-size", Integer.class)
            .orElse(500);

    String prefix = Keys.transactionIntentPointerByTargetPrefix(accountId);
    String token = "";
    StringBuilder next = new StringBuilder();
    int scanned = 0;
    int applied = 0;
    int skipped = 0;

    do {
      if (System.currentTimeMillis() > deadlineMs) {
        break;
      }
      List<Pointer> rows = pointerStore.listPointersByPrefix(prefix, pageSize, token, next);
      for (Pointer ptr : rows) {
        scanned++;
        TransactionIntent intent = readIntent(ptr.getBlobUri());
        if (intent == null) {
          skipped++;
          continue;
        }
        Transaction txn = txRepo.getById(accountId, intent.getTxId()).orElse(null);
        if (txn == null || txn.getState() != TransactionState.TS_COMMITTED) {
          skipped++;
          continue;
        }
        if (applyIntentBestEffort(intent)) {
          applied++;
        } else {
          skipped++;
        }
      }
      token = next.toString();
      next.setLength(0);
    } while (!token.isEmpty());

    return new Result(scanned, applied, skipped);
  }

  private TransactionIntent readIntent(String blobUri) {
    try {
      byte[] bytes = blobStore.get(blobUri);
      if (bytes == null) {
        return null;
      }
      return TransactionIntent.parseFrom(bytes);
    } catch (Exception e) {
      return null;
    }
  }

  private boolean applyIntentBestEffort(TransactionIntent intent) {
    String pointerKey = intent.getTargetPointerKey();
    var current = pointerStore.get(pointerKey).orElse(null);
    long expected = current == null ? 0L : current.getVersion();
    if (intent.getExpectedVersion() != 0L && expected != intent.getExpectedVersion()) {
      LOG.warnf(
          "intent apply skipped (version mismatch) key=%s expected=%d actual=%d",
          pointerKey, intent.getExpectedVersion(), expected);
      return false;
    }

    if (current != null && intent.getBlobUri().equals(current.getBlobUri())) {
      cleanupIntent(intent);
      return true;
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
    cleanupIntent(intent);
    return true;
  }

  private void cleanupIntent(TransactionIntent intent) {
    intentRepo.deleteByTarget(intent.getAccountId(), intent.getTargetPointerKey());
    intentRepo.deleteByTx(intent.getAccountId(), intent.getTxId(), intent.getTargetPointerKey());
  }

  private boolean isTableByIdPointer(String pointerKey) {
    return pointerKey != null && pointerKey.contains("/tables/by-id/");
  }

  private Table readTable(String blobUri) {
    try {
      byte[] bytes = blobStore.get(blobUri);
      if (bytes == null) {
        return null;
      }
      return Table.parseFrom(bytes);
    } catch (Exception e) {
      return null;
    }
  }

  private void updateTableNamePointers(Pointer currentPtr, Table nextTable, String nextBlobUri) {
    String accountId = nextTable.getResourceId().getAccountId();
    String newKey =
        Keys.tablePointerByName(
            accountId,
            nextTable.getCatalogId().getId(),
            nextTable.getNamespaceId().getId(),
            nextTable.getDisplayName());
    upsertPointer(newKey, nextBlobUri);

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

  private void upsertPointer(String key, String blobUri) {
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
}
