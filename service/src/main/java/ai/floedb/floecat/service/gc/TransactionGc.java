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

package ai.floedb.floecat.service.gc;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.transaction.rpc.Transaction;
import ai.floedb.floecat.transaction.rpc.TransactionIntent;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TransactionGc {

  private static final Logger LOG = Logger.getLogger(TransactionGc.class);

  @Inject PointerStore pointerStore;
  @Inject BlobStore blobStore;

  public record Result(int scanned, int deleted, int intentsDeleted) {}

  public Result runForAccount(String accountId, long deadlineMs) {
    int pageSize =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.gc.transaction.page-size", Integer.class)
            .orElse(200);
    long minAgeMs =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.gc.transaction.min-age-ms", Long.class)
            .orElse(60_000L);
    long nowMs = System.currentTimeMillis();

    int scanned = 0;
    int deleted = 0;
    int intentsDeleted = 0;

    String prefix = Keys.transactionPointerByIdPrefix(accountId);
    String token = "";
    StringBuilder next = new StringBuilder();
    do {
      if (System.currentTimeMillis() > deadlineMs) {
        break;
      }
      List<Pointer> rows = pointerStore.listPointersByPrefix(prefix, pageSize, token, next);
      for (Pointer p : rows) {
        scanned++;
        Transaction txn = readTransaction(p.getBlobUri());
        if (txn == null) {
          continue;
        }
        if (!shouldCollect(accountId, txn, nowMs, minAgeMs)) {
          continue;
        }
        if (!pointerStore.compareAndDelete(p.getKey(), p.getVersion())) {
          continue;
        }
        intentsDeleted += cleanupIntentsForTx(accountId, txn.getTxId(), pageSize);
        blobStore.deletePrefix(Keys.transactionBlobPrefix(accountId, txn.getTxId()));
        blobStore.deletePrefix(Keys.transactionIntentBlobPrefix(accountId, txn.getTxId()));
        blobStore.deletePrefix(Keys.transactionObjectBlobPrefix(accountId, txn.getTxId()));
        deleted++;
      }
      token = next.toString();
      next.setLength(0);
    } while (!token.isEmpty());

    intentsDeleted += cleanupDanglingTargetIntents(accountId, pageSize, deadlineMs);
    return new Result(scanned, deleted, intentsDeleted);
  }

  private boolean shouldCollect(String accountId, Transaction txn, long nowMs, long minAgeMs) {
    if (txn.getState() == TransactionState.TS_ABORTED) {
      return true;
    }
    if (txn.getState() == TransactionState.TS_COMMITTED) {
      if (!txn.hasExpiresAt()) {
        return false;
      }
      long exp = com.google.protobuf.util.Timestamps.toMillis(txn.getExpiresAt());
      if (exp + minAgeMs > nowMs) {
        return false;
      }
      return !hasIntentsForTx(accountId, txn.getTxId());
    }
    if (txn.hasExpiresAt()) {
      long exp = com.google.protobuf.util.Timestamps.toMillis(txn.getExpiresAt());
      return exp + minAgeMs <= nowMs;
    }
    return false;
  }

  private Transaction readTransaction(String blobUri) {
    try {
      byte[] bytes = blobStore.get(blobUri);
      if (bytes == null) {
        LOG.warnf("transaction blob missing: %s", blobUri);
        return null;
      }
      return Transaction.parseFrom(bytes);
    } catch (Exception e) {
      LOG.warnf("transaction blob parse failed: %s", e.getMessage());
      return null;
    }
  }

  private boolean hasIntentsForTx(String accountId, String txId) {
    String prefix = Keys.transactionIntentPointerByTxPrefix(accountId, txId);
    StringBuilder next = new StringBuilder();
    List<Pointer> rows = pointerStore.listPointersByPrefix(prefix, 1, "", next);
    return !rows.isEmpty();
  }

  private int cleanupIntentsForTx(String accountId, String txId, int pageSize) {
    String prefix = Keys.transactionIntentPointerByTxPrefix(accountId, txId);
    String token = "";
    StringBuilder next = new StringBuilder();
    int deleted = 0;
    do {
      List<Pointer> rows = pointerStore.listPointersByPrefix(prefix, pageSize, token, next);
      for (Pointer p : rows) {
        String target = decodeTargetFromTxIntentKey(p.getKey());
        if (!target.isBlank()) {
          String targetKey = Keys.transactionIntentPointerByTarget(accountId, target);
          pointerStore
              .get(targetKey)
              .ifPresent(
                  ptr -> {
                    if (p.getBlobUri().equals(ptr.getBlobUri())) {
                      pointerStore.compareAndDelete(ptr.getKey(), ptr.getVersion());
                    }
                  });
        }
        if (pointerStore.compareAndDelete(p.getKey(), p.getVersion())) {
          deleted++;
        }
      }
      token = next.toString();
      next.setLength(0);
    } while (!token.isEmpty());
    return deleted;
  }

  private int cleanupDanglingTargetIntents(String accountId, int pageSize, long deadlineMs) {
    String prefix = Keys.transactionIntentPointerByTargetPrefix(accountId);
    String token = "";
    StringBuilder next = new StringBuilder();
    int deleted = 0;
    do {
      if (System.currentTimeMillis() > deadlineMs) {
        break;
      }
      List<Pointer> rows = pointerStore.listPointersByPrefix(prefix, pageSize, token, next);
      for (Pointer p : rows) {
        TransactionIntent intent = readIntent(p.getBlobUri());
        if (intent == null || intent.getTxId().isBlank()) {
          if (pointerStore.compareAndDelete(p.getKey(), p.getVersion())) {
            deleted++;
          }
          continue;
        }
        Transaction txn = readTransactionById(accountId, intent.getTxId());
        if (txn == null || txn.getState() == TransactionState.TS_ABORTED) {
          deleteByTxIfBlobMatches(accountId, intent, p.getBlobUri());
          if (pointerStore.compareAndDelete(p.getKey(), p.getVersion())) {
            deleted++;
          }
        }
      }
      token = next.toString();
      next.setLength(0);
    } while (!token.isEmpty());
    return deleted;
  }

  private void deleteByTxIfBlobMatches(
      String accountId, TransactionIntent intent, String expectedBlobUri) {
    String targetPointerKey = intent.getTargetPointerKey();
    if (targetPointerKey == null || targetPointerKey.isBlank()) {
      return;
    }
    String byTxKey =
        Keys.transactionIntentPointerByTx(accountId, intent.getTxId(), targetPointerKey);
    pointerStore
        .get(byTxKey)
        .ifPresent(
            ptr -> {
              if (expectedBlobUri.equals(ptr.getBlobUri())) {
                pointerStore.compareAndDelete(ptr.getKey(), ptr.getVersion());
              }
            });
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

  private Transaction readTransactionById(String accountId, String txId) {
    String txPointer = Keys.transactionPointerById(accountId, txId);
    var ptr = pointerStore.get(txPointer).orElse(null);
    if (ptr == null) {
      return null;
    }
    return readTransaction(ptr.getBlobUri());
  }

  private String decodeTargetFromTxIntentKey(String key) {
    int idx = key.lastIndexOf("/intents/");
    if (idx < 0) {
      return "";
    }
    String encoded = key.substring(idx + "/intents/".length());
    if (encoded.isBlank()) {
      return "";
    }
    return URLDecoder.decode(encoded, StandardCharsets.UTF_8);
  }
}
