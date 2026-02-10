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

package ai.floedb.floecat.service.repo.util;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.transaction.rpc.Transaction;
import ai.floedb.floecat.transaction.rpc.TransactionIntent;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

@ApplicationScoped
public class TransactionOverlay implements PointerOverlay {

  @Inject PointerStore pointerStore;
  @Inject BlobStore blobStore;

  @Override
  public Optional<Pointer> resolveEffectivePointer(String key, Pointer current) {
    if (current == null || key == null || key.isBlank()) {
      return Optional.ofNullable(current);
    }

    // Avoid overlay recursion for transaction records/intents.
    if (key.contains("/transactions/")) {
      return Optional.of(current);
    }

    String accountId = parseAccountId(key);
    if (accountId.isBlank()) {
      return Optional.of(current);
    }

    String intentKey = Keys.transactionIntentPointerByTarget(accountId, key);
    var intentPtr = pointerStore.get(intentKey).orElse(null);
    if (intentPtr == null) {
      return Optional.of(current);
    }

    TransactionIntent intent = readIntent(intentPtr.getBlobUri());
    if (intent == null || intent.getTxId().isBlank()) {
      return Optional.of(current);
    }

    Transaction tx = readTransaction(accountId, intent.getTxId());
    if (tx == null || tx.getState() != TransactionState.TS_COMMITTED) {
      return Optional.of(current);
    }

    return Optional.of(current.toBuilder().setBlobUri(intent.getBlobUri()).build());
  }

  private TransactionIntent readIntent(String blobUri) {
    try {
      byte[] bytes = blobStore.get(blobUri);
      if (bytes == null) {
        return null;
      }
      return TransactionIntent.parseFrom(bytes);
    } catch (StorageNotFoundException e) {
      return null;
    } catch (Exception e) {
      return null;
    }
  }

  private Transaction readTransaction(String accountId, String txId) {
    String txPointer = Keys.transactionPointerById(accountId, txId);
    var ptr = pointerStore.get(txPointer).orElse(null);
    if (ptr == null) {
      return null;
    }
    try {
      byte[] bytes = blobStore.get(ptr.getBlobUri());
      if (bytes == null) {
        return null;
      }
      return Transaction.parseFrom(bytes);
    } catch (StorageNotFoundException e) {
      return null;
    } catch (Exception e) {
      return null;
    }
  }

  private String parseAccountId(String pointerKey) {
    final String marker = "/accounts/";
    int idx = pointerKey.indexOf(marker);
    if (idx < 0) {
      return "";
    }
    int start = idx + marker.length();
    int end = pointerKey.indexOf('/', start);
    if (end < 0) {
      return "";
    }
    String raw = pointerKey.substring(start, end);
    if (raw.isBlank()) {
      return "";
    }
    return URLDecoder.decode(Objects.requireNonNull(raw), StandardCharsets.UTF_8);
  }
}
