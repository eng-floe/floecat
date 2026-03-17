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

package ai.floedb.floecat.gateway.iceberg.minimal.services.transaction;

import ai.floedb.floecat.gateway.iceberg.rpc.IcebergCommitJournalEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergCommitReplayIndex;
import ai.floedb.floecat.storage.kv.Keys;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Optional;

@ApplicationScoped
public class TableCommitJournalService {
  @Inject Instance<PointerStore> pointerStores;
  @Inject Instance<BlobStore> blobStores;

  PointerStore pointerStore;
  BlobStore blobStore;

  public Optional<IcebergCommitJournalEntry> get(String accountId, String tableId, String txId) {
    byte[] payload =
        loadPayload(Keys.tableCommitJournalPointer(accountId, tableId, txId)).orElse(null);
    if (payload == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(IcebergCommitJournalEntry.parseFrom(payload));
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException("failed to parse commit journal", e);
    }
  }

  public Optional<IcebergCommitReplayIndex> getReplayIndex(String accountId, String txId) {
    byte[] payload = loadPayload(Keys.tableCommitReplayPointer(accountId, txId)).orElse(null);
    if (payload == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(IcebergCommitReplayIndex.parseFrom(payload));
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException("failed to parse commit replay index", e);
    }
  }

  private Optional<byte[]> loadPayload(String key) {
    PointerStore pointerStore = resolvePointerStore();
    BlobStore blobStore = resolveBlobStore();
    if (pointerStore == null || blobStore == null) {
      return Optional.empty();
    }
    var pointer = pointerStore.get(key).orElse(null);
    if (pointer == null || pointer.getBlobUri() == null || pointer.getBlobUri().isBlank()) {
      return Optional.empty();
    }
    byte[] payload = blobStore.get(pointer.getBlobUri());
    if (payload == null) {
      throw new IllegalStateException("missing journal blob: " + pointer.getBlobUri());
    }
    return Optional.of(payload);
  }

  private PointerStore resolvePointerStore() {
    if (pointerStore != null) {
      return pointerStore;
    }
    return pointerStores != null && pointerStores.isResolvable() ? pointerStores.get() : null;
  }

  private BlobStore resolveBlobStore() {
    if (blobStore != null) {
      return blobStore;
    }
    return blobStores != null && blobStores.isResolvable() ? blobStores.get() : null;
  }
}
