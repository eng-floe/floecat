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

package ai.floedb.floecat.service.query;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.QUERY_PINNED_SNAPSHOT_BLOB_MISSING;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.QUERY_PINNED_TABLE_BLOB_MISSING;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.catalog.impl.RootRepairRequests;
import ai.floedb.floecat.service.error.impl.GeneratedErrorMessages;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;

/**
 * Where a pinned read fails, and what it reports when it does.
 *
 * <p>A pinned read follows refs out of an immutable, content-addressed root, so a pin whose blobs
 * still read is coherent whatever has happened to the live pointer meanwhile. There is no up-front
 * probe: if a pinned blob is gone, the read that needs it fails here, at the point of the read,
 * rather than at a check taken beforehand.
 *
 * <p>Every integrity failure raised here also enqueues the table for repair. A missing pinned blob
 * means the table's committed root names data a read cannot load, and that state persists across
 * queries until the root is re-derived -- so beyond failing this query loudly, the table goes to
 * the periodic resync re-drive. What this owns is the catalog-integrity ERROR for a pinned blob
 * read on the query path. Two sites report a broken root without coming through here: pin
 * CONSTRUCTION in {@code SnapshotHelper}, which fails before any pinned read exists, and the
 * resolving-pin root guard in {@code QueryContextStoreImpl}, which raises a repository {@code
 * CorruptionException} rather than one of these. Both take {@link RootRepairRequests} directly.
 */
@ApplicationScoped
public class PinnedReadContract {

  private final RootRepairRequests repairs;

  @Inject
  public PinnedReadContract(RootRepairRequests repairs) {
    this.repairs = repairs;
  }

  /**
   * Unwrap a pinned-table-blob load, failing with the catalog-integrity error every pinned read
   * uses when the blob is gone.
   */
  public <T> T requirePinnedTableBlob(
      Optional<T> loaded, String correlationId, ResourceId tableId) {
    return require(
        loaded,
        correlationId,
        tableId,
        QUERY_PINNED_TABLE_BLOB_MISSING,
        Map.of("table_id", tableId.getId()));
  }

  /** Snapshot-blob variant of {@link #requirePinnedTableBlob} for sites without the snapshot id. */
  public <T> T requirePinnedSnapshotBlob(
      Optional<T> loaded, String correlationId, ResourceId tableId) {
    return require(
        loaded,
        correlationId,
        tableId,
        QUERY_PINNED_SNAPSHOT_BLOB_MISSING,
        Map.of("table_id", tableId.getId()));
  }

  /** Snapshot-blob variant carrying the snapshot id in the error payload. */
  public <T> T requirePinnedSnapshotBlob(
      Optional<T> loaded, String correlationId, ResourceId tableId, long snapshotId) {
    return require(
        loaded,
        correlationId,
        tableId,
        QUERY_PINNED_SNAPSHOT_BLOB_MISSING,
        Map.of("table_id", tableId.getId(), "snapshot_id", Long.toString(snapshotId)));
  }

  /**
   * The contract itself: a vanished pinned blob fails this query loudly AND enqueues the table for
   * repair, because the pinned root still names the vanished blob and every future query would fail
   * the same way until the root is re-derived.
   */
  private <T> T require(
      Optional<T> loaded,
      String correlationId,
      ResourceId tableId,
      GeneratedErrorMessages.MessageKey key,
      Map<String, String> payload) {
    return loaded.orElseThrow(
        () -> {
          repairs.request(tableId);
          return GrpcErrors.internal(correlationId, key, payload);
        });
  }
}
