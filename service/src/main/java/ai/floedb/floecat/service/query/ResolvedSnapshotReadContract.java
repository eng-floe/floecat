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
import ai.floedb.floecat.service.metagraph.snapshot.SnapshotRetentionPolicy;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;

/**
 * Where a resolved-snapshot read fails, and what it reports when it does.
 *
 * <p>A resolved-snapshot read follows refs out of an immutable, content-addressed root, so a
 * selection whose blobs still read is coherent whatever has happened to the live pointer meanwhile.
 * There is no up-front probe: if a resolved snapshot blob is gone, the read that needs it fails
 * here, at the point of the read, rather than at a check taken beforehand.
 *
 * <p>Every integrity failure raised here also enqueues the table for repair. A missing resolved
 * snapshot blob means the table's committed root names data a read cannot load, and that state
 * persists across queries until the root is re-derived -- so beyond failing this query loudly, the
 * table goes to the periodic resync re-drive. What this owns is the catalog-integrity ERROR for a
 * resolved snapshot blob read on the query path. Selection construction in {@code SnapshotHelper}
 * fails before any resolved read exists. Both paths take {@link RootRepairRequests} directly.
 */
@ApplicationScoped
public class ResolvedSnapshotReadContract {

  private final RootRepairRequests repairs;
  private final SnapshotRetentionPolicy retention;

  @Inject
  public ResolvedSnapshotReadContract(
      RootRepairRequests repairs, SnapshotRetentionPolicy retention) {
    this.repairs = repairs;
    this.retention = retention;
  }

  /** Compatibility constructor for embedded tests and standalone callers. */
  public ResolvedSnapshotReadContract(RootRepairRequests repairs) {
    this(
        repairs,
        new SnapshotRetentionPolicy(
            java.time.Clock.systemUTC(),
            java.time.Duration.ofDays(30),
            java.time.Duration.ofDays(7)));
  }

  /**
   * Unwrap a resolved-table-blob load, failing with the catalog-integrity error every resolved read
   * uses when the blob is gone.
   */
  public <T> T requireResolvedTableBlob(
      Optional<T> loaded, String correlationId, ResourceId tableId) {
    return require(
        loaded,
        correlationId,
        tableId,
        QUERY_PINNED_TABLE_BLOB_MISSING,
        Map.of("table_id", tableId.getId()));
  }

  /**
   * Snapshot-blob variant of {@link #requireResolvedTableBlob} for sites without the snapshot id.
   */
  public <T> T requireResolvedSnapshotBlob(
      Optional<T> loaded, String correlationId, ResourceId tableId) {
    return require(
        loaded,
        correlationId,
        tableId,
        QUERY_PINNED_SNAPSHOT_BLOB_MISSING,
        Map.of("table_id", tableId.getId()));
  }

  /** Snapshot-blob variant carrying the snapshot id in the error payload. */
  public <T> T requireResolvedSnapshotBlob(
      Optional<T> loaded, String correlationId, ResourceId tableId, long snapshotId) {
    return requireResolvedSnapshotBlob(loaded, correlationId, tableId, snapshotId, null);
  }

  /**
   * Variant carrying publication time. Once the grace period has elapsed, a live query must restart
   * instead of being reported as catalog corruption when its immutable snapshot is reclaimed.
   */
  public <T> T requireResolvedSnapshotBlob(
      Optional<T> loaded,
      String correlationId,
      ResourceId tableId,
      long snapshotId,
      Timestamp ingestedAt) {
    if (retention.gcEligible(ingestedAt)) {
      throw GrpcErrors.snapshotExpired(
          correlationId,
          null,
          Map.of("table_id", tableId.getId(), "snapshot_id", Long.toString(snapshotId)));
    }
    return require(
        loaded,
        correlationId,
        tableId,
        QUERY_PINNED_SNAPSHOT_BLOB_MISSING,
        Map.of("table_id", tableId.getId(), "snapshot_id", Long.toString(snapshotId)));
  }

  /**
   * The contract itself: a vanished resolved snapshot blob fails this query loudly AND enqueues the
   * table for repair, because the resolved root still names the vanished blob and every future
   * query would fail the same way until the root is re-derived.
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
