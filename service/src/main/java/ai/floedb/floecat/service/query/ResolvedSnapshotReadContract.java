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

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.QUERY_PINNED_TABLE_BLOB_MISSING;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.service.catalog.impl.RootRepairRequests;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.metagraph.snapshot.SnapshotRetentionPolicy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;

/**
 * Where a resolved-snapshot read fails, and what it reports when it does.
 *
 * <p>A resolved-snapshot read follows refs out of an immutable, content-addressed root, so a
 * selection whose blobs still read is coherent whatever has happened to the live pointer meanwhile.
 * There is no up-front probe: if a resolved snapshot blob is gone, the read that needs it fails
 * here, at the point of the read, rather than at a check taken beforehand.
 *
 * <p>A missing table blob is a catalog-integrity failure and is queued for repair. A missing
 * resolved snapshot blob is different: explicit snapshot deletion and normal immutable-object GC
 * may invalidate a running query. That path returns the snapshot-unavailable error and must not
 * enqueue root repair for an otherwise healthy table.
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

  /** Compatibility constructor for embedded graph builders and focused tests. */
  public ResolvedSnapshotReadContract(RootRepairRequests repairs) {
    this(repairs, SnapshotRetentionPolicy.disabled());
  }

  /**
   * Fails {@code MC_SNAPSHOT_EXPIRED} once a selection retention may expire is past retention plus
   * grace. A selection retention kept regardless of age carries no publication time and never
   * expires here.
   */
  public void requireReadable(String correlationId, TablePin selection) {
    if (selection.hasIngestedAt() && retention.expired(selection.getIngestedAt())) {
      throw expired(correlationId, selection.getTableId(), selection.getSnapshotId());
    }
  }

  /**
   * Reads a selected table definition. A missing blob is repaired only when {@code repairIfMissing}
   * says the live root still owns it; otherwise the selection lost it and reports the snapshot
   * unavailable. The probe runs only on a miss.
   */
  public <T> T requireResolvedTableBlob(
      Optional<T> loaded,
      String correlationId,
      ResourceId tableId,
      BooleanSupplier repairIfMissing) {
    return tableBlob(
        loaded, correlationId, tableId, Map.of("table_id", tableId.getId()), repairIfMissing);
  }

  public <T> T requireResolvedTableBlob(
      Optional<T> loaded,
      String correlationId,
      TablePin selection,
      BooleanSupplier repairIfMissing) {
    requireReadable(correlationId, selection);
    return tableBlob(
        loaded,
        correlationId,
        selection.getTableId(),
        Map.of(
            "table_id", selection.getTableId().getId(),
            "snapshot_id", Long.toString(selection.getSnapshotId())),
        repairIfMissing);
  }

  private <T> T tableBlob(
      Optional<T> loaded,
      String correlationId,
      ResourceId tableId,
      Map<String, String> payload,
      BooleanSupplier repairIfMissing) {
    if (loaded.isPresent()) {
      return loaded.orElseThrow();
    }
    if (repairIfMissing.getAsBoolean()) {
      repairs.request(tableId);
      throw GrpcErrors.internal(correlationId, QUERY_PINNED_TABLE_BLOB_MISSING, payload);
    }
    throw GrpcErrors.snapshotExpired(correlationId, null, payload);
  }

  /** Snapshot-blob unwrap for sites without a selection: a missing blob is unavailable. */
  public <T> T requireResolvedSnapshotBlob(
      Optional<T> loaded, String correlationId, ResourceId tableId) {
    return loaded.orElseThrow(
        () -> GrpcErrors.snapshotExpired(correlationId, null, Map.of("table_id", tableId.getId())));
  }

  public <T> T requireResolvedSnapshotBlob(
      Optional<T> loaded, String correlationId, TablePin selection) {
    requireReadable(correlationId, selection);
    return loaded.orElseThrow(
        () -> expired(correlationId, selection.getTableId(), selection.getSnapshotId()));
  }

  private static io.grpc.StatusRuntimeException expired(
      String correlationId, ResourceId tableId, long snapshotId) {
    return GrpcErrors.snapshotExpired(
        correlationId,
        null,
        Map.of("table_id", tableId.getId(), "snapshot_id", Long.toString(snapshotId)));
  }
}
