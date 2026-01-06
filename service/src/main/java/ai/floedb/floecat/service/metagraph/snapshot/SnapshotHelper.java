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

package ai.floedb.floecat.service.metagraph.snapshot;

import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc.SnapshotServiceBlockingStub;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph.SchemaResolution;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * Centralized snapshot handling ----------------------------------------- - Snapshot override
 * resolution - AS OF timestamp fallback - Default "CURRENT" snapshot semantics - Schema resolution
 * based on effective snapshot
 */
public class SnapshotHelper {

  public interface SnapshotClient {
    ai.floedb.floecat.catalog.rpc.GetSnapshotResponse getSnapshot(
        ai.floedb.floecat.catalog.rpc.GetSnapshotRequest request);
  }

  private SnapshotClient client;
  private final SnapshotRepository snapshots;

  public SnapshotHelper(SnapshotRepository snapshots, SnapshotServiceBlockingStub stub) {
    this.snapshots = snapshots;
    this.client =
        (stub != null)
            ? stub::getSnapshot
            : req -> {
              throw new IllegalStateException("Snapshot client missing");
            };
  }

  public void setSnapshotClient(SnapshotClient c) {
    this.client = c;
  }

  // ----------------------------------------------------------------------
  // Snapshot pinning
  // ----------------------------------------------------------------------

  public SnapshotPin snapshotPinFor(
      String cid, ResourceId tableId, SnapshotRef override, Optional<Timestamp> asOfDefault) {

    if (override != null && override.hasSnapshotId()) {
      return pin(tableId, override.getSnapshotId(), null);
    }

    if (override != null && override.hasAsOf()) {
      return pin(tableId, 0, override.getAsOf());
    }

    if (asOfDefault.isPresent()) {
      return pin(tableId, 0, asOfDefault.get());
    }

    var resp =
        client.getSnapshot(
            GetSnapshotRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT))
                .build());

    return pin(tableId, resp.getSnapshot().getSnapshotId(), null);
  }

  // ----------------------------------------------------------------------
  // Schema resolution
  // ----------------------------------------------------------------------

  public SchemaResolution schemaFor(
      String cid,
      UserTableNode tbl,
      SnapshotRef ref,
      java.util.function.Supplier<String> supplier) {

    return new SchemaResolution(tbl, schemaJsonFor(cid, tbl, ref, supplier));
  }

  public String schemaJsonFor(
      String cid,
      UserTableNode tbl,
      SnapshotRef ref,
      java.util.function.Supplier<String> supplier) {

    if (ref == null || ref.getWhichCase() == SnapshotRef.WhichCase.WHICH_NOT_SET) {
      return supplier.get();
    }

    Snapshot snap = resolveSnapshot(cid, tbl.id(), ref);

    if (snap == null || snap.getSchemaJson().isBlank()) {
      return supplier.get();
    }

    return snap.getSchemaJson();
  }

  private Snapshot resolveSnapshot(String cid, ResourceId tableId, SnapshotRef ref) {

    return switch (ref.getWhichCase()) {
      case SNAPSHOT_ID ->
          snapshots
              .getById(tableId, ref.getSnapshotId())
              .orElseThrow(
                  () ->
                      GrpcErrors.notFound(
                          cid,
                          "snapshot",
                          Map.of(
                              "table_id", tableId.getId(),
                              "snapshot_id", Long.toString(ref.getSnapshotId()))));

      case AS_OF ->
          snapshots
              .getAsOf(tableId, ref.getAsOf())
              .orElseThrow(
                  () ->
                      GrpcErrors.notFound(
                          cid,
                          "snapshot",
                          Map.of(
                              "table_id", tableId.getId(),
                              "as_of",
                                  Instant.ofEpochSecond(
                                          ref.getAsOf().getSeconds(), ref.getAsOf().getNanos())
                                      .toString())));

      case SPECIAL -> {
        if (ref.getSpecial() != SpecialSnapshot.SS_CURRENT) {
          throw GrpcErrors.invalidArgument(
              cid, "snapshot.special.unsupported", Map.of("requested", ref.getSpecial().name()));
        }

        yield snapshots
            .getCurrentSnapshot(tableId)
            .orElseThrow(
                () -> GrpcErrors.notFound(cid, "snapshot", Map.of("table_id", tableId.getId())));
      }

      case WHICH_NOT_SET ->
          throw GrpcErrors.invalidArgument(
              cid, "snapshot.missing", Map.of("table_id", tableId.getId()));

      default ->
          throw GrpcErrors.invalidArgument(
              cid, "snapshot.missing", Map.of("table_id", tableId.getId()));
    };
  }

  private SnapshotPin pin(ResourceId tableId, long id, Timestamp ts) {
    SnapshotPin.Builder b = SnapshotPin.newBuilder().setTableId(tableId);
    if (id > 0) b.setSnapshotId(id);
    if (ts != null) b.setAsOf(ts);
    return b.build();
  }
}
