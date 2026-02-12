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

package ai.floedb.floecat.gateway.iceberg.rest.services.metadata;

import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asString;

import ai.floedb.floecat.catalog.rpc.CreateSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotSpec;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.gateway.iceberg.rest.common.SnapshotMetadataUtil;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.SnapshotClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService.ImportedMetadata;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService.ImportedSnapshot;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@ApplicationScoped
public class SnapshotMetadataService {

  @Inject SnapshotClient snapshotClient;
  @Inject SnapshotImportCoordinator importCoordinator;
  @Inject SnapshotUpdateService updateService;

  public List<Map<String, Object>> snapshotAdditions(List<Map<String, Object>> updates) {
    if (updates == null || updates.isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (Map<String, Object> update : updates) {
      String action = asString(update == null ? null : update.get("action"));
      if ("add-snapshot".equals(action)) {
        out.add(update);
      }
    }
    return out;
  }

  public IcebergMetadata loadSnapshotMetadata(ResourceId tableId, long snapshotId) {
    try {
      var resp =
          snapshotClient.getSnapshot(
              GetSnapshotRequest.newBuilder()
                  .setTableId(tableId)
                  .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId))
                  .build());
      if (resp == null || !resp.hasSnapshot()) {
        return null;
      }
      Snapshot snapshot = resp.getSnapshot();
      return SnapshotMetadataUtil.parseSnapshotMetadata(snapshot);
    } catch (StatusRuntimeException e) {
      return null;
    }
  }

  public Response applySnapshotUpdates(
      TableGatewaySupport tableSupport,
      ResourceId tableId,
      List<String> namespacePath,
      String tableName,
      Supplier<Table> tableSupplier,
      List<Map<String, Object>> updates,
      String idempotencyKey) {
    return updateService.applySnapshotUpdates(
        tableSupport, tableId, namespacePath, tableName, tableSupplier, updates, idempotencyKey);
  }

  public Response validateSnapshotUpdates(List<Map<String, Object>> updates) {
    return updateService.validateSnapshotUpdates(updates);
  }

  public Response ensureImportedCurrentSnapshot(
      TableGatewaySupport tableSupport,
      ResourceId tableId,
      List<String> namespacePath,
      String tableName,
      Supplier<Table> tableSupplier,
      ImportedMetadata importedMetadata,
      String idempotencyKey) {
    return importCoordinator.ensureImportedCurrentSnapshot(
        tableSupport,
        tableId,
        namespacePath,
        tableName,
        tableSupplier,
        importedMetadata,
        idempotencyKey);
  }

  public void syncSnapshotsFromImportedMetadata(
      TableGatewaySupport tableSupport,
      ResourceId tableId,
      List<String> namespacePath,
      String tableName,
      Supplier<Table> tableSupplier,
      ImportedMetadata importedMetadata,
      String idempotencyKey,
      boolean pruneMissing) {
    importCoordinator.syncSnapshotsFromImportedMetadata(
        tableSupport,
        tableId,
        namespacePath,
        tableName,
        tableSupplier,
        importedMetadata,
        idempotencyKey,
        pruneMissing);
  }

  Response ensureSnapshotExists(
      TableGatewaySupport tableSupport,
      ResourceId tableId,
      List<String> namespacePath,
      String tableName,
      Supplier<Table> tableSupplier,
      ImportedSnapshot snapshot,
      String schemaJson,
      IcebergMetadata importedIcebergMetadata,
      String idempotencyKey) {
    return updateService.ensureSnapshotExists(
        tableSupport,
        tableId,
        namespacePath,
        tableName,
        tableSupplier,
        snapshot,
        schemaJson,
        importedIcebergMetadata,
        idempotencyKey);
  }

  public void deleteSnapshots(ResourceId tableId, List<Long> snapshotIds) {
    updateService.deleteSnapshots(tableId, snapshotIds);
  }

  public List<Snapshot> fetchSnapshots(ResourceId tableId, List<Long> snapshotIds) {
    if (tableId == null || snapshotIds == null || snapshotIds.isEmpty()) {
      return List.of();
    }
    List<Snapshot> out = new ArrayList<>();
    for (Long snapshotId : snapshotIds) {
      if (snapshotId == null || snapshotId <= 0) {
        continue;
      }
      try {
        var resp =
            snapshotClient.getSnapshot(
                GetSnapshotRequest.newBuilder()
                    .setTableId(tableId)
                    .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId))
                    .build());
        if (resp != null && resp.hasSnapshot()) {
          out.add(resp.getSnapshot());
        }
      } catch (StatusRuntimeException ignored) {
      }
    }
    return out;
  }

  public void restoreSnapshots(ResourceId tableId, List<Snapshot> snapshots) {
    if (tableId == null || snapshots == null || snapshots.isEmpty()) {
      return;
    }
    for (Snapshot snapshot : snapshots) {
      if (snapshot == null) {
        continue;
      }
      SnapshotSpec.Builder spec =
          SnapshotSpec.newBuilder().setTableId(tableId).setSnapshotId(snapshot.getSnapshotId());
      if (snapshot.hasUpstreamCreatedAt()) {
        spec.setUpstreamCreatedAt(snapshot.getUpstreamCreatedAt());
      }
      if (snapshot.hasIngestedAt()) {
        spec.setIngestedAt(snapshot.getIngestedAt());
      }
      if (snapshot.getParentSnapshotId() > 0) {
        spec.setParentSnapshotId(snapshot.getParentSnapshotId());
      }
      if (!snapshot.getSchemaJson().isBlank()) {
        spec.setSchemaJson(snapshot.getSchemaJson());
      }
      if (snapshot.hasPartitionSpec()) {
        spec.setPartitionSpec(snapshot.getPartitionSpec());
      }
      if (snapshot.getSequenceNumber() > 0) {
        spec.setSequenceNumber(snapshot.getSequenceNumber());
      }
      if (!snapshot.getManifestList().isBlank()) {
        spec.setManifestList(snapshot.getManifestList());
      }
      if (!snapshot.getSummaryMap().isEmpty()) {
        spec.putAllSummary(snapshot.getSummaryMap());
      }
      if (snapshot.getSchemaId() != 0) {
        spec.setSchemaId(snapshot.getSchemaId());
      }
      if (!snapshot.getFormatMetadataMap().isEmpty()) {
        spec.putAllFormatMetadata(snapshot.getFormatMetadataMap());
      }
      snapshotClient.createSnapshot(
          CreateSnapshotRequest.newBuilder().setSpec(spec.build()).build());
    }
  }

  public void updateSnapshotMetadataLocation(
      ResourceId tableId, Long snapshotId, String metadataLocation) {
    updateService.updateSnapshotMetadataLocation(tableId, snapshotId, metadataLocation);
  }

  // Snapshot metadata parsing lives in SnapshotMetadataUtil.
}
