package ai.floedb.floecat.gateway.iceberg.rest.services.metadata;

import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asString;

import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.Snapshot;
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

  void deleteSnapshots(ResourceId tableId, List<Long> snapshotIds) {
    updateService.deleteSnapshots(tableId, snapshotIds);
  }

  public void updateSnapshotMetadataLocation(
      ResourceId tableId, Long snapshotId, String metadataLocation) {
    updateService.updateSnapshotMetadataLocation(tableId, snapshotId, metadataLocation);
  }

  // Snapshot metadata parsing lives in SnapshotMetadataUtil.
}
