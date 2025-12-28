package ai.floedb.floecat.gateway.iceberg.rest.services.metadata;

import ai.floedb.floecat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.SnapshotClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService.ImportedMetadata;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService.ImportedSnapshot;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

@ApplicationScoped
public class SnapshotImportCoordinator {
  @Inject SnapshotMetadataService snapshotMetadataService;
  @Inject SnapshotClient snapshotClient;

  public Response ensureImportedCurrentSnapshot(
      TableGatewaySupport tableSupport,
      ResourceId tableId,
      List<String> namespacePath,
      String tableName,
      Supplier<Table> tableSupplier,
      ImportedMetadata importedMetadata,
      String idempotencyKey) {
    if (tableId == null || importedMetadata == null) {
      return null;
    }
    IcebergMetadata importedIcebergMetadata = importedMetadata.icebergMetadata();
    List<ImportedSnapshot> importedSnapshots = importedMetadata.snapshots();
    String schemaJson = importedMetadata.schemaJson();
    if (importedSnapshots != null && !importedSnapshots.isEmpty()) {
      for (ImportedSnapshot snapshot : importedSnapshots) {
        Response err =
            snapshotMetadataService.ensureSnapshotExists(
                tableSupport,
                tableId,
                namespacePath,
                tableName,
                tableSupplier,
                snapshot,
                schemaJson,
                importedIcebergMetadata,
                idempotencyKey);
        if (err != null) {
          return err;
        }
      }
    } else {
      ImportedSnapshot snapshot = importedMetadata.currentSnapshot();
      if (snapshot != null) {
        Response err =
            snapshotMetadataService.ensureSnapshotExists(
                tableSupport,
                tableId,
                namespacePath,
                tableName,
                tableSupplier,
                snapshot,
                schemaJson,
                importedIcebergMetadata,
                idempotencyKey);
        if (err != null) {
          return err;
        }
      }
    }
    ImportedSnapshot currentSnapshot = importedMetadata.currentSnapshot();
    Map<String, String> props = importedMetadata.properties();
    String metadataLocation = props == null ? null : props.get("metadata-location");
    if (currentSnapshot != null) {
      snapshotMetadataService.updateSnapshotMetadataLocation(
          tableId, currentSnapshot.snapshotId(), metadataLocation);
    }
    return null;
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
    if (tableId == null || importedMetadata == null) {
      return;
    }
    Set<Long> expectedIds = new LinkedHashSet<>();
    IcebergMetadata importedIcebergMetadata = importedMetadata.icebergMetadata();
    List<ImportedSnapshot> importedSnapshots = importedMetadata.snapshots();
    String schemaJson = importedMetadata.schemaJson();
    if (importedSnapshots != null && !importedSnapshots.isEmpty()) {
      for (ImportedSnapshot snapshot : importedSnapshots) {
        Response err =
            snapshotMetadataService.ensureSnapshotExists(
                tableSupport,
                tableId,
                namespacePath,
                tableName,
                tableSupplier,
                snapshot,
                schemaJson,
                importedIcebergMetadata,
                idempotencyKey);
        if (err == null && snapshot != null && snapshot.snapshotId() != null) {
          expectedIds.add(snapshot.snapshotId());
        }
      }
    } else if (importedMetadata.currentSnapshot() != null) {
      ImportedSnapshot snapshot = importedMetadata.currentSnapshot();
      Response err =
          snapshotMetadataService.ensureSnapshotExists(
              tableSupport,
              tableId,
              namespacePath,
              tableName,
              tableSupplier,
              snapshot,
              schemaJson,
              importedIcebergMetadata,
              idempotencyKey);
      if (err == null && snapshot != null && snapshot.snapshotId() != null) {
        expectedIds.add(snapshot.snapshotId());
      }
    }

    if (pruneMissing && !expectedIds.isEmpty()) {
      List<Snapshot> existing =
          snapshotClient
              .listSnapshots(ListSnapshotsRequest.newBuilder().setTableId(tableId).build())
              .getSnapshotsList();
      for (Snapshot snapshot : existing) {
        long snapshotId = snapshot.getSnapshotId();
        if (!expectedIds.contains(snapshotId)) {
          snapshotMetadataService.deleteSnapshots(tableId, List.of(snapshotId));
        }
      }
    }

    ImportedSnapshot currentSnapshot = importedMetadata.currentSnapshot();
    Map<String, String> props = importedMetadata.properties();
    String metadataLocation = props == null ? null : props.get("metadata-location");
    if (currentSnapshot != null) {
      snapshotMetadataService.updateSnapshotMetadataLocation(
          tableId, currentSnapshot.snapshotId(), metadataLocation);
    }
  }
}
