package ai.floedb.metacat.service.execution.impl;

import ai.floedb.metacat.catalog.rpc.FileColumnStats;
import ai.floedb.metacat.catalog.rpc.FileContent;
import ai.floedb.metacat.catalog.rpc.GetTableRequest;
import ai.floedb.metacat.catalog.rpc.ListFileColumnStatsRequest;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.metacat.common.rpc.PageRequest;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.connector.spi.MetacatConnector;
import ai.floedb.metacat.execution.rpc.ScanFile;
import ai.floedb.metacat.execution.rpc.ScanFileContent;
import ai.floedb.metacat.query.rpc.SnapshotPin;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import io.quarkus.grpc.GrpcClient;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.Map;

@ApplicationScoped
public class ScanBundleService {

  @GrpcClient("metacat")
  TableServiceGrpc.TableServiceBlockingStub tables;

  public MetacatConnector.ScanBundle fetch(
      String correlationId,
      ResourceId tableId,
      SnapshotPin snapshotPin,
      TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub stats) {

    // Load table metadata
    Table table =
        tables.getTable(GetTableRequest.newBuilder().setTableId(tableId).build()).getTable();

    // SnapshotPin: snapshot_id is a proto3 scalar, no hasSnapshotId()
    long snapshotId = snapshotPin.getSnapshotId();
    if (snapshotId == 0L) {
      throw GrpcErrors.invalidArgument(
          correlationId, "query.snapshot.required", Map.of("table_id", tableId.getId()));
    }

    // Build bundle based on statistics
    MetacatConnector.ScanBundle bundle = buildFromStats(table, snapshotId, stats);

    if (bundle == null) {
      throw GrpcErrors.internal(
          correlationId,
          "scanbundle.stats_unavailable",
          Map.of(
              "table_id", tableId.getId(),
              "snapshot_id", Long.toString(snapshotId)));
    }

    return bundle;
  }

  public MetacatConnector.ScanBundle buildFromStats(
      Table table,
      long snapshotId,
      TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub stats) {
    var data = new ArrayList<ScanFile>();
    var deletes = new ArrayList<ScanFile>();
    String format = table.getUpstream().getFormat().name();

    String pageToken = "";
    do {
      var req =
          ListFileColumnStatsRequest.newBuilder()
              .setTableId(table.getResourceId())
              .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId))
              .setPage(PageRequest.newBuilder().setPageSize(1000).setPageToken(pageToken))
              .build();

      var resp = stats.listFileColumnStats(req);

      for (FileColumnStats fcs : resp.getFileColumnsList()) {
        var scanFile =
            ScanFile.newBuilder()
                .setFilePath(fcs.getFilePath())
                .setFileFormat(format)
                .setFileSizeInBytes(fcs.getSizeBytes())
                .setRecordCount(fcs.getRowCount())
                .setPartitionDataJson(fcs.getPartitionDataJson())
                .setPartitionSpecId(fcs.getPartitionSpecId())
                .addAllEqualityFieldIds(fcs.getEqualityFieldIdsList())
                .setFileContent(mapContent(fcs.getFileContent()))
                .addAllColumns(fcs.getColumnsList())
                .build();

        if (fcs.getFileContent() == FileContent.FC_DATA) {
          data.add(scanFile);
        } else {
          deletes.add(scanFile);
        }
      }

      pageToken = resp.hasPage() ? resp.getPage().getNextPageToken() : "";
    } while (!pageToken.isBlank());

    return new MetacatConnector.ScanBundle(data, deletes);
  }

  //  Direct mapping from catalog FileContent -> execution ScanFileContent
  private ScanFileContent mapContent(FileContent fc) {
    return switch (fc) {
      case FC_EQUALITY_DELETES -> ScanFileContent.SCAN_FILE_CONTENT_EQUALITY_DELETES;
      case FC_POSITION_DELETES -> ScanFileContent.SCAN_FILE_CONTENT_POSITION_DELETES;
      default -> ScanFileContent.SCAN_FILE_CONTENT_DATA;
    };
  }
}
