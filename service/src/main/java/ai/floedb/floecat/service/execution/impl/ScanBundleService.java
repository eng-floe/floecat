package ai.floedb.floecat.service.execution.impl;

import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.FileContent;
import ai.floedb.floecat.catalog.rpc.GetTableRequest;
import ai.floedb.floecat.catalog.rpc.ListFileColumnStatsRequest;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.execution.rpc.ScanFileContent;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import io.quarkus.grpc.GrpcClient;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.Map;

@ApplicationScoped
public class ScanBundleService {

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub tables;

  public FloecatConnector.ScanBundle fetch(
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
    FloecatConnector.ScanBundle bundle = buildFromStats(table, snapshotId, stats);

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

  public FloecatConnector.ScanBundle buildFromStats(
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

    return new FloecatConnector.ScanBundle(data, deletes);
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
