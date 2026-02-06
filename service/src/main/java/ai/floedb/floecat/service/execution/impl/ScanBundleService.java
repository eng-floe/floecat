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

package ai.floedb.floecat.service.execution.impl;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.FileContent;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.execution.rpc.ScanFileContent;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.query.rpc.TableInfo;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

@ApplicationScoped
public class ScanBundleService {

  private final TableRepository tables;
  private final SnapshotRepository snapshots;
  private final StatsRepository stats;

  @Inject
  public ScanBundleService(
      TableRepository tables, SnapshotRepository snapshots, StatsRepository stats) {
    this.tables = tables;
    this.snapshots = snapshots;
    this.stats = stats;
  }

  public ScanBundleWithInfo fetch(
      String correlationId, ResourceId tableId, SnapshotPin snapshotPin) {

    Table table =
        tables
            .getById(tableId)
            .orElseThrow(
                () -> GrpcErrors.notFound(correlationId, TABLE, Map.of("id", tableId.getId())));

    long snapshotId = snapshotPin.getSnapshotId();
    if (snapshotId == 0L) {
      throw GrpcErrors.invalidArgument(
          correlationId, QUERY_SNAPSHOT_REQUIRED, Map.of("table_id", tableId.getId()));
    }

    Snapshot snapshot =
        snapshots
            .getById(tableId, snapshotId)
            .orElseThrow(
                () ->
                    GrpcErrors.notFound(
                        correlationId,
                        SNAPSHOT,
                        Map.of(
                            "table_id",
                            tableId.getId(),
                            "snapshot_id",
                            Long.toString(snapshotId))));

    FloecatConnector.ScanBundle bundle = buildFromStats(table, snapshotId);

    if (bundle == null) {
      throw GrpcErrors.internal(
          correlationId,
          SCANBUNDLE_STATS_UNAVAILABLE,
          Map.of("table_id", tableId.getId(), "snapshot_id", Long.toString(snapshotId)));
    }

    TableInfo info = buildTableInfo(table, snapshot);
    return new ScanBundleWithInfo(bundle, info);
  }

  private FloecatConnector.ScanBundle buildFromStats(Table table, long snapshotId) {
    var data = new ArrayList<ScanFile>();
    var deletes = new ArrayList<ScanFile>();

    String pageToken = "";
    do {
      StringBuilder next = new StringBuilder();
      var resp = stats.listFileStats(table.getResourceId(), snapshotId, 1000, pageToken, next);

      for (FileColumnStats fcs : resp) {
        var builder =
            ScanFile.newBuilder()
                .setFilePath(fcs.getFilePath())
                .setFileFormat(fcs.getFileFormat())
                .setFileSizeInBytes(fcs.getSizeBytes())
                .setRecordCount(fcs.getRowCount())
                .setPartitionDataJson(fcs.getPartitionDataJson())
                .setPartitionSpecId(fcs.getPartitionSpecId())
                .addAllEqualityFieldIds(fcs.getEqualityFieldIdsList())
                .setFileContent(mapContent(fcs.getFileContent()))
                .addAllColumns(fcs.getColumnsList());
        if (fcs.hasSequenceNumber()) {
          builder.setSequenceNumber(fcs.getSequenceNumber());
        }
        var scanFile = builder.build();

        if (fcs.getFileContent() == FileContent.FC_DATA) {
          data.add(scanFile);
        } else {
          deletes.add(scanFile);
        }
      }

      pageToken = next.toString();
    } while (!pageToken.isBlank());

    List<ScanFile> linkedData =
        deletes.isEmpty()
            ? data
            : data.stream()
                .map(
                    file ->
                        file.toBuilder()
                            .addAllDeleteFileIndices(
                                IntStream.range(0, deletes.size()).boxed().toList())
                            .build())
                .toList();

    return new FloecatConnector.ScanBundle(linkedData, deletes);
  }

  private TableInfo buildTableInfo(Table table, Snapshot snapshot) {
    TableInfo.Builder builder = TableInfo.newBuilder().setTableId(table.getResourceId());

    String schemaJson = snapshot.getSchemaJson();
    if (schemaJson == null || schemaJson.isBlank()) {
      schemaJson = table.getSchemaJson();
    }
    if (schemaJson != null && !schemaJson.isBlank()) {
      builder.setSchemaJson(schemaJson);
    }

    if (snapshot.hasPartitionSpec()) {
      builder.setPartitionSpecs(snapshot.getPartitionSpec());
    }

    if (table.getPropertiesCount() > 0) {
      builder.putAllProperties(table.getPropertiesMap());
    }

    String metadataLocation = table.getPropertiesMap().getOrDefault("metadata-location", "");
    if (!metadataLocation.isBlank()) {
      builder.setMetadataLocation(metadataLocation);
    }

    return builder.build();
  }

  //  Direct mapping from catalog FileContent -> execution ScanFileContent
  private ScanFileContent mapContent(FileContent fc) {
    return switch (fc) {
      case FC_EQUALITY_DELETES -> ScanFileContent.SCAN_FILE_CONTENT_EQUALITY_DELETES;
      case FC_POSITION_DELETES -> ScanFileContent.SCAN_FILE_CONTENT_POSITION_DELETES;
      default -> ScanFileContent.SCAN_FILE_CONTENT_DATA;
    };
  }

  public record ScanBundleWithInfo(FloecatConnector.ScanBundle bundle, TableInfo tableInfo) {}
}
