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

import ai.floedb.floecat.catalog.rpc.FileContent;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.common.resolver.DeltaSchemaNormalizer;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.execution.rpc.ScanFileContent;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.query.rpc.TableInfo;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.storage.impl.ServerSideFileIoPropertiesResolver;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

@ApplicationScoped
public class ScanBundleService {

  private final TableRepository tables;
  private final SnapshotRepository snapshots;
  private final StatsStore statsStore;
  private final ServerSideFileIoPropertiesResolver fileIoPropertiesResolver;

  @Inject
  public ScanBundleService(
      TableRepository tables,
      SnapshotRepository snapshots,
      StatsStore statsStore,
      ServerSideFileIoPropertiesResolver fileIoPropertiesResolver) {
    this.tables = tables;
    this.snapshots = snapshots;
    this.statsStore = statsStore;
    this.fileIoPropertiesResolver = fileIoPropertiesResolver;
  }

  public ScanBundleWithInfo fetch(
      String correlationId, ResourceId tableId, SnapshotPin snapshotPin) {
    Table table = requireTable(correlationId, tableId);
    Snapshot snapshot = requireSnapshot(correlationId, tableId, snapshotPin.getSnapshotId());
    FloecatConnector.ScanBundle bundle = buildFromStats(table, snapshotPin.getSnapshotId());
    TableInfo info = buildTableInfo(table, snapshot, snapshotPin.getSnapshotId());
    return new ScanBundleWithInfo(bundle, info);
  }

  public TableInfo fetchTableInfo(
      String correlationId, ResourceId tableId, SnapshotPin snapshotPin) {
    Table table = requireTable(correlationId, tableId);
    Snapshot snapshot = requireSnapshot(correlationId, tableId, snapshotPin.getSnapshotId());
    return buildTableInfo(table, snapshot, snapshotPin.getSnapshotId());
  }

  private Table requireTable(String correlationId, ResourceId tableId) {
    Table table =
        tables
            .getById(tableId)
            .orElseThrow(
                () -> GrpcErrors.notFound(correlationId, TABLE, Map.of("id", tableId.getId())));
    return table;
  }

  private Snapshot requireSnapshot(String correlationId, ResourceId tableId, long snapshotId) {
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
    return snapshot;
  }

  private FloecatConnector.ScanBundle buildFromStats(Table table, long snapshotId) {
    var data = new ArrayList<ScanFile>();
    var deletes = new ArrayList<ScanFile>();

    String pageToken = "";
    do {
      var page =
          statsStore.listTargetStats(
              table.getResourceId(),
              snapshotId,
              Optional.of(StatsTargetType.FILE),
              1000,
              pageToken);

      for (var record : page.records()) {
        if (!record.hasFile()) {
          continue;
        }
        FileTargetStats fcs = record.getFile();
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

      pageToken = page.nextPageToken();
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

  private TableInfo buildTableInfo(Table table, Snapshot snapshot, long snapshotId) {
    TableInfo.Builder builder =
        TableInfo.newBuilder().setTableId(table.getResourceId()).setSnapshotId(snapshotId);

    String schemaJson = snapshot.getSchemaJson();
    if (schemaJson == null || schemaJson.isBlank()) {
      schemaJson = table.getSchemaJson();
    }
    String rawSchemaJson = schemaJson;
    boolean deltaTable = DeltaSchemaNormalizer.isDeltaTable(table, table.getPropertiesMap());
    if (schemaJson != null && !schemaJson.isBlank()) {
      // Delta/Unity tables store the raw Delta schema JSON (name/type/nullable), but the scan
      // engine parses TableInfo.schema_json as an Iceberg schema (id/required). Convert it here,
      // mirroring what the Iceberg REST gateway does for the same tables. Variant is left as a
      // scalar type for the engine to rewrite into a struct.
      if (deltaTable) {
        schemaJson = DeltaSchemaNormalizer.normalizeSchemaJson(schemaJson, snapshot.getSchemaId());
      }
      builder.setSchemaJson(schemaJson);
    }

    if (snapshot.hasPartitionSpec()) {
      builder.setPartitionSpecs(snapshot.getPartitionSpec());
    } else {
      // The scan engine requires a partition spec. Unpartitioned tables (e.g. Delta tables
      // captured without a spec) get the canonical empty/unpartitioned spec (spec-id 0, no
      // fields), mirroring the Iceberg REST gateway's defaultPartitionSpec().
      builder.setPartitionSpecs(PartitionSpecInfo.newBuilder().setSpecId(0).build());
    }

    String metadataLocation = SnapshotRepository.metadataLocation(snapshot);
    Map<String, String> tableProperties =
        fileIoPropertiesResolver.applyToTableProperties(
            table, metadataLocation, table.getPropertiesMap());
    if (deltaTable
        && rawSchemaJson != null
        && !rawSchemaJson.isBlank()
        && !tableProperties.containsKey(DeltaSchemaNormalizer.DEFAULT_NAME_MAPPING_PROPERTY)) {
      String nameMappingJson = DeltaSchemaNormalizer.defaultNameMappingJson(rawSchemaJson);
      if (nameMappingJson != null && !nameMappingJson.isBlank()) {
        tableProperties = new LinkedHashMap<>(tableProperties);
        tableProperties.put(DeltaSchemaNormalizer.DEFAULT_NAME_MAPPING_PROPERTY, nameMappingJson);
      }
    }
    if (!tableProperties.isEmpty()) {
      builder.putAllProperties(tableProperties);
    }
    if (metadataLocation != null && !metadataLocation.isBlank()) {
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
