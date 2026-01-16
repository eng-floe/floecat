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

package ai.floedb.floecat.connector.iceberg.impl;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.FileContent;
import ai.floedb.floecat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.TableStats;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.common.GenericStatsEngine;
import ai.floedb.floecat.connector.common.ProtoStatsBuilder;
import ai.floedb.floecat.connector.common.StatsEngine;
import ai.floedb.floecat.connector.common.ndv.ColumnNdv;
import ai.floedb.floecat.connector.common.ndv.FilteringNdvProvider;
import ai.floedb.floecat.connector.common.ndv.NdvProvider;
import ai.floedb.floecat.connector.common.ndv.ParquetNdvProvider;
import ai.floedb.floecat.connector.common.ndv.SamplingNdvProvider;
import ai.floedb.floecat.connector.common.ndv.StaticOnceNdvProvider;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadataLogEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSchema;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSnapshotLogEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSortField;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSortOrder;
import ai.floedb.floecat.storage.spi.io.RuntimeFileIoOverrides;
import ai.floedb.floecat.types.LogicalType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.jboss.logging.Logger;

public abstract class IcebergConnector implements FloecatConnector {
  private static final Logger LOG = Logger.getLogger(IcebergConnector.class);
  private final String connectorId;
  protected final Table singleTable;
  private final String singleNamespaceFq;
  private final String singleTableName;
  private final boolean ndvEnabled;
  private final double ndvSampleFraction;
  private final long ndvMaxFiles;
  private final FileIO externalFileIO;

  protected IcebergConnector(
      String connectorId,
      Table singleTable,
      String singleNamespaceFq,
      String singleTableName,
      boolean ndvEnabled,
      double ndvSampleFraction,
      long ndvMaxFiles,
      FileIO externalFileIO) {
    this.connectorId = connectorId;
    this.singleTable = singleTable;
    this.singleNamespaceFq = singleNamespaceFq;
    this.singleTableName = singleTableName;
    this.ndvEnabled = ndvEnabled;
    this.ndvSampleFraction = ndvSampleFraction;
    this.ndvMaxFiles = ndvMaxFiles;
    this.externalFileIO = externalFileIO;
  }

  @Override
  public String id() {
    return connectorId;
  }

  @Override
  public ConnectorFormat format() {
    return ConnectorFormat.CF_ICEBERG;
  }

  @Override
  public abstract List<String> listNamespaces();

  @Override
  public abstract List<String> listTables(String namespaceFq);

  @Override
  public TableDescriptor describe(String namespaceFq, String tableName) {
    Table table = loadTable(namespaceFq, tableName);
    Schema schema = table.schema();
    String schemaJson = SchemaParser.toJson(schema);
    List<String> partitionKeys = table.spec().fields().stream().map(f -> f.name()).toList();

    return new TableDescriptor(
        namespaceFq, tableName, table.location(), schemaJson, partitionKeys, table.properties());
  }

  @Override
  public List<SnapshotBundle> enumerateSnapshotsWithStats(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      Set<String> includeColumns) {
    return enumerateSnapshotsWithStats(
        namespaceFq, tableName, destinationTableId, includeColumns, true);
  }

  @Override
  public List<SnapshotBundle> enumerateSnapshotsWithStats(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      Set<String> includeColumns,
      boolean includeStatistics) {
    Table table = loadTable(namespaceFq, tableName);
    IcebergMetadata icebergMetadata = buildIcebergMetadata(namespaceFq, tableName, table);

    final Set<Integer> includeIds;
    if (!includeStatistics) {
      includeIds = Set.of();
    } else if (includeColumns == null || includeColumns.isEmpty()) {
      includeIds =
          table.schema().columns().stream()
              .map(Types.NestedField::fieldId)
              .collect(Collectors.toCollection(LinkedHashSet::new));
    } else {
      includeIds = resolveFieldIdsNested(table.schema(), includeColumns);
    }

    List<SnapshotBundle> out = new ArrayList<>();
    for (Snapshot snapshot : table.snapshots()) {
      long snapshotId = snapshot.snapshotId();
      long parentId = snapshot.parentId() != null ? snapshot.parentId().longValue() : 0;
      long createdMs = snapshot.timestampMillis();

      Integer snapshotSchemaId = snapshot != null ? snapshot.schemaId() : null;
      int schemaId =
          snapshotSchemaId != null && snapshotSchemaId > 0
              ? snapshotSchemaId
              : table.schema().schemaId();
      Schema schema = Optional.ofNullable(table.schemas().get(schemaId)).orElse(table.schema());
      String schemaJson = SchemaParser.toJson(schema);

      TableStats tStats = null;
      List<ColumnStats> cStats = List.of();
      List<FileColumnStats> fileStats = List.of();
      if (includeStatistics) {
        EngineOut engineOutput = runEngine(table, snapshotId, includeIds);

        var columnNames = engineOutput.columnNames();
        var logicalTypes = engineOutput.logicalTypes();

        tStats =
            ProtoStatsBuilder.toTableStats(
                destinationTableId,
                snapshotId,
                createdMs,
                TableFormat.TF_ICEBERG,
                engineOutput.result());
        cStats =
            ProtoStatsBuilder.toColumnStats(
                destinationTableId,
                snapshotId,
                TableFormat.TF_ICEBERG,
                engineOutput.result().columns(),
                id -> {
                  String name = columnNames.get(id);
                  if (name != null && !name.isBlank()) {
                    return name;
                  }
                  var f = schema.findField(id);
                  return f == null ? "" : f.name();
                },
                id -> id,
                id -> {
                  LogicalType lt = logicalTypes.get(id);
                  if (lt != null) {
                    return lt;
                  }
                  var f = schema.findField(id);
                  return f == null ? null : IcebergTypeMapper.toLogical(f.type());
                },
                createdMs,
                engineOutput.result().totalRowCount());
        var baseFiles =
            ProtoStatsBuilder.toFileColumnStats(
                destinationTableId,
                snapshotId,
                TableFormat.TF_ICEBERG,
                engineOutput.result().files(),
                id -> {
                  String name = columnNames.get(id);
                  if (name != null && !name.isBlank()) {
                    return name;
                  }
                  var f = schema.findField(id);
                  return f == null ? "" : f.name();
                },
                id -> id,
                id -> {
                  LogicalType lt = logicalTypes.get(id);
                  if (lt != null) {
                    return lt;
                  }
                  var f = schema.findField(id);
                  return f == null ? null : IcebergTypeMapper.toLogical(f.type());
                },
                createdMs);

        List<FileColumnStats> deleteStats = new ArrayList<>();
        TableScan scan = table.newScan().useSnapshot(snapshotId);
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
          for (FileScanTask task : tasks) {
            for (var df : task.deletes()) {
              List<Integer> eqIds =
                  df.content() == org.apache.iceberg.FileContent.EQUALITY_DELETES
                      ? df.equalityFieldIds()
                      : List.of();
              var builder =
                  FileColumnStats.newBuilder()
                      .setTableId(destinationTableId)
                      .setSnapshotId(snapshotId)
                      .setFilePath(df.location())
                      .setRowCount(df.recordCount())
                      .setSizeBytes(df.fileSizeInBytes())
                      .addAllEqualityFieldIds(eqIds)
                      .setFileContent(
                          df.content() == org.apache.iceberg.FileContent.EQUALITY_DELETES
                              ? FileContent.FC_EQUALITY_DELETES
                              : FileContent.FC_POSITION_DELETES);
              Long sequenceNumber = df.fileSequenceNumber();
              if (sequenceNumber != null && sequenceNumber > 0) {
                builder.setSequenceNumber(sequenceNumber);
              }
              deleteStats.add(builder.build());
            }
          }
        } catch (Exception e) {
          throw new RuntimeException(
              "Failed to enumerate delete files for snapshot " + snapshotId, e);
        }

        List<FileColumnStats> allFiles = new ArrayList<>(baseFiles);
        allFiles.addAll(deleteStats);
        fileStats = allFiles;
      }

      Map<String, String> summary = snapshot.summary() == null ? Map.of() : snapshot.summary();
      Map<String, String> summaryWithOperation =
          summary.isEmpty() ? new LinkedHashMap<>() : new LinkedHashMap<>(summary);
      String operation = snapshot.operation();
      if (operation != null && !operation.isBlank()) {
        summaryWithOperation.putIfAbsent("operation", operation);
      }
      summary = summaryWithOperation.isEmpty() ? Map.of() : Map.copyOf(summaryWithOperation);
      String manifestList = snapshot.manifestListLocation();
      long sequenceNumber = snapshot.sequenceNumber();

      Map<String, ByteString> metadataAttachments =
          (icebergMetadata != null) ? Map.of("iceberg", icebergMetadata.toByteString()) : Map.of();
      out.add(
          new SnapshotBundle(
              snapshotId,
              parentId,
              createdMs,
              tStats,
              cStats,
              fileStats,
              schemaJson,
              toPartitionSpecInfo(table, snapshot),
              sequenceNumber,
              manifestList,
              summary,
              schemaId,
              metadataAttachments));
    }
    return out;
  }

  protected boolean isSingleTableMode() {
    return singleTable != null;
  }

  protected boolean namespaceMatches(String namespaceFq) {
    String stored = singleNamespaceFq == null ? "" : singleNamespaceFq;
    String incoming = namespaceFq == null ? "" : namespaceFq;
    return stored.equals(incoming);
  }

  protected List<String> listNamespacesSingle() {
    return (singleNamespaceFq == null || singleNamespaceFq.isBlank())
        ? List.of()
        : List.of(singleNamespaceFq);
  }

  protected List<String> listTablesSingle(String namespaceFq) {
    if (namespaceMatches(namespaceFq)) {
      return List.of(singleTableName);
    }
    return List.of();
  }

  private Table loadTable(String namespaceFq, String tableName) {
    return loadTableFromSource(namespaceFq, tableName);
  }

  static LoadedExternalTable loadExternalTable(
      String metadataLocation, Map<String, String> options) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      throw new IllegalArgumentException("metadataLocation is required");
    }
    Map<String, String> opts = options == null ? Map.of() : options;
    Map<String, String> ioProps = new HashMap<>();
    opts.forEach(
        (k, v) -> {
          if ("io-impl".equals(k)
              || k.startsWith("s3.")
              || k.startsWith("fs.")
              || k.startsWith("client.")
              || k.startsWith("aws.")) {
            ioProps.put(k, v);
          }
        });
    if (Boolean.parseBoolean(System.getProperty("floecat.connector.fileio.overrides", "true"))) {
      RuntimeFileIoOverrides.mergeInto(ioProps);
    }
    Map<String, String> sanitized = new LinkedHashMap<>();
    ioProps.forEach(
        (k, v) -> {
          if (k == null) {
            return;
          }
          String key = k.toLowerCase(Locale.ROOT);
          if (key.contains("secret")
              || key.contains("token")
              || key.contains("password")
              || key.contains("access")
              || key.endsWith("key")
              || key.contains("credentials")) {
            sanitized.put(k, "<redacted>");
          } else {
            sanitized.put(k, v);
          }
        });
    LOG.infof(
        "Iceberg external table load metadataLocation=%s ioProps=%s", metadataLocation, sanitized);
    String ioImpl = ioProps.getOrDefault("io-impl", "org.apache.iceberg.aws.s3.S3FileIO").trim();
    FileIO fileIO = instantiateFileIO(ioImpl);
    ioProps.remove("io-impl");
    fileIO.initialize(ioProps);
    String resolvedMetadataLocation = resolveMetadataLocation(metadataLocation);
    StaticTableOperations ops = new StaticTableOperations(resolvedMetadataLocation, fileIO);
    return new LoadedExternalTable(new BaseTable(ops, deriveTableName(metadataLocation)), fileIO);
  }

  static record LoadedExternalTable(Table table, FileIO fileIO) {}

  static String deriveTableName(String metadataLocation) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return "table";
    }
    String path = metadataLocation;
    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    int slash = path.lastIndexOf('/');
    if (slash >= 0 && slash + 1 < path.length()) {
      path = path.substring(slash + 1);
    }
    if (path.endsWith(".json")) {
      path = path.substring(0, path.length() - 5);
    }
    return path.isBlank() ? "table" : path;
  }

  private static FileIO instantiateFileIO(String className) {
    try {
      Class<?> clazz = Class.forName(className);
      Object instance = clazz.getDeclaredConstructor().newInstance();
      if (instance instanceof FileIO fileIO) {
        return fileIO;
      }
      throw new IllegalArgumentException(className + " does not implement FileIO");
    } catch (Exception e) {
      throw new IllegalStateException("Unable to instantiate FileIO " + className, e);
    }
  }

  private static String resolveMetadataLocation(String input) {
    String trimmed = input.trim();
    if (trimmed.endsWith(".json")) {
      return trimmed;
    }
    String base = trimmed.endsWith("/") ? trimmed.substring(0, trimmed.length() - 1) : trimmed;
    return base + "/metadata/metadata.json";
  }

  private String partitionJson(Table table, ContentFile<?> file) {
    PartitionSpec spec = table.specs().getOrDefault(file.specId(), table.spec());
    StructLike partition = file.partition();
    if (spec == null || spec.fields().isEmpty() || partition == null) {
      return "{\"partitionValues\":[]}";
    }
    try {
      List<Map<String, Object>> values = new ArrayList<>(spec.fields().size());
      for (int i = 0; i < spec.fields().size(); i++) {
        PartitionField field = spec.fields().get(i);
        Object val = partition.get(i, Object.class);
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("id", field.sourceId());
        entry.put("value", val);
        values.add(entry);
      }
      Map<String, Object> root = new LinkedHashMap<>();
      root.put("partitionValues", values);
      return new ObjectMapper().writeValueAsString(root);
    } catch (Exception e) {
      return "";
    }
  }

  private PartitionSpecInfo toPartitionSpecInfo(Table table, Snapshot snapshot) {
    if (snapshot == null) {
      return null;
    }

    Map<Integer, PartitionSpec> specs = table.specs();
    Integer snapshotSpecId = null;
    TableScan scan = table.newScan().useSnapshot(snapshot.snapshotId());
    CloseableIterable<FileScanTask> tasks = scan.planFiles();
    Set<Integer> specSet = new LinkedHashSet<>();
    for (FileScanTask task : tasks) {
      specSet.add(task.file().specId());
    }

    if (specSet.size() == 1) {
      snapshotSpecId = specSet.iterator().next();
    }

    if (snapshotSpecId == null && table.spec() != null) {
      snapshotSpecId = table.spec().specId();
    }

    PartitionSpec spec = snapshotSpecId == null ? null : specs.get(snapshotSpecId);
    if (spec == null) {
      return null;
    }

    return toPartitionSpecInfo(spec);
  }

  private PartitionSpecInfo toPartitionSpecInfo(PartitionSpec spec) {
    if (spec == null) {
      return null;
    }
    String specName = spec.isUnpartitioned() ? "unpartitioned" : "spec-" + spec.specId();
    PartitionSpecInfo.Builder builder =
        PartitionSpecInfo.newBuilder().setSpecId(spec.specId()).setSpecName(specName);
    for (PartitionField field : spec.fields()) {
      builder.addFields(
          ai.floedb.floecat.catalog.rpc.PartitionField.newBuilder()
              .setFieldId(field.sourceId())
              .setName(field.name())
              .setTransform(field.transform().toString())
              .build());
    }
    return builder.build();
  }

  private IcebergMetadata buildIcebergMetadata(String namespaceFq, String tableName, Table table) {
    TableMetadata metadata = tableMetadata(table);
    if (metadata == null) {
      String fallbackLocation = tableMetadataLocation(table);
      if (fallbackLocation == null || fallbackLocation.isBlank()) {
        return null;
      }
      IcebergMetadata.Builder minimal =
          IcebergMetadata.newBuilder()
              .setMetadataLocation(fallbackLocation)
              .setFormatVersion(2)
              .setTableUuid(
                  table
                      .properties()
                      .getOrDefault("table-uuid", tableName == null ? "" : tableName));
      Optional.ofNullable(table.properties().get("current-snapshot-id"))
          .map(this::safeLong)
          .ifPresent(minimal::setCurrentSnapshotId);
      return minimal.build();
    }
    return toIcebergMetadata(metadata);
  }

  private TableMetadata tableMetadata(Table table) {
    if (!(table instanceof HasTableOperations hasOps)) {
      return null;
    }
    return hasOps.operations().current();
  }

  private String tableMetadataLocation(Table table) {
    Map<String, String> props = table.properties();
    if (props == null || props.isEmpty()) {
      return null;
    }
    return props.get("metadata-location");
  }

  private IcebergMetadata toIcebergMetadata(TableMetadata metadata) {
    IcebergMetadata.Builder builder =
        IcebergMetadata.newBuilder()
            .setTableUuid(metadata.uuid())
            .setFormatVersion(metadata.formatVersion())
            .setMetadataLocation(metadata.metadataFileLocation())
            .setLastUpdatedMs(metadata.lastUpdatedMillis())
            .setLastColumnId(metadata.lastColumnId())
            .setCurrentSchemaId(metadata.currentSchemaId())
            .setDefaultSpecId(metadata.defaultSpecId())
            .setLastPartitionId(metadata.lastAssignedPartitionId())
            .setDefaultSortOrderId(metadata.defaultSortOrderId())
            .setLastSequenceNumber(metadata.lastSequenceNumber());

    Snapshot currentSnapshot = metadata.currentSnapshot();
    if (currentSnapshot != null) {
      builder.setCurrentSnapshotId(currentSnapshot.snapshotId());
    }

    if (metadata.schemas() != null) {
      for (Schema schema : metadata.schemas()) {
        builder.addSchemas(
            IcebergSchema.newBuilder()
                .setSchemaId(schema.schemaId())
                .setSchemaJson(SchemaParser.toJson(schema))
                .addAllIdentifierFieldIds(schema.identifierFieldIds())
                .setLastColumnId(schema.highestFieldId())
                .build());
      }
    }

    if (metadata.specsById() != null) {
      for (PartitionSpec spec : metadata.specsById().values()) {
        PartitionSpecInfo info = toPartitionSpecInfo(spec);
        if (info != null) {
          builder.addPartitionSpecs(info);
        }
      }
    }

    if (metadata.sortOrders() != null) {
      for (SortOrder order : metadata.sortOrders()) {
        IcebergSortOrder.Builder orderBuilder =
            IcebergSortOrder.newBuilder().setSortOrderId(order.orderId());
        for (SortField field : order.fields()) {
          orderBuilder.addFields(
              IcebergSortField.newBuilder()
                  .setSourceFieldId(field.sourceId())
                  .setTransform(
                      field.transform() == null ? "identity" : field.transform().toString())
                  .setDirection(field.direction().name())
                  .setNullOrder(field.nullOrder().name())
                  .build());
        }
        builder.addSortOrders(orderBuilder.build());
      }
    }

    for (HistoryEntry entry : metadata.snapshotLog()) {
      builder.addSnapshotLog(
          IcebergSnapshotLogEntry.newBuilder()
              .setSnapshotId(entry.snapshotId())
              .setTimestampMs(entry.timestampMillis())
              .build());
    }

    for (MetadataLogEntry entry : metadata.previousFiles()) {
      builder.addMetadataLog(
          IcebergMetadataLogEntry.newBuilder()
              .setFile(entry.file())
              .setTimestampMs(entry.timestampMillis())
              .build());
    }

    metadata
        .refs()
        .forEach(
            (name, ref) -> {
              IcebergRef.Builder refBuilder =
                  IcebergRef.newBuilder()
                      .setSnapshotId(ref.snapshotId())
                      .setType(ref.type().name());
              if (ref.maxRefAgeMs() != null) {
                refBuilder.setMaxReferenceAgeMs(ref.maxRefAgeMs());
              }
              if (ref.maxSnapshotAgeMs() != null) {
                refBuilder.setMaxSnapshotAgeMs(ref.maxSnapshotAgeMs());
              }
              if (ref.minSnapshotsToKeep() != null) {
                refBuilder.setMinSnapshotsToKeep(ref.minSnapshotsToKeep());
              }
              builder.putRefs(name, refBuilder.build());
            });

    return builder.build();
  }

  private Long safeLong(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException ignored) {
      return null;
    }
  }

  @Override
  public void close() {
    closeCatalog();
    try {
    } catch (Exception ignore) {
    }
    if (externalFileIO instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (Exception ignoreClose) {
      }
    }
  }

  protected abstract Table loadTableFromSource(String namespaceFq, String tableName);

  protected void closeCatalog() {}

  protected Table getSingleTable() {
    return singleTable;
  }

  private record EngineOut(
      StatsEngine.Result<Integer> result,
      Map<Integer, String> columnNames,
      Map<Integer, LogicalType> logicalTypes) {}

  private EngineOut runEngine(Table table, long snapshotId, Set<Integer> colIds) {
    try (var planner = new IcebergPlanner(table, snapshotId, colIds, null)) {

      Map<Integer, String> colNames = planner.columnNamesByKey();
      Map<Integer, LogicalType> logicalTypes = planner.logicalTypesByKey();

      if (!ndvEnabled) {
        NdvProvider none = null;
        StatsEngine<Integer> engine =
            new GenericStatsEngine<>(planner, none, null, colNames, logicalTypes);
        var result = engine.compute();
        return new EngineOut(result, colNames, logicalTypes);
      }

      Map<String, ColumnNdv> puffinMap = null;
      try {
        puffinMap = PuffinNdvProvider.readPuffinNdvWithSketches(table, snapshotId, colNames::get);
      } catch (IOException ioe) {
      }

      NdvProvider bootstrap =
          (puffinMap == null || puffinMap.isEmpty()) ? null : new StaticOnceNdvProvider(puffinMap);

      final int totalCols = colNames.size();
      int missing;
      if (bootstrap != null) {
        final Map<String, ColumnNdv> m = puffinMap;
        int miss = 0;
        for (String columnName : colNames.values()) {
          ColumnNdv ndv = m.get(columnName);
          boolean hasData =
              ndv != null
                  && (ndv.approx != null || (ndv.sketches != null && !ndv.sketches.isEmpty()));
          if (!hasData) miss++;
        }
        missing = miss;
      } else {
        missing = totalCols;
      }

      NdvProvider perFileNdv = null;
      if (missing > 0) {
        var parquetNdv = ParquetNdvProvider.forIcebergIO(path -> table.io().newInputFile(path));
        NdvProvider base = FilteringNdvProvider.bySuffix(Set.of(".parquet", ".parq"), parquetNdv);

        if (ndvSampleFraction < 1.0 || ndvMaxFiles > 0) {
          base = new SamplingNdvProvider(base, ndvSampleFraction, ndvMaxFiles);
        }

        perFileNdv = base;
      }

      var engine = new GenericStatsEngine<>(planner, perFileNdv, bootstrap, colNames, logicalTypes);

      var result = engine.compute();
      return new EngineOut(result, colNames, logicalTypes);
    } catch (Exception e) {
      throw new RuntimeException("Stats compute failed for snapshot " + snapshotId, e);
    }
  }

  private static Set<Integer> resolveFieldIdsNested(Schema schema, Set<String> selectors) {
    Map<String, Integer> byPath = new LinkedHashMap<>();
    for (Types.NestedField top : schema.columns()) {
      collectNested(top, "", byPath);
    }

    Set<Integer> out = new LinkedHashSet<>();
    for (String sel : selectors) {
      if (sel == null || sel.isBlank()) {
        continue;
      }

      String s = sel.trim();
      if (s.startsWith("#")) {
        out.add(Integer.parseInt(s.substring(1)));
      } else {
        Integer id = byPath.get(s);
        if (id == null) {
          throw new IllegalArgumentException("Unknown column selector: " + s);
        }
        out.add(id);
      }
    }
    return out;
  }

  private static void collectNested(Types.NestedField f, String prefix, Map<String, Integer> out) {
    final String name = prefix.isEmpty() ? f.name() : prefix + "." + f.name();
    final Type t = f.type();

    out.put(name, f.fieldId());

    if (t.isStructType()) {
      for (Types.NestedField child : t.asStructType().fields()) {
        collectNested(child, name, out);
      }
    } else if (t.isListType()) {
      Types.ListType lt = t.asListType();
      Types.NestedField elem = lt.fields().get(0);
      collectNested(elem, name, out);
    } else if (t.isMapType()) {
      Types.MapType mt = t.asMapType();
      Types.NestedField key = mt.fields().get(0);
      Types.NestedField val = mt.fields().get(1);
      collectNested(key, name, out);
      collectNested(val, name, out);
    }
  }

  static boolean parseNdvEnabled(Map<String, String> options) {
    if (options != null) {
      String v = options.getOrDefault("stats.ndv.enabled", "true");
      return !v.equalsIgnoreCase("false");
    }
    return false;
  }

  static double parseNdvSampleFraction(Map<String, String> options) {
    if (options == null) {
      return 0.0d;
    }

    String raw = options.get("stats.ndv.sample_fraction");
    if (raw == null || raw.isBlank()) {
      return 1.0d;
    }
    try {
      double f = Double.parseDouble(raw.trim());
      if (f <= 0.0d) {
        return 0.0d;
      }
      if (f > 1.0d) {
        return 1.0d;
      }
      return f;
    } catch (NumberFormatException nfe) {
      return 1.0d;
    }
  }

  final class NdvOnlyResult<K> implements StatsEngine.Result<K> {
    private final long rows;
    private final long bytes;
    private final long files;
    private final Map<K, StatsEngine.ColumnAgg> cols;

    NdvOnlyResult(long rows, long bytes, long files, Map<K, StatsEngine.ColumnAgg> cols) {
      this.rows = rows;
      this.bytes = bytes;
      this.files = files;
      this.cols = cols;
    }

    @Override
    public long totalRowCount() {
      return rows;
    }

    @Override
    public long totalSizeBytes() {
      return bytes;
    }

    @Override
    public long fileCount() {
      return files;
    }

    @Override
    public Map<K, StatsEngine.ColumnAgg> columns() {
      return cols;
    }

    static <K> StatsEngine.ColumnAgg ndvOnly(ColumnNdv ndv) {
      return new StatsEngine.ColumnAgg() {
        @Override
        public Long ndvExact() {
          return null;
        }

        @Override
        public ColumnNdv ndv() {
          return ndv;
        }

        @Override
        public Long valueCount() {
          return null;
        }

        @Override
        public Long nullCount() {
          return null;
        }

        @Override
        public Long nanCount() {
          return null;
        }

        @Override
        public Object min() {
          return null;
        }

        @Override
        public Object max() {
          return null;
        }
      };
    }
  }
}
