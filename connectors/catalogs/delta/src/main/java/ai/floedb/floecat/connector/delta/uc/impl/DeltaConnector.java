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

package ai.floedb.floecat.connector.delta.uc.impl;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.FileContent;
import ai.floedb.floecat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.TableStats;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.common.ConnectorStatsViewBuilder;
import ai.floedb.floecat.connector.common.GenericStatsEngine;
import ai.floedb.floecat.connector.common.StatsEngine;
import ai.floedb.floecat.connector.common.ndv.NdvProvider;
import ai.floedb.floecat.connector.common.ndv.ParquetNdvProvider;
import ai.floedb.floecat.connector.common.ndv.SamplingNdvProvider;
import ai.floedb.floecat.connector.common.resolver.LogicalSchemaMapper;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.types.LogicalType;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.parquet.io.InputFile;

abstract class DeltaConnector implements FloecatConnector {

  protected static final ObjectMapper M = new ObjectMapper();

  private final String connectorId;
  protected final Engine engine;
  protected final Function<String, InputFile> parquetInput;
  protected final boolean ndvEnabled;
  protected final double ndvSampleFraction;
  protected final long ndvMaxFiles;

  protected DeltaConnector(
      String connectorId,
      Engine engine,
      Function<String, InputFile> parquetInput,
      boolean ndvEnabled,
      double ndvSampleFraction,
      long ndvMaxFiles) {
    this.connectorId = connectorId;
    this.engine = engine;
    this.parquetInput = parquetInput;
    this.ndvEnabled = ndvEnabled;
    this.ndvSampleFraction = ndvSampleFraction;
    this.ndvMaxFiles = ndvMaxFiles;
  }

  @Override
  public String id() {
    return connectorId;
  }

  @Override
  public ConnectorFormat format() {
    return ConnectorFormat.CF_DELTA;
  }

  @Override
  public List<SnapshotBundle> enumerateSnapshotsWithStats(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      Set<String> includeColumns) {
    return enumerateSnapshotsWithStats(
        namespaceFq,
        tableName,
        destinationTableId,
        includeColumns,
        FloecatConnector.SnapshotEnumerationOptions.full(true));
  }

  @Override
  public List<SnapshotBundle> enumerateSnapshotsWithStats(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      Set<String> includeColumns,
      FloecatConnector.SnapshotEnumerationOptions options) {

    final String tableRoot = storageLocation(namespaceFq, tableName);

    final Table table = Table.forPath(engine, tableRoot);
    final Snapshot snapshot = table.getLatestSnapshot(engine);

    final long version = snapshot.getVersion();
    boolean includeStatistics = options == null || options.includeStatistics();
    boolean fullRescan = options == null || options.fullRescan();
    Set<Long> knownSnapshotIds = options == null ? Set.of() : options.knownSnapshotIds();
    if (!fullRescan && knownSnapshotIds.contains(version)) {
      return List.of();
    }
    final long createdMs = snapshot.getTimestamp(engine);
    final long parent = Math.max(0L, version - 1L);

    final StructType kernelSchema = snapshot.getSchema();
    final Map<String, LogicalType> nameToType = DeltaTypeMapper.deltaTypeMap(kernelSchema);
    final String schemaJson = kernelSchema.toJson();
    final PartitionSpecInfo partitionSpec = toPartitionSpecInfo(snapshot);

    final Set<String> includeNames =
        (includeColumns == null || includeColumns.isEmpty())
            ? new LinkedHashSet<>(nameToType.keySet())
            : includeColumns.stream()
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toCollection(LinkedHashSet::new));

    // Always return at least the snapshot + schema; stats may be omitted.
    TableStats tStats = null;

    List<FloecatConnector.ColumnStatsView> cStats = List.of();
    List<FloecatConnector.FileColumnStatsView> fileStats = List.of();

    if (includeStatistics) {
      EngineOut engineOut = runEngine(tableRoot, version, includeNames, nameToType);
      if (engineOut.hasInlineDeletionVectors()) {
        throw new UnsupportedOperationException(
            "Delta table uses inline deletion vectors; not supported for snapshot " + version);
      }
      var result = engineOut.result();
      var logicalTypes = engineOut.logicalTypes();

      tStats =
          ConnectorStatsViewBuilder.toTableStats(
              destinationTableId, version, createdMs, TableFormat.TF_DELTA, result);

      var positions =
          LogicalSchemaMapper.buildColumnOrdinals(
              ColumnIdAlgorithm.CID_PATH_ORDINAL, TableFormat.TF_DELTA, schemaJson);

      cStats =
          ConnectorStatsViewBuilder.toColumnStatsView(
              result.columns(),
              name -> name,
              name -> name,
              name -> positions.getOrDefault(name, 0),
              name -> 0,
              name -> {
                var lt = logicalTypes.get(name);
                return (lt != null) ? lt : nameToType.get(name);
              },
              result.totalRowCount());

      var mutableFiles =
          new ArrayList<FloecatConnector.FileColumnStatsView>(
              ConnectorStatsViewBuilder.toFileColumnStatsView(
                  result.files(),
                  name -> name,
                  name -> name,
                  name -> positions.getOrDefault(name, 0),
                  name -> 0,
                  name -> {
                    var lt = logicalTypes.get(name);
                    return (lt != null) ? lt : nameToType.get(name);
                  }));

      for (DeletionVectorDescriptor dv : engineOut.deletionVectors()) {
        String dvPath =
            (dv.isOnDisk() && dv.getPathOrInlineDv() != null) ? dv.getPathOrInlineDv() : "";
        long rowCount = dv.getCardinality();
        long sizeBytes = dv.getSizeInBytes();
        mutableFiles.add(
            new FloecatConnector.FileColumnStatsView(
                dvPath,
                "",
                rowCount,
                sizeBytes,
                FileContent.FC_POSITION_DELETES,
                "",
                0,
                List.of(),
                null,
                List.of()));
      }

      fileStats = List.copyOf(mutableFiles);
    }

    return List.of(
        new SnapshotBundle(
            version,
            parent,
            createdMs,
            tStats,
            cStats,
            fileStats,
            schemaJson,
            partitionSpec,
            0L,
            null,
            Map.of(),
            0,
            Map.of()));
  }

  @Override
  public void close() {}

  protected abstract String storageLocation(String namespaceFq, String tableName);

  protected TableDescriptor describeFromDelta(
      String tableRoot, String namespaceFq, String tableName) {
    try {
      Table table = Table.forPath(engine, tableRoot);
      Snapshot snapshot = table.getLatestSnapshot(engine);
      StructType kernelSchema = snapshot.getSchema();

      var fields = M.createArrayNode();
      var kernelFields = kernelSchema.fields();
      for (int i = 0; i < kernelFields.size(); i++) {
        var c = kernelFields.get(i);
        var n = M.createObjectNode();
        n.put("name", c.getName());
        n.put("type", c.getDataType().toString());
        n.put("nullable", c.isNullable());
        n.set("metadata", M.createObjectNode());
        fields.add(n);
      }
      var schemaNode = M.createObjectNode();
      schemaNode.put("type", "struct");
      schemaNode.set("fields", fields);

      Map<String, String> props = new LinkedHashMap<>();
      props.put("data_source_format", "DELTA");
      props.put("storage_location", tableRoot);

      return new TableDescriptor(
          namespaceFq,
          tableName,
          tableRoot,
          schemaNode.toString(),
          List.of(),
          ColumnIdAlgorithm.CID_PATH_ORDINAL,
          props);
    } catch (Exception e) {
      throw new RuntimeException("describe failed", e);
    }
  }

  protected record EngineOut(
      StatsEngine.Result<String> result,
      Map<String, LogicalType> logicalTypes,
      boolean hasDeletionVectors,
      boolean hasInlineDeletionVectors,
      List<DeletionVectorDescriptor> deletionVectors) {}

  protected EngineOut runEngine(
      String tableRoot,
      long version,
      Set<String> includeNames,
      Map<String, LogicalType> nameToType) {

    NdvProvider bootstrap = null;

    NdvProvider ndvProvider = null;

    if (ndvEnabled) {
      NdvProvider base = new ParquetNdvProvider(parquetInput);
      if (ndvSampleFraction < 1.0 || ndvMaxFiles > 0) {
        base = new SamplingNdvProvider(base, ndvSampleFraction, ndvMaxFiles);
      }

      ndvProvider = base;
    }

    try (var planner =
        new DeltaPlanner(
            this.engine,
            this.parquetInput,
            tableRoot,
            version,
            includeNames,
            nameToType,
            ndvProvider,
            true)) {

      var columnNames = planner.columnNamesByKey();
      var logicalTypes = planner.logicalTypesByKey();

      var engine =
          new GenericStatsEngine<>(planner, ndvProvider, bootstrap, columnNames, logicalTypes);

      var result = engine.compute();
      return new EngineOut(
          result,
          logicalTypes,
          planner.hasDeletionVectors(),
          planner.hasInlineDeletionVectors(),
          planner.deletionVectors());
    } catch (Exception e) {
      throw new RuntimeException("Delta stats compute failed (version " + version + ")", e);
    }
  }

  protected Snapshot resolveSnapshot(Table table, long snapshotId, long asOfTime) {
    if (snapshotId > 0) {
      return table.getSnapshotAsOfVersion(engine, snapshotId);
    }
    if (asOfTime > 0) {
      return table.getSnapshotAsOfTimestamp(engine, asOfTime);
    }

    return table.getLatestSnapshot(engine);
  }

  protected static PartitionSpecInfo toPartitionSpecInfo(Snapshot snapshot) {
    if (snapshot == null) {
      return null;
    }
    var partitionCols = snapshot.getPartitionColumnNames();
    if (partitionCols == null || partitionCols.isEmpty()) {
      return null;
    }
    PartitionSpecInfo.Builder builder =
        PartitionSpecInfo.newBuilder().setSpecId(0).setSpecName("delta");
    int order = 0;
    for (String column : partitionCols) {
      builder.addFields(
          ai.floedb.floecat.catalog.rpc.PartitionField.newBuilder()
              .setFieldId(++order)
              .setName(column)
              .setTransform("identity")
              .build());
    }
    return builder.build();
  }
}
