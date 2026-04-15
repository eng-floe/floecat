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
import ai.floedb.floecat.catalog.rpc.ConstraintColumnRef;
import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.ConstraintEnforcement;
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.catalog.rpc.FileContent;
import ai.floedb.floecat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.common.ConnectorStatsViewBuilder;
import ai.floedb.floecat.connector.common.GenericStatsEngine;
import ai.floedb.floecat.connector.common.StatsEngine;
import ai.floedb.floecat.connector.common.ndv.NdvProvider;
import ai.floedb.floecat.connector.common.ndv.ParquetNdvProvider;
import ai.floedb.floecat.connector.common.ndv.SamplingNdvProvider;
import ai.floedb.floecat.connector.common.resolver.ColumnIdComputer;
import ai.floedb.floecat.connector.common.resolver.LogicalSchemaMapper;
import ai.floedb.floecat.connector.common.resolver.StatsProtoEmitter;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind;
import ai.floedb.floecat.types.LogicalType;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.parquet.io.InputFile;

abstract class DeltaConnector implements FloecatConnector {

  protected static final ObjectMapper M = new ObjectMapper();
  private static final String DELTA_CHECK_CONSTRAINT_PREFIX = "delta.constraints.";

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
  public List<SnapshotBundle> enumerateSnapshots(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      FloecatConnector.SnapshotEnumerationOptions options) {

    final String tableRoot = storageLocation(namespaceFq, tableName);
    final Table table = loadTable(tableRoot);
    boolean fullRescan = options == null || options.fullRescan();
    Set<Long> knownSnapshotIds = options == null ? Set.of() : options.knownSnapshotIds();
    final Snapshot latestSnapshot = table.getLatestSnapshot(engine);
    if (latestSnapshot == null) {
      return List.of();
    }
    final long latestVersion = latestSnapshot.getVersion();

    List<Long> versions = versionsToEnumerate(latestVersion, fullRescan, knownSnapshotIds);
    if (versions.isEmpty()) {
      return List.of();
    }
    List<SnapshotBundle> bundles = new ArrayList<>(versions.size());
    for (long version : versions) {
      Snapshot snapshot =
          (version == latestVersion)
              ? latestSnapshot
              : table.getSnapshotAsOfVersion(engine, version);
      if (snapshot == null) {
        continue;
      }
      bundles.add(buildSnapshotBundle(tableRoot, version, snapshot));
    }
    return List.copyOf(bundles);
  }

  @Override
  public List<TargetStatsRecord> captureSnapshotTargetStats(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      long snapshotId,
      Set<String> includeColumns) {
    return captureSnapshotTargetStats(
        namespaceFq,
        tableName,
        destinationTableId,
        snapshotId,
        includeColumns,
        Set.of(
            StatsTargetKind.TABLE,
            StatsTargetKind.COLUMN,
            StatsTargetKind.FILE,
            StatsTargetKind.EXPRESSION));
  }

  @Override
  public List<TargetStatsRecord> captureSnapshotTargetStats(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      long snapshotId,
      Set<String> includeColumns,
      Set<StatsTargetKind> includeTargetKinds) {
    if (snapshotId < 0) {
      return List.of();
    }
    final String tableRoot = storageLocation(namespaceFq, tableName);
    final Table table = loadTable(tableRoot);
    Snapshot snapshot = table.getSnapshotAsOfVersion(engine, snapshotId);
    if (snapshot == null) {
      return List.of();
    }
    return buildTargetStats(
        tableRoot, destinationTableId, includeColumns, snapshotId, snapshot, includeTargetKinds);
  }

  @Override
  public void close() {}

  @Override
  public Optional<SnapshotConstraints> snapshotConstraints(
      String namespaceFq, String tableName, ResourceId destinationTableId, long snapshotId) {
    if (snapshotId < 0) {
      return Optional.empty();
    }
    final String tableRoot = storageLocation(namespaceFq, tableName);
    final Table table = loadTable(tableRoot);
    Snapshot snapshot = table.getSnapshotAsOfVersion(engine, snapshotId);
    if (snapshot == null) {
      return Optional.empty();
    }
    Map<String, String> tableProperties =
        mergeConstraintProperties(
            fallbackTablePropertiesForConstraints(namespaceFq, tableName),
            snapshotTableProperties(snapshot));
    List<ConstraintDefinition> constraints =
        mapDeltaConstraints(snapshot.getSchema(), tableProperties);
    if (constraints.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(SnapshotConstraints.newBuilder().addAllConstraints(constraints).build());
  }

  protected abstract String storageLocation(String namespaceFq, String tableName);

  /**
   * Snapshot-scoped properties used for constraint extraction.
   *
   * <p>Default behavior reads Delta snapshot metadata when available.
   */
  protected Map<String, String> snapshotTableProperties(Snapshot snapshot) {
    return extractTableProperties(snapshot);
  }

  /**
   * Best-effort catalog-level fallback when snapshot metadata does not expose properties.
   *
   * <p>Returned values are merged with {@link #snapshotTableProperties(Snapshot)}. Snapshot
   * properties win on key collisions.
   */
  protected Map<String, String> fallbackTablePropertiesForConstraints(
      String namespaceFq, String tableName) {
    return Map.of();
  }

  private static Map<String, String> mergeConstraintProperties(
      Map<String, String> fallbackProperties, Map<String, String> snapshotProperties) {
    if (fallbackProperties.isEmpty() && snapshotProperties.isEmpty()) {
      return Map.of();
    }
    Map<String, String> merged = new LinkedHashMap<>(fallbackProperties);
    // Snapshot metadata is preferred when both sources expose the same property key.
    merged.putAll(snapshotProperties);
    return Map.copyOf(merged);
  }

  protected Table loadTable(String tableRoot) {
    return Table.forPath(engine, tableRoot);
  }

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

  protected List<Long> versionsToEnumerate(
      long latestVersion, boolean fullRescan, Set<Long> knownSnapshotIds) {
    List<Long> versions = new ArrayList<>();
    for (long version = 0L; version <= latestVersion; version++) {
      if (!fullRescan && knownSnapshotIds.contains(version)) {
        continue;
      }
      versions.add(version);
    }
    return List.copyOf(versions);
  }

  static List<ConstraintDefinition> mapDeltaConstraints(StructType schema) {
    return mapDeltaConstraints(schema, Map.of());
  }

  static List<ConstraintDefinition> mapDeltaConstraints(
      StructType schema, Map<String, String> tableProperties) {
    if (schema == null) {
      return List.of();
    }
    // Compute path → ordinal map so NOT NULL constraints carry stable CID_PATH_ORDINAL column IDs,
    // consistent with how column stats are keyed for Delta tables.
    Map<String, Integer> ordinals = Map.of();
    try {
      ordinals =
          LogicalSchemaMapper.buildColumnOrdinals(
              ColumnIdAlgorithm.CID_PATH_ORDINAL, TableFormat.TF_DELTA, schema.toJson());
    } catch (Exception ignored) {
      // Fall back to columnId=0 if schema JSON conversion fails.
    }
    List<ConstraintDefinition> out = new ArrayList<>();
    collectDeltaNotNullConstraints(schema.fields(), "", out, ordinals);
    out.addAll(mapDeltaCheckConstraints(tableProperties));
    return List.copyOf(out);
  }

  private SnapshotBundle buildSnapshotBundle(String tableRoot, long version, Snapshot snapshot) {
    final long createdMs = snapshot.getTimestamp(engine);
    final long parent = Math.max(0L, version - 1L);

    final StructType kernelSchema = snapshot.getSchema();
    final String schemaJson = kernelSchema.toJson();
    final PartitionSpecInfo partitionSpec = toPartitionSpecInfo(snapshot);
    return new SnapshotBundle(
        version, parent, createdMs, schemaJson, partitionSpec, 0L, null, Map.of(), 0, Map.of());
  }

  private List<TargetStatsRecord> buildTargetStats(
      String tableRoot,
      ResourceId destinationTableId,
      Set<String> includeColumns,
      long version,
      Snapshot snapshot,
      Set<StatsTargetKind> includeTargetKinds) {
    boolean emitTable = includeTargetKinds.contains(StatsTargetKind.TABLE);
    boolean emitColumns = includeTargetKinds.contains(StatsTargetKind.COLUMN);
    boolean emitFiles = includeTargetKinds.contains(StatsTargetKind.FILE);
    if (!emitTable && !emitColumns && !emitFiles) {
      return List.of();
    }

    final StructType kernelSchema = snapshot.getSchema();
    final Map<String, LogicalType> nameToType = DeltaTypeMapper.deltaTypeMap(kernelSchema);
    final String schemaJson = kernelSchema.toJson();
    final Set<String> includeNames =
        (includeColumns == null || includeColumns.isEmpty())
            ? new LinkedHashSet<>(nameToType.keySet())
            : includeColumns.stream()
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toCollection(LinkedHashSet::new));

    EngineOut engineOut = runEngine(tableRoot, version, includeNames, nameToType);
    if (engineOut.hasInlineDeletionVectors()) {
      throw new UnsupportedOperationException(
          "Delta table uses inline deletion vectors; not supported for snapshot " + version);
    }
    var result = engineOut.result();
    var logicalTypes = engineOut.logicalTypes();

    var tStats =
        ConnectorStatsViewBuilder.toTableValueStats(
            version, snapshot.getTimestamp(engine), TableFormat.TF_DELTA, result);

    var positions =
        LogicalSchemaMapper.buildColumnOrdinals(
            ColumnIdAlgorithm.CID_PATH_ORDINAL, TableFormat.TF_DELTA, schemaJson);

    List<FloecatConnector.ColumnStatsView> cStats =
        emitColumns
            ? ConnectorStatsViewBuilder.toColumnStatsView(
                result.columns(),
                name -> name,
                name -> name,
                name -> positions.getOrDefault(name, 0),
                name -> 0,
                name -> {
                  var lt = logicalTypes.get(name);
                  return (lt != null) ? lt : nameToType.get(name);
                },
                result.totalRowCount())
            : List.of();

    List<FloecatConnector.FileColumnStatsView> files = List.of();
    if (emitFiles) {
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
      files = List.copyOf(mutableFiles);
    }

    List<TargetStatsRecord> materialized = new ArrayList<>();
    if (emitTable) {
      materialized.add(
          StatsProtoEmitter.tableStatsToTargetRecord(destinationTableId, version, tStats));
    }
    if (emitColumns) {
      materialized.addAll(
          StatsProtoEmitter.toTargetColumnStatsFromViews(
              destinationTableId, version, ColumnIdAlgorithm.CID_PATH_ORDINAL, cStats));
    }
    if (emitFiles) {
      materialized.addAll(
          StatsProtoEmitter.toTargetFileStatsFromViews(
              destinationTableId, version, ColumnIdAlgorithm.CID_PATH_ORDINAL, files));
    }
    return List.copyOf(materialized);
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

  private static void collectDeltaNotNullConstraints(
      List<StructField> fields,
      String prefix,
      List<ConstraintDefinition> out,
      Map<String, Integer> ordinals) {
    for (StructField field : fields) {
      String path = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
      boolean fieldIsNonNull = !field.isNullable();
      boolean isStruct = field.getDataType() instanceof StructType;
      if (fieldIsNonNull && !isStruct) {
        int ordinal = ordinals.getOrDefault(path, 0);
        long columnId =
            (ordinal > 0)
                ? ColumnIdComputer.compute(
                    ColumnIdAlgorithm.CID_PATH_ORDINAL, path, null, ordinal, 0)
                : 0L;
        // Name encodes columnId when available (stable for column renames iff path+ordinal
        // unchanged — same invariant as the column_id itself), or path for nested struct leaves
        // where no stable ID is computable without catalog support.
        String constraintName = (columnId != 0L) ? "nn_" + columnId : "nn_" + path;
        out.add(
            ConstraintDefinition.newBuilder()
                .setName(constraintName)
                .setType(ConstraintType.CT_NOT_NULL)
                .setEnforcement(ConstraintEnforcement.CE_ENFORCED)
                .addColumns(
                    ConstraintColumnRef.newBuilder()
                        .setColumnId(columnId)
                        .setColumnName(path)
                        .setOrdinal(1)
                        .build())
                .build());
      }
      if (isStruct && fieldIsNonNull) {
        // Only descend into a struct when the struct itself is non-nullable; a non-nullable child
        // inside a nullable parent struct is conditionally present, not flat-relational NOT NULL.
        collectDeltaNotNullConstraints(
            ((StructType) field.getDataType()).fields(), path, out, ordinals);
      }
    }
  }

  private static List<ConstraintDefinition> mapDeltaCheckConstraints(
      Map<String, String> tableProperties) {
    if (tableProperties == null || tableProperties.isEmpty()) {
      return List.of();
    }
    List<ConstraintDefinition> checks = new ArrayList<>();
    tableProperties.entrySet().stream()
        .filter(e -> e.getKey() != null && e.getKey().startsWith(DELTA_CHECK_CONSTRAINT_PREFIX))
        .sorted(Map.Entry.comparingByKey())
        .forEach(
            e -> {
              String name = e.getKey().substring(DELTA_CHECK_CONSTRAINT_PREFIX.length()).trim();
              String expression = e.getValue() == null ? "" : e.getValue().trim();
              if (name.isEmpty() || expression.isEmpty()) {
                return;
              }
              checks.add(
                  ConstraintDefinition.newBuilder()
                      .setName(name)
                      .setType(ConstraintType.CT_CHECK)
                      .setCheckExpression(expression)
                      .setEnforcement(ConstraintEnforcement.CE_ENFORCED)
                      .build());
            });
    return List.copyOf(checks);
  }

  private static Map<String, String> extractTableProperties(Snapshot snapshot) {
    if (snapshot instanceof SnapshotImpl snapshotImpl && snapshotImpl.getMetadata() != null) {
      return snapshotImpl.getMetadata().getConfiguration();
    }
    return Map.of();
  }
}
