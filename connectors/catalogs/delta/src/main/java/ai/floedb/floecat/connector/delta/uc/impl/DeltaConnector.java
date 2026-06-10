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
import ai.floedb.floecat.connector.common.ConnectorPlanningSupport;
import ai.floedb.floecat.connector.common.ConnectorStatsViewBuilder;
import ai.floedb.floecat.connector.common.GenericStatsEngine;
import ai.floedb.floecat.connector.common.ParquetPageIndexReader;
import ai.floedb.floecat.connector.common.PlannedFile;
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
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.parquet.io.InputFile;
import org.jboss.logging.Logger;

abstract class DeltaConnector implements FloecatConnector {

  protected static final ObjectMapper M = new ObjectMapper();
  private static final String DELTA_CHECK_CONSTRAINT_PREFIX = "delta.constraints.";
  private static final Logger LOG = Logger.getLogger(DeltaConnector.class);

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
  public List<PlannedTableTask> planTableTasks(TablePlanningRequest request) {
    return ConnectorPlanningSupport.planTableTasks(request, this::listTables);
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
    Set<Long> targetSnapshotIds = options == null ? Set.of() : options.targetSnapshotIds();
    final Snapshot latestSnapshot = table.getLatestSnapshot(engine);
    if (latestSnapshot == null) {
      return List.of();
    }
    final long latestVersion = latestSnapshot.getVersion();

    List<Long> versions =
        versionsToEnumerate(
            latestVersion,
            fullRescan,
            knownSnapshotIds,
            targetSnapshotIds,
            options == null ? FloecatConnector.SnapshotSelectionKind.ALL : options.selectionKind(),
            options == null ? Set.of() : options.selectionSnapshotIds(),
            options == null ? 0 : options.latestN());
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
    return captureSnapshotTargetStats(
        namespaceFq,
        tableName,
        destinationTableId,
        snapshotId,
        includeColumns,
        includeTargetKinds,
        ColumnSelectorPolicy.defaults());
  }

  @Override
  public List<TargetStatsRecord> captureSnapshotTargetStats(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      long snapshotId,
      Set<String> includeColumns,
      Set<StatsTargetKind> includeTargetKinds,
      ColumnSelectorPolicy columnSelectorPolicy) {
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
        tableRoot,
        destinationTableId,
        includeColumns,
        columnSelectorPolicy,
        snapshotId,
        snapshot,
        includeTargetKinds,
        Set.of());
  }

  @Override
  public Optional<DirectSnapshotStatsCapture> captureSnapshotTargetStatsDirect(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      long snapshotId,
      Set<String> includeColumns,
      Set<StatsTargetKind> includeTargetKinds,
      ColumnSelectorPolicy columnSelectorPolicy) {
    if (snapshotId < 0) {
      return Optional.empty();
    }
    EnumSet<StatsTargetKind> requestedKinds =
        includeTargetKinds == null || includeTargetKinds.isEmpty()
            ? EnumSet.noneOf(StatsTargetKind.class)
            : EnumSet.copyOf(includeTargetKinds);
    requestedKinds.remove(StatsTargetKind.EXPRESSION);
    if (requestedKinds.isEmpty()) {
      return Optional.of(DirectSnapshotStatsCapture.of(List.of(), 0));
    }

    final String tableRoot = storageLocation(namespaceFq, tableName);
    final Table table = loadTable(tableRoot);
    Snapshot snapshot = table.getSnapshotAsOfVersion(engine, snapshotId);
    if (snapshot == null) {
      return Optional.empty();
    }

    final StructType kernelSchema = snapshot.getSchema();
    final Map<String, LogicalType> nameToType = DeltaTypeMapper.deltaTypeMap(kernelSchema);
    final Set<String> includeNames =
        FloecatConnector.resolveIncludedColumns(
            List.copyOf(nameToType.keySet()), includeColumns, columnSelectorPolicy);

    try (var planner =
        new DeltaPlanner(
            this.engine,
            this.parquetInput,
            tableRoot,
            snapshotId,
            includeNames,
            Set.of(),
            nameToType,
            null,
            true,
            false)) {
      if (planner.missingLogStats()) {
        LOG.infof(
            "Delta direct snapshot stats fallback table=%s.%s snapshotId=%d reason=missing_log_stats requestedKinds=%s includeColumns=%d requestedColumnTypes=%s checkpointStructRecoveredFiles=%d checkpointStructRecoverySamples=%s missingStatsFiles=%d missingStatsSamples=%s",
            namespaceFq,
            tableName,
            snapshotId,
            requestedKinds,
            includeNames.size(),
            requestedColumnTypes(includeNames, nameToType),
            planner.checkpointStructRecoveryFileCount(),
            planner.checkpointStructRecoverySamplePaths(),
            planner.missingLogStatsFileCount(),
            planner.missingLogStatsSamplePaths());
        return Optional.empty();
      }
      if (planner.hasDeletionVectors()) {
        LOG.infof(
            "Delta direct snapshot stats fallback table=%s.%s snapshotId=%d reason=deletion_vectors requestedKinds=%s includeColumns=%d requestedColumnTypes=%s checkpointStructRecoveredFiles=%d checkpointStructRecoverySamples=%s inlineDeletionVectors=%d onDiskDeletionVectors=%d deletionVectorSamples=%s",
            namespaceFq,
            tableName,
            snapshotId,
            requestedKinds,
            includeNames.size(),
            requestedColumnTypes(includeNames, nameToType),
            planner.checkpointStructRecoveryFileCount(),
            planner.checkpointStructRecoverySamplePaths(),
            planner.inlineDeletionVectorCount(),
            planner.onDiskDeletionVectorCount(),
            planner.deletionVectorSamplePaths());
        return Optional.empty();
      }
      if (planner.checkpointStructRecoveryFileCount() > 0) {
        LOG.infof(
            "Delta direct snapshot stats recovered_from_checkpoint_struct table=%s.%s snapshotId=%d requestedKinds=%s includeColumns=%d requestedColumnTypes=%s recoveredFiles=%d recoverySamples=%s",
            namespaceFq,
            tableName,
            snapshotId,
            requestedKinds,
            includeNames.size(),
            requestedColumnTypes(includeNames, nameToType),
            planner.checkpointStructRecoveryFileCount(),
            planner.checkpointStructRecoverySamplePaths());
      }
    }
    int sourceFileCount =
        planSnapshotFiles(namespaceFq, tableName, destinationTableId, snapshotId)
            .map(plan -> plan.dataFiles().size() + plan.deleteFiles().size())
            .orElse(0);
    return Optional.of(
        DirectSnapshotStatsCapture.of(
            buildTargetStats(
                tableRoot,
                destinationTableId,
                includeColumns,
                columnSelectorPolicy,
                snapshotId,
                snapshot,
                requestedKinds,
                Set.of(),
                false),
            sourceFileCount));
  }

  @Override
  public FileGroupCaptureResult capturePlannedFileGroup(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      long snapshotId,
      Set<String> plannedFilePaths,
      Set<String> includeColumns,
      Set<StatsTargetKind> includeTargetKinds,
      boolean captureIndexes) {
    return capturePlannedFileGroup(
        namespaceFq,
        tableName,
        destinationTableId,
        snapshotId,
        plannedFilePaths,
        includeColumns,
        includeTargetKinds,
        captureIndexes,
        ColumnSelectorPolicy.defaults());
  }

  @Override
  public FileGroupCaptureResult capturePlannedFileGroup(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      long snapshotId,
      Set<String> plannedFilePaths,
      Set<String> includeColumns,
      Set<StatsTargetKind> includeTargetKinds,
      boolean captureIndexes,
      ColumnSelectorPolicy columnSelectorPolicy) {
    if (snapshotId < 0 || plannedFilePaths == null || plannedFilePaths.isEmpty()) {
      return FileGroupCaptureResult.empty();
    }
    final String tableRoot = storageLocation(namespaceFq, tableName);
    final Table table = loadTable(tableRoot);
    Snapshot snapshot = table.getSnapshotAsOfVersion(engine, snapshotId);
    if (snapshot == null) {
      return FileGroupCaptureResult.empty();
    }
    List<TargetStatsRecord> stats =
        buildTargetStats(
            tableRoot,
            destinationTableId,
            includeColumns,
            columnSelectorPolicy,
            snapshotId,
            snapshot,
            includeTargetKinds == null || includeTargetKinds.isEmpty()
                ? Set.of(StatsTargetKind.FILE)
                : includeTargetKinds,
            plannedFilePaths);
    List<ParquetPageIndexEntry> pageIndexEntries =
        captureIndexes
            ? new ParquetPageIndexReader(parquetInput).readEntries(plannedFilePaths)
            : List.of();
    return FileGroupCaptureResult.of(stats, pageIndexEntries);
  }

  @Override
  public Optional<SnapshotFilePlan> planSnapshotFiles(
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

    try (var planner =
        new DeltaPlanner(
            engine,
            parquetInput,
            tableRoot,
            snapshotId,
            Set.of(),
            Set.of(),
            null,
            null,
            false,
            true)) {
      List<SnapshotFileEntry> dataFiles = new ArrayList<>();
      for (PlannedFile<String> planned : planner) {
        dataFiles.add(toDataScanFile(planned));
      }
      return Optional.of(new SnapshotFilePlan(List.copyOf(dataFiles), List.of()));
    }
  }

  @Override
  public void close() {}

  @Override
  public Optional<SnapshotConstraints> snapshotConstraints(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      SnapshotBundle snapshotBundle) {
    if (snapshotBundle == null
        || snapshotBundle.snapshotId() < 0
        || snapshotBundle.schemaJson() == null
        || snapshotBundle.schemaJson().isBlank()) {
      return Optional.empty();
    }
    StructType schema = DataTypeJsonSerDe.deserializeStructType(snapshotBundle.schemaJson());
    List<ConstraintDefinition> constraints =
        mapDeltaConstraints(
            schema,
            fallbackTablePropertiesForConstraints(namespaceFq, tableName),
            snapshotBundle.schemaJson());
    if (constraints.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(SnapshotConstraints.newBuilder().addAllConstraints(constraints).build());
  }

  protected abstract String storageLocation(String namespaceFq, String tableName);

  /**
   * Best-effort catalog-level fallback when snapshot metadata does not expose properties.
   *
   * <p>Returned values are used for connector-level CHECK constraint extraction.
   */
  protected Map<String, String> fallbackTablePropertiesForConstraints(
      String namespaceFq, String tableName) {
    return Map.of();
  }

  private static SnapshotFileEntry toDataScanFile(PlannedFile<String> planned) {
    return new SnapshotFileEntry(
        planned.path(),
        planned.format(),
        planned.sizeBytes(),
        planned.rowCount(),
        FileContent.FC_DATA,
        planned.partitionDataJson(),
        planned.partitionSpecId(),
        List.of(),
        planned.sequenceNumber());
  }

  protected Table loadTable(String tableRoot) {
    return Table.forPath(engine, tableRoot);
  }

  private static Map<String, String> requestedColumnTypes(
      Set<String> includeNames, Map<String, LogicalType> nameToType) {
    LinkedHashMap<String, String> typed = new LinkedHashMap<>();
    if (includeNames == null || includeNames.isEmpty()) {
      return typed;
    }
    for (String name : includeNames) {
      if (name == null || name.isBlank()) {
        continue;
      }
      LogicalType logicalType = nameToType == null ? null : nameToType.get(name);
      typed.put(name, logicalType == null ? "UNKNOWN" : logicalType.toString());
    }
    return typed;
  }

  protected String describeTableSchemaJson(String tableRoot) {
    Table table = loadTable(tableRoot);
    Snapshot snapshot = table.getLatestSnapshot(engine);
    if (snapshot == null) {
      throw new IllegalStateException("Delta table has no latest snapshot at " + tableRoot);
    }
    return snapshotSchemaJson(snapshot);
  }

  protected TableDescriptor describeFromDelta(
      String tableRoot, String namespaceFq, String tableName) {
    try {
      Map<String, String> props = new LinkedHashMap<>();
      props.put("data_source_format", "DELTA");
      props.put("storage_location", tableRoot);

      return new TableDescriptor(
          namespaceFq,
          tableName,
          tableRoot,
          describeTableSchemaJson(tableRoot),
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
      Set<String> plannedFilePaths,
      Map<String, LogicalType> nameToType,
      boolean allowFooterFallback) {

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
            plannedFilePaths,
            nameToType,
            ndvProvider,
            true,
            allowFooterFallback)) {

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
      long latestVersion,
      boolean fullRescan,
      Set<Long> knownSnapshotIds,
      Set<Long> targetSnapshotIds,
      FloecatConnector.SnapshotSelectionKind selectionKind,
      Set<Long> selectionSnapshotIds,
      int latestN) {
    List<Long> candidates = new ArrayList<>();
    switch (selectionKind) {
      case CURRENT -> candidates.add(latestVersion);
      case LATEST_N -> {
        long start = Math.max(0L, latestVersion - Math.max(0, latestN) + 1L);
        for (long version = start; version <= latestVersion; version++) {
          candidates.add(version);
        }
      }
      case EXPLICIT ->
          selectionSnapshotIds.stream()
              .filter(version -> version >= 0L && version <= latestVersion)
              .sorted()
              .forEach(candidates::add);
      case ALL -> {
        for (long version = 0L; version <= latestVersion; version++) {
          candidates.add(version);
        }
      }
    }
    List<Long> versions = new ArrayList<>(candidates.size());
    for (long version : candidates) {
      if (!fullRescan && knownSnapshotIds.contains(version)) {
        continue;
      }
      if (targetSnapshotIds != null
          && !targetSnapshotIds.isEmpty()
          && !targetSnapshotIds.contains(version)) {
        continue;
      }
      versions.add(version);
    }
    return List.copyOf(versions);
  }

  static List<ConstraintDefinition> mapDeltaConstraints(StructType schema, String schemaJson) {
    return mapDeltaConstraints(schema, Map.of(), schemaJson);
  }

  static List<ConstraintDefinition> mapDeltaConstraints(
      StructType schema, Map<String, String> tableProperties, String schemaJson) {
    if (schema == null) {
      return List.of();
    }
    Map<String, Integer> ordinals =
        LogicalSchemaMapper.buildColumnOrdinals(
            ColumnIdAlgorithm.CID_PATH_ORDINAL, TableFormat.TF_DELTA, schemaJson);
    List<ConstraintDefinition> out = new ArrayList<>();
    collectDeltaNotNullConstraints(schema.fields(), "", out, ordinals);
    out.addAll(mapDeltaCheckConstraints(tableProperties));
    return List.copyOf(out);
  }

  private SnapshotBundle buildSnapshotBundle(String tableRoot, long version, Snapshot snapshot) {
    final long createdMs = snapshot.getTimestamp(engine);
    final long parent = Math.max(0L, version - 1L);

    final StructType kernelSchema = snapshot.getSchema();
    final String schemaJson = snapshotSchemaJson(snapshot);
    final PartitionSpecInfo partitionSpec = toPartitionSpecInfo(snapshot);
    return new SnapshotBundle(
        version, parent, createdMs, schemaJson, partitionSpec, 0L, null, Map.of(), 0, null);
  }

  private List<TargetStatsRecord> buildTargetStats(
      String tableRoot,
      ResourceId destinationTableId,
      Set<String> includeColumns,
      ColumnSelectorPolicy columnSelectorPolicy,
      long version,
      Snapshot snapshot,
      Set<StatsTargetKind> includeTargetKinds,
      Set<String> plannedFilePaths) {
    return buildTargetStats(
        tableRoot,
        destinationTableId,
        includeColumns,
        columnSelectorPolicy,
        version,
        snapshot,
        includeTargetKinds,
        plannedFilePaths,
        true);
  }

  private List<TargetStatsRecord> buildTargetStats(
      String tableRoot,
      ResourceId destinationTableId,
      Set<String> includeColumns,
      ColumnSelectorPolicy columnSelectorPolicy,
      long version,
      Snapshot snapshot,
      Set<StatsTargetKind> includeTargetKinds,
      Set<String> plannedFilePaths,
      boolean allowFooterFallback) {
    boolean emitTable = includeTargetKinds.contains(StatsTargetKind.TABLE);
    boolean emitColumns = includeTargetKinds.contains(StatsTargetKind.COLUMN);
    boolean emitFiles = includeTargetKinds.contains(StatsTargetKind.FILE);
    if (!emitTable && !emitColumns && !emitFiles) {
      return List.of();
    }

    final StructType kernelSchema = snapshot.getSchema();
    final Map<String, LogicalType> nameToType = DeltaTypeMapper.deltaTypeMap(kernelSchema);
    final String schemaJson = snapshotSchemaJson(snapshot);
    final Set<String> includeNames =
        FloecatConnector.resolveIncludedColumns(
            List.copyOf(nameToType.keySet()), includeColumns, columnSelectorPolicy);

    EngineOut engineOut =
        runEngine(
            tableRoot, version, includeNames, plannedFilePaths, nameToType, allowFooterFallback);
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

  protected String snapshotSchemaJson(Snapshot snapshot) {
    if (snapshot instanceof SnapshotImpl snapshotImpl && snapshotImpl.getMetadata() != null) {
      String schemaJson = snapshotImpl.getMetadata().getSchemaString();
      if (schemaJson != null && !schemaJson.isBlank()) {
        return schemaJson;
      }
    }
    throw new IllegalStateException("Delta snapshot metadata schema JSON is required");
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
}
