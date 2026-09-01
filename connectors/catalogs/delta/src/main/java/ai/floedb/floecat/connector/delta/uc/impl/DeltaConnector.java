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
import ai.floedb.floecat.connector.common.ndv.ParquetAvgWidthProvider;
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
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.TableChangesUtils;
import io.delta.kernel.internal.TableImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.parquet.io.InputFile;
import org.jboss.logging.Logger;

abstract class DeltaConnector implements FloecatConnector {

  protected static final ObjectMapper M = new ObjectMapper();
  private static final String DELTA_CHECK_CONSTRAINT_PREFIX = "delta.constraints.";
  private static final Pattern EARLIEST_AVAILABLE_VERSION_PATTERN =
      Pattern.compile("earliest available version is\\s+(\\d+)", Pattern.CASE_INSENSITIVE);
  private static final int MAX_DELTA_CHANGE_READ_PARALLELISM = 16;
  private static final int TARGET_COMMITS_PER_CHANGE_READER = 64;
  private static final Logger LOG = Logger.getLogger(DeltaConnector.class);

  private final String connectorId;
  private final AutoCloseable engineResources;
  protected final Engine engine;
  protected final Function<String, InputFile> parquetInput;
  protected final boolean ndvEnabled;
  protected final double ndvSampleFraction;
  protected final long ndvMaxFiles;

  /**
   * @param engineResources what the engine was built on -- for an S3-backed engine the refreshing
   *     client holding its connection pool and credentials provider -- or null when the engine owns
   *     nothing releasable. Nothing else retains it, so {@link #close()} is its only release point.
   */
  protected DeltaConnector(
      String connectorId,
      Engine engine,
      Function<String, InputFile> parquetInput,
      boolean ndvEnabled,
      double ndvSampleFraction,
      long ndvMaxFiles,
      AutoCloseable engineResources) {
    this.engineResources = engineResources;
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

    final String storageLocation = storageLocation(namespaceFq, tableName);
    final Table table = loadTable(storageLocation);
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
    long earliestAvailableVersion = 0L;
    for (long version : versions) {
      if (version < earliestAvailableVersion) {
        continue;
      }
      SnapshotLoadResult snapshotResult =
          version == latestVersion
              ? SnapshotLoadResult.snapshot(latestSnapshot)
              : loadSnapshotAsOfVersion(table, version, storageLocation, earliestAvailableVersion);
      if (snapshotResult.earliestAvailableVersion() > earliestAvailableVersion) {
        earliestAvailableVersion = snapshotResult.earliestAvailableVersion();
      }
      Snapshot snapshot = snapshotResult.snapshot();
      if (snapshot == null) {
        continue;
      }
      bundles.add(buildSnapshotBundle(storageLocation, version, snapshot));
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
    final String storageLocation = storageLocation(namespaceFq, tableName);
    final Table table = loadTable(storageLocation);
    Snapshot snapshot = table.getSnapshotAsOfVersion(engine, snapshotId);
    if (snapshot == null) {
      return List.of();
    }
    return buildTargetStats(
        storageLocation,
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
      return Optional.of(DirectSnapshotStatsCapture.of(List.of(), 0, List.of()));
    }

    final String storageLocation = storageLocation(namespaceFq, tableName);
    final Table table = loadTable(storageLocation);
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
            storageLocation,
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
    Set<String> realizedStatsSelectors = new java.util.TreeSet<>();
    if (requestedKinds.contains(StatsTargetKind.COLUMN)) {
      var columnIds =
          LogicalSchemaMapper.buildColumnOrdinals(
              ColumnIdAlgorithm.CID_PATH_ORDINAL,
              TableFormat.TF_DELTA,
              snapshotSchemaJson(snapshot));
      for (String name : includeNames) {
        realizedStatsSelectors.add(name);
        long columnId = columnIds.getOrDefault(name, 0);
        if (columnId > 0L) {
          realizedStatsSelectors.add("#" + columnId);
        }
      }
    }
    return Optional.of(
        DirectSnapshotStatsCapture.of(
            buildTargetStats(
                storageLocation,
                destinationTableId,
                includeColumns,
                columnSelectorPolicy,
                snapshotId,
                snapshot,
                requestedKinds,
                Set.of(),
                false),
            sourceFileCount,
            List.copyOf(realizedStatsSelectors)));
  }

  @Override
  public FileGroupCaptureResult capturePlannedFileGroup(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      long snapshotId,
      Set<String> plannedFilePaths,
      Set<String> includeColumns,
      Set<String> indexColumns,
      Set<StatsTargetKind> includeTargetKinds,
      boolean captureIndexes,
      ColumnSelectorPolicy columnSelectorPolicy) {
    if (snapshotId < 0 || plannedFilePaths == null || plannedFilePaths.isEmpty()) {
      return FileGroupCaptureResult.empty();
    }
    final String storageLocation = storageLocation(namespaceFq, tableName);
    final Table table = loadTable(storageLocation);
    Snapshot snapshot = table.getSnapshotAsOfVersion(engine, snapshotId);
    if (snapshot == null) {
      return FileGroupCaptureResult.empty();
    }
    List<TargetStatsRecord> stats =
        buildTargetStats(
            storageLocation,
            destinationTableId,
            includeColumns,
            columnSelectorPolicy,
            snapshotId,
            snapshot,
            includeTargetKinds == null || includeTargetKinds.isEmpty()
                ? Set.of(StatsTargetKind.FILE)
                : includeTargetKinds,
            plannedFilePaths);
    Set<StatsTargetKind> requestedKinds =
        includeTargetKinds == null || includeTargetKinds.isEmpty()
            ? Set.of(StatsTargetKind.FILE)
            : includeTargetKinds;
    List<String> realizedStatsSelectors =
        requestedKinds.contains(StatsTargetKind.COLUMN)
            ? FloecatConnector.resolveIncludedColumns(
                    List.copyOf(DeltaTypeMapper.deltaTypeMap(snapshot.getSchema()).keySet()),
                    includeColumns,
                    columnSelectorPolicy)
                .stream()
                .sorted()
                .toList()
            : List.of();
    ParquetPageIndexReader.ReadResult pageIndexes =
        captureIndexes
            ? new ParquetPageIndexReader(parquetInput).read(plannedFilePaths)
            : ParquetPageIndexReader.ReadResult.empty();
    if (!captureIndexes) {
      return FileGroupCaptureResult.of(
          stats, pageIndexes.entries(), pageIndexes.rowGroups(), realizedStatsSelectors);
    }
    List<ParquetPageIndexEntry> selectedPageIndexes =
        selectPageIndexEntries(
            snapshot,
            indexColumns,
            columnSelectorPolicy,
            plannedFilePaths,
            pageIndexes.entries(),
            pageIndexes.rowGroups());
    return FileGroupCaptureResult.ofSelectedPageIndexes(
        stats, selectedPageIndexes, pageIndexes.rowGroups(), realizedStatsSelectors);
  }

  @Override
  public Optional<List<ParquetPageIndexEntry>> selectPageIndexEntries(
      String namespaceFq,
      String tableName,
      long snapshotId,
      Set<String> selectors,
      ColumnSelectorPolicy columnSelectorPolicy,
      Set<String> plannedFilePaths,
      List<ParquetPageIndexEntry> entries,
      List<ParquetRowGroup> rowGroups) {
    Snapshot snapshot =
        loadTable(storageLocation(namespaceFq, tableName))
            .getSnapshotAsOfVersion(engine, snapshotId);
    if (snapshot == null) {
      throw new IllegalArgumentException("Unknown Delta snapshot: " + snapshotId);
    }
    return Optional.of(
        selectPageIndexEntries(
            snapshot, selectors, columnSelectorPolicy, plannedFilePaths, entries, rowGroups));
  }

  private List<ParquetPageIndexEntry> selectPageIndexEntries(
      Snapshot snapshot,
      Set<String> selectors,
      ColumnSelectorPolicy columnSelectorPolicy,
      Set<String> plannedFilePaths,
      List<ParquetPageIndexEntry> entries,
      List<ParquetRowGroup> rowGroups) {
    var schemaDescriptor =
        new LogicalSchemaMapper()
            .mapRaw(
                ColumnIdAlgorithm.CID_PATH_ORDINAL,
                TableFormat.TF_DELTA,
                snapshotSchemaJson(snapshot),
                Set.of());
    Map<Long, ai.floedb.floecat.query.rpc.SchemaColumn> columnsById = new LinkedHashMap<>();
    Map<String, ai.floedb.floecat.query.rpc.SchemaColumn> columnsByPath = new LinkedHashMap<>();
    Map<String, List<ai.floedb.floecat.query.rpc.SchemaColumn>> columnsByName =
        new LinkedHashMap<>();
    for (var column : schemaDescriptor.getColumnsList()) {
      if (!column.getLeaf()) {
        continue;
      }
      columnsById.put(column.getId(), column);
      columnsByPath.put(column.getPhysicalPath(), column);
      columnsByName.computeIfAbsent(column.getName(), ignored -> new ArrayList<>()).add(column);
    }
    Map<String, SyntheticPageIndexColumn> syntheticColumnsByPath =
        deltaSyntheticPageIndexColumns(snapshot.getSchema());
    Map<Long, SyntheticPageIndexColumn> syntheticColumnsById = new LinkedHashMap<>();
    Map<String, ai.floedb.floecat.query.rpc.SchemaColumn> columnsByParquetPath =
        new LinkedHashMap<>();
    columnsById
        .values()
        .forEach(
            column -> {
              SyntheticPageIndexColumn synthetic =
                  syntheticColumnsByPath.get(column.getPhysicalPath());
              if (synthetic != null) {
                syntheticColumnsById.put(column.getId(), synthetic);
                columnsByParquetPath.put(synthetic.parquetPath(), column);
              }
            });
    Map<Integer, ai.floedb.floecat.query.rpc.SchemaColumn> columnsByFormatFieldId =
        new LinkedHashMap<>();
    columnsById.values().stream()
        .filter(column -> column.getFieldId() > 0)
        .forEach(column -> columnsByFormatFieldId.put(column.getFieldId(), column));
    List<ParquetPageIndexEntry> availableEntries = entries == null ? List.of() : entries;

    Set<String> effectiveSelectors =
        selectors == null || selectors.isEmpty()
            ? FloecatConnector.resolveIncludedColumns(
                schemaDescriptor.getColumnsList().stream()
                    .filter(ai.floedb.floecat.query.rpc.SchemaColumn::getLeaf)
                    .filter(column -> syntheticColumnsById.containsKey(column.getId()))
                    .map(ai.floedb.floecat.query.rpc.SchemaColumn::getPhysicalPath)
                    .toList(),
                Set.of(),
                columnSelectorPolicy)
            : selectors;
    if (effectiveSelectors.isEmpty()) {
      return List.of();
    }

    Map<Long, LinkedHashSet<String>> aliasesByColumnId = new LinkedHashMap<>();
    for (String selector : effectiveSelectors) {
      String normalized = selector == null ? "" : selector.trim();
      if (normalized.isBlank()) {
        continue;
      }
      ai.floedb.floecat.query.rpc.SchemaColumn selected;
      if (normalized.startsWith("#")) {
        final long columnId;
        try {
          columnId = Long.parseLong(normalized.substring(1));
        } catch (NumberFormatException error) {
          throw new IllegalArgumentException("Invalid Delta column selector: " + normalized, error);
        }
        selected = columnsById.get(columnId);
      } else {
        selected = columnsByPath.get(normalized);
        if (selected == null) {
          List<ai.floedb.floecat.query.rpc.SchemaColumn> named =
              columnsByName.getOrDefault(normalized, List.of());
          selected = named.size() == 1 ? named.getFirst() : null;
        }
      }
      if (selected == null) {
        throw new IllegalArgumentException("Unknown Delta column selector: " + normalized);
      }
      LinkedHashSet<String> aliases =
          aliasesByColumnId.computeIfAbsent(selected.getId(), ignored -> new LinkedHashSet<>());
      aliases.add("#" + selected.getId());
      aliases.add(selected.getPhysicalPath());
      aliases.add(normalized);
    }

    Map<String, Set<Long>> matchedByFile = new LinkedHashMap<>();
    List<ParquetPageIndexEntry> selectedEntries = new ArrayList<>();
    Map<Long, ParquetPageIndexEntry> templateByColumnId = new LinkedHashMap<>();
    for (ParquetPageIndexEntry entry : availableEntries) {
      if (entry == null) {
        continue;
      }
      ai.floedb.floecat.query.rpc.SchemaColumn column =
          deltaColumnForEntry(entry, columnsByFormatFieldId, columnsByParquetPath);
      if (column == null || !aliasesByColumnId.containsKey(column.getId())) {
        continue;
      }
      matchedByFile
          .computeIfAbsent(entry.filePath(), ignored -> new LinkedHashSet<>())
          .add(column.getId());
      LinkedHashSet<String> aliases = new LinkedHashSet<>();
      aliases.addAll(aliasesByColumnId.get(column.getId()));
      selectedEntries.add(entry.withSelectorAliases(aliases));
      templateByColumnId.putIfAbsent(column.getId(), entry);
    }
    Map<String, List<ParquetPageIndexEntry>> entriesByFile = new LinkedHashMap<>();
    if (plannedFilePaths != null) {
      plannedFilePaths.stream()
          .filter(path -> path != null && !path.isBlank())
          .forEach(path -> entriesByFile.put(path, new ArrayList<>()));
    }
    for (ParquetPageIndexEntry entry : availableEntries) {
      if (entry != null) {
        entriesByFile.computeIfAbsent(entry.filePath(), ignored -> new ArrayList<>()).add(entry);
      }
    }
    Map<String, List<ParquetRowGroup>> rowGroupsByFile = new LinkedHashMap<>();
    if (rowGroups != null) {
      rowGroups.stream()
          .filter(rowGroup -> rowGroup != null && !rowGroup.filePath().isBlank())
          .forEach(
              rowGroup ->
                  rowGroupsByFile
                      .computeIfAbsent(rowGroup.filePath(), ignored -> new ArrayList<>())
                      .add(rowGroup));
    }
    entriesByFile.forEach(
        (filePath, fileEntries) -> {
          Set<Long> matched =
              matchedByFile.computeIfAbsent(filePath, ignored -> new LinkedHashSet<>());
          for (long columnId : aliasesByColumnId.keySet()) {
            if (matched.contains(columnId)) {
              continue;
            }
            var column = columnsById.get(columnId);
            ParquetPageIndexEntry template = templateByColumnId.get(columnId);
            SyntheticPageIndexColumn syntheticColumn = syntheticColumnsById.get(columnId);
            if (syntheticColumn == null) {
              throw new IllegalArgumentException(
                  "Delta selector has no supported page-index representation: "
                      + aliasesByColumnId.get(columnId));
            }
            selectedEntries.addAll(
                syntheticAllNullPageIndexEntries(
                    filePath,
                    fileEntries,
                    rowGroupsByFile.getOrDefault(filePath, List.of()),
                    column,
                    template,
                    syntheticColumn,
                    aliasesByColumnId.get(columnId)));
            matched.add(columnId);
          }
        });
    matchedByFile.forEach(
        (filePath, matched) -> {
          if (!matched.containsAll(aliasesByColumnId.keySet())) {
            throw new IllegalArgumentException(
                "Delta page indexes for "
                    + filePath
                    + " do not cover selectors "
                    + effectiveSelectors);
          }
        });
    return List.copyOf(selectedEntries);
  }

  private static ai.floedb.floecat.query.rpc.SchemaColumn deltaColumnForEntry(
      ParquetPageIndexEntry entry,
      Map<Integer, ai.floedb.floecat.query.rpc.SchemaColumn> columnsByFormatFieldId,
      Map<String, ai.floedb.floecat.query.rpc.SchemaColumn> columnsByParquetPath) {
    if (entry == null) {
      return null;
    }
    ai.floedb.floecat.query.rpc.SchemaColumn column =
        entry.parquetFieldId() == null ? null : columnsByFormatFieldId.get(entry.parquetFieldId());
    return column == null
        ? columnsByParquetPath.get(canonicalDeltaPagePath(entry.columnName()))
        : column;
  }

  private static List<ParquetPageIndexEntry> syntheticAllNullPageIndexEntries(
      String filePath,
      List<ParquetPageIndexEntry> fileEntries,
      List<ParquetRowGroup> fileRowGroups,
      ai.floedb.floecat.query.rpc.SchemaColumn column,
      ParquetPageIndexEntry template,
      SyntheticPageIndexColumn syntheticColumn,
      Set<String> aliases) {
    Map<Integer, Integer> rowsByGroup = new LinkedHashMap<>();
    if (fileRowGroups != null) {
      fileRowGroups.forEach(
          rowGroup -> rowsByGroup.merge(rowGroup.rowGroup(), rowGroup.rowCount(), Math::max));
    }
    for (ParquetPageIndexEntry entry : fileEntries) {
      long rowGroupEnd = entry.firstRowIndex() + entry.rowCount();
      rowsByGroup.merge(
          entry.rowGroup(),
          rowGroupEnd >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) Math.max(0L, rowGroupEnd),
          Math::max);
    }
    if (rowsByGroup.isEmpty()) {
      rowsByGroup.put(0, 0);
    }
    String parquetPhysicalType =
        template == null ? syntheticColumn.parquetPhysicalType() : template.parquetPhysicalType();
    String parquetCompression = template == null ? "UNCOMPRESSED" : template.parquetCompression();
    short maxDefinitionLevel =
        template == null ? syntheticColumn.maxDefinitionLevel() : template.parquetMaxDefLevel();
    short maxRepetitionLevel = template == null ? (short) 0 : template.parquetMaxRepLevel();
    Integer precision =
        template == null ? syntheticColumn.decimalPrecision() : template.decimalPrecision();
    Integer scale = template == null ? syntheticColumn.decimalScale() : template.decimalScale();
    Integer bits = template == null ? syntheticColumn.decimalBits() : template.decimalBits();
    Integer fieldId = column.getFieldId() > 0 ? column.getFieldId() : null;
    List<ParquetPageIndexEntry> synthetic = new ArrayList<>();
    rowsByGroup.forEach(
        (rowGroup, rowCount) ->
            synthetic.add(
                new ParquetPageIndexEntry(
                        filePath,
                        syntheticColumn.parquetPath(),
                        rowGroup,
                        0,
                        0L,
                        rowCount,
                        rowCount,
                        null,
                        0,
                        null,
                        null,
                        false,
                        parquetPhysicalType,
                        parquetCompression,
                        maxDefinitionLevel,
                        maxRepetitionLevel,
                        precision,
                        scale,
                        bits)
                    .withParquetFieldId(fieldId)
                    .withSelectorAliases(aliases)));
    return List.copyOf(synthetic);
  }

  private static Map<String, SyntheticPageIndexColumn> deltaSyntheticPageIndexColumns(
      StructType schema) {
    Map<String, SyntheticPageIndexColumn> columns = new LinkedHashMap<>();
    collectDeltaSyntheticPageIndexColumns(schema, "", "", 0, columns);
    return Map.copyOf(columns);
  }

  private static void collectDeltaSyntheticPageIndexColumns(
      StructType schema,
      String logicalPrefix,
      String parquetPrefix,
      int inheritedDefinitionLevel,
      Map<String, SyntheticPageIndexColumn> columns) {
    for (StructField field : schema.fields()) {
      String logicalPath =
          logicalPrefix.isEmpty() ? field.getName() : logicalPrefix + "." + field.getName();
      String physicalName = DeltaPlanner.physicalName(field.getMetadata());
      if (physicalName == null || physicalName.isBlank()) {
        physicalName = field.getName();
      }
      String parquetPath =
          parquetPrefix.isEmpty() ? physicalName : parquetPrefix + "." + physicalName;
      int definitionLevel = inheritedDefinitionLevel + (field.isNullable() ? 1 : 0);
      DataType dataType = field.getDataType();
      if (dataType instanceof StructType structType) {
        collectDeltaSyntheticPageIndexColumns(
            structType, logicalPath, parquetPath, definitionLevel, columns);
      } else if (!(dataType instanceof ArrayType) && !(dataType instanceof MapType)) {
        SyntheticPageIndexColumn column =
            syntheticDeltaPrimitive(parquetPath, definitionLevel, dataType);
        if (column != null) {
          columns.put(logicalPath, column);
        }
      }
    }
  }

  private static SyntheticPageIndexColumn syntheticDeltaPrimitive(
      String parquetPath, int definitionLevel, DataType dataType) {
    String physicalType;
    Integer precision = null;
    Integer scale = null;
    Integer bits = null;
    if (dataType instanceof BooleanType) {
      physicalType = "BOOLEAN";
    } else if (dataType instanceof ByteType
        || dataType instanceof ShortType
        || dataType instanceof IntegerType
        || dataType instanceof DateType) {
      physicalType = "INT32";
    } else if (dataType instanceof LongType
        || dataType instanceof TimestampType
        || dataType instanceof TimestampNTZType) {
      physicalType = "INT64";
    } else if (dataType instanceof FloatType) {
      physicalType = "FLOAT";
    } else if (dataType instanceof DoubleType) {
      physicalType = "DOUBLE";
    } else if (dataType instanceof StringType) {
      physicalType = "BINARY";
    } else if (dataType instanceof DecimalType decimal) {
      precision = decimal.getPrecision();
      scale = decimal.getScale();
      bits = precision <= 9 ? 32 : precision <= 18 ? 64 : precision <= 38 ? 128 : 256;
      physicalType = precision <= 9 ? "INT32" : precision <= 18 ? "INT64" : "FIXED_LEN_BYTE_ARRAY";
    } else {
      return null;
    }
    return new SyntheticPageIndexColumn(
        parquetPath,
        physicalType,
        (short) Math.min(Short.MAX_VALUE, definitionLevel),
        precision,
        scale,
        bits);
  }

  private record SyntheticPageIndexColumn(
      String parquetPath,
      String parquetPhysicalType,
      short maxDefinitionLevel,
      Integer decimalPrecision,
      Integer decimalScale,
      Integer decimalBits) {}

  private static String canonicalDeltaPagePath(String parquetPath) {
    if (parquetPath == null || parquetPath.isBlank()) {
      return "";
    }
    return parquetPath
        .trim()
        .replace(".list.element", "[]")
        .replace(".key_value.key", ".key")
        .replace(".key_value.value", "{}");
  }

  @Override
  public Optional<SnapshotFilePlan> planSnapshotFiles(
      String namespaceFq, String tableName, ResourceId destinationTableId, long snapshotId) {
    if (snapshotId < 0) {
      return Optional.empty();
    }
    final String storageLocation = storageLocation(namespaceFq, tableName);
    final Table table = loadTable(storageLocation);
    if (table.getSnapshotAsOfVersion(engine, snapshotId) == null) {
      return Optional.empty();
    }
    try (var planner =
        new DeltaPlanner(
            engine,
            parquetInput,
            storageLocation,
            snapshotId,
            Set.of(),
            Set.of(),
            null,
            null,
            false,
            true)) {
      List<SnapshotFileEntry> dataFiles = new ArrayList<>();
      for (PlannedFile<String> planned : planner) {
        dataFiles.add(toDataScanFile(planned, planner.deletionVectorForFile(planned.path())));
      }
      return Optional.of(
          new SnapshotFilePlan(
              List.copyOf(dataFiles),
              List.of(),
              DataTypeJsonSerDe.serializeStructType(planner.schema())));
    }
  }

  @Override
  public Optional<SnapshotFileDelta> planSnapshotFileDelta(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      long baseSnapshotId,
      long targetSnapshotId) {
    if (baseSnapshotId < 0 || targetSnapshotId <= baseSnapshotId) {
      return Optional.empty();
    }
    String storageLocation = storageLocation(namespaceFq, tableName);
    long startedNanos = System.nanoTime();
    Table table = loadTable(storageLocation);
    Snapshot target = table.getSnapshotAsOfVersion(engine, targetSnapshotId);
    if (!(table instanceof TableImpl) || target == null) {
      return Optional.empty();
    }

    List<FileStatus> commitFiles;
    long listingStartedNanos = System.nanoTime();
    try {
      commitFiles =
          DeltaLogActionUtils.getCommitFilesForVersionRange(
              engine, new Path(storageLocation), baseSnapshotId + 1, Optional.of(targetSnapshotId));
    } catch (Exception e) {
      throw new RuntimeException(
          "Delta change planning failed (versions "
              + baseSnapshotId
              + ".."
              + targetSnapshotId
              + ")",
          e);
    }
    long listedNanos = System.nanoTime();

    List<DeltaChangeChunk> chunks;
    int readerCount = deltaChangeReaderCount(commitFiles.size());
    AtomicBoolean nonAppend = new AtomicBoolean();
    try {
      chunks = readDeltaChangeChunks(storageLocation, commitFiles, readerCount, nonAppend);
    } catch (Exception e) {
      throw new RuntimeException(
          "Delta change planning failed (versions "
              + baseSnapshotId
              + ".."
              + targetSnapshotId
              + ")",
          e);
    }
    long completedNanos = System.nanoTime();

    // A single non-append change anywhere in the range disqualifies the whole range, so the reuse
    // planner only ever needs the append-only verdict. Report it by declining the delta outright,
    // matching the Iceberg connector, instead of returning partly populated removal fields.
    LinkedHashMap<String, SnapshotFileEntry> additions = new LinkedHashMap<>();
    for (DeltaChangeChunk chunk : chunks) {
      if (chunk.nonAppend()) {
        return Optional.empty();
      }
      chunk.additions().forEach(additions::put);
    }
    LOG.infof(
        "Delta change planning timing versions=%d..%d commits=%d readers=%d"
            + " snapshotMs=%d listMs=%d readMs=%d totalMs=%d",
        baseSnapshotId + 1,
        targetSnapshotId,
        commitFiles.size(),
        readerCount,
        TimeUnit.NANOSECONDS.toMillis(listingStartedNanos - startedNanos),
        TimeUnit.NANOSECONDS.toMillis(listedNanos - listingStartedNanos),
        TimeUnit.NANOSECONDS.toMillis(completedNanos - listedNanos),
        TimeUnit.NANOSECONDS.toMillis(completedNanos - startedNanos));

    return Optional.of(
        new SnapshotFileDelta(
            List.copyOf(additions.values()),
            List.of(),
            false,
            DataTypeJsonSerDe.serializeStructType(target.getSchema())));
  }

  private List<DeltaChangeChunk> readDeltaChangeChunks(
      String storageLocation,
      List<FileStatus> commitFiles,
      int readerCount,
      AtomicBoolean nonAppend)
      throws Exception {
    List<List<FileStatus>> partitions = partitionCommitFiles(commitFiles, readerCount);
    if (partitions.size() == 1) {
      return List.of(readDeltaChangeChunk(storageLocation, partitions.getFirst(), nonAppend));
    }
    List<Callable<DeltaChangeChunk>> readers =
        partitions.stream()
            .<Callable<DeltaChangeChunk>>map(
                partition -> () -> readDeltaChangeChunk(storageLocation, partition, nonAppend))
            .toList();
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      List<Future<DeltaChangeChunk>> futures;
      try {
        futures = executor.invokeAll(readers);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw e;
      }
      List<DeltaChangeChunk> chunks = new ArrayList<>(futures.size());
      for (Future<DeltaChangeChunk> future : futures) {
        try {
          chunks.add(future.get());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw e;
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          if (cause instanceof Exception exception) {
            throw exception;
          }
          if (cause instanceof Error error) {
            throw error;
          }
          throw new RuntimeException(cause);
        }
      }
      return List.copyOf(chunks);
    }
  }

  /**
   * Reads one contiguous slice of the commit range in a single pass.
   *
   * <p>Each batch is disqualified before anything is materialized from it. Remove actions are found
   * with a columnar null scan over the {@code remove} vector, so a range that a compaction or
   * delete has touched costs no {@link SnapshotFileEntry} allocations at all, no matter whether the
   * commit log happens to list its add actions before its remove actions. Deletion vectors are only
   * visible through the {@code add} struct, so they are checked per row immediately before that
   * row's entry is built; the most that can be built and discarded is one batch.
   */
  private DeltaChangeChunk readDeltaChangeChunk(
      String storageLocation, List<FileStatus> commitFiles, AtomicBoolean nonAppend)
      throws Exception {
    LinkedHashMap<String, SnapshotFileEntry> additions = new LinkedHashMap<>();
    try (var commits =
            DeltaLogActionUtils.getActionsFromCommitFilesWithProtocolValidation(
                engine, storageLocation, commitFiles, Set.of(DeltaAction.ADD, DeltaAction.REMOVE));
        CloseableIterator<io.delta.kernel.data.ColumnarBatch> batches =
            TableChangesUtils.flattenCommitsAndAddMetadata(engine, commits)) {
      while (batches.hasNext()) {
        if (nonAppend.get()) {
          return DeltaChangeChunk.nonAppendChunk();
        }
        var batch = batches.next();
        int addOrdinal = batch.getSchema().indexOf("add");
        int removeOrdinal = batch.getSchema().indexOf("remove");
        if (removeOrdinal >= 0
            && hasNonNull(batch.getColumnVector(removeOrdinal), batch.getSize())) {
          nonAppend.set(true);
          return DeltaChangeChunk.nonAppendChunk();
        }
        if (addOrdinal < 0) {
          continue;
        }
        try (var rows = batch.getRows()) {
          while (rows.hasNext()) {
            if (nonAppend.get()) {
              return DeltaChangeChunk.nonAppendChunk();
            }
            var row = rows.next();
            if (row.isNullAt(addOrdinal)) {
              continue;
            }
            AddFile add = new AddFile(row.getStruct(addOrdinal));
            if (add.getDeletionVector().isPresent()) {
              nonAppend.set(true);
              return DeltaChangeChunk.nonAppendChunk();
            }
            String path = DeltaPlanner.absoluteDataPath(storageLocation, add.getPath());
            additions.put(path, toSnapshotDataFile(path, add, null));
          }
        }
      }
    }
    return new DeltaChangeChunk(additions, false);
  }

  /** Scans a batch's null bitmap for a present action without materializing any row. */
  static boolean hasNonNull(ColumnVector vector, int size) {
    for (int index = 0; index < size; index++) {
      if (!vector.isNullAt(index)) {
        return true;
      }
    }
    return false;
  }

  static int deltaChangeReaderCount(int commitCount) {
    if (commitCount <= 0) {
      return 1;
    }
    return Math.min(
        MAX_DELTA_CHANGE_READ_PARALLELISM,
        Math.max(1, Math.ceilDiv(commitCount, TARGET_COMMITS_PER_CHANGE_READER)));
  }

  static List<List<FileStatus>> partitionCommitFiles(
      List<FileStatus> commitFiles, int readerCount) {
    if (commitFiles.isEmpty()) {
      return List.of(List.of());
    }
    int partitionSize = Math.ceilDiv(commitFiles.size(), Math.max(1, readerCount));
    List<List<FileStatus>> partitions = new ArrayList<>();
    for (int offset = 0; offset < commitFiles.size(); offset += partitionSize) {
      partitions.add(
          List.copyOf(
              commitFiles.subList(offset, Math.min(commitFiles.size(), offset + partitionSize))));
    }
    return List.copyOf(partitions);
  }

  /**
   * One reader's slice of the change range: either its additions, or the fact that the range is not
   * append-only. Which change disqualified the range is deliberately not carried, because the reuse
   * planner needs only the verdict and a partial removal list would invite false trust.
   */
  record DeltaChangeChunk(Map<String, SnapshotFileEntry> additions, boolean nonAppend) {
    static DeltaChangeChunk nonAppendChunk() {
      return new DeltaChangeChunk(Map.of(), true);
    }
  }

  private static SnapshotFileEntry toSnapshotDataFile(
      String absolutePath, AddFile add, DeletionVectorDescriptor deletionVector) {
    long rowCount = add.getNumRecords().orElse(0L);
    return new SnapshotFileEntry(
        absolutePath,
        "PARQUET",
        add.getSize(),
        rowCount,
        FileContent.FC_DATA,
        DeltaPlanner.encodePartition(add),
        0,
        List.of(),
        null,
        deletionVector == null
            ? null
            : new SnapshotDeletionVector(
                deletionVector.getStorageType(),
                deletionVector.getPathOrInlineDv(),
                deletionVector.getOffset().orElse(null),
                deletionVector.getSizeInBytes(),
                deletionVector.getCardinality()),
        List.of(),
        DeltaPlanner.contentIdentity(
            add.getModificationTime(),
            add.getBaseRowId().orElse(null),
            add.getDefaultRowCommitVersion().orElse(null)));
  }

  @Override
  public void close() {
    if (engineResources == null) {
      return;
    }
    try {
      engineResources.close();
    } catch (Exception e) {
      LOG.warnf(e, "Failed to close engine resources for connector %s", connectorId);
    }
  }

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
    return toDataScanFile(planned, null);
  }

  private static SnapshotFileEntry toDataScanFile(
      PlannedFile<String> planned, DeletionVectorDescriptor deletionVector) {
    return new SnapshotFileEntry(
        planned.path(),
        planned.format(),
        planned.sizeBytes(),
        planned.rowCount(),
        FileContent.FC_DATA,
        planned.partitionDataJson(),
        planned.partitionSpecId(),
        List.of(),
        planned.sequenceNumber(),
        deletionVector == null
            ? null
            : new SnapshotDeletionVector(
                deletionVector.getStorageType(),
                deletionVector.getPathOrInlineDv(),
                deletionVector.getOffset().orElse(null),
                deletionVector.getSizeInBytes(),
                deletionVector.getCardinality()),
        List.of(),
        planned.contentIdentity());
  }

  protected Table loadTable(String storageLocation) {
    return Table.forPath(engine, storageLocation);
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

  protected String describeTableSchemaJson(String storageLocation) {
    Table table = loadTable(storageLocation);
    Snapshot snapshot = table.getLatestSnapshot(engine);
    if (snapshot == null) {
      throw new IllegalStateException("Delta table has no latest snapshot at " + storageLocation);
    }
    return snapshotSchemaJson(snapshot);
  }

  protected TableDescriptor describeFromDelta(
      String storageLocation, String namespaceFq, String tableName) {
    try {
      Map<String, String> props = new LinkedHashMap<>();
      props.put("data_source_format", "DELTA");
      props.put("storage_location", storageLocation);

      return new TableDescriptor(
          namespaceFq,
          tableName,
          storageLocation,
          describeTableSchemaJson(storageLocation),
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
      String storageLocation,
      long version,
      Set<String> includeNames,
      Set<String> plannedFilePaths,
      Map<String, LogicalType> nameToType,
      boolean allowFooterFallback) {

    NdvProvider bootstrap = null;

    NdvProvider ndvProvider = null;
    ParquetAvgWidthProvider avgWidthProvider = new ParquetAvgWidthProvider(parquetInput);

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
            storageLocation,
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
          new GenericStatsEngine<>(
              planner, ndvProvider, bootstrap, avgWidthProvider, columnNames, logicalTypes);

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
        int keep = Math.max(0, latestN);
        if (targetSnapshotIds != null && !targetSnapshotIds.isEmpty()) {
          List<Long> eligibleTargets =
              targetSnapshotIds.stream()
                  .filter(version -> version != null && version >= 0L && version <= latestVersion)
                  .sorted()
                  .toList();
          int from = Math.max(0, eligibleTargets.size() - keep);
          candidates.addAll(eligibleTargets.subList(from, eligibleTargets.size()));
        } else {
          long start = Math.max(0L, latestVersion - keep + 1L);
          for (long version = start; version <= latestVersion; version++) {
            candidates.add(version);
          }
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

  private SnapshotLoadResult loadSnapshotAsOfVersion(
      Table table, long version, String storageLocation, long currentEarliestAvailableVersion) {
    try {
      return SnapshotLoadResult.snapshot(table.getSnapshotAsOfVersion(engine, version));
    } catch (KernelException e) {
      OptionalLongMatch earliest = parseEarliestAvailableVersion(e);
      if (earliest.present() && version < earliest.value()) {
        LOG.debugf(
            "Skipping Delta snapshot version %d for %s because retained history starts at %d",
            Long.valueOf(version), storageLocation, Long.valueOf(earliest.value()));
        return SnapshotLoadResult.skipUntil(earliest.value());
      }
      if (version < currentEarliestAvailableVersion) {
        return SnapshotLoadResult.skipUntil(currentEarliestAvailableVersion);
      }
      throw e;
    }
  }

  static OptionalLongMatch parseEarliestAvailableVersion(Throwable error) {
    if (error == null || error.getMessage() == null) {
      return OptionalLongMatch.empty();
    }
    Matcher matcher = EARLIEST_AVAILABLE_VERSION_PATTERN.matcher(error.getMessage());
    if (!matcher.find()) {
      return OptionalLongMatch.empty();
    }
    try {
      return OptionalLongMatch.of(Long.parseLong(matcher.group(1)));
    } catch (NumberFormatException ignored) {
      return OptionalLongMatch.empty();
    }
  }

  static final class OptionalLongMatch {
    private static final OptionalLongMatch EMPTY = new OptionalLongMatch(false, 0L);

    private final boolean present;
    private final long value;

    private OptionalLongMatch(boolean present, long value) {
      this.present = present;
      this.value = value;
    }

    static OptionalLongMatch empty() {
      return EMPTY;
    }

    static OptionalLongMatch of(long value) {
      return new OptionalLongMatch(true, value);
    }

    boolean present() {
      return present;
    }

    long value() {
      return value;
    }
  }

  static final class SnapshotLoadResult {
    private final Snapshot snapshot;
    private final long earliestAvailableVersion;

    private SnapshotLoadResult(Snapshot snapshot, long earliestAvailableVersion) {
      this.snapshot = snapshot;
      this.earliestAvailableVersion = earliestAvailableVersion;
    }

    static SnapshotLoadResult snapshot(Snapshot snapshot) {
      return new SnapshotLoadResult(snapshot, 0L);
    }

    static SnapshotLoadResult skipUntil(long earliestAvailableVersion) {
      return new SnapshotLoadResult(null, earliestAvailableVersion);
    }

    Snapshot snapshot() {
      return snapshot;
    }

    long earliestAvailableVersion() {
      return earliestAvailableVersion;
    }
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

  private SnapshotBundle buildSnapshotBundle(
      String storageLocation, long version, Snapshot snapshot) {
    final long createdMs = snapshot.getTimestamp(engine);
    final long parent = version > 0L ? version - 1L : -1L;

    final StructType kernelSchema = snapshot.getSchema();
    final String schemaJson = snapshotSchemaJson(snapshot);
    final PartitionSpecInfo partitionSpec = toPartitionSpecInfo(snapshot);
    return new SnapshotBundle(
        version, parent, createdMs, schemaJson, partitionSpec, 0L, null, Map.of(), 0, null);
  }

  private List<TargetStatsRecord> buildTargetStats(
      String storageLocation,
      ResourceId destinationTableId,
      Set<String> includeColumns,
      ColumnSelectorPolicy columnSelectorPolicy,
      long version,
      Snapshot snapshot,
      Set<StatsTargetKind> includeTargetKinds,
      Set<String> plannedFilePaths) {
    return buildTargetStats(
        storageLocation,
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
      String storageLocation,
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
            storageLocation,
            version,
            includeNames,
            plannedFilePaths,
            nameToType,
            allowFooterFallback);
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
