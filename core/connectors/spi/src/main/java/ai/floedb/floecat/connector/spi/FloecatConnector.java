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

package ai.floedb.floecat.connector.spi;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.FileContent;
import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.execution.rpc.ScanFile;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface FloecatConnector extends Closeable {
  enum SnapshotSelectionKind {
    ALL,
    CURRENT,
    LATEST_N,
    EXPLICIT
  }

  String id();

  ConnectorFormat format();

  List<String> listNamespaces();

  List<String> listTables(String namespaceFq);

  TableDescriptor describe(String namespaceFq, String tableName);

  List<SnapshotBundle> enumerateSnapshots(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      SnapshotEnumerationOptions options);

  default List<SnapshotBundle> enumerateSnapshots(
      String namespaceFq, String tableName, ResourceId destinationTableId, boolean fullRescan) {
    return enumerateSnapshots(
        namespaceFq, tableName, destinationTableId, SnapshotEnumerationOptions.full(fullRescan));
  }

  /**
   * Plans table-scoped reconcile work for this connector.
   *
   * <p>Default behavior derives tasks from {@link #listTables(String)} and applies an optional
   * source table filter. Connectors can override this when they need more specialized planning.
   */
  default List<PlannedTableTask> planTableTasks(TablePlanningRequest request) {
    if (request == null
        || request.sourceNamespaceFq() == null
        || request.sourceNamespaceFq().isBlank()) {
      return List.of();
    }

    List<String> sourceTables = listTables(request.sourceNamespaceFq());
    if (sourceTables == null || sourceTables.isEmpty()) {
      return List.of();
    }

    String requestedSourceTable = request.sourceTable() == null ? "" : request.sourceTable().trim();
    String requestedDestinationTable =
        request.destinationTableDisplayName() == null
            ? ""
            : request.destinationTableDisplayName().trim();
    String destinationTableDisplayHint =
        request.destinationTableDisplayHint() == null
            ? ""
            : request.destinationTableDisplayHint().trim();

    return sourceTables.stream()
        .filter(table -> requestedSourceTable.isBlank() || requestedSourceTable.equals(table))
        .filter(
            table -> {
              String destinationTableDisplay =
                  destinationTableDisplayHint.isBlank() ? table : destinationTableDisplayHint;
              return requestedDestinationTable.isBlank()
                  || requestedDestinationTable.equals(destinationTableDisplay);
            })
        .map(
            table ->
                new PlannedTableTask(
                    request.sourceNamespaceFq(),
                    table,
                    destinationTableDisplayHint.isBlank() ? table : destinationTableDisplayHint))
        .toList();
  }

  /**
   * Plans view-scoped reconcile work for this connector.
   *
   * <p>Default behavior derives tasks from {@link #listViewDescriptors(String)} after applying the
   * destination namespace scope. Views are intentionally not filtered by destination table scope.
   *
   * <p>Connectors that surface a view through this planning path must also support {@link
   * #describeView(String, String)} for the same source identity unless the view has been removed
   * concurrently.
   */
  default List<PlannedViewTask> planViewTasks(ViewPlanningRequest request) {
    if (request == null
        || request.sourceNamespaceFq() == null
        || request.sourceNamespaceFq().isBlank()) {
      return List.of();
    }

    String destinationNamespaceFq =
        request.destinationNamespaceFq() == null ? "" : request.destinationNamespaceFq().trim();
    if (!request.destinationNamespacePaths().isEmpty()
        && (destinationNamespaceFq.isBlank()
            || request.destinationNamespacePaths().stream()
                .map(path -> String.join(".", path))
                .noneMatch(destinationNamespaceFq::equals))) {
      return List.of();
    }

    return listViewDescriptors(request.sourceNamespaceFq()).stream()
        .map(
            view ->
                new PlannedViewTask(
                    request.sourceNamespaceFq(),
                    view.name(),
                    destinationNamespaceFq.isBlank() ? view.namespaceFq() : destinationNamespaceFq,
                    view.name()))
        .toList();
  }

  /**
   * Captures target stats for one snapshot and optional selector scope.
   *
   * <p>Selector semantics are best-effort and connector-native:
   *
   * <ul>
   *   <li>`#<id>` selectors target stable destination column ids when available.
   *   <li>name/path selectors are interpreted as connector-specific display or physical paths.
   *   <li>unknown selectors are ignored; connectors may return a strict subset of requested
   *       targets.
   * </ul>
   */
  List<TargetStatsRecord> captureSnapshotTargetStats(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      long snapshotId,
      Set<String> includeColumns);

  /**
   * Captures target stats for one snapshot and optional selector scope with explicit target-kind
   * hints.
   *
   * <p>Default behavior delegates to {@link #captureSnapshotTargetStats(String, String, ResourceId,
   * long, Set)} and ignores {@code includeTargetKinds}. Connectors may override this to skip
   * unnecessary work (for example file-level planning when only column targets are requested).
   *
   * <p>When {@link StatsTargetKind#TABLE} is included, callers may also include {@link
   * StatsTargetKind#COLUMN} and {@link StatsTargetKind#FILE} so full-bundle capture and persistence
   * can happen in one pass. Connectors may return any subset of the requested kinds.
   */
  default List<TargetStatsRecord> captureSnapshotTargetStats(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      long snapshotId,
      Set<String> includeColumns,
      Set<StatsTargetKind> includeTargetKinds) {
    return captureSnapshotTargetStats(
        namespaceFq, tableName, destinationTableId, snapshotId, includeColumns);
  }

  /** Captures requested outputs for one planned file-group within a snapshot. */
  FileGroupCaptureResult capturePlannedFileGroup(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      long snapshotId,
      Set<String> plannedFilePaths,
      Set<String> includeColumns,
      Set<StatsTargetKind> includeTargetKinds,
      boolean captureIndexes);

  record FileGroupCaptureResult(
      List<TargetStatsRecord> statsRecords, List<ParquetPageIndexEntry> pageIndexEntries) {
    public FileGroupCaptureResult {
      statsRecords = statsRecords == null ? List.of() : List.copyOf(statsRecords);
      pageIndexEntries = pageIndexEntries == null ? List.of() : List.copyOf(pageIndexEntries);
    }

    public static FileGroupCaptureResult of(
        List<TargetStatsRecord> statsRecords, List<ParquetPageIndexEntry> pageIndexEntries) {
      return new FileGroupCaptureResult(statsRecords, pageIndexEntries);
    }

    public static FileGroupCaptureResult empty() {
      return new FileGroupCaptureResult(List.of(), List.of());
    }
  }

  enum StatsTargetKind {
    TABLE,
    COLUMN,
    FILE,
    EXPRESSION
  }

  /**
   * Returns per-snapshot constraints for a table snapshot, if available.
   *
   * <p>Default: no constraints emitted.
   */
  default Optional<SnapshotConstraints> snapshotConstraints(
      String namespaceFq, String tableName, ResourceId destinationTableId, long snapshotId) {
    return Optional.empty();
  }

  /**
   * Returns per-snapshot constraints for the provided snapshot bundle, if available.
   *
   * <p>Default: delegates to {@link #snapshotConstraints(String, String, ResourceId, long)}.
   */
  default Optional<SnapshotConstraints> snapshotConstraints(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      SnapshotBundle snapshotBundle) {
    if (snapshotBundle == null || snapshotBundle.snapshotId() < 0) {
      return Optional.empty();
    }
    return snapshotConstraints(
        namespaceFq, tableName, destinationTableId, snapshotBundle.snapshotId());
  }

  /**
   * Plans snapshot file membership for reconcile/execution planning.
   *
   * <p>This is intentionally separate from query scan bundles. Connectors may return {@link
   * Optional#empty()} when they cannot expose snapshot file membership directly.
   */
  default Optional<SnapshotFilePlan> planSnapshotFiles(
      String namespaceFq, String tableName, ResourceId destinationTableId, long snapshotId) {
    return Optional.empty();
  }

  record SnapshotEnumerationOptions(
      boolean fullRescan,
      Set<Long> knownSnapshotIds,
      Set<Long> targetSnapshotIds,
      SnapshotSelectionKind selectionKind,
      Set<Long> selectionSnapshotIds,
      int latestN) {
    public SnapshotEnumerationOptions {
      knownSnapshotIds =
          knownSnapshotIds == null ? Set.of() : Set.copyOf(new LinkedHashSet<>(knownSnapshotIds));
      targetSnapshotIds =
          targetSnapshotIds == null ? Set.of() : Set.copyOf(new LinkedHashSet<>(targetSnapshotIds));
      selectionKind = selectionKind == null ? SnapshotSelectionKind.ALL : selectionKind;
      selectionSnapshotIds =
          selectionSnapshotIds == null
              ? Set.of()
              : Set.copyOf(new LinkedHashSet<>(selectionSnapshotIds));
      latestN = Math.max(0, latestN);
    }

    public static SnapshotEnumerationOptions full(boolean fullRescan) {
      return new SnapshotEnumerationOptions(
          fullRescan, Set.of(), Set.of(), SnapshotSelectionKind.ALL, Set.of(), 0);
    }

    public static SnapshotEnumerationOptions full(boolean fullRescan, Set<Long> targetSnapshotIds) {
      return new SnapshotEnumerationOptions(
          fullRescan, Set.of(), targetSnapshotIds, SnapshotSelectionKind.ALL, Set.of(), 0);
    }

    public static SnapshotEnumerationOptions incremental(Set<Long> knownSnapshotIds) {
      return new SnapshotEnumerationOptions(
          false, knownSnapshotIds, Set.of(), SnapshotSelectionKind.ALL, Set.of(), 0);
    }

    public static SnapshotEnumerationOptions incremental(
        Set<Long> knownSnapshotIds, Set<Long> targetSnapshotIds) {
      return new SnapshotEnumerationOptions(
          false, knownSnapshotIds, targetSnapshotIds, SnapshotSelectionKind.ALL, Set.of(), 0);
    }

    public static SnapshotEnumerationOptions fullCurrent(boolean fullRescan) {
      return new SnapshotEnumerationOptions(
          fullRescan, Set.of(), Set.of(), SnapshotSelectionKind.CURRENT, Set.of(), 0);
    }

    public static SnapshotEnumerationOptions incrementalCurrent(Set<Long> knownSnapshotIds) {
      return new SnapshotEnumerationOptions(
          false, knownSnapshotIds, Set.of(), SnapshotSelectionKind.CURRENT, Set.of(), 0);
    }

    public static SnapshotEnumerationOptions fullLatestN(boolean fullRescan, int latestN) {
      return new SnapshotEnumerationOptions(
          fullRescan, Set.of(), Set.of(), SnapshotSelectionKind.LATEST_N, Set.of(), latestN);
    }

    public static SnapshotEnumerationOptions incrementalLatestN(
        Set<Long> knownSnapshotIds, int latestN) {
      return new SnapshotEnumerationOptions(
          false, knownSnapshotIds, Set.of(), SnapshotSelectionKind.LATEST_N, Set.of(), latestN);
    }

    public static SnapshotEnumerationOptions fullExplicit(
        boolean fullRescan, Set<Long> snapshotIds) {
      return new SnapshotEnumerationOptions(
          fullRescan, Set.of(), Set.of(), SnapshotSelectionKind.EXPLICIT, snapshotIds, 0);
    }

    public static SnapshotEnumerationOptions incrementalExplicit(
        Set<Long> knownSnapshotIds, Set<Long> snapshotIds) {
      return new SnapshotEnumerationOptions(
          false, knownSnapshotIds, Set.of(), SnapshotSelectionKind.EXPLICIT, snapshotIds, 0);
    }
  }

  record TablePlanningRequest(
      String sourceNamespaceFq,
      String sourceTable,
      String destinationNamespaceFq,
      String destinationTableDisplayHint,
      List<List<String>> destinationNamespacePaths,
      String destinationTableDisplayName) {
    public TablePlanningRequest {
      destinationNamespacePaths =
          destinationNamespacePaths == null
              ? List.of()
              : destinationNamespacePaths.stream()
                  .filter(path -> path != null && !path.isEmpty())
                  .map(List::copyOf)
                  .toList();
    }
  }

  record PlannedTableTask(
      String sourceNamespaceFq, String sourceTable, String destinationTableDisplayName) {
    public PlannedTableTask {
      sourceNamespaceFq = sourceNamespaceFq == null ? "" : sourceNamespaceFq;
      sourceTable = sourceTable == null ? "" : sourceTable;
      destinationTableDisplayName =
          destinationTableDisplayName == null ? "" : destinationTableDisplayName;
    }

    public PlannedTableTask(String sourceNamespaceFq, String sourceTable) {
      this(sourceNamespaceFq, sourceTable, "");
    }
  }

  record ViewPlanningRequest(
      String sourceNamespaceFq,
      String destinationNamespaceFq,
      List<List<String>> destinationNamespacePaths) {
    public ViewPlanningRequest {
      destinationNamespacePaths =
          destinationNamespacePaths == null
              ? List.of()
              : destinationNamespacePaths.stream()
                  .filter(path -> path != null && !path.isEmpty())
                  .map(List::copyOf)
                  .toList();
    }
  }

  record PlannedViewTask(
      String sourceNamespaceFq,
      String sourceView,
      String destinationNamespaceFq,
      String destinationViewDisplayName) {
    public PlannedViewTask {
      sourceNamespaceFq = sourceNamespaceFq == null ? "" : sourceNamespaceFq;
      sourceView = sourceView == null ? "" : sourceView;
      destinationNamespaceFq = destinationNamespaceFq == null ? "" : destinationNamespaceFq;
      destinationViewDisplayName =
          destinationViewDisplayName == null ? "" : destinationViewDisplayName;
    }
  }

  @Override
  void close();

  /**
   * columnIdAlgorithm is the *persisted* policy for how column_id must be computed for this table.
   * Reconciler copies it into TableSpec.upstream (so service layer can run without connectors).
   */
  record TableDescriptor(
      String namespaceFq,
      String tableName,
      String location,
      String schemaJson,
      List<String> partitionKeys,
      ColumnIdAlgorithm columnIdAlgorithm,
      Map<String, String> properties) {}

  /**
   * Column identity inputs. Fields may be unknown: - physicalPath may be blank if unknown - ordinal
   * may be 0 if unknown - fieldId may be 0 if unknown
   */
  record ColumnRef(
      String name, // leaf name (required for many engines)
      String physicalPath, // canonical leaf path (recommended; may be blank)
      int ordinal, // 1-based within parent struct, or 0 if unknown
      int fieldId // 0 if unknown
      ) {}

  record ColumnStatsView(
      ColumnRef ref,
      String logicalType,
      Long valueCount,
      Long nullCount,
      Long nanCount,
      String min,
      String max,
      Ndv ndv,
      Map<String, String> properties) {}

  record FileColumnStatsView(
      String filePath,
      String fileFormat,
      long rowCount,
      long sizeBytes,
      FileContent fileContent,
      String partitionDataJson,
      int partitionSpecId,
      List<Integer> equalityFieldIds,
      Long sequenceNumber,
      List<ColumnStatsView> columns) {}

  record SnapshotBundle(
      long snapshotId,
      long parentId,
      long upstreamCreatedAtMs,
      String schemaJson,
      PartitionSpecInfo partitionSpec,
      long sequenceNumber,
      String manifestList,
      Map<String, String> summary,
      int schemaId,
      String metadataLocation) {}

  record SnapshotFileEntry(
      String filePath,
      String fileFormat,
      long fileSizeInBytes,
      long recordCount,
      FileContent fileContent,
      String partitionDataJson,
      int partitionSpecId,
      List<Integer> equalityFieldIds,
      Long sequenceNumber) {
    public SnapshotFileEntry {
      filePath = filePath == null ? "" : filePath;
      fileFormat = fileFormat == null ? "" : fileFormat;
      fileSizeInBytes = Math.max(0L, fileSizeInBytes);
      recordCount = Math.max(0L, recordCount);
      fileContent = fileContent == null ? FileContent.FC_DATA : fileContent;
      partitionDataJson = partitionDataJson == null ? "" : partitionDataJson;
      equalityFieldIds = equalityFieldIds == null ? List.of() : List.copyOf(equalityFieldIds);
    }
  }

  record SnapshotFilePlan(List<SnapshotFileEntry> dataFiles, List<SnapshotFileEntry> deleteFiles) {
    public SnapshotFilePlan {
      dataFiles = dataFiles == null ? List.of() : List.copyOf(dataFiles);
      deleteFiles = deleteFiles == null ? List.of() : List.copyOf(deleteFiles);
    }
  }

  record ParquetPageIndexEntry(
      String filePath,
      String columnName,
      int rowGroup,
      int pageOrdinal,
      long firstRowIndex,
      int rowCount,
      int liveRowCount,
      Long pageHeaderOffset,
      int pageTotalCompressedSize,
      Long dictionaryPageHeaderOffset,
      Integer dictionaryPageTotalCompressedSize,
      boolean requiresDictionaryPage,
      String parquetPhysicalType,
      String parquetCompression,
      short parquetMaxDefLevel,
      short parquetMaxRepLevel,
      Integer decimalPrecision,
      Integer decimalScale,
      Integer decimalBits,
      Integer minI32,
      Integer maxI32,
      Long minI64,
      Long maxI64,
      Float minF32,
      Float maxF32,
      Double minF64,
      Double maxF64,
      Boolean minBool,
      Boolean maxBool,
      String minUtf8,
      String maxUtf8,
      byte[] minDecimal128Unscaled,
      byte[] maxDecimal128Unscaled,
      byte[] minDecimal256Unscaled,
      byte[] maxDecimal256Unscaled) {
    public ParquetPageIndexEntry {
      filePath = filePath == null ? "" : filePath;
      columnName = columnName == null ? "" : columnName;
      rowGroup = Math.max(0, rowGroup);
      pageOrdinal = Math.max(0, pageOrdinal);
      firstRowIndex = Math.max(0L, firstRowIndex);
      rowCount = Math.max(0, rowCount);
      liveRowCount = Math.max(0, liveRowCount);
      pageTotalCompressedSize = Math.max(0, pageTotalCompressedSize);
      parquetPhysicalType = parquetPhysicalType == null ? "" : parquetPhysicalType;
      parquetCompression = parquetCompression == null ? "" : parquetCompression;
      minUtf8 = minUtf8 == null ? null : minUtf8;
      maxUtf8 = maxUtf8 == null ? null : maxUtf8;
      minDecimal128Unscaled =
          minDecimal128Unscaled == null
              ? null
              : java.util.Arrays.copyOf(minDecimal128Unscaled, minDecimal128Unscaled.length);
      maxDecimal128Unscaled =
          maxDecimal128Unscaled == null
              ? null
              : java.util.Arrays.copyOf(maxDecimal128Unscaled, maxDecimal128Unscaled.length);
      minDecimal256Unscaled =
          minDecimal256Unscaled == null
              ? null
              : java.util.Arrays.copyOf(minDecimal256Unscaled, minDecimal256Unscaled.length);
      maxDecimal256Unscaled =
          maxDecimal256Unscaled == null
              ? null
              : java.util.Arrays.copyOf(maxDecimal256Unscaled, maxDecimal256Unscaled.length);
    }

    public ParquetPageIndexEntry(
        String filePath,
        String columnName,
        int rowGroup,
        int pageOrdinal,
        long firstRowIndex,
        int rowCount,
        int liveRowCount,
        Long pageHeaderOffset,
        int pageTotalCompressedSize,
        Long dictionaryPageHeaderOffset,
        Integer dictionaryPageTotalCompressedSize,
        boolean requiresDictionaryPage,
        String parquetPhysicalType,
        String parquetCompression,
        short parquetMaxDefLevel,
        short parquetMaxRepLevel,
        Integer decimalPrecision,
        Integer decimalScale,
        Integer decimalBits) {
      this(
          filePath,
          columnName,
          rowGroup,
          pageOrdinal,
          firstRowIndex,
          rowCount,
          liveRowCount,
          pageHeaderOffset,
          pageTotalCompressedSize,
          dictionaryPageHeaderOffset,
          dictionaryPageTotalCompressedSize,
          requiresDictionaryPage,
          parquetPhysicalType,
          parquetCompression,
          parquetMaxDefLevel,
          parquetMaxRepLevel,
          decimalPrecision,
          decimalScale,
          decimalBits,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null);
    }
  }

  record ScanBundle(List<ScanFile> dataFiles, List<ScanFile> deleteFiles) {}

  /**
   * Describes a SQL view sourced from the external catalog.
   *
   * @param namespaceFq fully-qualified namespace (e.g. "catalog.schema")
   * @param name view display name
   * @param sql the view's defining SQL query
   * @param dialect SQL dialect (e.g. "spark")
   * @param searchPath default namespace search path used when the view's SQL was authored (e.g.
   *     {@code ["catalog", "schema"]})
   * @param schemaJson schema JSON in the same format as {@link TableDescriptor#schemaJson()} —
   *     interpreted by {@code LogicalSchemaMapper.mapRaw()} using the connector's {@link
   *     ConnectorFormat}
   */
  record ViewDescriptor(
      String namespaceFq,
      String name,
      List<ViewSqlDefinition> sqlDefinitions,
      List<String> searchPath,
      String schemaJson) {
    public ViewDescriptor {
      sqlDefinitions = sqlDefinitions == null ? List.of() : List.copyOf(sqlDefinitions);
      searchPath = searchPath == null ? List.of() : List.copyOf(searchPath);
    }

    public ViewDescriptor(
        String namespaceFq,
        String name,
        String sql,
        String dialect,
        List<String> searchPath,
        String schemaJson) {
      this(
          namespaceFq,
          name,
          (sql == null || sql.isBlank()) ? List.of() : List.of(new ViewSqlDefinition(sql, dialect)),
          searchPath,
          schemaJson);
    }

    public String sql() {
      return preferredSqlDefinition().map(ViewSqlDefinition::sql).orElse("");
    }

    public String dialect() {
      return preferredSqlDefinition().map(ViewSqlDefinition::dialect).orElse("");
    }

    public Optional<ViewSqlDefinition> preferredSqlDefinition() {
      return sqlDefinitions.stream()
          .filter(def -> def != null && def.sql() != null && !def.sql().isBlank())
          .sorted(
              (left, right) ->
                  Integer.compare(
                      definitionPriority(left.dialect()), definitionPriority(right.dialect())))
          .findFirst();
    }

    private static int definitionPriority(String dialect) {
      if (dialect == null) {
        return 3;
      }
      return switch (dialect.trim().toLowerCase()) {
        case "floe" -> 0;
        case "ansi" -> 1;
        case "spark" -> 2;
        default -> 3;
      };
    }
  }

  record ViewSqlDefinition(String sql, String dialect) {
    public ViewSqlDefinition {
      sql = sql == null ? "" : sql;
      dialect = dialect == null ? "" : dialect;
    }
  }

  /**
   * Returns the names of views in the given namespace. Default: empty list.
   *
   * @implSpec Prefer overriding {@link #listViewDescriptors} directly when the underlying catalog
   *     can return view definitions in a single request, to avoid an additional round-trip per
   *     view.
   */
  default List<String> listViews(String namespaceFq) {
    return List.of();
  }

  /**
   * Describes a view by name. Returns {@link Optional#empty()} if the view does not exist or cannot
   * be described. Default: empty.
   *
   * <p>Connector implementations that expose views through {@link
   * #planViewTasks(ViewPlanningRequest)} or {@link #listViewDescriptors(String)} must return a
   * matching descriptor here for the same source identity unless the view has been removed
   * concurrently.
   *
   * @implSpec Prefer overriding {@link #listViewDescriptors} directly when the underlying catalog
   *     can return all view definitions in a single request, to avoid an additional round-trip per
   *     view.
   */
  default Optional<ViewDescriptor> describeView(String namespaceFq, String name) {
    return Optional.empty();
  }

  /**
   * Returns full descriptors for all views in the given namespace in a single operation. Connectors
   * that can batch this more efficiently than one {@link #describeView} call per name should
   * override this method.
   *
   * <p>Each returned descriptor must be resolvable through {@link #describeView(String, String)}
   * using the descriptor namespace and name unless the view has been removed concurrently.
   *
   * <p><b>Default implementation:</b> chains {@link #listViews} and {@link #describeView} per name.
   * This default is <em>fail-fast</em>: if any {@code describeView} call throws, the exception
   * propagates immediately and remaining views are skipped.
   */
  default List<ViewDescriptor> listViewDescriptors(String namespaceFq) {
    List<ViewDescriptor> result = new ArrayList<>();
    for (String name : listViews(namespaceFq)) {
      describeView(namespaceFq, name).ifPresent(result::add);
    }
    return result;
  }
}
