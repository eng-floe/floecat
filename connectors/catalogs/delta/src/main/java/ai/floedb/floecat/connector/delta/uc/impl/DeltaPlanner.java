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

import ai.floedb.floecat.connector.common.ParquetFooterStats;
import ai.floedb.floecat.connector.common.PlannedFile;
import ai.floedb.floecat.connector.common.Planner;
import ai.floedb.floecat.connector.common.ndv.NdvProvider;
import ai.floedb.floecat.types.LogicalCoercions;
import ai.floedb.floecat.types.LogicalComparators;
import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.FileReadResult;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.skipping.StatsSchemaHelper;
import io.delta.kernel.statistics.DataFileStatistics;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.parquet.io.InputFile;

final class DeltaPlanner implements Planner<String> {
  static final String COLUMN_MAPPING_PHYSICAL_NAME_KEY = "delta.columnMapping.physicalName";
  static final String STATS_PARSED_FIELD = "stats_parsed";
  static final String ADD_FIELD = "add";
  static final String PATH_FIELD = "path";
  private static final String PARQUET_FORMAT = "PARQUET";
  private static final long MICROS_PER_SECOND = 1_000_000L;
  private static final int MAX_DIAGNOSTIC_SAMPLES = 8;

  private final List<PlannedFile<String>> files = new ArrayList<>();
  private final Map<String, LogicalType> nameToLogical = new LinkedHashMap<>();
  private final Map<String, String> statsNameToLogical = new LinkedHashMap<>();
  private final NdvProvider ndvProvider;
  private final Set<String> columnSet;
  private final Set<String> plannedFilePaths;
  private final boolean allowFooterFallback;
  private final Engine engine;
  private final Snapshot snapshot;
  private final String storageLocation;
  private final List<DeletionVectorDescriptor> diskDeletionVectors = new ArrayList<>();
  private final List<String> missingLogStatsSamplePaths = new ArrayList<>();
  private final List<String> checkpointStructRecoverySamplePaths = new ArrayList<>();
  private final List<String> deletionVectorSamplePaths = new ArrayList<>();
  private boolean hasInlineDeletionVectors = false;
  private boolean missingLogStats = false;
  private int missingLogStatsFileCount = 0;
  private int checkpointStructRecoveryFileCount = 0;
  private int onDiskDeletionVectorCount = 0;
  private int inlineDeletionVectorCount = 0;
  private boolean checkpointStructStatsLoaded = false;
  private Map<String, DataFileStatistics> checkpointStructStatsByPath = Map.of();

  DeltaPlanner(
      Engine engine,
      Function<String, InputFile> parquetInput,
      String storageLocation,
      long version,
      Set<String> includeColumns,
      Set<String> plannedFilePaths,
      Map<String, LogicalType> nameToType,
      NdvProvider ndvProvider,
      boolean includeStats,
      boolean allowFooterFallback) {
    this.engine = engine;
    this.ndvProvider = ndvProvider;
    this.allowFooterFallback = allowFooterFallback;
    this.storageLocation = Objects.requireNonNull(storageLocation);
    this.plannedFilePaths =
        plannedFilePaths == null || plannedFilePaths.isEmpty()
            ? Collections.emptySet()
            : Collections.unmodifiableSet(new LinkedHashSet<>(plannedFilePaths));

    final Table table = Table.forPath(engine, Objects.requireNonNull(storageLocation));
    final Snapshot snapshot = table.getSnapshotAsOfVersion(engine, version);
    this.snapshot = snapshot;

    final StructType schema = snapshot.getSchema();
    if (nameToType == null || nameToType.isEmpty()) {
      nameToLogical.putAll(DeltaTypeMapper.deltaTypeMap(schema));
    } else {
      nameToLogical.putAll(nameToType);
    }
    statsNameToLogical.putAll(statsNameMap(schema));

    if (includeColumns == null || includeColumns.isEmpty()) {
      this.columnSet = Collections.unmodifiableSet(new LinkedHashSet<>(nameToLogical.keySet()));
    } else {
      LinkedHashSet<String> filtered =
          includeColumns.stream()
              .filter(nameToLogical::containsKey)
              .collect(Collectors.toCollection(LinkedHashSet::new));
      this.columnSet = Collections.unmodifiableSet(filtered);
    }

    final ScanBuilder sb = snapshot.getScanBuilder();
    final Scan scan = sb.build();

    try (CloseableIterator<FilteredColumnarBatch> it = scan.getScanFiles(engine)) {
      while (it.hasNext()) {
        FilteredColumnarBatch batch = it.next();
        try (var rows = batch.getRows()) {
          while (rows.hasNext()) {
            Row scanFileRow = rows.next();

            FileStatus fs = InternalScanFileUtils.getAddFileStatus(scanFileRow);
            String path = fs.getPath();
            if (!this.plannedFilePaths.isEmpty() && !this.plannedFilePaths.contains(path)) {
              continue;
            }
            long sizeBytes = fs.getSize();

            long rowCount = 0L;
            Map<String, Long> rowCounts = null;
            Map<String, Long> nullCounts = null;
            Map<String, Long> nanCounts = null;
            Map<String, Object> mins = null;
            Map<String, Object> maxs = null;
            String partitionJson = "{\"partitionValues\":[]}";

            if (includeStats) {
              AddFile add =
                  new AddFile(scanFileRow.getStruct(InternalScanFileUtils.ADD_FILE_ORDINAL));
              add.getDeletionVector()
                  .ifPresent(
                      dv -> {
                        if (dv.isInline()) {
                          hasInlineDeletionVectors = true;
                          inlineDeletionVectorCount++;
                          addDiagnosticSample(deletionVectorSamplePaths, path + "#inline");
                        } else if (dv.isOnDisk()) {
                          diskDeletionVectors.add(dv);
                          onDiskDeletionVectorCount++;
                          addDiagnosticSample(
                              deletionVectorSamplePaths,
                              path
                                  + "#ondisk:"
                                  + (dv.getPathOrInlineDv() == null ? "" : dv.getPathOrInlineDv()));
                        }
                      });
              partitionJson = encodePartition(add);
              Optional<DataFileStatistics> optStats = add.getStats(snapshot.getSchema());
              if (optStats.isEmpty()) {
                optStats = checkpointStatsForPath(path);
                if (optStats.isPresent()) {
                  checkpointStructRecoveryFileCount++;
                  addDiagnosticSample(checkpointStructRecoverySamplePaths, path);
                }
              }
              if (optStats.isPresent()) {
                DataFileStatistics stats = optStats.get();

                rowCount = stats.getNumRecords();

                Map<Column, Long> nc = stats.getNullCount();
                if (nc != null && !nc.isEmpty()) {
                  nullCounts = new LinkedHashMap<>(nc.size());
                  for (var e : nc.entrySet()) {
                    String colName = resolveStatsColumnName(firstName(e.getKey()));
                    Long n = e.getValue();
                    if (colName != null && n != null) nullCounts.put(colName, n);
                  }
                }

                if (rowCount > 0 && nullCounts != null && !nullCounts.isEmpty()) {
                  rowCounts = new LinkedHashMap<>(nullCounts.size());
                  for (var e : nullCounts.entrySet()) {
                    rowCounts.put(e.getKey(), rowCount);
                  }
                }

                Map<Column, Literal> minsMap = stats.getMinValues();
                if (minsMap != null && !minsMap.isEmpty()) {
                  mins = new LinkedHashMap<>(minsMap.size());
                  for (var e : minsMap.entrySet()) {
                    String colName = resolveStatsColumnName(firstName(e.getKey()));
                    if (colName != null) {
                      var lt = nameToLogical.get(colName);
                      Object raw = (e.getValue() == null) ? null : e.getValue().getValue();
                      raw = canonicalizeStatValue(lt, raw);
                      Object typed = (lt == null) ? raw : LogicalComparators.normalize(lt, raw);
                      mins.put(colName, typed);
                    }
                  }
                }

                Map<Column, Literal> maxsMap = stats.getMaxValues();
                if (maxsMap != null && !maxsMap.isEmpty()) {
                  maxs = new LinkedHashMap<>(maxsMap.size());
                  for (var e : maxsMap.entrySet()) {
                    String colName = resolveStatsColumnName(firstName(e.getKey()));
                    if (colName != null) {
                      var lt = nameToLogical.get(colName);
                      Object raw = (e.getValue() == null) ? null : e.getValue().getValue();
                      raw = canonicalizeStatValue(lt, raw);
                      Object typed = (lt == null) ? raw : LogicalComparators.normalize(lt, raw);
                      maxs.put(colName, typed);
                    }
                  }
                }
              } else {
                if (!this.allowFooterFallback) {
                  missingLogStats = true;
                  missingLogStatsFileCount++;
                  addDiagnosticSample(missingLogStatsSamplePaths, path);
                  continue;
                }
                var in = parquetInput.apply(path);

                var bfs = ParquetFooterStats.read(in, columnSet, nameToLogical);

                rowCount = bfs.rowCount;
                nullCounts = new LinkedHashMap<>();
                mins = new LinkedHashMap<>();
                maxs = new LinkedHashMap<>();
                rowCounts = new LinkedHashMap<>();

                for (var e : bfs.cols.entrySet()) {
                  String name = e.getKey();
                  var c = e.getValue();
                  nullCounts.put(name, c.nulls);
                  if (rowCount > 0) rowCounts.put(name, rowCount);
                  var lt = nameToLogical.get(name);
                  if (lt != null) {
                    if (c.min != null) {
                      mins.put(name, LogicalCoercions.coerceStatValue(lt, c.min));
                    }
                    if (c.max != null) {
                      maxs.put(name, LogicalCoercions.coerceStatValue(lt, c.max));
                    }
                  }
                }
              }
            }

            files.add(
                new PlannedFile<>(
                    path,
                    PARQUET_FORMAT,
                    rowCount,
                    sizeBytes,
                    rowCounts,
                    nullCounts,
                    nanCounts,
                    mins,
                    maxs,
                    partitionJson,
                    0,
                    null));
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Delta planning failed (version " + version + ")", e);
    }
  }

  boolean hasDeletionVectors() {
    return !diskDeletionVectors.isEmpty() || hasInlineDeletionVectors;
  }

  boolean missingLogStats() {
    return missingLogStats;
  }

  int missingLogStatsFileCount() {
    return missingLogStatsFileCount;
  }

  int checkpointStructRecoveryFileCount() {
    return checkpointStructRecoveryFileCount;
  }

  List<String> checkpointStructRecoverySamplePaths() {
    return Collections.unmodifiableList(checkpointStructRecoverySamplePaths);
  }

  List<String> missingLogStatsSamplePaths() {
    return Collections.unmodifiableList(missingLogStatsSamplePaths);
  }

  boolean hasInlineDeletionVectors() {
    return hasInlineDeletionVectors;
  }

  int onDiskDeletionVectorCount() {
    return onDiskDeletionVectorCount;
  }

  int inlineDeletionVectorCount() {
    return inlineDeletionVectorCount;
  }

  List<String> deletionVectorSamplePaths() {
    return Collections.unmodifiableList(deletionVectorSamplePaths);
  }

  List<DeletionVectorDescriptor> deletionVectors() {
    return Collections.unmodifiableList(diskDeletionVectors);
  }

  private String encodePartition(AddFile add) {
    try {
      Map<String, String> partitionValues = toPartitionMap(add.getPartitionValues());
      if (partitionValues == null || partitionValues.isEmpty()) {
        return "{\"partitionValues\":[]}";
      }
      StringBuilder sb = new StringBuilder();
      sb.append("{\"partitionValues\":[");
      int i = 0;
      for (var entry : partitionValues.entrySet()) {
        if (i++ > 0) {
          sb.append(',');
        }
        sb.append("{\"id\":").append('\"').append(escape(entry.getKey())).append('\"');
        sb.append(",\"value\":").append(toJsonValue(entry.getValue())).append('}');
      }
      sb.append("]}");
      return sb.toString();
    } catch (Exception e) {
      return "";
    }
  }

  private static void addDiagnosticSample(List<String> samples, String value) {
    if (value == null || value.isBlank() || samples.size() >= MAX_DIAGNOSTIC_SAMPLES) {
      return;
    }
    samples.add(value);
  }

  private Optional<DataFileStatistics> checkpointStatsForPath(String absolutePath) {
    if (!checkpointStructStatsLoaded) {
      checkpointStructStatsByPath = loadCheckpointStructStats();
      checkpointStructStatsLoaded = true;
    }
    return Optional.ofNullable(checkpointStructStatsByPath.get(absolutePath));
  }

  private Map<String, DataFileStatistics> loadCheckpointStructStats() {
    if (!(snapshot instanceof SnapshotImpl snapshotImpl)) {
      return Map.of();
    }
    List<io.delta.kernel.utils.FileStatus> checkpointFiles =
        snapshotImpl.getLogSegment().getCheckpoints();
    if (checkpointFiles == null || checkpointFiles.isEmpty()) {
      return Map.of();
    }

    StructType projectedSchema = projectedStatsDataSchema(snapshot.getSchema(), columnSet);
    StructType statsSchema = StatsSchemaHelper.getStatsSchema(projectedSchema, Set.of());
    if (statsSchema.length() == 0) {
      return Map.of();
    }

    StructType addSchema =
        new StructType()
            .add(PATH_FIELD, StringType.STRING, true)
            .add(STATS_PARSED_FIELD, statsSchema, true);
    StructType readSchema = new StructType().add(ADD_FIELD, addSchema, true);

    LinkedHashMap<String, DataFileStatistics> byPath = new LinkedHashMap<>();
    try (var files = closeableIterator(checkpointFiles);
        var results =
            engine.getParquetHandler().readParquetFiles(files, readSchema, Optional.empty())) {
      while (results.hasNext()) {
        FileReadResult result = results.next();
        try (var rows = result.getData().getRows()) {
          while (rows.hasNext()) {
            Row row = rows.next();
            Row addRow = checkpointAddRow(row);
            if (addRow == null) {
              continue;
            }
            int pathOrdinal = addRow.getSchema().indexOf(PATH_FIELD);
            if (pathOrdinal < 0 || addRow.isNullAt(pathOrdinal)) {
              continue;
            }
            String resolvedPath = absoluteDataPath(storageLocation, addRow.getString(pathOrdinal));
            if (!plannedFilePaths.isEmpty() && !plannedFilePaths.contains(resolvedPath)) {
              continue;
            }
            checkpointStructStatsFromAddRow(addRow, statsNameToLogical, columnSet)
                .ifPresent(stats -> byPath.put(resolvedPath, stats));
          }
        }
      }
    } catch (Exception e) {
      return Map.of();
    }
    return Collections.unmodifiableMap(byPath);
  }

  static Optional<DataFileStatistics> checkpointStructStatsFromAddRow(
      Row addFileRow, Map<String, String> statsNameToLogical, Set<String> columnSet) {
    if (addFileRow == null) {
      return Optional.empty();
    }
    StructType addSchema = addFileRow.getSchema();
    int statsOrdinal = addSchema.indexOf(STATS_PARSED_FIELD);
    if (statsOrdinal < 0 || addFileRow.isNullAt(statsOrdinal)) {
      return Optional.empty();
    }
    Row statsRow = addFileRow.getStruct(statsOrdinal);
    if (statsRow == null) {
      return Optional.empty();
    }

    StructType statsSchema = statsRow.getSchema();
    int numRecordsOrdinal = statsSchema.indexOf(StatsSchemaHelper.NUM_RECORDS);
    if (numRecordsOrdinal < 0 || statsRow.isNullAt(numRecordsOrdinal)) {
      return Optional.empty();
    }
    long numRecords = readLongLike(statsRow, numRecordsOrdinal);

    Map<Column, Literal> minValues =
        readLiteralStruct(
            statsRow, statsSchema.indexOf(StatsSchemaHelper.MIN), statsNameToLogical, columnSet);
    Map<Column, Literal> maxValues =
        readLiteralStruct(
            statsRow, statsSchema.indexOf(StatsSchemaHelper.MAX), statsNameToLogical, columnSet);
    Map<Column, Long> nullCounts =
        readNullCountStruct(
            statsRow,
            statsSchema.indexOf(StatsSchemaHelper.NULL_COUNT),
            statsNameToLogical,
            columnSet);

    return Optional.of(
        new DataFileStatistics(numRecords, minValues, maxValues, nullCounts, Optional.empty()));
  }

  private static Row checkpointAddRow(Row checkpointRow) {
    if (checkpointRow == null) {
      return null;
    }
    StructType schema = checkpointRow.getSchema();
    int addOrdinal = schema.indexOf(ADD_FIELD);
    if (addOrdinal < 0 || checkpointRow.isNullAt(addOrdinal)) {
      return null;
    }
    return checkpointRow.getStruct(addOrdinal);
  }

  static StructType projectedStatsDataSchema(StructType schema, Set<String> includeColumns) {
    if (schema == null || includeColumns == null || includeColumns.isEmpty()) {
      return schema;
    }
    StructType projected = new StructType();
    for (StructField field : schema.fields()) {
      if (includeColumns.contains(field.getName())) {
        projected = projected.add(field);
      }
    }
    return projected;
  }

  static String absoluteDataPath(String storageLocation, String addPath) {
    if (addPath == null || addPath.isBlank()) {
      return addPath;
    }
    Path candidate = new Path(addPath);
    if (candidate.isAbsolute()) {
      return candidate.toString();
    }
    return new Path(new Path(storageLocation), candidate).toString();
  }

  private static Map<Column, Literal> readLiteralStruct(
      Row parentRow, int ordinal, Map<String, String> statsNameToLogical, Set<String> columnSet) {
    if (ordinal < 0 || parentRow.isNullAt(ordinal)) {
      return Map.of();
    }
    Row structRow = parentRow.getStruct(ordinal);
    if (structRow == null) {
      return Map.of();
    }
    LinkedHashMap<Column, Literal> values = new LinkedHashMap<>();
    StructType schema = structRow.getSchema();
    for (int i = 0; i < schema.length(); i++) {
      if (structRow.isNullAt(i)) {
        continue;
      }
      DataType dataType = schema.at(i).getDataType();
      if (isContainerStatsType(dataType)) {
        continue;
      }
      String name = resolveStatsColumnName(schema.at(i).getName(), statsNameToLogical, columnSet);
      if (name == null) {
        continue;
      }
      values.put(new Column(name), toLiteral(structRow, i, dataType));
    }
    return values;
  }

  private static Map<Column, Long> readNullCountStruct(
      Row parentRow, int ordinal, Map<String, String> statsNameToLogical, Set<String> columnSet) {
    if (ordinal < 0 || parentRow.isNullAt(ordinal)) {
      return Map.of();
    }
    Row structRow = parentRow.getStruct(ordinal);
    if (structRow == null) {
      return Map.of();
    }
    LinkedHashMap<Column, Long> values = new LinkedHashMap<>();
    StructType schema = structRow.getSchema();
    for (int i = 0; i < schema.length(); i++) {
      if (structRow.isNullAt(i)) {
        continue;
      }
      if (isContainerStatsType(schema.at(i).getDataType())) {
        continue;
      }
      String name = resolveStatsColumnName(schema.at(i).getName(), statsNameToLogical, columnSet);
      if (name == null) {
        continue;
      }
      values.put(new Column(name), readLongLike(structRow, i));
    }
    return values;
  }

  private String resolveStatsColumnName(String statsName) {
    return resolveStatsColumnName(statsName, statsNameToLogical, columnSet);
  }

  private static String resolveStatsColumnName(
      String statsName, Map<String, String> statsNameToLogical, Set<String> columnSet) {
    if (statsName == null || statsName.isBlank()) {
      return null;
    }
    String logicalName = statsNameToLogical.get(statsName);
    if (logicalName != null && columnSet.contains(logicalName)) {
      return logicalName;
    }
    return columnSet.contains(statsName) ? statsName : null;
  }

  private static boolean isContainerStatsType(DataType dataType) {
    return dataType instanceof StructType;
  }

  static Map<String, String> statsNameMap(StructType schema) {
    if (schema == null) {
      return Map.of();
    }
    LinkedHashMap<String, String> mapping = new LinkedHashMap<>();
    for (StructField field : schema.fields()) {
      String logicalName = field.getName();
      if (logicalName == null || logicalName.isBlank()) {
        continue;
      }
      mapping.put(logicalName, logicalName);
      String physicalName = physicalName(field.getMetadata());
      if (physicalName != null && !physicalName.isBlank()) {
        mapping.put(physicalName, logicalName);
      }
    }
    return Collections.unmodifiableMap(mapping);
  }

  static String physicalName(FieldMetadata metadata) {
    return metadata == null ? null : metadata.getString(COLUMN_MAPPING_PHYSICAL_NAME_KEY);
  }

  private static io.delta.kernel.utils.CloseableIterator<io.delta.kernel.utils.FileStatus>
      closeableIterator(List<io.delta.kernel.utils.FileStatus> files) {
    Iterator<io.delta.kernel.utils.FileStatus> delegate = files.iterator();
    return new io.delta.kernel.utils.CloseableIterator<>() {
      @Override
      public boolean hasNext() {
        return delegate.hasNext();
      }

      @Override
      public io.delta.kernel.utils.FileStatus next() {
        return delegate.next();
      }

      @Override
      public void close() {}
    };
  }

  private static long readLongLike(Row row, int ordinal) {
    DataType dataType = row.getSchema().at(ordinal).getDataType();
    if (dataType instanceof LongType) {
      return row.getLong(ordinal);
    }
    if (dataType instanceof IntegerType) {
      return row.getInt(ordinal);
    }
    if (dataType instanceof ShortType) {
      return row.getShort(ordinal);
    }
    if (dataType instanceof ByteType) {
      return row.getByte(ordinal);
    }
    throw new IllegalArgumentException("Unsupported integral stats type: " + dataType);
  }

  private static Object readDataValue(Row row, int ordinal, DataType dataType) {
    if (dataType instanceof BooleanType) {
      return row.getBoolean(ordinal);
    }
    if (dataType instanceof ByteType) {
      return row.getByte(ordinal);
    }
    if (dataType instanceof ShortType) {
      return row.getShort(ordinal);
    }
    if (dataType instanceof IntegerType || dataType instanceof DateType) {
      return row.getInt(ordinal);
    }
    if (dataType instanceof LongType
        || dataType instanceof TimestampType
        || dataType instanceof TimestampNTZType) {
      return row.getLong(ordinal);
    }
    if (dataType instanceof FloatType) {
      return row.getFloat(ordinal);
    }
    if (dataType instanceof DoubleType) {
      return row.getDouble(ordinal);
    }
    if (dataType instanceof StringType) {
      return row.getString(ordinal);
    }
    if (dataType instanceof DecimalType) {
      return row.getDecimal(ordinal);
    }
    if (dataType instanceof BinaryType) {
      return row.getBinary(ordinal);
    }
    throw new IllegalArgumentException("Unsupported stats value type: " + dataType);
  }

  private static Literal toLiteral(Row row, int ordinal, DataType dataType) {
    if (dataType instanceof BooleanType) {
      return Literal.ofBoolean(row.getBoolean(ordinal));
    }
    if (dataType instanceof ByteType) {
      return Literal.ofByte(row.getByte(ordinal));
    }
    if (dataType instanceof ShortType) {
      return Literal.ofShort(row.getShort(ordinal));
    }
    if (dataType instanceof IntegerType) {
      return Literal.ofInt(row.getInt(ordinal));
    }
    if (dataType instanceof LongType) {
      return Literal.ofLong(row.getLong(ordinal));
    }
    if (dataType instanceof FloatType) {
      return Literal.ofFloat(row.getFloat(ordinal));
    }
    if (dataType instanceof DoubleType) {
      return Literal.ofDouble(row.getDouble(ordinal));
    }
    if (dataType instanceof StringType) {
      return Literal.ofString(row.getString(ordinal));
    }
    if (dataType instanceof BinaryType) {
      return Literal.ofBinary(row.getBinary(ordinal));
    }
    if (dataType instanceof DateType) {
      return Literal.ofDate(row.getInt(ordinal));
    }
    if (dataType instanceof TimestampType) {
      return Literal.ofTimestamp(row.getLong(ordinal));
    }
    if (dataType instanceof TimestampNTZType) {
      return Literal.ofTimestampNtz(row.getLong(ordinal));
    }
    if (dataType instanceof DecimalType decimalType) {
      return Literal.ofDecimal(
          row.getDecimal(ordinal), decimalType.getPrecision(), decimalType.getScale());
    }
    throw new IllegalArgumentException("Unsupported literal stats type: " + dataType);
  }

  private String toJsonValue(String value) {
    if (value == null) {
      return "null";
    }
    if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
      return value.toLowerCase();
    }
    try {
      Double.parseDouble(value);
      return value;
    } catch (NumberFormatException ignore) {
      // fall through
    }
    return "\"" + escape(value) + "\"";
  }

  private String escape(String s) {
    return String.valueOf(s).replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private Map<String, String> toPartitionMap(MapValue mapValue) {
    if (mapValue == null) {
      return Map.of();
    }
    var keys = mapValue.getKeys();
    var vals = mapValue.getValues();
    int size = mapValue.getSize();
    Map<String, String> out = new LinkedHashMap<>(size);
    for (int i = 0; i < size; i++) {
      if (keys.isNullAt(i)) {
        continue;
      }
      String key = keys.getString(i);
      String value = vals.isNullAt(i) ? null : vals.getString(i);
      out.put(key, value);
    }
    return out;
  }

  @Override
  public Map<String, String> columnNamesByKey() {
    var out = new LinkedHashMap<String, String>(columnSet.size());
    for (String name : columnSet) {
      out.put(name, name);
    }
    return Collections.unmodifiableMap(out);
  }

  @Override
  public Map<String, LogicalType> logicalTypesByKey() {
    var out = new LinkedHashMap<String, LogicalType>(columnSet.size());
    for (String name : columnSet) {
      var lt = nameToLogical.get(name);
      if (lt != null) {
        out.put(name, lt);
      }
    }
    return Collections.unmodifiableMap(out);
  }

  @Override
  public Iterator<PlannedFile<String>> iterator() {
    return files.iterator();
  }

  @Override
  public void close() {}

  @Override
  public Function<String, String> nameOf() {
    return Function.identity();
  }

  @Override
  public Function<String, LogicalType> typeOf() {
    return nameToLogical::get;
  }

  @Override
  public NdvProvider ndvProvider() {
    return ndvProvider;
  }

  @Override
  public Set<String> columns() {
    return columnSet;
  }

  private static String firstName(Column c) {
    try {
      String[] names = (c == null) ? null : c.getNames();
      return (names == null || names.length == 0) ? null : names[0];
    } catch (Throwable ignore) {
      return null;
    }
  }

  static Object canonicalizeStatValue(LogicalType logicalType, Object raw) {
    if (logicalType == null || raw == null || !(raw instanceof Number n)) {
      return raw;
    }
    if (logicalType.kind() == LogicalKind.TIMESTAMP) {
      return localDateTimeFromMicros(n.longValue());
    }
    if (logicalType.kind() == LogicalKind.TIMESTAMPTZ) {
      return instantFromMicros(n.longValue());
    }
    return raw;
  }

  private static Instant instantFromMicros(long micros) {
    long seconds = Math.floorDiv(micros, MICROS_PER_SECOND);
    long microsRemainder = Math.floorMod(micros, MICROS_PER_SECOND);
    return Instant.ofEpochSecond(seconds, microsRemainder * 1_000L);
  }

  private static LocalDateTime localDateTimeFromMicros(long micros) {
    return LocalDateTime.ofInstant(instantFromMicros(micros), ZoneOffset.UTC);
  }
}
