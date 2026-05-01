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

package ai.floedb.floecat.reconciler.impl;

import ai.floedb.floecat.catalog.rpc.FileContent;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.NdvApprox;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.common.ndv.ColumnNdv;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import ai.floedb.floecat.types.LogicalComparators;
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.LogicalTypeProtoAdapter;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Derives complete table/column/file target stats for a file-group from file-target records.
 *
 * <p>This keeps the file-group capture contract self-contained: callers receive complete
 * persistable outputs for the requested file group even when the underlying connector only emits
 * file-scoped primitives.
 */
public final class FileGroupTargetStatsRollup {
  public List<TargetStatsRecord> complete(
      ResourceId tableId,
      long snapshotId,
      Set<FloecatConnector.StatsTargetKind> requestedKinds,
      List<TargetStatsRecord> captured) {
    if (captured == null || captured.isEmpty()) {
      return List.of();
    }

    List<TargetStatsRecord> fileRecords =
        captured.stream().filter(TargetStatsRecord::hasFile).toList();
    if (fileRecords.isEmpty()) {
      return captured;
    }

    LinkedHashMap<String, TargetStatsRecord> completed = new LinkedHashMap<>();
    for (TargetStatsRecord record : captured) {
      if (record != null && record.hasTarget()) {
        completed.putIfAbsent(
            ai.floedb.floecat.stats.identity.StatsTargetIdentity.storageId(record.getTarget()),
            record);
      }
    }

    if (requestedKinds.contains(FloecatConnector.StatsTargetKind.TABLE)
        && captured.stream().noneMatch(TargetStatsRecord::hasTable)) {
      TargetStatsRecord tableRecord = aggregateTable(tableId, snapshotId, fileRecords);
      if (tableRecord != null) {
        completed.putIfAbsent(
            ai.floedb.floecat.stats.identity.StatsTargetIdentity.storageId(tableRecord.getTarget()),
            tableRecord);
      }
    }

    if (requestedKinds.contains(FloecatConnector.StatsTargetKind.COLUMN)) {
      for (TargetStatsRecord columnRecord : aggregateColumns(tableId, snapshotId, fileRecords)) {
        completed.putIfAbsent(
            ai.floedb.floecat.stats.identity.StatsTargetIdentity.storageId(
                columnRecord.getTarget()),
            columnRecord);
      }
    }

    return List.copyOf(completed.values());
  }

  public static List<TargetStatsRecord> completeSnapshotFromFileRecords(
      ResourceId tableId,
      long snapshotId,
      Set<FloecatConnector.StatsTargetKind> requestedKinds,
      List<TargetStatsRecord> fileRecords) {
    return new FileGroupTargetStatsRollup()
        .complete(tableId, snapshotId, requestedKinds, fileRecords);
  }

  private static TargetStatsRecord aggregateTable(
      ResourceId tableId, long snapshotId, List<TargetStatsRecord> fileRecords) {
    long rowCount = 0L;
    long sizeBytes = 0L;
    long dataFileCount = 0L;
    TargetStatsRecord metadataSource = null;

    for (TargetStatsRecord record : fileRecords) {
      FileTargetStats file = record.getFile();
      if (isDeleteFile(file)) {
        continue;
      }
      metadataSource = metadataSource == null ? record : metadataSource;
      rowCount += Math.max(0L, file.getRowCount());
      sizeBytes += Math.max(0L, file.getSizeBytes());
      dataFileCount++;
    }

    if (dataFileCount == 0L && rowCount == 0L && sizeBytes == 0L) {
      return null;
    }

    TargetStatsRecord.Builder builder =
        TargetStatsRecords.tableRecord(
            tableId,
            snapshotId,
            TableValueStats.newBuilder()
                .setRowCount(rowCount)
                .setDataFileCount(dataFileCount)
                .setTotalSizeBytes(sizeBytes)
                .build(),
            null)
            .toBuilder();
    if (metadataSource != null && metadataSource.hasMetadata()) {
      builder.setMetadata(metadataSource.getMetadata());
    }
    return builder.build();
  }

  private static List<TargetStatsRecord> aggregateColumns(
      ResourceId tableId, long snapshotId, List<TargetStatsRecord> fileRecords) {
    LinkedHashMap<Long, ColumnAccumulator> byColumnId = new LinkedHashMap<>();
    for (TargetStatsRecord record : fileRecords) {
      FileTargetStats file = record.getFile();
      if (isDeleteFile(file)) {
        continue;
      }
      for (var fileColumn : file.getColumnsList()) {
        if (!fileColumn.hasScalar() || fileColumn.getColumnId() <= 0L) {
          continue;
        }
        byColumnId
            .computeIfAbsent(fileColumn.getColumnId(), ignored -> new ColumnAccumulator())
            .add(record, fileColumn.getScalar());
      }
    }

    List<TargetStatsRecord> out = new ArrayList<>(byColumnId.size());
    for (Map.Entry<Long, ColumnAccumulator> entry : byColumnId.entrySet()) {
      ScalarStats scalar = entry.getValue().toScalar();
      if (scalar == null) {
        continue;
      }
      TargetStatsRecord.Builder builder =
          TargetStatsRecords.columnRecord(tableId, snapshotId, entry.getKey(), scalar, null)
              .toBuilder();
      if (entry.getValue().metadataSource != null
          && entry.getValue().metadataSource.hasMetadata()) {
        builder.setMetadata(entry.getValue().metadataSource.getMetadata());
      }
      out.add(builder.build());
    }
    return List.copyOf(out);
  }

  private static boolean isDeleteFile(FileTargetStats file) {
    return file != null
        && (file.getFileContent() == FileContent.FC_POSITION_DELETES
            || file.getFileContent() == FileContent.FC_EQUALITY_DELETES);
  }

  private static final class ColumnAccumulator {
    private TargetStatsRecord metadataSource;
    private String displayName = "";
    private String logicalType = "";
    private long valueCount = 0L;
    private Long nullCount;
    private Long nanCount;
    private String min;
    private String max;
    private final ColumnNdv ndv = new ColumnNdv();
    private Ndv singleSourceNdv;
    private int scalarContributors = 0;

    void add(TargetStatsRecord source, ScalarStats scalar) {
      scalarContributors++;
      if (source != null && metadataSource == null && source.hasMetadata()) {
        metadataSource = source;
      }
      if (scalar.getDisplayName() != null
          && !scalar.getDisplayName().isBlank()
          && displayName.isBlank()) {
        displayName = scalar.getDisplayName();
      }
      if (scalar.getLogicalType() != null
          && !scalar.getLogicalType().isBlank()
          && logicalType.isBlank()) {
        logicalType = scalar.getLogicalType();
      }
      valueCount += Math.max(0L, scalar.getValueCount());
      if (scalar.hasNullCount()) {
        nullCount = (nullCount == null ? 0L : nullCount) + Math.max(0L, scalar.getNullCount());
      }
      if (scalar.hasNanCount()) {
        nanCount = (nanCount == null ? 0L : nanCount) + Math.max(0L, scalar.getNanCount());
      }
      min = lowerEncoded(logicalType, min, scalar.hasMin() ? scalar.getMin() : null);
      max = higherEncoded(logicalType, max, scalar.hasMax() ? scalar.getMax() : null);
      mergeNdv(scalar);
    }

    ScalarStats toScalar() {
      ScalarStats.Builder builder =
          ScalarStats.newBuilder().setValueCount(valueCount).setDisplayName(displayName);
      if (!logicalType.isBlank()) {
        builder.setLogicalType(logicalType);
      }
      if (nullCount != null) {
        builder.setNullCount(nullCount);
      }
      if (nanCount != null) {
        builder.setNanCount(nanCount);
      }
      if (min != null) {
        builder.setMin(min);
      }
      if (max != null) {
        builder.setMax(max);
      }
      Ndv aggregatedNdv = aggregateNdv();
      if (aggregatedNdv != null) {
        builder.setNdv(aggregatedNdv);
      }
      return builder.build();
    }

    private void mergeNdv(ScalarStats scalar) {
      if (!scalar.hasNdv()) {
        return;
      }
      if (scalarContributors == 1) {
        singleSourceNdv = scalar.getNdv();
      } else {
        singleSourceNdv = null;
      }
      for (var sketch : scalar.getNdv().getSketchesList()) {
        String type = sketch.getType() == null ? "" : sketch.getType().toLowerCase();
        if (type.contains("datasketches") && type.contains("theta")) {
          ndv.mergeTheta(sketch.getData().toByteArray());
        }
      }
    }

    private Ndv aggregateNdv() {
      ndv.finalizeTheta();
      if (ndv.approx != null || (ndv.sketches != null && !ndv.sketches.isEmpty())) {
        Ndv.Builder builder = Ndv.newBuilder();
        if (ndv.approx != null) {
          NdvApprox.Builder approx = NdvApprox.newBuilder();
          if (ndv.approx.estimate != null) {
            approx.setEstimate(ndv.approx.estimate);
          }
          if (ndv.approx.rse != null) {
            approx.setRelativeStandardError(ndv.approx.rse);
          }
          if (ndv.approx.ciLower != null) {
            approx.setConfidenceLower(ndv.approx.ciLower);
          }
          if (ndv.approx.ciUpper != null) {
            approx.setConfidenceUpper(ndv.approx.ciUpper);
          }
          if (ndv.approx.ciLevel != null) {
            approx.setConfidenceLevel(ndv.approx.ciLevel);
          }
          if (ndv.approx.rowsSeen != null) {
            approx.setRowsSeen(ndv.approx.rowsSeen);
          }
          if (ndv.approx.rowsTotal != null) {
            approx.setRowsTotal(ndv.approx.rowsTotal);
          }
          if (ndv.approx.method != null) {
            approx.setMethod(ndv.approx.method);
          }
          if (ndv.approx.params != null && !ndv.approx.params.isEmpty()) {
            approx.putAllProperties(ndv.approx.params);
          }
          builder.setApprox(approx);
        }
        if (ndv.sketches != null) {
          for (var sketch : ndv.sketches) {
            builder.addSketches(
                ai.floedb.floecat.catalog.rpc.NdvSketch.newBuilder()
                    .setType(sketch.type == null ? "" : sketch.type)
                    .setData(
                        sketch.data == null ? ByteString.EMPTY : ByteString.copyFrom(sketch.data))
                    .setEncoding(sketch.encoding == null ? "" : sketch.encoding)
                    .setCompression(sketch.compression == null ? "" : sketch.compression)
                    .setVersion(sketch.version == null ? 0 : sketch.version)
                    .putAllProperties(sketch.params == null ? Map.of() : sketch.params)
                    .build());
          }
        }
        return builder.build();
      }
      return scalarContributors == 1 && singleSourceNdv != null ? singleSourceNdv : null;
    }

    private static String lowerEncoded(String logicalType, String current, String candidate) {
      if (candidate == null || candidate.isBlank()) {
        return current;
      }
      if (current == null || current.isBlank()) {
        return candidate;
      }
      LogicalType type = LogicalTypeProtoAdapter.decodeLogicalType(logicalType);
      if (!LogicalComparators.isStatsOrderable(type)) {
        return current;
      }
      try {
        return LogicalTypeProtoAdapter.compareEncoded(type, candidate, current) < 0
            ? candidate
            : current;
      } catch (RuntimeException ignored) {
        return current;
      }
    }

    private static String higherEncoded(String logicalType, String current, String candidate) {
      if (candidate == null || candidate.isBlank()) {
        return current;
      }
      if (current == null || current.isBlank()) {
        return candidate;
      }
      LogicalType type = LogicalTypeProtoAdapter.decodeLogicalType(logicalType);
      if (!LogicalComparators.isStatsOrderable(type)) {
        return current;
      }
      try {
        return LogicalTypeProtoAdapter.compareEncoded(type, candidate, current) > 0
            ? candidate
            : current;
      } catch (RuntimeException ignored) {
        return current;
      }
    }
  }
}
