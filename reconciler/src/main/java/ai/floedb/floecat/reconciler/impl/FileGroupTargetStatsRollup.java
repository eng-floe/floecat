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
import ai.floedb.floecat.catalog.rpc.StatsCompleteness;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
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
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.jboss.logging.Logger;

/**
 * Derives complete table/column/file target stats for a file-group from file-target records.
 *
 * <p>This keeps the file-group capture contract self-contained: callers receive complete
 * persistable outputs for the requested file group even when the underlying connector only emits
 * file-scoped primitives.
 */
public final class FileGroupTargetStatsRollup {
  private static final Logger LOG = Logger.getLogger(FileGroupTargetStatsRollup.class);
  private static final String WIDTH_WEIGHT_ROWS_PROPERTY = "floecat.rollup.avg_width_weight_rows";

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

  public static List<TargetStatsRecord> partialAggregatesFromFileRecords(
      ResourceId tableId,
      long snapshotId,
      Set<FloecatConnector.StatsTargetKind> requestedKinds,
      List<TargetStatsRecord> fileRecords) {
    return completeSnapshotFromFileRecords(tableId, snapshotId, requestedKinds, fileRecords)
        .stream()
        .filter(record -> record != null && !record.hasFile())
        .toList();
  }

  public static List<TargetStatsRecord> mergeSnapshotAggregatePartials(
      ResourceId tableId,
      long snapshotId,
      Set<FloecatConnector.StatsTargetKind> requestedKinds,
      List<TargetStatsRecord> partials) {
    if (partials == null || partials.isEmpty()) {
      return List.of();
    }
    LinkedHashMap<String, TargetStatsRecord> merged = new LinkedHashMap<>();
    if (requestedKinds.contains(FloecatConnector.StatsTargetKind.TABLE)) {
      TargetStatsRecord tableRecord = aggregateTableFromPartials(tableId, snapshotId, partials);
      if (tableRecord != null) {
        merged.put(
            ai.floedb.floecat.stats.identity.StatsTargetIdentity.storageId(tableRecord.getTarget()),
            tableRecord);
      }
    }
    if (requestedKinds.contains(FloecatConnector.StatsTargetKind.COLUMN)) {
      for (TargetStatsRecord columnRecord :
          aggregateColumnsFromPartials(tableId, snapshotId, partials)) {
        merged.put(
            ai.floedb.floecat.stats.identity.StatsTargetIdentity.storageId(
                columnRecord.getTarget()),
            columnRecord);
      }
    }
    return List.copyOf(merged.values());
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

    return buildTableRecord(
        tableId, snapshotId, rowCount, dataFileCount, sizeBytes, metadataSource);
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

    return columnRecordsFromAccumulators(tableId, snapshotId, byColumnId);
  }

  private static TargetStatsRecord aggregateTableFromPartials(
      ResourceId tableId, long snapshotId, List<TargetStatsRecord> partials) {
    long rowCount = 0L;
    long sizeBytes = 0L;
    long dataFileCount = 0L;
    TargetStatsRecord metadataSource = null;
    boolean sawTable = false;
    for (TargetStatsRecord record : partials) {
      if (record == null || !record.hasTable()) {
        continue;
      }
      TableValueStats table = record.getTable();
      sawTable = true;
      metadataSource = metadataSource == null ? record : metadataSource;
      rowCount += Math.max(0L, table.getRowCount());
      sizeBytes += Math.max(0L, table.getTotalSizeBytes());
      dataFileCount += Math.max(0L, table.getDataFileCount());
    }
    if (!sawTable) {
      return null;
    }
    return buildTableRecord(
        tableId, snapshotId, rowCount, dataFileCount, sizeBytes, metadataSource);
  }

  private static List<TargetStatsRecord> aggregateColumnsFromPartials(
      ResourceId tableId, long snapshotId, List<TargetStatsRecord> partials) {
    LinkedHashMap<Long, ColumnAccumulator> byColumnId = new LinkedHashMap<>();
    for (TargetStatsRecord record : partials) {
      if (record == null || !record.hasScalar()) {
        continue;
      }
      StatsTarget target = record.getTarget();
      if (target == null || !target.hasColumn() || target.getColumn().getColumnId() <= 0L) {
        continue;
      }
      byColumnId
          .computeIfAbsent(target.getColumn().getColumnId(), ignored -> new ColumnAccumulator())
          .add(record, record.getScalar());
    }
    return columnRecordsFromAccumulators(tableId, snapshotId, byColumnId);
  }

  private static TargetStatsRecord buildTableRecord(
      ResourceId tableId,
      long snapshotId,
      long rowCount,
      long dataFileCount,
      long sizeBytes,
      TargetStatsRecord metadataSource) {
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

  private static List<TargetStatsRecord> columnRecordsFromAccumulators(
      ResourceId tableId, long snapshotId, LinkedHashMap<Long, ColumnAccumulator> byColumnId) {
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

    /** Total sources folded into this column (files or partials), sketch-bearing or not. */
    private int contributors = 0;

    /**
     * The contributing source's stats, held verbatim while it remains the ONLY contributor; {@code
     * null} once a second source folds in.
     *
     * <p>This is the single sole-source rule of the rollup: producer-owned sketch payloads are
     * never merged, re-encoded, or synthesized here — a payload from one of several sources
     * describes only that source's rows and must not be advertised as the column's distribution.
     * But when exactly one source contributed the whole column, its payloads ARE the column's
     * distribution, so {@link #toScalar} propagates its scalar sketches and {@link #aggregateNdv}
     * its entire NDV envelope untouched.
     */
    private ScalarStats soleSource;

    /** Cached decoded form of logicalType — decoding is not free, so cache after first decode. */
    private LogicalType decodedLogicalType = null;

    private long rowCount = 0L;
    private Long nullCount;
    private Long nanCount;
    private String min;
    private String max;
    private final ColumnNdv ndv = new ColumnNdv();
    private final Set<String> droppedNonThetaNdvTypes = new java.util.LinkedHashSet<>();
    private Double fallbackNdvEstimate;
    private long ndvRowsSeen = 0L;
    private long ndvRowsTotal = 0L;
    /* Weighted-average avg_width accumulation across files. */
    private long totalWidthBytes = 0L;
    private long totalRowsForWidth = 0L;

    void add(TargetStatsRecord source, ScalarStats scalar) {
      contributors++;
      // Exactly one contributor means its stats describe the whole column (see soleSource); a
      // second contributor voids that claim for good.
      soleSource = contributors == 1 ? scalar : null;
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
        decodedLogicalType = null; /* invalidate cache when type string changes */
      }
      rowCount += Math.max(0L, scalar.getRowCount());
      if (scalar.hasNullCount()) nullCount = accumulate(nullCount, scalar.getNullCount());
      if (scalar.hasNanCount()) nanCount = accumulate(nanCount, scalar.getNanCount());
      if (decodedLogicalType == null && !logicalType.isBlank()) {
        decodedLogicalType = LogicalTypeProtoAdapter.decodeLogicalType(logicalType);
      }
      min = pickEncoded(decodedLogicalType, min, scalar.hasMin() ? scalar.getMin() : null, true);
      max = pickEncoded(decodedLogicalType, max, scalar.hasMax() ? scalar.getMax() : null, false);
      mergeNdv(scalar);
      /* Guard > 0: files with avg_width_bytes=0 (e.g. all-null columns or zero-row files)
       * are excluded from the weighted average to prevent them from pulling the result toward 0. */
      if (scalar.hasAvgWidthBytes() && scalar.getAvgWidthBytes() > 0 && scalar.getRowCount() > 0) {
        long widthRows = widthWeightRows(scalar);
        if (widthRows > 0) {
          totalWidthBytes += scalar.getAvgWidthBytes() * widthRows;
          totalRowsForWidth += widthRows;
        }
      }
    }

    private static long widthWeightRows(ScalarStats scalar) {
      String encoded = scalar.getPropertiesOrDefault(WIDTH_WEIGHT_ROWS_PROPERTY, "");
      if (!encoded.isBlank()) {
        try {
          return Math.max(0L, Long.parseLong(encoded));
        } catch (NumberFormatException ignored) {
          return 0L;
        }
      }
      return Math.max(0L, scalar.getRowCount());
    }

    ScalarStats toScalar() {
      ScalarStats.Builder builder = ScalarStats.newBuilder().setDisplayName(displayName);
      builder.setRowCount(rowCount);
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
      if (soleSource != null) {
        // Sole-source rule: propagated verbatim — never re-encoded, never merged.
        builder.addAllSketches(soleSource.getSketchesList());
      }
      if (totalRowsForWidth > 0) {
        builder.setAvgWidthBytes(
            Math.max(1L, (totalWidthBytes + totalRowsForWidth - 1) / totalRowsForWidth));
        builder.putProperties(WIDTH_WEIGHT_ROWS_PROPERTY, Long.toString(totalRowsForWidth));
      }
      return builder.build();
    }

    private void mergeNdv(ScalarStats scalar) {
      if (!scalar.hasNdv()) {
        return;
      }
      Ndv currentNdv = scalar.getNdv();
      collectNdvFallback(currentNdv);
      for (var sketch : currentNdv.getSketchesList()) {
        String type =
            sketch.getSketchType() == null ? "" : sketch.getSketchType().toLowerCase(Locale.ROOT);
        byte[] data = sketch.getData().toByteArray();
        if (type.contains("theta")) {
          ndv.mergeTheta(data);
        } else if (!type.isBlank()) {
          // Only theta sketches can be unioned here; HLL/tuple NDV sketches from a multi-file group
          // are not mergeable, so they are not folded in. Record the type so aggregateNdv can flag
          // that the emitted estimate is a floor for mixed-format inputs rather than dropping it
          // silently. (Irrelevant for a sole source, whose envelope is returned verbatim.)
          droppedNonThetaNdvTypes.add(sketch.getSketchType());
        }
      }
    }

    private void collectNdvFallback(Ndv ndvValue) {
      double estimate;
      if (ndvValue.hasApprox() && ndvValue.getApprox().getEstimate() > 0.0) {
        estimate = ndvValue.getApprox().getEstimate();
      } else if (ndvValue.hasExact()) {
        estimate = ndvValue.getExact();
      } else {
        estimate = Double.NaN;
      }
      if (!Double.isNaN(estimate)) {
        fallbackNdvEstimate =
            fallbackNdvEstimate == null ? estimate : Math.max(fallbackNdvEstimate, estimate);
      }
      if (ndvValue.hasApprox()) {
        NdvApprox approx = ndvValue.getApprox();
        if (approx.getRowsSeen() > 0) {
          ndvRowsSeen += Math.max(0L, approx.getRowsSeen());
        }
        if (approx.getRowsTotal() > 0) {
          ndvRowsTotal += Math.max(0L, approx.getRowsTotal());
        }
      }
    }

    private Ndv aggregateNdv() {
      if (soleSource != null) {
        // Sole-source rule: the one contributor's envelope IS the column's — estimate, theta, and
        // producer-owned payloads (tuple, HLL, …) propagate verbatim, never re-encoded or merged.
        return soleSource.hasNdv() ? soleSource.getNdv() : null;
      }
      ndv.finalizeTheta();
      if (!droppedNonThetaNdvTypes.isEmpty()) {
        // Non-theta NDV sketches across multiple files could not be merged, so the aggregate
        // estimate reflects only the theta-bearing (or rollup-max) subset — a floor, not exact.
        LOG.warnf(
            "NDV rollup could not merge non-theta sketch type(s) %s across %d sources; "
                + "aggregate NDV estimate is a floor for this column",
            droppedNonThetaNdvTypes, contributors);
      }
      if (ndv.approx != null || (ndv.sketches != null && !ndv.sketches.isEmpty())) {
        Ndv.Builder builder = Ndv.newBuilder();
        if (ndv.approx != null) {
          if (ndv.approx.rowsSeen == null && ndvRowsSeen > 0) {
            ndv.approx.rowsSeen = ndvRowsSeen;
          }
          if (ndv.approx.rowsTotal == null && ndvRowsTotal > 0) {
            ndv.approx.rowsTotal = ndvRowsTotal;
          }
          NdvApprox.Builder approx = NdvApprox.newBuilder();
          setIfPresent(ndv.approx.estimate, approx::setEstimate);
          setIfPresent(ndv.approx.rse, approx::setRelativeStandardError);
          setIfPresent(ndv.approx.ciLower, approx::setConfidenceLower);
          setIfPresent(ndv.approx.ciUpper, approx::setConfidenceUpper);
          setIfPresent(ndv.approx.ciLevel, approx::setConfidenceLevel);
          setIfPresent(ndv.approx.rowsSeen, approx::setRowsSeen);
          setIfPresent(ndv.approx.rowsTotal, approx::setRowsTotal);
          setIfPresent(ndv.approx.method, approx::setMethod);
          if (ndv.approx.params != null && !ndv.approx.params.isEmpty()) {
            approx.putAllProperties(ndv.approx.params);
          }
          builder.setApprox(approx);
        }
        if (ndv.sketches != null) {
          for (var sketch : ndv.sketches) {
            var sb =
                ai.floedb.floecat.catalog.rpc.SketchPayload.newBuilder()
                    .setRole(ai.floedb.floecat.catalog.rpc.SketchRole.SKETCH_ROLE_NDV)
                    .setCapturedAtMs(System.currentTimeMillis())
                    .setCompleteness(StatsCompleteness.SC_COMPLETE)
                    .setSketchType(sketch.type == null ? "" : sketch.type)
                    .setData(
                        sketch.data == null ? ByteString.EMPTY : ByteString.copyFrom(sketch.data));
            if (sketch.encoding != null) sb.putParams("encoding", sketch.encoding);
            if (sketch.compression != null) sb.putParams("compression", sketch.compression);
            if (sketch.version != null) sb.putParams("version", String.valueOf(sketch.version));
            if (sketch.params != null && !sketch.params.isEmpty()) sb.putAllParams(sketch.params);
            builder.addSketches(sb.build());
          }
        }
        return builder.build();
      }
      if (fallbackNdvEstimate != null) {
        NdvApprox.Builder approx =
            NdvApprox.newBuilder().setEstimate(fallbackNdvEstimate).setMethod("rollup-max");
        if (ndvRowsSeen > 0) approx.setRowsSeen(ndvRowsSeen);
        if (ndvRowsTotal > 0) approx.setRowsTotal(ndvRowsTotal);
        return Ndv.newBuilder().setApprox(approx).build();
      }
      return null;
    }

    /**
     * Picks the lower ({@code wantLower}) or higher encoded value of {@code current}/{@code
     * candidate}.
     */
    private static String pickEncoded(
        LogicalType type, String current, String candidate, boolean wantLower) {
      if (candidate == null || candidate.isBlank()) return current;
      if (current == null || current.isBlank()) return candidate;
      if (type == null || !LogicalComparators.isStatsOrderable(type)) return current;
      try {
        int cmp = LogicalTypeProtoAdapter.compareEncoded(type, candidate, current);
        return (wantLower ? cmp < 0 : cmp > 0) ? candidate : current;
      } catch (RuntimeException ignored) {
        return current;
      }
    }

    /**
     * Accumulates a nullable running sum: starts at null (absent), then sums non-negative values.
     */
    private static Long accumulate(Long current, long incoming) {
      return (current == null ? 0L : current) + Math.max(0L, incoming);
    }

    /**
     * Sets a proto builder field only when the value is non-null; eliminates if/set boilerplate.
     */
    private static <T> void setIfPresent(T value, java.util.function.Consumer<T> setter) {
      if (value != null) setter.accept(value);
    }
  }
}
