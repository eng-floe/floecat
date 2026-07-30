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
import ai.floedb.floecat.catalog.rpc.StatsCoverage;
import ai.floedb.floecat.catalog.rpc.StatsMetadata;
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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;

/**
 * Derives complete table/column/file target stats for a file-group from file-target records.
 *
 * <p>This keeps the file-group capture contract self-contained: callers receive complete
 * persistable outputs for the requested file group even when the underlying connector only emits
 * file-scoped primitives.
 */
public final class FileGroupTargetStatsRollup {
  private static final String WIDTH_WEIGHT_ROWS_PROPERTY = "floecat.rollup.avg_width_weight_rows";
  private static final String HLL_SKETCH_TYPE = "apache-datasketches-hll-v1";
  private static final String TUPLE_SKETCH_TYPE = "floedb-tuple-v2";
  private static final int DEFAULT_HLL_LG_K = 12;
  private static final int MIN_HLL_LG_K = 4;
  private static final int MAX_HLL_LG_K = 21;
  private static final int DEFAULT_TUPLE_NOMINAL_ENTRIES = 4096;
  private static final int MIN_TUPLE_NOMINAL_ENTRIES = 16;
  private static final int MAX_TUPLE_NOMINAL_ENTRIES = 1 << 26;

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
    MetadataAccumulator metadata = new MetadataAccumulator();

    for (TargetStatsRecord record : fileRecords) {
      FileTargetStats file = record.getFile();
      if (isDeleteFile(file)) {
        continue;
      }
      metadata.add(record);
      rowCount += Math.max(0L, file.getRowCount());
      sizeBytes += Math.max(0L, file.getSizeBytes());
      dataFileCount++;
    }

    if (dataFileCount == 0L && rowCount == 0L && sizeBytes == 0L) {
      return null;
    }

    return buildTableRecord(
        tableId, snapshotId, rowCount, dataFileCount, sizeBytes, metadata.finish());
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
    MetadataAccumulator metadata = new MetadataAccumulator();
    boolean sawTable = false;
    for (TargetStatsRecord record : partials) {
      if (record == null || !record.hasTable()) {
        continue;
      }
      TableValueStats table = record.getTable();
      sawTable = true;
      metadata.add(record);
      rowCount += Math.max(0L, table.getRowCount());
      sizeBytes += Math.max(0L, table.getTotalSizeBytes());
      dataFileCount += Math.max(0L, table.getDataFileCount());
    }
    if (!sawTable) {
      return null;
    }
    return buildTableRecord(
        tableId, snapshotId, rowCount, dataFileCount, sizeBytes, metadata.finish());
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
      StatsMetadata metadata) {
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
    if (metadata != null) {
      builder.setMetadata(metadata);
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
      StatsMetadata metadata = entry.getValue().metadata.finish();
      if (metadata != null) {
        builder.setMetadata(metadata);
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

  private static long saturatedNonnegativeAdd(long left, long right) {
    long normalizedLeft = Math.max(0L, left);
    long normalizedRight = Math.max(0L, right);
    return normalizedLeft > Long.MAX_VALUE - normalizedRight
        ? Long.MAX_VALUE
        : normalizedLeft + normalizedRight;
  }

  private static final class MetadataAccumulator {
    private StatsMetadata base;
    private StatsCoverage coverageBase;
    private boolean rowsScannedPresent;
    private boolean filesScannedPresent;
    private boolean rowGroupsSampledPresent;
    private boolean bytesScannedPresent;
    private long rowsScanned;
    private long filesScanned;
    private long rowGroupsSampled;
    private long bytesScanned;

    void add(TargetStatsRecord record) {
      if (record == null || !record.hasMetadata()) {
        return;
      }
      StatsMetadata metadata = record.getMetadata();
      if (base == null) {
        base = metadata;
      }
      if (!metadata.hasCoverage()) {
        return;
      }
      StatsCoverage coverage = metadata.getCoverage();
      if (coverageBase == null) {
        coverageBase = coverage;
      }
      if (coverage.hasRowsScanned()) {
        rowsScannedPresent = true;
        rowsScanned = saturatedNonnegativeAdd(rowsScanned, coverage.getRowsScanned());
      }
      if (coverage.hasFilesScanned()) {
        filesScannedPresent = true;
        filesScanned = saturatedNonnegativeAdd(filesScanned, coverage.getFilesScanned());
      }
      if (coverage.hasRowGroupsSampled()) {
        rowGroupsSampledPresent = true;
        rowGroupsSampled =
            saturatedNonnegativeAdd(rowGroupsSampled, coverage.getRowGroupsSampled());
      }
      if (coverage.hasBytesScanned()) {
        bytesScannedPresent = true;
        bytesScanned = saturatedNonnegativeAdd(bytesScanned, coverage.getBytesScanned());
      }
    }

    StatsMetadata finish() {
      if (base == null || coverageBase == null) {
        return base;
      }
      StatsCoverage.Builder coverage = coverageBase.toBuilder();
      if (rowsScannedPresent) coverage.setRowsScanned(rowsScanned);
      if (filesScannedPresent) coverage.setFilesScanned(filesScanned);
      if (rowGroupsSampledPresent) coverage.setRowGroupsSampled(rowGroupsSampled);
      if (bytesScannedPresent) coverage.setBytesScanned(bytesScanned);
      return base.toBuilder().setCoverage(coverage).build();
    }
  }

  private static final class ColumnAccumulator {
    private final MetadataAccumulator metadata = new MetadataAccumulator();
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
    private org.apache.datasketches.hll.Union hllUnion;
    private Integer hllLgK;
    private TupleNdvUnion tupleUnion;
    private Integer tupleNominalEntries;
    private boolean hasTheta;
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
      metadata.add(source);
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
          hasTheta = true;
          ndv.mergeTheta(data);
        } else if (sketch.getRole() == ai.floedb.floecat.catalog.rpc.SketchRole.SKETCH_ROLE_NDV
            && type.equals(HLL_SKETCH_TYPE)) {
          mergeHll(sketch);
        } else if (sketch.getRole()
                == ai.floedb.floecat.catalog.rpc.SketchRole.SKETCH_ROLE_TUPLE_NDV
            && type.equals(TUPLE_SKETCH_TYPE)) {
          mergeTuple(sketch);
        }
      }
    }

    private void mergeHll(ai.floedb.floecat.catalog.rpc.SketchPayload sketch) {
      int incomingLgK =
          normalizedIntParam(
              sketch.getParamsMap(), "lg_k", DEFAULT_HLL_LG_K, MIN_HLL_LG_K, MAX_HLL_LG_K);
      try {
        HllSketch incoming = HllSketch.heapify(sketch.getData().toByteArray());
        if (hllUnion == null) {
          hllLgK = incomingLgK;
          hllUnion = new org.apache.datasketches.hll.Union(incomingLgK);
        }
        hllUnion.update(incoming);
      } catch (RuntimeException ignored) {
        // Rust's HLL union ignores payloads it cannot deserialize. Keep the reference behavior
        // identical so an opaque or corrupt optional sketch does not discard usable scalar stats.
      }
    }

    private void mergeTuple(ai.floedb.floecat.catalog.rpc.SketchPayload sketch) {
      int incomingNominalEntries =
          normalizedPowerOfTwoParam(
              sketch.getParamsMap(),
              "nominal_entries",
              DEFAULT_TUPLE_NOMINAL_ENTRIES,
              MIN_TUPLE_NOMINAL_ENTRIES,
              MAX_TUPLE_NOMINAL_ENTRIES);
      int effectiveEntries =
          tupleNominalEntries == null
              ? incomingNominalEntries
              : Math.min(tupleNominalEntries, incomingNominalEntries);
      if (tupleUnion == null || tupleNominalEntries != effectiveEntries) {
        byte[] previous = tupleUnion == null ? null : tupleUnion.serialize();
        tupleUnion = new TupleNdvUnion(effectiveEntries);
        tupleNominalEntries = effectiveEntries;
        if (previous != null) {
          tupleUnion.update(previous);
        }
      }
      tupleUnion.update(sketch.getData().toByteArray());
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
      HllSketch hllResult = hllUnion == null ? null : hllUnion.getResult(TgtHllType.HLL_8);
      byte[] tupleResult = tupleUnion == null ? null : tupleUnion.serialize();
      if (hasTheta || hllResult != null || tupleResult != null) {
        Ndv.Builder builder = Ndv.newBuilder();
        NdvApprox.Builder approx = NdvApprox.newBuilder();
        if (hasTheta && ndv.approx != null && ndv.approx.estimate != null) {
          approx.setEstimate(ndv.approx.estimate).setMethod("apache-datasketches-theta");
          setIfPresent(ndv.approx.rse, approx::setRelativeStandardError);
        } else if (hllResult != null) {
          int effectiveLgK = hllLgK == null ? DEFAULT_HLL_LG_K : hllLgK;
          approx
              .setEstimate(hllResult.getEstimate())
              .setRelativeStandardError(1.04 / Math.sqrt(1L << effectiveLgK))
              .setMethod("apache-datasketches-hll");
        } else if (tupleUnion != null) {
          approx.setEstimate(tupleUnion.estimate()).setMethod("floedb-tuple");
        }
        if (ndvRowsSeen > 0) approx.setRowsSeen(ndvRowsSeen);
        if (ndvRowsTotal > 0) approx.setRowsTotal(ndvRowsTotal);
        builder.setApprox(approx);
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
        if (hllResult != null) {
          builder.addSketches(
              ai.floedb.floecat.catalog.rpc.SketchPayload.newBuilder()
                  .setRole(ai.floedb.floecat.catalog.rpc.SketchRole.SKETCH_ROLE_NDV)
                  .setSketchType(HLL_SKETCH_TYPE)
                  .setData(ByteString.copyFrom(hllResult.toCompactByteArray()))
                  .putParams("lg_k", Integer.toString(hllLgK == null ? DEFAULT_HLL_LG_K : hllLgK))
                  .setCompleteness(StatsCompleteness.SC_COMPLETE));
        }
        if (tupleResult != null) {
          builder.addSketches(
              ai.floedb.floecat.catalog.rpc.SketchPayload.newBuilder()
                  .setRole(ai.floedb.floecat.catalog.rpc.SketchRole.SKETCH_ROLE_TUPLE_NDV)
                  .setSketchType(TUPLE_SKETCH_TYPE)
                  .setData(ByteString.copyFrom(tupleResult))
                  .putParams("nominal_entries", Integer.toString(tupleNominalEntries))
                  .putParams("version", "2")
                  .setCompleteness(StatsCompleteness.SC_COMPLETE));
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

  private static int normalizedIntParam(
      Map<String, String> params, String name, int defaultValue, int minimum, int maximum) {
    String encoded = params == null ? null : params.get(name);
    if (encoded == null || encoded.isBlank()) {
      return defaultValue;
    }
    try {
      return Math.max(minimum, Math.min(maximum, Integer.parseInt(encoded)));
    } catch (NumberFormatException ignored) {
      return defaultValue;
    }
  }

  private static int normalizedPowerOfTwoParam(
      Map<String, String> params, String name, int defaultValue, int minimum, int maximum) {
    int value = normalizedIntParam(params, name, defaultValue, minimum, maximum);
    int highest = Integer.highestOneBit(value);
    int rounded = highest == value ? value : highest << 1;
    return Math.max(minimum, Math.min(maximum, rounded));
  }

  /** Java implementation of the Rust {@code floedb-tuple-v2} union wire contract. */
  private static final class TupleNdvUnion {
    private static final long MAX_THETA = Long.MAX_VALUE;

    private final int nominalEntries;
    private final NavigableMap<Long, TupleSummary> entries = new TreeMap<>();
    private long theta = MAX_THETA;
    private long rowsHashed;

    TupleNdvUnion(int nominalEntries) {
      this.nominalEntries = nominalEntries;
    }

    void update(byte[] serialized) {
      if (serialized == null || serialized.length < 24) {
        return;
      }
      ByteBuffer input = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);
      long incomingTheta = input.getLong();
      long incomingRowsHashed = input.getLong();
      long entryCount = input.getLong();
      if (incomingTheta <= 0 || incomingRowsHashed < 0 || entryCount < 0) {
        return;
      }

      rowsHashed = saturatedNonnegativeAdd(rowsHashed, incomingRowsHashed);
      theta = Math.min(theta, incomingTheta);
      entries.tailMap(theta, true).clear();
      for (long i = 0; i < entryCount; i++) {
        if (input.remaining() < 24) {
          break;
        }
        long hash = input.getLong();
        long count = input.getLong();
        long sumWidth = input.getLong();
        if (hash > 0 && hash < theta && count >= 0 && sumWidth >= 0) {
          entries.computeIfAbsent(hash, ignored -> new TupleSummary()).add(count, sumWidth);
        }
      }
      while (entries.size() > nominalEntries) {
        long removedHash = entries.lastKey();
        entries.pollLastEntry();
        theta = Math.min(theta, removedHash);
      }
    }

    byte[] serialize() {
      ByteBuffer output =
          ByteBuffer.allocate(24 + entries.size() * 24).order(ByteOrder.LITTLE_ENDIAN);
      output.putLong(theta);
      output.putLong(rowsHashed);
      output.putLong(entries.size());
      for (Map.Entry<Long, TupleSummary> entry : entries.entrySet()) {
        output.putLong(entry.getKey());
        output.putLong(entry.getValue().count);
        output.putLong(entry.getValue().sumWidth);
      }
      return output.array();
    }

    double estimate() {
      double fraction = (double) theta / (double) MAX_THETA;
      return fraction >= 1.0 ? entries.size() : entries.size() / fraction;
    }
  }

  private static final class TupleSummary {
    private long count;
    private long sumWidth;

    void add(long incomingCount, long incomingSumWidth) {
      count = saturatedNonnegativeAdd(count, incomingCount);
      sumWidth = saturatedNonnegativeAdd(sumWidth, incomingSumWidth);
    }
  }
}
