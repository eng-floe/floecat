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

package ai.floedb.floecat.stats.identity;

import ai.floedb.floecat.catalog.rpc.EngineExpressionStatsTarget;
import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.NdvApprox;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.SketchPayload;
import ai.floedb.floecat.catalog.rpc.StatsCoverage;
import ai.floedb.floecat.catalog.rpc.StatsMetadata;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.catalog.rpc.UpstreamStamp;
import ai.floedb.floecat.common.rpc.ResourceId;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

/** Shared factory methods for canonical target-scoped stats records. */
public final class TargetStatsRecords {

  private TargetStatsRecords() {}

  /**
   * Builds a table-target stats record envelope.
   *
   * @param tableId table resource identifier
   * @param snapshotId table snapshot identifier
   * @param tableValueStats table-level value payload
   * @param metadata optional stats metadata; pass {@code null} to omit
   */
  public static TargetStatsRecord tableRecord(
      ResourceId tableId,
      long snapshotId,
      TableValueStats tableValueStats,
      StatsMetadata metadata) {
    TargetStatsRecord.Builder builder =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setTarget(StatsTargetIdentity.tableTarget())
            .setTable(tableValueStats);
    if (metadata != null) {
      builder.setMetadata(metadata);
    }
    return builder.build();
  }

  /**
   * Builds a column-target stats record envelope.
   *
   * @param tableId table resource identifier
   * @param snapshotId table snapshot identifier
   * @param columnId stable column identifier
   * @param scalarStats scalar payload for the column target
   * @param metadata optional stats metadata; pass {@code null} to omit
   */
  public static TargetStatsRecord columnRecord(
      ResourceId tableId,
      long snapshotId,
      long columnId,
      ScalarStats scalarStats,
      StatsMetadata metadata) {
    TargetStatsRecord.Builder builder =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setTarget(StatsTargetIdentity.columnTarget(columnId))
            .setScalar(scalarStats);
    if (metadata != null) {
      builder.setMetadata(metadata);
    }
    return builder.build();
  }

  /**
   * Builds an expression-target stats record envelope without metadata.
   *
   * @see #expressionRecord(ResourceId, long, EngineExpressionStatsTarget, ScalarStats,
   *     StatsMetadata)
   */
  public static TargetStatsRecord expressionRecord(
      ResourceId tableId,
      long snapshotId,
      EngineExpressionStatsTarget expressionTarget,
      ScalarStats scalarStats) {
    return expressionRecord(tableId, snapshotId, expressionTarget, scalarStats, null);
  }

  /**
   * Builds a file-target stats record envelope without metadata.
   *
   * @see #fileRecord(ResourceId, long, FileTargetStats, StatsMetadata)
   */
  public static TargetStatsRecord fileRecord(
      ResourceId tableId, long snapshotId, FileTargetStats fileStats) {
    return fileRecord(tableId, snapshotId, fileStats, null);
  }

  /**
   * Builds an expression-target stats record envelope.
   *
   * @param tableId table resource identifier
   * @param snapshotId table snapshot identifier
   * @param expressionTarget expression target identity
   * @param scalarStats scalar payload for the expression target
   * @param metadata optional stats metadata; pass {@code null} to omit
   */
  public static TargetStatsRecord expressionRecord(
      ResourceId tableId,
      long snapshotId,
      EngineExpressionStatsTarget expressionTarget,
      ScalarStats scalarStats,
      StatsMetadata metadata) {
    TargetStatsRecord.Builder builder =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setTarget(StatsTargetIdentity.expressionTarget(expressionTarget))
            .setScalar(scalarStats);
    if (metadata != null) {
      builder.setMetadata(metadata);
    }
    return builder.build();
  }

  /**
   * Builds a file-target stats record envelope.
   *
   * <p>The file payload is normalized so {@code table_id} and {@code snapshot_id} embedded in
   * {@code fileStats} match the enclosing record envelope.
   *
   * @param tableId table resource identifier
   * @param snapshotId table snapshot identifier
   * @param fileStats file-level payload
   * @param metadata optional stats metadata; pass {@code null} to omit
   */
  public static TargetStatsRecord fileRecord(
      ResourceId tableId, long snapshotId, FileTargetStats fileStats, StatsMetadata metadata) {
    FileTargetStats normalized = canonicalFileStats(tableId, snapshotId, fileStats);
    TargetStatsRecord.Builder builder =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setTarget(StatsTargetIdentity.fileTarget(normalized.getFilePath()))
            .setFile(normalized);
    if (metadata != null) {
      builder.setMetadata(metadata);
    }
    return builder.build();
  }

  public static TargetStatsRecord canonicalize(TargetStatsRecord record) {
    if (record == null || !record.hasFile()) {
      return record;
    }
    return fileRecord(
        record.getTableId(),
        record.getSnapshotId(),
        record.getFile(),
        record.hasMetadata() ? record.getMetadata() : null);
  }

  /**
   * Returns a copy of {@code record} with volatile operational fields cleared and every {@code map}
   * field reordered by key, for use as the content-hash image behind a content-addressed storage
   * key.
   *
   * <p>Two sources of instability would otherwise make an identical resubmission hash differently —
   * so the stable target pointer could no longer be re-bound to the same blob and the write would
   * fail with a "pointer bound to different blob" conflict:
   *
   * <ul>
   *   <li><b>Wall-clock fields</b> applied at capture time: {@link StatsMetadata} {@code
   *       captured_at}/{@code refreshed_at}, every {@link UpstreamStamp} {@code fetched_at}, and
   *       every {@link SketchPayload} {@code captured_at_ms}.
   *   <li><b>Protobuf {@code map} iteration order</b>: producers build these maps from unordered
   *       maps (e.g. a Rust {@code HashMap}), and protobuf serializes a map in iteration order, so
   *       the same logical map can serialize to different bytes. Every {@code map<string,string>}
   *       reachable from the record — {@code properties} on the record, metadata, coverage,
   *       upstream, table, scalar and NDV-approx payloads, plus {@link SketchPayload} {@code
   *       params} — is rebuilt in key order.
   * </ul>
   *
   * <p>The stored record is untouched; only the hash image is normalized.
   */
  public static TargetStatsRecord contentHashImage(TargetStatsRecord record) {
    if (record == null) {
      return null;
    }
    TargetStatsRecord.Builder builder = record.toBuilder();
    if (builder.hasMetadata()) {
      builder.setMetadata(metadataHashImage(builder.getMetadata()));
    }
    canonicalizeMap(
        builder.getPropertiesMap(), builder::clearProperties, builder::putAllProperties);
    switch (record.getValueCase()) {
      case TABLE -> builder.setTable(tableHashImage(record.getTable()));
      case SCALAR -> builder.setScalar(scalarHashImage(record.getScalar()));
      case FILE -> builder.setFile(fileHashImage(record.getFile()));
      case VALUE_NOT_SET -> {}
    }
    return builder.build();
  }

  /** Rebuilds a string map in key order. No-op for 0/1-entry maps (already order-stable). */
  private static void canonicalizeMap(
      Map<String, String> current, Runnable clear, Consumer<Map<String, String>> putAll) {
    if (current.size() > 1) {
      // Copy BEFORE clearing: getXMap() returns a live view over the builder's map, so clearing
      // first would empty the copy and drop the map from the hash image entirely.
      Map<String, String> sorted = new TreeMap<>(current);
      clear.run();
      putAll.accept(sorted);
    }
  }

  private static StatsMetadata metadataHashImage(StatsMetadata metadata) {
    StatsMetadata.Builder builder = metadata.toBuilder().clearCapturedAt().clearRefreshedAt();
    if (builder.hasCoverage()) {
      StatsCoverage.Builder coverage = builder.getCoverage().toBuilder();
      canonicalizeMap(
          coverage.getPropertiesMap(), coverage::clearProperties, coverage::putAllProperties);
      builder.setCoverage(coverage);
    }
    canonicalizeMap(
        builder.getPropertiesMap(), builder::clearProperties, builder::putAllProperties);
    return builder.build();
  }

  private static TableValueStats tableHashImage(TableValueStats table) {
    TableValueStats.Builder builder = table.toBuilder();
    if (table.hasUpstream()) {
      builder.setUpstream(upstreamHashImage(table.getUpstream()));
    }
    canonicalizeMap(
        builder.getPropertiesMap(), builder::clearProperties, builder::putAllProperties);
    return builder.build();
  }

  private static UpstreamStamp upstreamHashImage(UpstreamStamp upstream) {
    UpstreamStamp.Builder builder = upstream.toBuilder().clearFetchedAt();
    canonicalizeMap(
        builder.getPropertiesMap(), builder::clearProperties, builder::putAllProperties);
    return builder.build();
  }

  private static ScalarStats scalarHashImage(ScalarStats scalar) {
    // display_name is cosmetic (see stats.proto) and MUST NOT affect identity or storage keys;
    // two captures of the same column that differ only in label must share one blob.
    ScalarStats.Builder builder = scalar.toBuilder().clearDisplayName();
    if (scalar.hasUpstream()) {
      builder.setUpstream(upstreamHashImage(scalar.getUpstream()));
    }
    if (scalar.hasNdv()) {
      builder.setNdv(ndvHashImage(scalar.getNdv()));
    }
    for (int i = 0; i < builder.getSketchesCount(); i++) {
      builder.setSketches(i, sketchHashImage(builder.getSketches(i)));
    }
    canonicalizeMap(
        builder.getPropertiesMap(), builder::clearProperties, builder::putAllProperties);
    return builder.build();
  }

  private static Ndv ndvHashImage(Ndv ndv) {
    Ndv.Builder builder = ndv.toBuilder();
    if (builder.hasApprox()) {
      NdvApprox.Builder approx = builder.getApprox().toBuilder();
      canonicalizeMap(approx.getPropertiesMap(), approx::clearProperties, approx::putAllProperties);
      builder.setApprox(approx);
    }
    for (int i = 0; i < builder.getSketchesCount(); i++) {
      builder.setSketches(i, sketchHashImage(builder.getSketches(i)));
    }
    return builder.build();
  }

  private static SketchPayload sketchHashImage(SketchPayload sketch) {
    SketchPayload.Builder builder = sketch.toBuilder().clearCapturedAtMs();
    canonicalizeMap(builder.getParamsMap(), builder::clearParams, builder::putAllParams);
    return builder.build();
  }

  private static FileTargetStats fileHashImage(FileTargetStats file) {
    FileTargetStats.Builder builder = file.toBuilder();
    for (int i = 0; i < builder.getColumnsCount(); i++) {
      FileColumnStats column = builder.getColumns(i);
      // Every column with a scalar must be normalized, not only those carrying an upstream stamp:
      // scalarHashImage also clears sketch captured_at_ms and reorders sketch params, both of which
      // vary per capture. File-group rollups stamp captured_at_ms without an upstream stamp, so
      // gating on hasUpstream() let that wall-clock value leak into the content hash and broke the
      // stable file-target pointer re-bind on recapture.
      if (column.hasScalar()) {
        builder.setColumns(i, column.toBuilder().setScalar(scalarHashImage(column.getScalar())));
      }
    }
    return builder.build();
  }

  private static FileTargetStats canonicalFileStats(
      ResourceId tableId, long snapshotId, FileTargetStats fileStats) {
    FileTargetStats.Builder builder =
        fileStats.toBuilder().setTableId(tableId).setSnapshotId(snapshotId);

    if (fileStats.getEqualityFieldIdsCount() > 1) {
      List<Integer> equalityFieldIds = new ArrayList<>(fileStats.getEqualityFieldIdsList());
      equalityFieldIds.sort(Comparator.naturalOrder());
      builder.clearEqualityFieldIds().addAllEqualityFieldIds(equalityFieldIds);
    }

    if (fileStats.getColumnsCount() > 1) {
      List<FileColumnStats> columns = new ArrayList<>(fileStats.getColumnsList());
      columns.sort(
          Comparator.comparingLong(FileColumnStats::getColumnId)
              .thenComparing(col -> scalarSortKey(col.hasScalar() ? col.getScalar() : null)));
      builder.clearColumns().addAllColumns(columns);
    }
    return builder.build();
  }

  private static String scalarSortKey(ScalarStats scalar) {
    return scalar == null ? "" : Base64.getEncoder().encodeToString(scalar.toByteArray());
  }
}
