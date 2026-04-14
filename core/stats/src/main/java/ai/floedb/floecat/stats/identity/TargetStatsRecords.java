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
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.StatsMetadata;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;

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
    FileTargetStats normalized =
        fileStats.toBuilder().setTableId(tableId).setSnapshotId(snapshotId).build();
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
}
