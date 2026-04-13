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

package ai.floedb.floecat.service.statistics.impl;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.STATS_INCONSISTENT_TARGET;

import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import java.util.Map;

/** Normalizes and validates target-record identity before stats persistence. */
final class TargetStatsRecordValidator {

  private TargetStatsRecordValidator() {}

  static TargetStatsRecord validateForPut(
      TargetStatsRecord record,
      ResourceId expectedTableId,
      long expectedSnapshotId,
      String correlationId) {
    if (record.hasTableId() && !record.getTableId().equals(expectedTableId)) {
      throw invalid(
          correlationId,
          "table_id",
          "table_id does not match request table",
          expectedTableId.getId(),
          record.getTableId().getId());
    }
    if (record.getSnapshotId() != expectedSnapshotId) {
      throw invalid(
          correlationId,
          "snapshot_id",
          "snapshot_id does not match request snapshot",
          Long.toString(expectedSnapshotId),
          Long.toString(record.getSnapshotId()));
    }
    if (!record.hasTarget()
        || record.getTarget().getTargetCase() == StatsTarget.TargetCase.TARGET_NOT_SET) {
      throw invalid(correlationId, "target", "target must be set");
    }

    StatsTarget normalizedTarget = record.getTarget();
    TargetStatsRecord.Builder normalized =
        record.toBuilder().setTableId(expectedTableId).setSnapshotId(expectedSnapshotId);

    switch (record.getTarget().getTargetCase()) {
      case TABLE -> {
        if (!record.hasTable()) {
          throw invalid(correlationId, "value", "TABLE target requires table payload");
        }
        normalizedTarget = StatsTargetIdentity.tableTarget();
      }
      case COLUMN -> {
        if (!record.hasScalar()) {
          throw invalid(correlationId, "value", "COLUMN target requires scalar payload");
        }
        long columnId = record.getTarget().getColumn().getColumnId();
        try {
          normalizedTarget = StatsTargetIdentity.columnTarget(columnId);
        } catch (IllegalArgumentException e) {
          throw invalid(correlationId, "target.column.column_id", e.getMessage());
        }
      }
      case EXPRESSION -> {
        if (!record.hasScalar()) {
          throw invalid(correlationId, "value", "EXPRESSION target requires scalar payload");
        }
        try {
          normalizedTarget =
              StatsTargetIdentity.expressionTarget(record.getTarget().getExpression());
        } catch (IllegalArgumentException e) {
          throw invalid(correlationId, "target.expression", e.getMessage());
        }
      }
      case FILE -> {
        if (!record.hasFile()) {
          throw invalid(correlationId, "value", "FILE target requires file payload");
        }

        String normalizedTargetPath;
        try {
          normalizedTargetPath =
              StatsTargetIdentity.filePath(record.getTarget().getFile().getFilePath());
          normalizedTarget = StatsTargetIdentity.fileTarget(normalizedTargetPath);
        } catch (IllegalArgumentException e) {
          throw invalid(correlationId, "target.file.file_path", e.getMessage());
        }

        FileTargetStats file = record.getFile();
        if (file.hasTableId() && !file.getTableId().equals(expectedTableId)) {
          throw invalid(
              correlationId,
              "file.table_id",
              "file payload table_id does not match request table",
              expectedTableId.getId(),
              file.getTableId().getId());
        }
        if (file.getSnapshotId() != expectedSnapshotId) {
          throw invalid(
              correlationId,
              "file.snapshot_id",
              "file payload snapshot_id does not match request snapshot",
              Long.toString(expectedSnapshotId),
              Long.toString(file.getSnapshotId()));
        }

        String normalizedFilePath;
        try {
          normalizedFilePath = StatsTargetIdentity.filePath(file.getFilePath());
        } catch (IllegalArgumentException e) {
          throw invalid(correlationId, "file.file_path", e.getMessage());
        }
        if (!normalizedTargetPath.equals(normalizedFilePath)) {
          throw invalid(
              correlationId,
              "file.file_path",
              "file payload path does not match file target path",
              normalizedTargetPath,
              normalizedFilePath);
        }

        normalized.setFile(
            file.toBuilder()
                .setTableId(expectedTableId)
                .setSnapshotId(expectedSnapshotId)
                .setFilePath(normalizedFilePath)
                .build());
      }
      case TARGET_NOT_SET -> throw invalid(correlationId, "target", "target must be set");
    }

    return normalized.setTarget(normalizedTarget).build();
  }

  private static RuntimeException invalid(String correlationId, String field, String reason) {
    return GrpcErrors.invalidArgument(
        correlationId, STATS_INCONSISTENT_TARGET, Map.of("field", field, "reason", reason));
  }

  private static RuntimeException invalid(
      String correlationId, String field, String reason, String expected, String actual) {
    return GrpcErrors.invalidArgument(
        correlationId,
        STATS_INCONSISTENT_TARGET,
        Map.of("field", field, "reason", reason, "expected", expected, "actual", actual));
  }
}
