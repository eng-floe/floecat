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

package ai.floedb.floecat.reconciler.jobs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;

public record ReconcileSnapshotTask(
    String tableId,
    long snapshotId,
    String sourceNamespace,
    String sourceTable,
    List<ReconcileFileGroupTask> fileGroups,
    boolean fileGroupPlanRecorded,
    CompletionMode completionMode,
    String fileGroupPlanBlobUri,
    int fileGroupCount) {

  public enum CompletionMode {
    FILE_GROUPS,
    DIRECT_STATS;

    public static CompletionMode fromString(String value) {
      if (value == null || value.isBlank()) {
        return FILE_GROUPS;
      }
      try {
        return CompletionMode.valueOf(value.trim());
      } catch (IllegalArgumentException ignored) {
        return FILE_GROUPS;
      }
    }
  }

  public ReconcileSnapshotTask {
    tableId = tableId == null ? "" : tableId.trim();
    snapshotId = snapshotId < -1L ? -1L : snapshotId;
    sourceNamespace = sourceNamespace == null ? "" : sourceNamespace.trim();
    sourceTable = sourceTable == null ? "" : sourceTable.trim();
    fileGroups =
        fileGroups == null
            ? List.of()
            : fileGroups.stream().filter(group -> group != null && !group.isEmpty()).toList();
    completionMode = completionMode == null ? CompletionMode.FILE_GROUPS : completionMode;
    fileGroupPlanBlobUri = fileGroupPlanBlobUri == null ? "" : fileGroupPlanBlobUri.trim();
    fileGroupCount = Math.max(0, fileGroupCount);
    if (fileGroupCount == 0 && !fileGroups.isEmpty()) {
      fileGroupCount = fileGroups.size();
    }
  }

  public static ReconcileSnapshotTask of(
      String tableId, long snapshotId, String sourceNamespace, String sourceTable) {
    return of(
        tableId,
        snapshotId,
        sourceNamespace,
        sourceTable,
        List.of(),
        false,
        CompletionMode.FILE_GROUPS,
        "",
        0);
  }

  public static ReconcileSnapshotTask of(
      String tableId,
      long snapshotId,
      String sourceNamespace,
      String sourceTable,
      List<ReconcileFileGroupTask> fileGroups) {
    return of(
        tableId,
        snapshotId,
        sourceNamespace,
        sourceTable,
        fileGroups,
        false,
        CompletionMode.FILE_GROUPS,
        "",
        0);
  }

  public static ReconcileSnapshotTask of(
      String tableId,
      long snapshotId,
      String sourceNamespace,
      String sourceTable,
      List<ReconcileFileGroupTask> fileGroups,
      boolean fileGroupPlanRecorded) {
    return of(
        tableId,
        snapshotId,
        sourceNamespace,
        sourceTable,
        fileGroups,
        fileGroupPlanRecorded,
        CompletionMode.FILE_GROUPS,
        "",
        fileGroups == null ? 0 : fileGroups.size());
  }

  public static ReconcileSnapshotTask of(
      String tableId,
      long snapshotId,
      String sourceNamespace,
      String sourceTable,
      List<ReconcileFileGroupTask> fileGroups,
      boolean fileGroupPlanRecorded,
      CompletionMode completionMode) {
    return of(
        tableId,
        snapshotId,
        sourceNamespace,
        sourceTable,
        fileGroups,
        fileGroupPlanRecorded,
        completionMode,
        "",
        fileGroups == null ? 0 : fileGroups.size());
  }

  public static ReconcileSnapshotTask of(
      String tableId,
      long snapshotId,
      String sourceNamespace,
      String sourceTable,
      List<ReconcileFileGroupTask> fileGroups,
      boolean fileGroupPlanRecorded,
      CompletionMode completionMode,
      String fileGroupPlanBlobUri,
      int fileGroupCount) {
    if ((tableId == null || tableId.isBlank())
        && snapshotId < 0L
        && (sourceNamespace == null || sourceNamespace.isBlank())
        && (sourceTable == null || sourceTable.isBlank())
        && (fileGroups == null || fileGroups.isEmpty())
        && !fileGroupPlanRecorded
        && (completionMode == null || completionMode == CompletionMode.FILE_GROUPS)
        && (fileGroupPlanBlobUri == null || fileGroupPlanBlobUri.isBlank())
        && fileGroupCount <= 0) {
      return empty();
    }
    return new ReconcileSnapshotTask(
        tableId,
        snapshotId,
        sourceNamespace,
        sourceTable,
        fileGroups,
        fileGroupPlanRecorded,
        completionMode,
        fileGroupPlanBlobUri,
        fileGroupCount);
  }

  public static ReconcileSnapshotTask empty() {
    return new ReconcileSnapshotTask(
        "", -1L, "", "", List.of(), false, CompletionMode.FILE_GROUPS, "", 0);
  }

  @JsonIgnore
  public boolean isEmpty() {
    return tableId.isBlank()
        && snapshotId < 0L
        && sourceNamespace.isBlank()
        && sourceTable.isBlank()
        && fileGroups.isEmpty()
        && !fileGroupPlanRecorded
        && completionMode == CompletionMode.FILE_GROUPS
        && fileGroupPlanBlobUri.isBlank()
        && fileGroupCount <= 0;
  }
}
