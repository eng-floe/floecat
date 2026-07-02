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

import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;

public record ReconcileFileGroupTask(
    String planId,
    String groupId,
    String tableId,
    long snapshotId,
    int fileCount,
    String fileStatsBlobUri,
    int fileStatsRecordCount,
    List<String> filePaths,
    List<ReconcileFileResult> fileResults,
    List<TargetStatsRecord> partialAggregateRecords) {

  public ReconcileFileGroupTask {
    planId = planId == null ? "" : planId.trim();
    groupId = groupId == null ? "" : groupId.trim();
    tableId = tableId == null ? "" : tableId.trim();
    snapshotId = snapshotId < -1L ? -1L : snapshotId;
    fileCount = Math.max(0, fileCount);
    fileStatsBlobUri = fileStatsBlobUri == null ? "" : fileStatsBlobUri.trim();
    fileStatsRecordCount = Math.max(0, fileStatsRecordCount);
    filePaths =
        filePaths == null
            ? List.of()
            : filePaths.stream()
                .filter(path -> path != null && !path.isBlank())
                .map(String::trim)
                .toList();
    if (fileCount == 0 && !filePaths.isEmpty()) {
      fileCount = filePaths.size();
    }
    fileResults =
        fileResults == null
            ? List.of()
            : fileResults.stream().filter(result -> result != null && !result.isEmpty()).toList();
    partialAggregateRecords =
        partialAggregateRecords == null
            ? List.of()
            : partialAggregateRecords.stream()
                .filter(record -> record != null && record.hasTarget())
                .toList();
  }

  public static ReconcileFileGroupTask of(
      String planId,
      String groupId,
      String tableId,
      long snapshotId,
      int fileCount,
      String fileStatsBlobUri,
      int fileStatsRecordCount,
      List<String> filePaths,
      List<ReconcileFileResult> fileResults) {
    return of(
        planId,
        groupId,
        tableId,
        snapshotId,
        fileCount,
        fileStatsBlobUri,
        fileStatsRecordCount,
        filePaths,
        fileResults,
        List.of());
  }

  public static ReconcileFileGroupTask of(
      String planId,
      String groupId,
      String tableId,
      long snapshotId,
      int fileCount,
      String fileStatsBlobUri,
      int fileStatsRecordCount,
      List<String> filePaths,
      List<ReconcileFileResult> fileResults,
      List<TargetStatsRecord> partialAggregateRecords) {
    if ((planId == null || planId.isBlank())
        && (groupId == null || groupId.isBlank())
        && (tableId == null || tableId.isBlank())
        && snapshotId < 0L
        && fileCount <= 0
        && (fileStatsBlobUri == null || fileStatsBlobUri.isBlank())
        && fileStatsRecordCount <= 0
        && (filePaths == null || filePaths.isEmpty())
        && (fileResults == null || fileResults.isEmpty())
        && (partialAggregateRecords == null || partialAggregateRecords.isEmpty())) {
      return empty();
    }
    return new ReconcileFileGroupTask(
        planId,
        groupId,
        tableId,
        snapshotId,
        fileCount,
        fileStatsBlobUri,
        fileStatsRecordCount,
        filePaths,
        fileResults,
        partialAggregateRecords);
  }

  public static ReconcileFileGroupTask of(
      String planId, String groupId, String tableId, long snapshotId, List<String> filePaths) {
    return of(planId, groupId, tableId, snapshotId, 0, "", 0, filePaths, List.of(), List.of());
  }

  public static ReconcileFileGroupTask of(
      String planId,
      String groupId,
      String tableId,
      long snapshotId,
      List<String> filePaths,
      List<ReconcileFileResult> fileResults) {
    return of(planId, groupId, tableId, snapshotId, 0, "", 0, filePaths, fileResults, List.of());
  }

  public static ReconcileFileGroupTask of(
      String planId,
      String groupId,
      String tableId,
      long snapshotId,
      int fileCount,
      List<String> filePaths) {
    return of(
        planId, groupId, tableId, snapshotId, fileCount, "", 0, filePaths, List.of(), List.of());
  }

  public static ReconcileFileGroupTask of(
      String planId,
      String groupId,
      String tableId,
      long snapshotId,
      int fileCount,
      List<String> filePaths,
      List<ReconcileFileResult> fileResults) {
    return of(
        planId, groupId, tableId, snapshotId, fileCount, "", 0, filePaths, fileResults, List.of());
  }

  public static ReconcileFileGroupTask of(
      String planId,
      String groupId,
      String tableId,
      long snapshotId,
      int fileCount,
      List<String> filePaths,
      List<ReconcileFileResult> fileResults,
      List<TargetStatsRecord> partialAggregateRecords) {
    return of(
        planId,
        groupId,
        tableId,
        snapshotId,
        fileCount,
        "",
        0,
        filePaths,
        fileResults,
        partialAggregateRecords);
  }

  public static ReconcileFileGroupTask empty() {
    return new ReconcileFileGroupTask("", "", "", -1L, 0, "", 0, List.of(), List.of(), List.of());
  }

  public ReconcileFileGroupTask asReference() {
    if (isEmpty()) {
      return this;
    }
    return new ReconcileFileGroupTask(
        planId,
        groupId,
        tableId,
        snapshotId,
        fileCount,
        fileStatsBlobUri,
        fileStatsRecordCount,
        List.of(),
        List.of(),
        List.of());
  }

  public ReconcileFileGroupTask withFileResults(List<ReconcileFileResult> fileResults) {
    return new ReconcileFileGroupTask(
        planId,
        groupId,
        tableId,
        snapshotId,
        fileCount,
        fileStatsBlobUri,
        fileStatsRecordCount,
        filePaths,
        fileResults,
        partialAggregateRecords);
  }

  public ReconcileFileGroupTask withFileStatsBlob(String blobUri, int recordCount) {
    return new ReconcileFileGroupTask(
        planId,
        groupId,
        tableId,
        snapshotId,
        fileCount,
        blobUri,
        recordCount,
        filePaths,
        fileResults,
        partialAggregateRecords);
  }

  public ReconcileFileGroupTask withPartialAggregateRecords(
      List<TargetStatsRecord> partialAggregateRecords) {
    return new ReconcileFileGroupTask(
        planId,
        groupId,
        tableId,
        snapshotId,
        fileCount,
        fileStatsBlobUri,
        fileStatsRecordCount,
        filePaths,
        fileResults,
        partialAggregateRecords);
  }

  @JsonIgnore
  public boolean isEmpty() {
    return planId.isBlank()
        && groupId.isBlank()
        && tableId.isBlank()
        && snapshotId < 0L
        && fileCount <= 0
        && fileStatsBlobUri.isBlank()
        && fileStatsRecordCount <= 0
        && filePaths.isEmpty()
        && fileResults.isEmpty()
        && partialAggregateRecords.isEmpty();
  }
}
