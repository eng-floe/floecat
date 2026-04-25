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

public record ReconcileFileGroupTask(
    String planId,
    String groupId,
    String tableId,
    long snapshotId,
    List<String> filePaths,
    List<ReconcileFileResult> fileResults) {

  public ReconcileFileGroupTask {
    planId = planId == null ? "" : planId.trim();
    groupId = groupId == null ? "" : groupId.trim();
    tableId = tableId == null ? "" : tableId.trim();
    snapshotId = Math.max(0L, snapshotId);
    filePaths =
        filePaths == null
            ? List.of()
            : filePaths.stream()
                .filter(path -> path != null && !path.isBlank())
                .map(String::trim)
                .toList();
    fileResults =
        fileResults == null
            ? List.of()
            : fileResults.stream().filter(result -> result != null && !result.isEmpty()).toList();
  }

  public static ReconcileFileGroupTask of(
      String planId,
      String groupId,
      String tableId,
      long snapshotId,
      List<String> filePaths,
      List<ReconcileFileResult> fileResults) {
    if ((planId == null || planId.isBlank())
        && (groupId == null || groupId.isBlank())
        && (tableId == null || tableId.isBlank())
        && snapshotId <= 0L
        && (filePaths == null || filePaths.isEmpty())
        && (fileResults == null || fileResults.isEmpty())) {
      return empty();
    }
    return new ReconcileFileGroupTask(planId, groupId, tableId, snapshotId, filePaths, fileResults);
  }

  public static ReconcileFileGroupTask of(
      String planId, String groupId, String tableId, long snapshotId, List<String> filePaths) {
    return of(planId, groupId, tableId, snapshotId, filePaths, List.of());
  }

  public static ReconcileFileGroupTask empty() {
    return new ReconcileFileGroupTask("", "", "", 0L, List.of(), List.of());
  }

  public ReconcileFileGroupTask asReference() {
    if (isEmpty()) {
      return this;
    }
    return new ReconcileFileGroupTask(planId, groupId, tableId, snapshotId, List.of(), List.of());
  }

  public ReconcileFileGroupTask withFileResults(List<ReconcileFileResult> fileResults) {
    return new ReconcileFileGroupTask(planId, groupId, tableId, snapshotId, filePaths, fileResults);
  }

  @JsonIgnore
  public boolean isEmpty() {
    return planId.isBlank()
        && groupId.isBlank()
        && tableId.isBlank()
        && snapshotId == 0L
        && filePaths.isEmpty()
        && fileResults.isEmpty();
  }
}
