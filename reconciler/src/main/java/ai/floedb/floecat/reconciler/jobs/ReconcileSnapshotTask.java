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
    List<ReconcileFileGroupTask> fileGroups) {

  public ReconcileSnapshotTask {
    tableId = tableId == null ? "" : tableId.trim();
    snapshotId = Math.max(0L, snapshotId);
    sourceNamespace = sourceNamespace == null ? "" : sourceNamespace.trim();
    sourceTable = sourceTable == null ? "" : sourceTable.trim();
    fileGroups =
        fileGroups == null
            ? List.of()
            : fileGroups.stream().filter(group -> group != null && !group.isEmpty()).toList();
  }

  public static ReconcileSnapshotTask of(
      String tableId, long snapshotId, String sourceNamespace, String sourceTable) {
    return of(tableId, snapshotId, sourceNamespace, sourceTable, List.of());
  }

  public static ReconcileSnapshotTask of(
      String tableId,
      long snapshotId,
      String sourceNamespace,
      String sourceTable,
      List<ReconcileFileGroupTask> fileGroups) {
    if ((tableId == null || tableId.isBlank())
        && snapshotId <= 0L
        && (sourceNamespace == null || sourceNamespace.isBlank())
        && (sourceTable == null || sourceTable.isBlank())
        && (fileGroups == null || fileGroups.isEmpty())) {
      return empty();
    }
    return new ReconcileSnapshotTask(tableId, snapshotId, sourceNamespace, sourceTable, fileGroups);
  }

  public static ReconcileSnapshotTask empty() {
    return new ReconcileSnapshotTask("", 0L, "", "", List.of());
  }

  @JsonIgnore
  public boolean isEmpty() {
    return tableId.isBlank()
        && snapshotId == 0L
        && sourceNamespace.isBlank()
        && sourceTable.isBlank()
        && fileGroups.isEmpty();
  }
}
