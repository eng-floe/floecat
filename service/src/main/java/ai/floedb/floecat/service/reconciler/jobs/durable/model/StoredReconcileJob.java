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

package ai.floedb.floecat.service.reconciler.jobs.durable.model;

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import java.util.Map;

public class StoredReconcileJob {
  public String jobId;
  public String accountId;
  public String connectorId;
  public String jobKind;
  public String parentJobId;
  public boolean fullRescan;
  public String captureMode;
  public String executionClass;
  public String executionLane;
  public Map<String, String> executionAttributes = Map.of();
  public String pinnedExecutorId;
  public String executorId;
  public String snapshotTaskTableId;
  public long snapshotTaskSnapshotId;
  public String snapshotTaskSourceNamespace;
  public String snapshotTaskSourceTable;
  public boolean snapshotTaskFileGroupPlanRecorded;
  public String snapshotTaskCompletionMode;
  public int snapshotTaskSourceFileCount;
  public String snapshotTaskDirectStatsBlobUri;
  public int snapshotTaskDirectStatsRecordCount;
  public Map<Integer, Integer> snapshotTaskDirectStatsPersistedRecordCountsByChunk = Map.of();
  public String fileGroupPlanId;
  public String fileGroupGroupId;
  public String fileGroupTableId;
  public long fileGroupSnapshotId;
  public int fileGroupFileCount;
  public String fileGroupResultBlobUri;
  public StoredJobDefinition definition = new StoredJobDefinition();
  public String snapshotPlanBlobUri;
  public String state;
  public String message;
  public long startedAtMs;
  public long finishedAtMs;
  public long tablesScanned;
  public long tablesChanged;
  public long viewsScanned;
  public long viewsChanged;
  public long errors;
  public long snapshotsProcessed;
  public long statsProcessed;
  public long indexesProcessed;
  public long plannedFileGroups;
  public long plannedFiles;
  public long completedFileGroups;
  public long failedFileGroups;
  public long completedFiles;
  public long failedFiles;
  public long expectedDirectChildren;
  public boolean childrenFinalized;
  public long projectionRequestedGeneration;
  public long projectionAppliedGeneration;
  public int attempt;
  public long nextAttemptAtMs;
  public String lastError;
  public String laneKey;
  public String dedupeKeyHash;
  public String readyPointerKey;
  public String connectorIndexPointerKey;
  public String canonicalPointerKey;
  public long createdAtMs;
  public long updatedAtMs;

  public CaptureMode captureMode() {
    if (captureMode == null || captureMode.isBlank()) {
      throw new IllegalStateException("reconcile job missing capture mode");
    }
    try {
      return CaptureMode.valueOf(captureMode);
    } catch (IllegalArgumentException ignored) {
      throw new IllegalStateException("reconcile job has invalid capture mode: " + captureMode);
    }
  }

  public ReconcileJobKind jobKind() {
    return ReconcileJobKind.fromString(jobKind);
  }

  public ReconcileExecutionPolicy executionPolicy() {
    return ReconcileExecutionPolicy.of(
        ReconcileExecutionClass.fromString(executionClass), executionLane, executionAttributes);
  }

  public String pinnedExecutorId() {
    return pinnedExecutorId == null ? "" : pinnedExecutorId;
  }

  public String executorId() {
    return executorId == null ? "" : executorId;
  }

  public String parentJobId() {
    return parentJobId == null ? "" : parentJobId;
  }
}
