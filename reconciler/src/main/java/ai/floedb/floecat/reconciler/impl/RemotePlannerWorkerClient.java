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

import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import java.util.List;

interface RemotePlannerWorkerClient {
  default int planTableChunkMaxCount() {
    return 8;
  }

  default int planTableChunkTargetBytes() {
    return 128 * 1024;
  }

  default int estimatedPlanTableChunkItemBytes(
      RemoteLeasedJob lease, PlannedSnapshotJob snapshotJob) {
    return 1;
  }

  record PlanViewSubmitResult(boolean accepted, long viewsChanged) {}

  StandalonePlanConnectorPayload getPlanConnectorInput(RemoteLeasedJob lease);

  boolean submitPlanConnectorSuccess(
      RemoteLeasedJob lease, List<PlannedTableJob> tableJobs, List<PlannedViewJob> viewJobs);

  boolean submitPlanConnectorFailure(
      RemoteLeasedJob lease,
      ReconcileExecutor.ExecutionResult.FailureKind failureKind,
      ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition,
      ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      String message);

  StandalonePlanTablePayload getPlanTableInput(RemoteLeasedJob lease);

  boolean submitPlanTableChunk(
      RemoteLeasedJob lease, int chunkIndex, List<PlannedSnapshotJob> snapshotJobs);

  /**
   * @param plannedSnapshotJobs snapshot jobs submitted across {@code chunkCount} chunks. The
   *     planner reports its own total because each chunk was acknowledged synchronously and its
   *     children enqueued before the planner moved on; the service must not have to re-read the
   *     staged chunk records, which are not meant to outlive a long planning phase.
   */
  boolean submitPlanTableSuccess(
      RemoteLeasedJob lease,
      int chunkCount,
      long plannedSnapshotJobs,
      long tablesScanned,
      long tablesChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed);

  boolean submitPlanTableFailure(
      RemoteLeasedJob lease,
      ReconcileExecutor.ExecutionResult.FailureKind failureKind,
      ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition,
      ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      String message);

  StandalonePlanViewPayload getPlanViewInput(RemoteLeasedJob lease);

  PlanViewSubmitResult submitPlanViewSuccess(RemoteLeasedJob lease, PlannedViewMutation mutation);

  boolean submitPlanViewFailure(
      RemoteLeasedJob lease,
      ReconcileExecutor.ExecutionResult.FailureKind failureKind,
      ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition,
      ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      String message);

  StandalonePlanSnapshotPayload getPlanSnapshotInput(RemoteLeasedJob lease);

  boolean submitPlanSnapshotSuccess(
      RemoteLeasedJob lease,
      ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask snapshotTask,
      List<PlannedFileGroupJob> fileGroupJobs,
      List<TargetStatsRecord> directStats);

  boolean submitAppendOnlyPlanSnapshotSuccess(
      RemoteLeasedJob lease,
      ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask snapshotTask,
      List<PlannedFileGroupJob> fileGroupJobs,
      List<TargetStatsRecord> directStats,
      SnapshotPlanBlobStore.AppendOnlyBase appendOnlyBase);

  boolean submitPlanSnapshotFailure(
      RemoteLeasedJob lease,
      ReconcileExecutor.ExecutionResult.FailureKind failureKind,
      ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition,
      ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      String message);
}
