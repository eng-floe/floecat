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

import java.util.List;

interface RemotePlannerWorkerClient {
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

  boolean submitPlanTableSuccess(RemoteLeasedJob lease, List<PlannedSnapshotJob> snapshotJobs);

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

  boolean submitPlanSnapshotSuccess(RemoteLeasedJob lease, List<PlannedFileGroupJob> fileGroupJobs);

  boolean submitPlanSnapshotFailure(
      RemoteLeasedJob lease,
      ReconcileExecutor.ExecutionResult.FailureKind failureKind,
      ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition,
      ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      String message);
}
