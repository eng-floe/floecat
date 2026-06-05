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

package ai.floedb.floecat.service.reconciler.impl;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import io.grpc.Status;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class LeasedSnapshotFinalizeInputService {
  @Inject ReconcileJobStore jobs;
  @Inject SnapshotFinalizeChildStateService childStateService;

  record SnapshotFinalizeGroupManifest(
      String planId, String groupId, String fileStatsBlobUri, int fileStatsRecordCount) {}

  record SnapshotFinalizeInput(
      String jobId,
      String leaseEpoch,
      String parentJobId,
      ResourceId tableId,
      long snapshotId,
      List<SnapshotFinalizeGroupManifest> completedGroups) {}

  public SnapshotFinalizeInput resolve(
      PrincipalContext principalContext, String jobId, String leaseEpoch) {
    ReconcileJobStore.LeasedJob lease =
        requireLeasedSnapshotFinalizeJob(principalContext.getCorrelationId(), jobId, leaseEpoch);
    ReconcileSnapshotTask snapshotTask =
        lease.snapshotTask == null ? ReconcileSnapshotTask.empty() : lease.snapshotTask;
    if (snapshotTask.isEmpty() || lease.parentJobId == null || lease.parentJobId.isBlank()) {
      throw Status.FAILED_PRECONDITION
          .withDescription("snapshot finalization requires parent snapshot plan job")
          .asRuntimeException();
    }
    SnapshotFinalizeChildStateService.ChildState childState =
        childStateService.childState(
            lease.accountId, lease.parentJobId, lease.jobId, snapshotTask.fileGroups());
    requireReadyForFinalize(childState);
    return new SnapshotFinalizeInput(
        lease.jobId,
        lease.leaseEpoch,
        lease.parentJobId,
        tableId(lease, snapshotTask),
        snapshotTask.snapshotId(),
        completedGroupManifests(childState));
  }

  private static ResourceId tableId(
      ReconcileJobStore.LeasedJob lease, ReconcileSnapshotTask snapshotTask) {
    return ResourceId.newBuilder()
        .setAccountId(lease.accountId == null ? "" : lease.accountId)
        .setKind(ResourceKind.RK_TABLE)
        .setId(snapshotTask.tableId())
        .build();
  }

  private List<SnapshotFinalizeGroupManifest> completedGroupManifests(
      SnapshotFinalizeChildStateService.ChildState childState) {
    List<SnapshotFinalizeGroupManifest> manifests = new ArrayList<>();
    for (ReconcileFileGroupTask persistedGroup : childState.completedGroupTasks()) {
      if (persistedGroup == null || persistedGroup.fileStatsBlobUri().isBlank()) {
        throw Status.FAILED_PRECONDITION
            .withDescription(
                "snapshot finalization found succeeded file-group jobs without persisted stats blobs")
            .asRuntimeException();
      }
      manifests.add(
          new SnapshotFinalizeGroupManifest(
              persistedGroup.planId(),
              persistedGroup.groupId(),
              persistedGroup.fileStatsBlobUri(),
              persistedGroup.fileStatsRecordCount()));
    }
    return List.copyOf(manifests);
  }

  private ReconcileJobStore.LeasedJob requireLeasedSnapshotFinalizeJob(
      String corr, String jobId, String leaseEpoch) {
    boolean renewed = jobs.renewLease(jobId, leaseEpoch);
    if (!renewed) {
      throw Status.FAILED_PRECONDITION
          .withDescription("reconcile lease is no longer valid")
          .asRuntimeException();
    }
    ReconcileJobStore.ReconcileJob job =
        jobs.getLeaseView(jobId)
            .orElseThrow(
                () ->
                    Status.NOT_FOUND
                        .withDescription("reconcile job not found " + jobId)
                        .asRuntimeException());
    if (job.jobKind != ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE) {
      throw Status.FAILED_PRECONDITION
          .withDescription("reconcile job is not a FINALIZE_SNAPSHOT_CAPTURE job")
          .asRuntimeException();
    }
    if (!isActiveLeasedState(job.state)) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "reconcile job is no longer active for lease "
                  + jobId
                  + " state="
                  + (job.state == null ? "" : job.state))
          .asRuntimeException();
    }
    return new ReconcileJobStore.LeasedJob(
        job.jobId,
        job.accountId,
        job.connectorId,
        job.fullRescan,
        job.captureMode,
        job.scope,
        job.executionPolicy,
        leaseEpoch,
        "",
        job.executorId,
        job.jobKind,
        job.tableTask,
        job.viewTask,
        job.snapshotTask,
        job.fileGroupTask,
        job.parentJobId);
  }

  private static void requireReadyForFinalize(
      SnapshotFinalizeChildStateService.ChildState childState) {
    if (!childState.duplicateGroups().isEmpty()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "snapshot finalization found duplicate EXEC_FILE_GROUP children for planned groups "
                  + childState.duplicateGroups())
          .asRuntimeException();
    }
    if (!childState.invalidSucceededGroups().isEmpty()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "snapshot finalization found succeeded file-group jobs without persisted success"
                  + " results "
                  + childState.invalidSucceededGroups())
          .asRuntimeException();
    }
    if (!childState.failedGroups().isEmpty()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "snapshot finalization blocked by failed file-group jobs "
                  + childState.failedGroups())
          .asRuntimeException();
    }
    if (!childState.cancelledGroups().isEmpty()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "snapshot finalization blocked by cancelled file-group jobs "
                  + childState.cancelledGroups())
          .asRuntimeException();
    }
    if (!childState.pendingGroups().isEmpty()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "snapshot finalization waiting for snapshot file groups "
                  + childState.completedGroups()
                  + "/"
                  + childState.expectedGroups()
                  + " pending="
                  + childState.pendingGroups())
          .asRuntimeException();
    }
    if (!childState.missingGroups().isEmpty()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "snapshot finalization missing EXEC_FILE_GROUP children for planned groups "
                  + childState.missingGroups())
          .asRuntimeException();
    }
  }

  private static boolean isActiveLeasedState(String state) {
    return "JS_RUNNING".equals(state) || "JS_CANCELLING".equals(state);
  }
}
