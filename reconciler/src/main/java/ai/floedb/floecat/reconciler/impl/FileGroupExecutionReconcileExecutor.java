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
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileIndexArtifactResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcileExecutor;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class FileGroupExecutionReconcileExecutor implements ReconcileExecutor {
  private final boolean enabled;
  private final ReconcileJobStore jobs;
  private final ReconcilerBackend backend;

  @Inject
  public FileGroupExecutionReconcileExecutor(
      ReconcileJobStore jobs,
      ReconcilerBackend backend,
      @ConfigProperty(
              name = "floecat.reconciler.executor.file-group.enabled",
              defaultValue = "true")
          boolean enabled) {
    this.jobs = jobs;
    this.backend = backend;
    this.enabled = enabled;
  }

  @Override
  public String id() {
    return "file_group_reconciler";
  }

  @Override
  public boolean enabled() {
    return enabled;
  }

  @Override
  public int priority() {
    return 30;
  }

  @Override
  public Set<ReconcileJobKind> supportedJobKinds() {
    return EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP);
  }

  @Override
  public Set<String> supportedLanes() {
    return Set.of();
  }

  @Override
  public boolean supportsLane(String lane) {
    return true;
  }

  @Override
  public boolean supports(ReconcileJobStore.LeasedJob lease) {
    return lease != null && lease.jobKind == ReconcileJobKind.EXEC_FILE_GROUP;
  }

  @Override
  public ExecutionResult execute(ExecutionContext context) {
    var lease = context.lease();
    if (lease == null || lease.jobKind != ReconcileJobKind.EXEC_FILE_GROUP) {
      return ExecutionResult.failure(
          0, 0, 0, 0, 1, 0, 0, "Unsupported reconcile job kind", new IllegalArgumentException());
    }
    ReconcileFileGroupTask task =
        lease.fileGroupTask == null ? ReconcileFileGroupTask.empty() : lease.fileGroupTask;
    if (task.isEmpty() || task.tableId().isBlank()) {
      return ExecutionResult.failure(
          0,
          0,
          0,
          0,
          1,
          0,
          0,
          "file group reference is required for EXEC_FILE_GROUP jobs",
          new IllegalArgumentException("file group task is required"));
    }
    Optional<ReconcileFileGroupTask> resolvedTask = resolvePlannedTask(lease, task);
    if (resolvedTask.isEmpty()) {
      return ExecutionResult.failure(
          0,
          0,
          0,
          0,
          1,
          0,
          0,
          "planned file group could not be resolved from parent snapshot plan",
          new IllegalStateException("planned file group could not be resolved"));
    }
    ReconcileFileGroupTask plannedTask = resolvedTask.orElseThrow();
    if (plannedTask.filePaths().isEmpty()) {
      return ExecutionResult.failure(
          0,
          0,
          0,
          0,
          1,
          0,
          0,
          "planned file group does not contain any file handles",
          new IllegalStateException("planned file group does not contain any file handles"));
    }
    if (context.shouldStop().getAsBoolean()) {
      return ExecutionResult.cancelled(0, 0, 0, 0, 0, 0, 0, "Cancelled");
    }
    ReconcileContext reconcileContext = reconcileContext(lease);
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(lease.accountId)
            .setKind(ResourceKind.RK_TABLE)
            .setId(plannedTask.tableId())
            .build();
    try {
      var stats =
          backend.capturePlannedFileGroupStats(
              reconcileContext, tableId, plannedTask.snapshotId(), plannedTask.filePaths());
      var pageIndexEntries =
          backend.capturePlannedFileGroupPageIndexEntries(
              reconcileContext, tableId, plannedTask.snapshotId(), plannedTask.filePaths());
      var artifacts =
          backend.materializePlannedFileGroupIndexArtifacts(
              reconcileContext,
              tableId,
              plannedTask.snapshotId(),
              plannedTask.filePaths(),
              stats,
              pageIndexEntries);
      if (!stats.isEmpty()) {
        backend.putTargetStats(reconcileContext, stats);
      }
      if (!artifacts.isEmpty()) {
        backend.putIndexArtifacts(reconcileContext, artifacts);
      }
      jobs.persistFileGroupResult(
          lease.jobId,
          plannedTask.withFileResults(fileResultsForSuccess(plannedTask, stats, artifacts)));
      long statsProcessed = stats.size();
      context
          .progressListener()
          .onProgress(
              0,
              0,
              0,
              0,
              0,
              0,
              statsProcessed,
              "Executed file group "
                  + plannedTask.groupId()
                  + " with "
                  + plannedTask.filePaths().size()
                  + " planned handles"
                  + (statsProcessed > 0 ? " and " + statsProcessed + " captured stats" : ""));
      return ExecutionResult.success(
          0, 0, 0, 0, 0, 0, statsProcessed, "Executed file group " + plannedTask.groupId());
    } catch (RuntimeException e) {
      jobs.persistFileGroupResult(
          lease.jobId, plannedTask.withFileResults(fileResultsForFailure(plannedTask, e)));
      return ExecutionResult.failure(
          0, 0, 0, 0, 1, 0, 0, "File-group capture failed: " + e.getMessage(), e);
    }
  }

  private static List<ReconcileFileResult> fileResultsForSuccess(
      ReconcileFileGroupTask plannedTask,
      List<TargetStatsRecord> stats,
      List<ReconcilerBackend.StagedIndexArtifact> artifacts) {
    LinkedHashMap<String, Long> statsByFile = new LinkedHashMap<>();
    HashMap<String, ReconcileIndexArtifactResult> artifactsByFile = new HashMap<>();
    for (String filePath : plannedTask.filePaths()) {
      statsByFile.put(filePath, 0L);
    }
    for (ReconcilerBackend.StagedIndexArtifact artifact : artifacts) {
      if (artifact == null) {
        continue;
      }
      var record = artifact.record();
      if (!record.hasTarget() || !record.getTarget().hasFile()) {
        continue;
      }
      String filePath = record.getTarget().getFile().getFilePath();
      if (filePath == null || filePath.isBlank()) {
        continue;
      }
      artifactsByFile.put(
          filePath,
          ReconcileIndexArtifactResult.of(
              record.getArtifactUri(),
              record.getArtifactFormat(),
              record.getArtifactFormatVersion()));
    }
    for (TargetStatsRecord record : stats) {
      if (!record.hasFile()) {
        continue;
      }
      String filePath = record.getFile().getFilePath();
      if (filePath == null || filePath.isBlank() || !statsByFile.containsKey(filePath)) {
        continue;
      }
      statsByFile.computeIfPresent(filePath, (ignored, count) -> count + 1L);
    }
    return statsByFile.entrySet().stream()
        .map(
            entry ->
                ReconcileFileResult.succeeded(
                    entry.getKey(),
                    entry.getValue(),
                    artifactsByFile.getOrDefault(
                        entry.getKey(), ReconcileIndexArtifactResult.empty())))
        .toList();
  }

  private static List<ReconcileFileResult> fileResultsForFailure(
      ReconcileFileGroupTask plannedTask, RuntimeException failure) {
    String message = failure == null || failure.getMessage() == null ? "" : failure.getMessage();
    return plannedTask.filePaths().stream()
        .map(filePath -> ReconcileFileResult.failed(filePath, message))
        .toList();
  }

  private Optional<ReconcileFileGroupTask> resolvePlannedTask(
      ReconcileJobStore.LeasedJob lease, ReconcileFileGroupTask task) {
    if (!task.filePaths().isEmpty()) {
      return Optional.of(task);
    }
    if (lease.parentJobId == null || lease.parentJobId.isBlank()) {
      return Optional.empty();
    }
    return jobs.get(lease.accountId, lease.parentJobId)
        .map(parent -> parent.snapshotTask)
        .filter(snapshotTask -> snapshotTask != null && !snapshotTask.isEmpty())
        .flatMap(snapshotTask -> findPlannedGroup(snapshotTask, task));
  }

  private static Optional<ReconcileFileGroupTask> findPlannedGroup(
      ReconcileSnapshotTask snapshotTask, ReconcileFileGroupTask groupRef) {
    return snapshotTask.fileGroups().stream()
        .filter(group -> group != null && !group.isEmpty())
        .filter(group -> group.groupId().equals(groupRef.groupId()))
        .filter(group -> group.planId().equals(groupRef.planId()))
        .findFirst();
  }

  private ReconcileContext reconcileContext(ReconcileJobStore.LeasedJob lease) {
    PrincipalContext principal =
        PrincipalContext.newBuilder()
            .setAccountId(lease.accountId)
            .setSubject("reconciler.scheduler")
            .setCorrelationId("reconciler-job-" + lease.jobId)
            .build();
    return new ReconcileContext(
        "reconciler-job-" + lease.jobId, principal, id(), Instant.now(), Optional.empty());
  }
}
