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

import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@ApplicationScoped
public class SnapshotPlanBlobStore {
  @Inject BlobStore blobStore;
  @Inject ObjectMapper mapper;

  public ReconcileSnapshotTask persistPlan(
      String accountId,
      String jobId,
      ReconcileSnapshotTask snapshotTask,
      List<PlannedFileGroupJob> fileGroupJobs) {
    ReconcileSnapshotTask effective =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    if (effective.completionMode() != ReconcileSnapshotTask.CompletionMode.FILE_GROUPS
        || !effective.fileGroupPlanRecorded()) {
      return effective;
    }
    List<PlannedFileGroupJob> sanitizedJobs =
        fileGroupJobs == null
            ? List.of()
            : fileGroupJobs.stream()
                .filter(
                    job ->
                        job != null
                            && job.fileGroupTask() != null
                            && !job.fileGroupTask().isEmpty())
                .toList();
    String blobUri = buildBlobUri(accountId, jobId);
    try {
      blobStore.put(
          blobUri,
          mapper.writeValueAsBytes(new SnapshotPlanBlob(sanitizedJobs)),
          "application/json; charset=" + StandardCharsets.UTF_8.name());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to persist snapshot plan blob", e);
    }
    return ReconcileSnapshotTask.of(
        effective.tableId(),
        effective.snapshotId(),
        effective.sourceNamespace(),
        effective.sourceTable(),
        List.of(),
        true,
        effective.completionMode(),
        blobUri,
        sanitizedJobs.size());
  }

  public List<PlannedFileGroupJob> loadPlanJobs(ReconcileSnapshotTask snapshotTask) {
    ReconcileSnapshotTask effective =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    if (effective.completionMode() != ReconcileSnapshotTask.CompletionMode.FILE_GROUPS
        || !effective.fileGroupPlanRecorded()) {
      return List.of();
    }
    if (effective.fileGroupCount() == 0) {
      return List.of();
    }
    if (effective.fileGroupPlanBlobUri().isBlank()) {
      throw new IllegalStateException(
          "Missing snapshot plan blob URI for planned file-group snapshot task");
    }
    try {
      return mapper
          .readValue(blobStore.get(effective.fileGroupPlanBlobUri()), SnapshotPlanBlob.class)
          .fileGroupJobs();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to load snapshot plan blob " + effective.fileGroupPlanBlobUri(), e);
    }
  }

  public List<ReconcileFileGroupTask> loadFileGroups(ReconcileSnapshotTask snapshotTask) {
    ReconcileSnapshotTask effective =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    return loadPlanJobs(effective).stream().map(PlannedFileGroupJob::fileGroupTask).toList();
  }

  public Optional<ReconcileFileGroupTask> findFileGroup(
      ReconcileSnapshotTask snapshotTask, ReconcileFileGroupTask groupRef) {
    if (groupRef == null || groupRef.isEmpty()) {
      return Optional.empty();
    }
    return loadFileGroups(snapshotTask).stream()
        .filter(group -> group != null && !group.isEmpty())
        .filter(group -> group.groupId().equals(groupRef.groupId()))
        .filter(group -> group.planId().equals(groupRef.planId()))
        .findFirst();
  }

  public long totalPlannedFiles(ReconcileSnapshotTask snapshotTask) {
    return loadFileGroups(snapshotTask).stream().mapToLong(group -> group.filePaths().size()).sum();
  }

  private static String buildBlobUri(String accountId, String jobId) {
    String acct = accountId == null ? "" : accountId.trim();
    String job = jobId == null ? "" : jobId.trim();
    if (acct.isBlank() || job.isBlank()) {
      throw new IllegalArgumentException(
          "accountId and jobId are required for snapshot plan blobs");
    }
    return "/accounts/"
        + acct
        + "/reconcile/jobs/"
        + job
        + "/snapshot-plan/"
        + UUID.randomUUID()
        + ".json";
  }

  public record SnapshotPlanBlob(List<PlannedFileGroupJob> fileGroupJobs) {
    public SnapshotPlanBlob {
      fileGroupJobs = fileGroupJobs == null ? List.of() : List.copyOf(fileGroupJobs);
    }
  }
}
