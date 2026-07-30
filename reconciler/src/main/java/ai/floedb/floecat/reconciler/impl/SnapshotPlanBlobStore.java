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
import ai.floedb.floecat.reconciler.jobs.ReconcileFileExecutionPlan;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.SnapshotPlanManifestIds;
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
    String blobUri =
        SnapshotPlanManifestIds.manifestBlobUri(
            accountId,
            jobId,
            sanitizedJobs.stream().map(PlannedFileGroupJob::fileGroupTask).toList());
    try {
      blobStore.put(
          blobUri,
          mapper.writeValueAsBytes(SnapshotPlanBlob.of(sanitizedJobs)),
          "application/json; charset=" + StandardCharsets.UTF_8.name());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to persist snapshot plan blob", e);
    }
    int sourceFileCount =
        effective.sourceFileCount() > 0
            ? effective.sourceFileCount()
            : sanitizedJobs.stream()
                .map(PlannedFileGroupJob::fileGroupTask)
                .mapToInt(group -> group.filePaths().size())
                .sum();
    return ReconcileSnapshotTask.of(
        effective.tableId(),
        effective.snapshotId(),
        effective.sourceNamespace(),
        effective.sourceTable(),
        List.of(),
        true,
        effective.completionMode(),
        blobUri,
        sanitizedJobs.size(),
        sourceFileCount,
        effective.directStatsBlobUri(),
        effective.directStatsRecordCount(),
        effective.sourceRevision(),
        effective.metadataFingerprint(),
        effective.requestedCoverage(),
        effective.indexPredecessor());
  }

  public ReconcileSnapshotTask persistDirectStats(
      String accountId,
      String jobId,
      ReconcileSnapshotTask snapshotTask,
      List<TargetStatsRecord> directStats) {
    ReconcileSnapshotTask effective =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    if (effective.completionMode() != ReconcileSnapshotTask.CompletionMode.DIRECT_STATS) {
      return effective;
    }
    List<TargetStatsRecord> sanitizedStats =
        directStats == null
            ? List.of()
            : directStats.stream().filter(java.util.Objects::nonNull).toList();
    String blobUri = buildBlobUri(accountId, jobId, "direct-stats");
    try {
      blobStore.put(
          blobUri,
          mapper.writeValueAsBytes(
              new DirectStatsBlob(
                  sanitizedStats.stream().map(TargetStatsRecord::toByteArray).toList())),
          "application/json; charset=" + StandardCharsets.UTF_8.name());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to persist direct stats blob", e);
    }
    return ReconcileSnapshotTask.of(
        effective.tableId(),
        effective.snapshotId(),
        effective.sourceNamespace(),
        effective.sourceTable(),
        List.of(),
        true,
        effective.completionMode(),
        effective.fileGroupPlanBlobUri(),
        effective.fileGroupCount(),
        effective.sourceFileCount(),
        blobUri,
        sanitizedStats.size(),
        effective.sourceRevision(),
        effective.metadataFingerprint(),
        effective.requestedCoverage(),
        effective.indexPredecessor());
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
    return loadPlanJobs(effective.fileGroupPlanBlobUri());
  }

  public List<PlannedFileGroupJob> loadPlanJobs(String snapshotPlanUri) {
    String effectiveSnapshotPlanUri = snapshotPlanUri == null ? "" : snapshotPlanUri.trim();
    if (effectiveSnapshotPlanUri.isBlank()) {
      throw new IllegalStateException("Missing snapshot plan blob URI");
    }
    try {
      return mapper
          .readValue(blobStore.get(effectiveSnapshotPlanUri), SnapshotPlanBlob.class)
          .toPlannedFileGroupJobs();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to load snapshot plan blob " + effectiveSnapshotPlanUri, e);
    }
  }

  public List<ReconcileFileGroupTask> loadFileGroups(ReconcileSnapshotTask snapshotTask) {
    ReconcileSnapshotTask effective =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    return loadPlanJobs(effective).stream().map(PlannedFileGroupJob::fileGroupTask).toList();
  }

  public List<ReconcileFileGroupTask> loadFileGroupsByUri(String snapshotPlanUri) {
    return loadPlanJobs(snapshotPlanUri).stream().map(PlannedFileGroupJob::fileGroupTask).toList();
  }

  public List<TargetStatsRecord> loadDirectStats(ReconcileSnapshotTask snapshotTask) {
    ReconcileSnapshotTask effective =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    if (effective.completionMode() != ReconcileSnapshotTask.CompletionMode.DIRECT_STATS) {
      return List.of();
    }
    if (effective.directStatsRecordCount() == 0) {
      return List.of();
    }
    if (effective.directStatsBlobUri().isBlank()) {
      throw new IllegalStateException(
          "Missing direct stats blob URI for direct-stats snapshot task");
    }
    try {
      return mapper
          .readValue(blobStore.get(effective.directStatsBlobUri()), DirectStatsBlob.class)
          .records()
          .stream()
          .map(SnapshotPlanBlobStore::parseTargetStatsRecord)
          .toList();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to load direct stats blob " + effective.directStatsBlobUri(), e);
    }
  }

  public List<TargetStatsRecord> loadFileGroupStats(String blobUri) {
    return loadTargetStatsBlob(blobUri, "file-group stats");
  }

  public List<TargetStatsRecord> loadTargetStatsBlob(String blobUri) {
    return loadTargetStatsBlob(blobUri, "target stats");
  }

  private List<TargetStatsRecord> loadTargetStatsBlob(String blobUri, String description) {
    String effectiveBlobUri = blobUri == null ? "" : blobUri.trim();
    if (effectiveBlobUri.isBlank()) {
      return List.of();
    }
    try {
      return mapper
          .readValue(blobStore.get(effectiveBlobUri), DirectStatsBlob.class)
          .records()
          .stream()
          .map(SnapshotPlanBlobStore::parseTargetStatsRecord)
          .toList();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to load " + description + " blob " + effectiveBlobUri, e);
    }
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

  private static String buildBlobUri(String accountId, String jobId, String kind) {
    String acct = accountId == null ? "" : accountId.trim();
    String job = jobId == null ? "" : jobId.trim();
    String safeKind = kind == null ? "" : kind.trim();
    if (acct.isBlank() || job.isBlank()) {
      throw new IllegalArgumentException(
          "accountId and jobId are required for snapshot plan blobs");
    }
    if (safeKind.isBlank()) {
      throw new IllegalArgumentException("blob kind is required for snapshot plan blobs");
    }
    return "/accounts/"
        + acct
        + "/reconcile/jobs/"
        + job
        + "/"
        + safeKind
        + "/"
        + UUID.randomUUID()
        + ".json";
  }

  public static final class SnapshotPlanBlob {
    public List<StoredPlannedFileGroupJob> fileGroupJobs = List.of();

    public static SnapshotPlanBlob of(List<PlannedFileGroupJob> plannedFileGroupJobs) {
      SnapshotPlanBlob blob = new SnapshotPlanBlob();
      List<PlannedFileGroupJob> sanitizedJobs =
          plannedFileGroupJobs == null ? List.of() : List.copyOf(plannedFileGroupJobs);
      blob.fileGroupJobs = sanitizedJobs.stream().map(StoredPlannedFileGroupJob::from).toList();
      return blob;
    }

    public List<ReconcileFileGroupTask> fileGroups() {
      if (fileGroupJobs == null || fileGroupJobs.isEmpty()) {
        return List.of();
      }
      return fileGroupJobs.stream().map(StoredPlannedFileGroupJob::toFileGroupTask).toList();
    }

    public List<PlannedFileGroupJob> toPlannedFileGroupJobs() {
      if (fileGroupJobs == null || fileGroupJobs.isEmpty()) {
        return List.of();
      }
      return fileGroupJobs.stream()
          .filter(job -> job != null && !job.toFileGroupTask().isEmpty())
          .map(job -> new PlannedFileGroupJob(job.scope, job.toFileGroupTask()))
          .toList();
    }
  }

  static final class StoredPlannedFileGroupJob {
    public ReconcileScope scope = ReconcileScope.empty();
    public StoredFileGroupTask fileGroupTask = StoredFileGroupTask.empty();

    static StoredPlannedFileGroupJob from(PlannedFileGroupJob job) {
      StoredPlannedFileGroupJob stored = new StoredPlannedFileGroupJob();
      PlannedFileGroupJob effective =
          job == null
              ? new PlannedFileGroupJob(ReconcileScope.empty(), ReconcileFileGroupTask.empty())
              : job;
      stored.scope = effective.scope() == null ? ReconcileScope.empty() : effective.scope();
      stored.fileGroupTask = StoredFileGroupTask.from(effective.fileGroupTask());
      return stored;
    }

    ReconcileFileGroupTask toFileGroupTask() {
      return fileGroupTask == null ? ReconcileFileGroupTask.empty() : fileGroupTask.toTask();
    }
  }

  static final class StoredFileGroupTask {
    public String planId = "";
    public String groupId = "";
    public String tableId = "";
    public long snapshotId = -1L;
    public int fileCount = 0;
    public List<String> filePaths = List.of();
    public String executionSchemaJson = "";
    public List<StoredFileExecutionPlan> fileExecutionPlans = List.of();

    static StoredFileGroupTask from(ReconcileFileGroupTask task) {
      StoredFileGroupTask stored = new StoredFileGroupTask();
      ReconcileFileGroupTask effective = task == null ? ReconcileFileGroupTask.empty() : task;
      stored.planId = effective.planId();
      stored.groupId = effective.groupId();
      stored.tableId = effective.tableId();
      stored.snapshotId = effective.snapshotId();
      stored.fileCount = effective.fileCount();
      stored.filePaths = effective.filePaths();
      stored.executionSchemaJson = effective.executionSchemaJson();
      stored.fileExecutionPlans =
          effective.fileExecutionPlans().stream().map(StoredFileExecutionPlan::from).toList();
      return stored;
    }

    static StoredFileGroupTask empty() {
      return from(ReconcileFileGroupTask.empty());
    }

    ReconcileFileGroupTask toTask() {
      return ReconcileFileGroupTask.of(
          planId,
          groupId,
          tableId,
          snapshotId,
          fileCount,
          "",
          0,
          filePaths,
          List.of(),
          List.of(),
          executionSchemaJson,
          fileExecutionPlans == null
              ? List.of()
              : fileExecutionPlans.stream().map(StoredFileExecutionPlan::toPlan).toList());
    }
  }

  static final class StoredFileExecutionPlan {
    public String filePath = "";
    public long fileSizeInBytes = 0L;
    public String partitionDataJson = "";
    public StoredDeltaDeletionVector deletionVector;
    public String fileFormat = "";
    public int partitionSpecId = 0;
    public List<StoredIcebergDeleteFile> icebergDeleteFiles = List.of();
    public String contentIdentity = "";

    static StoredFileExecutionPlan from(ReconcileFileExecutionPlan plan) {
      ReconcileFileExecutionPlan effective =
          plan == null ? ReconcileFileExecutionPlan.of("", 0L, "", null) : plan;
      StoredFileExecutionPlan stored = new StoredFileExecutionPlan();
      stored.filePath = effective.filePath();
      stored.fileSizeInBytes = effective.fileSizeInBytes();
      stored.partitionDataJson = effective.partitionDataJson();
      stored.deletionVector = StoredDeltaDeletionVector.from(effective.deletionVector());
      stored.fileFormat = effective.fileFormat();
      stored.partitionSpecId = effective.partitionSpecId();
      stored.icebergDeleteFiles =
          effective.icebergDeleteFiles().stream().map(StoredIcebergDeleteFile::from).toList();
      stored.contentIdentity = effective.contentIdentity();
      return stored;
    }

    ReconcileFileExecutionPlan toPlan() {
      return ReconcileFileExecutionPlan.of(
          filePath,
          fileSizeInBytes,
          partitionDataJson,
          deletionVector == null ? null : deletionVector.toDeletionVector(),
          fileFormat,
          partitionSpecId,
          icebergDeleteFiles == null
              ? List.of()
              : icebergDeleteFiles.stream().map(StoredIcebergDeleteFile::toDeleteFile).toList(),
          contentIdentity);
    }
  }

  static final class StoredDeltaDeletionVector {
    public String storageType = "";
    public String pathOrInlineDv = "";
    public Integer offset;
    public int sizeInBytes = 0;
    public long cardinality = 0L;

    static StoredDeltaDeletionVector from(
        ReconcileFileExecutionPlan.DeltaDeletionVector deletionVector) {
      if (deletionVector == null) {
        return null;
      }
      StoredDeltaDeletionVector stored = new StoredDeltaDeletionVector();
      stored.storageType = deletionVector.storageType();
      stored.pathOrInlineDv = deletionVector.pathOrInlineDv();
      stored.offset = deletionVector.offset();
      stored.sizeInBytes = deletionVector.sizeInBytes();
      stored.cardinality = deletionVector.cardinality();
      return stored;
    }

    ReconcileFileExecutionPlan.DeltaDeletionVector toDeletionVector() {
      return new ReconcileFileExecutionPlan.DeltaDeletionVector(
          storageType, pathOrInlineDv, offset, sizeInBytes, cardinality);
    }
  }

  static final class StoredIcebergDeleteFile {
    public String filePath = "";
    public long fileSizeInBytes = 0L;
    public String content = ReconcileFileExecutionPlan.IcebergDeleteContent.UNSPECIFIED.name();
    public int partitionSpecId = 0;
    public List<Integer> equalityFieldIds = List.of();
    public String contentIdentity = "";

    static StoredIcebergDeleteFile from(ReconcileFileExecutionPlan.IcebergDeleteFile deleteFile) {
      StoredIcebergDeleteFile stored = new StoredIcebergDeleteFile();
      if (deleteFile == null) {
        return stored;
      }
      stored.filePath = deleteFile.filePath();
      stored.fileSizeInBytes = deleteFile.fileSizeInBytes();
      stored.content = deleteFile.content().name();
      stored.partitionSpecId = deleteFile.partitionSpecId();
      stored.equalityFieldIds = deleteFile.equalityFieldIds();
      stored.contentIdentity = deleteFile.contentIdentity();
      return stored;
    }

    ReconcileFileExecutionPlan.IcebergDeleteFile toDeleteFile() {
      ReconcileFileExecutionPlan.IcebergDeleteContent parsedContent;
      try {
        parsedContent = ReconcileFileExecutionPlan.IcebergDeleteContent.valueOf(content);
      } catch (IllegalArgumentException | NullPointerException ignored) {
        parsedContent = ReconcileFileExecutionPlan.IcebergDeleteContent.UNSPECIFIED;
      }
      return new ReconcileFileExecutionPlan.IcebergDeleteFile(
          filePath,
          fileSizeInBytes,
          parsedContent,
          partitionSpecId,
          equalityFieldIds,
          contentIdentity);
    }
  }

  private static TargetStatsRecord parseTargetStatsRecord(byte[] payload) {
    try {
      return TargetStatsRecord.parseFrom(payload);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to decode direct stats record payload", e);
    }
  }

  public record DirectStatsBlob(List<byte[]> records) {
    public DirectStatsBlob {
      records = records == null ? List.of() : List.copyOf(records);
    }
  }
}
