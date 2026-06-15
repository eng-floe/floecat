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

package ai.floedb.floecat.service.reconciler.jobs.durable.storage;

import ai.floedb.floecat.reconciler.impl.SnapshotPlanBlobStore.SnapshotPlanBlob;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredFileGroupResultPayload;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobDefinition;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobLease;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobListSummary;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcilePayloadStore {
  private static final Logger LOG = Logger.getLogger(ReconcilePayloadStore.class);

  private static final String INLINE_JOB_STATE_PREFIX = "inline:reconcile-job:";
  private static final String INLINE_JOB_LEASE_PREFIX = "inline:reconcile-lease:";
  private static final String INLINE_JOB_PROJECTION_PREFIX = "inline:reconcile-job-projection:";
  private static final String INLINE_JOB_LIST_SUMMARY_PREFIX = "inline:reconcile-job-list-summary:";

  @Inject BlobStore blobStore;
  @Inject PointerStore pointerStore;
  @Inject ObjectMapper mapper;

  public void bind(BlobStore blobStore, PointerStore pointerStore, ObjectMapper mapper) {
    this.blobStore = blobStore;
    this.pointerStore = pointerStore;
    this.mapper = mapper;
  }

  public <T> Optional<T> readBlob(String blobUri, Class<T> type) {
    if (blobUri == null || blobUri.isBlank()) {
      return Optional.empty();
    }
    byte[] payload;
    try {
      payload = blobStore.get(blobUri);
    } catch (StorageNotFoundException e) {
      LOG.debugf(e, "Reconcile payload blob missing blob=%s", blobUri);
      return Optional.empty();
    }
    if (payload == null || payload.length == 0) {
      return Optional.empty();
    }
    try {
      return Optional.ofNullable(mapper.readValue(payload, type));
    } catch (Exception e) {
      LOG.warnf(e, "Failed to decode reconcile payload blob=%s type=%s", blobUri, type.getName());
      return Optional.empty();
    }
  }

  public <T> T requireBlob(String blobUri, Class<T> type, String payloadName, String jobId) {
    return readBlob(blobUri, type)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format(
                        "Reconcile job %s is missing required %s blob=%s",
                        blankToEmpty(jobId), payloadName, blankToEmpty(blobUri))));
  }

  public String writeBlob(String blobUri, Object payload, String failureMessage) {
    try {
      blobStore.put(
          blobUri,
          mapper.writeValueAsBytes(payload),
          "application/json; charset=" + StandardCharsets.UTF_8.name());
      return blobUri;
    } catch (Exception e) {
      throw new IllegalStateException(failureMessage, e);
    }
  }

  public String encodeInlineJobState(StoredReconcileJob payload) {
    return encodeInlineJson(INLINE_JOB_STATE_PREFIX, payload);
  }

  public Optional<StoredReconcileJob> readInlineJobState(String reference) {
    return decodeInlineJson(reference, INLINE_JOB_STATE_PREFIX, StoredReconcileJob.class);
  }

  public String encodeInlineJobLease(StoredJobLease payload) {
    return encodeInlineJson(INLINE_JOB_LEASE_PREFIX, payload);
  }

  public Optional<StoredJobLease> readInlineJobLease(String reference) {
    return decodeInlineJson(reference, INLINE_JOB_LEASE_PREFIX, StoredJobLease.class);
  }

  public String encodeInlineJobProjection(StoredReconcileJobProjection payload) {
    return encodeInlineJson(INLINE_JOB_PROJECTION_PREFIX, payload);
  }

  public Optional<StoredReconcileJobProjection> readInlineJobProjection(String reference) {
    return decodeInlineJson(
        reference, INLINE_JOB_PROJECTION_PREFIX, StoredReconcileJobProjection.class);
  }

  public String encodeInlineJobListSummary(StoredReconcileJobListSummary payload) {
    return encodeInlineJson(INLINE_JOB_LIST_SUMMARY_PREFIX, payload);
  }

  public Optional<StoredReconcileJobListSummary> readInlineJobListSummary(String reference) {
    return decodeInlineJson(
        reference, INLINE_JOB_LIST_SUMMARY_PREFIX, StoredReconcileJobListSummary.class);
  }

  Optional<StoredJobDefinition> loadDefinition(StoredReconcileJob state) {
    if (state == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(state.definition);
  }

  StoredJobDefinition requireDefinition(StoredReconcileJob state) {
    StoredJobDefinition definition = state == null ? null : state.definition;
    if (definition == null) {
      throw new IllegalStateException(
          String.format(
              "Reconcile job %s is missing required job definition",
              state == null ? "" : blankToEmpty(state.jobId)));
    }
    return definition;
  }

  List<ReconcileFileGroupTask> loadSnapshotFileGroupsForDetail(StoredReconcileJob state) {
    if (state == null || blank(state.snapshotPlanBlobUri)) {
      return List.of();
    }
    return readBlob(state.snapshotPlanBlobUri, SnapshotPlanBlob.class)
        .map(SnapshotPlanBlob::fileGroups)
        .orElse(List.of());
  }

  List<ReconcileFileGroupTask> loadSnapshotFileGroupsForExecution(StoredReconcileJob state) {
    if (state == null || blank(state.snapshotPlanBlobUri)) {
      return List.of();
    }
    return requireBlob(
            state.snapshotPlanBlobUri, SnapshotPlanBlob.class, "snapshot plan payload", state.jobId)
        .fileGroups();
  }

  List<ReconcileFileResult> loadFileGroupResultsForDetail(StoredReconcileJob state) {
    if (state == null) {
      return List.of();
    }
    return loadFileGroupResultPayloadForDetail(state)
        .map(StoredFileGroupResultPayload::fileResults)
        .orElse(List.of());
  }

  Optional<StoredFileGroupResultPayload> loadFileGroupResultPayloadForDetail(
      StoredReconcileJob state) {
    if (state == null || blank(state.fileGroupResultBlobUri)) {
      return Optional.empty();
    }
    return readBlob(state.fileGroupResultBlobUri, StoredFileGroupResultPayload.class);
  }

  Optional<StoredFileGroupResultPayload> loadFileGroupResultPayloadForExecution(
      StoredReconcileJob state) {
    if (state == null || blank(state.fileGroupResultBlobUri)) {
      return Optional.empty();
    }
    return Optional.of(
        requireBlob(
            state.fileGroupResultBlobUri,
            StoredFileGroupResultPayload.class,
            "file-group result payload",
            state.jobId));
  }

  ReconcileSnapshotTask snapshotTaskForDetail(StoredReconcileJob state) {
    if (state == null) {
      return ReconcileSnapshotTask.empty();
    }
    List<ReconcileFileGroupTask> fileGroups = loadSnapshotFileGroupsForDetail(state);
    return buildSnapshotTask(state, fileGroups);
  }

  ReconcileSnapshotTask snapshotTaskForExecution(StoredReconcileJob state) {
    if (state == null) {
      return ReconcileSnapshotTask.empty();
    }
    List<ReconcileFileGroupTask> fileGroups = loadSnapshotFileGroupsForExecution(state);
    return buildSnapshotTask(state, fileGroups);
  }

  ReconcileFileGroupTask fileGroupTaskForDetail(StoredReconcileJob state) {
    if (state == null) {
      return ReconcileFileGroupTask.empty();
    }
    StoredFileGroupResultPayload resultPayload =
        loadFileGroupResultPayloadForDetail(state).orElse(null);
    return buildFileGroupTask(state, resultPayload);
  }

  ReconcileFileGroupTask fileGroupTaskForExecution(StoredReconcileJob state) {
    if (state == null) {
      return ReconcileFileGroupTask.empty();
    }
    StoredFileGroupResultPayload resultPayload =
        loadFileGroupResultPayloadForExecution(state).orElse(null);
    return buildFileGroupTask(state, resultPayload);
  }

  private ReconcileSnapshotTask buildSnapshotTask(
      StoredReconcileJob state, List<ReconcileFileGroupTask> fileGroups) {
    int fileGroupCount =
        fileGroups.isEmpty() ? (int) Math.max(0L, state.plannedFileGroups) : fileGroups.size();
    return ReconcileSnapshotTask.of(
        state.snapshotTaskTableId,
        state.snapshotTaskSnapshotId,
        state.snapshotTaskSourceNamespace,
        state.snapshotTaskSourceTable,
        fileGroups,
        state.snapshotTaskFileGroupPlanRecorded,
        ReconcileSnapshotTask.CompletionMode.fromString(state.snapshotTaskCompletionMode),
        blankToEmpty(state.snapshotPlanBlobUri),
        fileGroupCount,
        state.snapshotTaskSourceFileCount,
        blankToEmpty(state.snapshotTaskDirectStatsBlobUri),
        state.snapshotTaskDirectStatsRecordCount);
  }

  private ReconcileFileGroupTask buildFileGroupTask(
      StoredReconcileJob state, StoredFileGroupResultPayload resultPayload) {
    return ReconcileFileGroupTask.of(
        state.fileGroupPlanId,
        state.fileGroupGroupId,
        state.fileGroupTableId,
        state.fileGroupSnapshotId,
        state.fileGroupFileCount,
        resultPayload == null ? "" : resultPayload.fileStatsBlobUri(),
        resultPayload == null ? 0 : resultPayload.fileStatsRecordCount(),
        resultPayload == null ? List.of() : resultPayload.filePaths(),
        resultPayload == null ? List.of() : resultPayload.fileResults(),
        resultPayload == null ? List.of() : resultPayload.partialAggregateRecords());
  }

  private <T> String encodeInlineJson(String prefix, T payload) {
    try {
      return prefix
          + Base64.getUrlEncoder()
              .withoutPadding()
              .encodeToString(mapper.writeValueAsBytes(payload));
    } catch (Exception e) {
      throw new IllegalStateException("Failed to encode inline reconcile payload", e);
    }
  }

  private <T> Optional<T> decodeInlineJson(String reference, String prefix, Class<T> type) {
    if (reference == null
        || reference.isBlank()
        || prefix == null
        || !reference.startsWith(prefix)) {
      return Optional.empty();
    }
    try {
      byte[] payload = Base64.getUrlDecoder().decode(reference.substring(prefix.length()));
      return Optional.ofNullable(mapper.readValue(payload, type));
    } catch (Exception e) {
      LOG.warnf(e, "Failed to decode inline reconcile payload type=%s", type.getName());
      return Optional.empty();
    }
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }
}
