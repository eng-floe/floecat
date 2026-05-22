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

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.reconciler.impl.SnapshotPlanBlobStore.SnapshotPlanBlob;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredFileGroupPlanPayload;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredFileGroupResultPayload;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobContribution;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobDefinition;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobLease;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.repo.model.Keys;
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
  private static final String INLINE_JOB_CONTRIBUTION_PREFIX = "inline:reconcile-contribution:";

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
      LOG.warnf(e, "Reconcile payload blob missing blob=%s", blobUri);
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

  public String encodeInlineJobContribution(StoredJobContribution payload) {
    return encodeInlineJson(INLINE_JOB_CONTRIBUTION_PREFIX, payload);
  }

  public Optional<StoredJobContribution> readInlineJobContribution(String reference) {
    return decodeInlineJson(reference, INLINE_JOB_CONTRIBUTION_PREFIX, StoredJobContribution.class);
  }

  public Optional<StoredJobDefinition> loadDefinition(StoredReconcileJob state) {
    if (state == null) {
      return Optional.empty();
    }
    return readBlob(state.definitionBlobUri, StoredJobDefinition.class);
  }

  public StoredJobDefinition requireDefinition(StoredReconcileJob state) {
    if (state == null) {
      throw new IllegalStateException("Reconcile job definition missing for null job state");
    }
    return requireBlob(
        state.definitionBlobUri, StoredJobDefinition.class, "job definition", state.jobId);
  }

  public List<ReconcileFileGroupTask> loadSnapshotFileGroups(StoredReconcileJob state) {
    if (state == null || blank(state.snapshotPlanBlobUri)) {
      return List.of();
    }
    return requireBlob(
            state.snapshotPlanBlobUri, SnapshotPlanBlob.class, "snapshot plan payload", state.jobId)
        .fileGroups();
  }

  public List<String> loadFileGroupPaths(StoredReconcileJob state) {
    if (state == null) {
      return List.of();
    }
    return readBlob(state.fileGroupPlanBlobUri, StoredFileGroupPlanPayload.class)
        .map(StoredFileGroupPlanPayload::filePaths)
        .orElse(List.of());
  }

  public Optional<StoredFileGroupPlanPayload> loadFileGroupPlanPayload(StoredReconcileJob state) {
    if (state == null || blank(state.fileGroupPlanBlobUri)) {
      return Optional.empty();
    }
    return Optional.of(
        requireBlob(
            state.fileGroupPlanBlobUri,
            StoredFileGroupPlanPayload.class,
            "file-group plan payload",
            state.jobId));
  }

  public List<ReconcileFileResult> loadFileGroupResults(StoredReconcileJob state) {
    if (state == null) {
      return List.of();
    }
    return loadFileGroupResultPayload(state)
        .map(StoredFileGroupResultPayload::fileResults)
        .orElse(List.of());
  }

  public Optional<StoredFileGroupResultPayload> loadFileGroupResultPayload(
      StoredReconcileJob state) {
    if (state == null) {
      return Optional.empty();
    }
    // Result payload pointers are payload references, not canonical job-index pointers.
    if (!blank(state.accountId) && !blank(state.jobId)) {
      Pointer resultPointer =
          pointerStore
              .get(Keys.reconcileJobResultPointerById(state.accountId, state.jobId))
              .orElse(null);
      if (resultPointer != null && !blank(resultPointer.getBlobUri())) {
        return Optional.of(
            requireBlob(
                resultPointer.getBlobUri(),
                StoredFileGroupResultPayload.class,
                "file-group result payload",
                state.jobId));
      }
    }
    return Optional.empty();
  }

  public ReconcileSnapshotTask snapshotTaskFor(StoredReconcileJob state) {
    if (state == null) {
      return ReconcileSnapshotTask.empty();
    }
    List<ReconcileFileGroupTask> fileGroups = loadSnapshotFileGroups(state);
    return ReconcileSnapshotTask.of(
        state.snapshotTaskTableId,
        state.snapshotTaskSnapshotId,
        state.snapshotTaskSourceNamespace,
        state.snapshotTaskSourceTable,
        fileGroups,
        state.snapshotTaskFileGroupPlanRecorded,
        ReconcileSnapshotTask.CompletionMode.fromString(state.snapshotTaskCompletionMode),
        blankToEmpty(state.snapshotPlanBlobUri),
        fileGroups.size(),
        blankToEmpty(state.snapshotTaskDirectStatsBlobUri),
        state.snapshotTaskDirectStatsRecordCount);
  }

  public ReconcileFileGroupTask fileGroupTaskFor(StoredReconcileJob state) {
    if (state == null) {
      return ReconcileFileGroupTask.empty();
    }
    StoredFileGroupResultPayload resultPayload = loadFileGroupResultPayload(state).orElse(null);
    return ReconcileFileGroupTask.of(
        state.fileGroupPlanId,
        state.fileGroupGroupId,
        state.fileGroupTableId,
        state.fileGroupSnapshotId,
        state.fileGroupFileCount,
        resultPayload == null ? "" : resultPayload.fileStatsBlobUri(),
        resultPayload == null ? 0 : resultPayload.fileStatsRecordCount(),
        resultPayload == null ? loadFileGroupPaths(state) : resultPayload.filePaths(),
        resultPayload == null ? loadFileGroupResults(state) : resultPayload.fileResults());
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
