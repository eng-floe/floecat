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

import ai.floedb.floecat.catalog.rpc.BlobRef;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.impl.ReconcileLeaseGrpcStatus;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifestDescriptor;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedSnapshotFinalizeResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedSnapshotFinalizeResultResponse;
import ai.floedb.floecat.service.catalog.impl.CurrentSnapshotPointerService;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.spi.BlobStore;
import io.grpc.Status;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HexFormat;
import org.jboss.logging.Logger;

/** Registers fenced snapshot capture manifests without ingesting their artifact payloads. */
@ApplicationScoped
public class LeasedSnapshotFinalizeExecutionService extends BaseServiceImpl {
  private static final Logger LOG = Logger.getLogger(LeasedSnapshotFinalizeExecutionService.class);

  @Inject ReconcileJobStore jobs;
  @Inject ai.floedb.floecat.service.repo.IdempotencyRepository idempotencyStore;
  @Inject SnapshotFinalizeChildStateService childStateService;
  @Inject CurrentSnapshotPointerService currentSnapshotPointerService;
  @Inject BlobStore blobStore;

  public boolean persistSuccess(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      String resultId,
      SnapshotCaptureManifestDescriptor descriptor) {
    long totalStartNanos = System.nanoTime();
    long[] leaseNanos = {0L};
    long[] validateNanos = {0L};
    long[] manifestHeadNanos = {0L};
    long[] childScanNanos = {0L};
    long[] commitNanos = {0L};
    long[] publishNanos = {0L};
    long[] leaseOutcomeNanos = {0L};
    long[] rpcRequestBytes = {0L};
    long[] manifestBytes = {descriptor == null ? 0L : descriptor.getManifestBytes()};
    String[] outcome = {"failed"};
    try {
      String requiredResultId = requireResultId(resultId);
      String manifestSha256 =
          descriptor == null
              ? ""
              : HexFormat.of().formatHex(descriptor.getManifestSha256().toByteArray());
      ReconcileJobStore.ReconcileJob existing = jobs.getCompactLeaseView(jobId).orElse(null);
      if (existing != null && "JS_SUCCEEDED".equals(existing.state)) {
        boolean replayed =
            jobs.completeSnapshotFinalizeSuccess(
                jobId,
                leaseEpoch,
                requiredResultId,
                descriptor == null ? "" : descriptor.getManifestUri(),
                descriptor == null ? 0L : descriptor.getManifestBytes(),
                manifestSha256,
                descriptor == null ? 0 : descriptor.getFileGroupCount(),
                descriptor == null ? 0 : descriptor.getSourceFileCount(),
                descriptor == null ? 0L : descriptor.getStatsRecordCount(),
                System.currentTimeMillis(),
                "Registered snapshot capture manifest");
        requireAcceptedLeaseOutcome(replayed, jobId);
        outcome[0] = "replayed";
        return true;
      }
      long leaseStartNanos = System.nanoTime();
      ReconcileJobStore.LeasedJob lease;
      try {
        lease =
            requireLeasedSnapshotFinalizeJob(
                principalContext.getCorrelationId(), jobId, leaseEpoch);
      } finally {
        leaseNanos[0] = System.nanoTime() - leaseStartNanos;
      }

      long validateStartNanos = System.nanoTime();
      ReconcileSnapshotTask snapshotTask;
      ResourceId tableId;
      SnapshotCaptureManifestDescriptor validated;
      try {
        snapshotTask = requireSnapshotTask(lease);
        tableId = tableId(lease, snapshotTask);
        validated =
            validateManifestDescriptorIdentity(lease, snapshotTask, requiredResultId, descriptor);
      } finally {
        validateNanos[0] = System.nanoTime() - validateStartNanos;
      }
      long manifestHeadStartNanos = System.nanoTime();
      try {
        validateManifestObject(lease, validated);
      } finally {
        manifestHeadNanos[0] = System.nanoTime() - manifestHeadStartNanos;
      }
      var successPayload = successPayload(requiredResultId, validated);
      rpcRequestBytes[0] =
          SubmitLeasedSnapshotFinalizeResultRequest.newBuilder()
              .setJobId(jobId)
              .setLeaseEpoch(leaseEpoch)
              .setSuccess(successPayload)
              .build()
              .getSerializedSize();

      long commitStartNanos = System.nanoTime();
      boolean accepted = false;
      try {
        long childStartNanos = System.nanoTime();
        try {
          requireReadyChildState(lease, snapshotTask);
        } finally {
          childScanNanos[0] = System.nanoTime() - childStartNanos;
        }
        long publishStartNanos = System.nanoTime();
        try {
          currentSnapshotPointerService.publishCaptureManifest(
              tableId,
              snapshotTask.snapshotId(),
              BlobRef.newBuilder()
                  .setUri(validated.getManifestUri())
                  .setVersion(manifestSha256)
                  .build(),
              lease.jobId);
        } finally {
          publishNanos[0] = System.nanoTime() - publishStartNanos;
        }
        long leaseOutcomeStartNanos = System.nanoTime();
        try {
          accepted =
              jobs.completeSnapshotFinalizeSuccess(
                  lease.jobId,
                  lease.leaseEpoch,
                  requiredResultId,
                  validated.getManifestUri(),
                  validated.getManifestBytes(),
                  manifestSha256,
                  validated.getFileGroupCount(),
                  validated.getSourceFileCount(),
                  validated.getStatsRecordCount(),
                  System.currentTimeMillis(),
                  "Registered snapshot capture manifest " + snapshotTask.snapshotId());
          requireAcceptedLeaseOutcome(accepted, lease.jobId);
        } finally {
          leaseOutcomeNanos[0] = System.nanoTime() - leaseOutcomeStartNanos;
        }
      } finally {
        commitNanos[0] = System.nanoTime() - commitStartNanos;
      }
      outcome[0] = accepted ? "accepted" : "rejected";
      return accepted;
    } finally {
      logFinalizeTiming(
          jobId,
          outcome[0],
          totalStartNanos,
          leaseNanos[0],
          validateNanos[0],
          manifestHeadNanos[0],
          childScanNanos[0],
          commitNanos[0],
          publishNanos[0],
          leaseOutcomeNanos[0],
          rpcRequestBytes[0],
          manifestBytes[0]);
    }
  }

  private static void logFinalizeTiming(
      String jobId,
      String outcome,
      long totalStartNanos,
      long leaseNanos,
      long validateNanos,
      long manifestHeadNanos,
      long childScanNanos,
      long commitNanos,
      long publishNanos,
      long leaseOutcomeNanos,
      long rpcRequestBytes,
      long manifestBytes) {
    long totalNanos = System.nanoTime() - totalStartNanos;
    long accountedNanos = leaseNanos + validateNanos + manifestHeadNanos + commitNanos;
    long otherNanos = Math.max(0L, totalNanos - accountedNanos);
    LOG.infof(
        "snapshot_finalize_submission_timing jobId=%s outcome=%s totalMs=%.3f leaseMs=%.3f"
            + " validateMs=%.3f manifestHeadMs=%.3f commitMs=%.3f childScanMs=%.3f"
            + " publishMs=%.3f leaseOutcomeMs=%.3f otherMs=%.3f rpcRequestBytes=%d"
            + " manifestBytes=%d",
        jobId,
        outcome,
        totalNanos / 1_000_000.0,
        leaseNanos / 1_000_000.0,
        validateNanos / 1_000_000.0,
        manifestHeadNanos / 1_000_000.0,
        commitNanos / 1_000_000.0,
        childScanNanos / 1_000_000.0,
        publishNanos / 1_000_000.0,
        leaseOutcomeNanos / 1_000_000.0,
        otherNanos / 1_000_000.0,
        rpcRequestBytes,
        manifestBytes);
  }

  private SnapshotCaptureManifestDescriptor validateManifestDescriptorIdentity(
      ReconcileJobStore.LeasedJob lease,
      ReconcileSnapshotTask snapshotTask,
      String resultId,
      SnapshotCaptureManifestDescriptor descriptor) {
    if (descriptor == null || descriptor.getFormatVersion() != 1) {
      throw new IllegalArgumentException("snapshot capture manifest format_version must be 1");
    }
    String expectedUri =
        Keys.reconcileSnapshotCaptureManifestUri(
            lease.accountId, lease.parentJobId, lease.jobId, lease.leaseEpoch);
    if (!lease.accountId.equals(descriptor.getAccountId())
        || !lease.connectorId.equals(descriptor.getConnectorId())
        || !lease.parentJobId.equals(descriptor.getParentJobId())
        || !lease.jobId.equals(descriptor.getFinalizeJobId())
        || !snapshotTask.tableId().equals(descriptor.getTableId())
        || snapshotTask.snapshotId() != descriptor.getSnapshotId()
        || !lease.leaseEpoch.equals(descriptor.getLeaseEpoch())
        || !resultId.equals(descriptor.getResultId())) {
      throw new IllegalArgumentException("snapshot capture manifest descriptor identity mismatch");
    }
    if (!expectedUri.equals(descriptor.getManifestUri())) {
      throw new IllegalArgumentException(
          "snapshot capture manifest URI is outside the leased result location");
    }
    if (descriptor.getManifestBytes() <= 0L || descriptor.getManifestSha256().size() != 32) {
      throw new IllegalArgumentException("snapshot capture manifest size and sha256 are required");
    }
    if (descriptor.getFileGroupCount() != snapshotTask.fileGroupCount()
        || descriptor.getSourceFileCount() != snapshotTask.sourceFileCount()) {
      throw new IllegalArgumentException("snapshot capture manifest coverage mismatch");
    }
    return descriptor;
  }

  private void validateManifestObject(
      ReconcileJobStore.LeasedJob lease, SnapshotCaptureManifestDescriptor descriptor) {
    String expectedUri = descriptor.getManifestUri();
    var header =
        blobStore
            .head(expectedUri)
            .orElseThrow(
                () ->
                    new StorageAbortRetryableException(
                        "snapshot capture manifest is not committed jobId="
                            + lease.jobId
                            + " uri="
                            + expectedUri));
    if (header.getContentLength() != descriptor.getManifestBytes()) {
      throw new IllegalArgumentException("snapshot capture manifest object size mismatch");
    }
  }

  private void requireReadyChildState(
      ReconcileJobStore.LeasedJob lease, ReconcileSnapshotTask snapshotTask) {
    if (snapshotTask.fileGroupCount() == 0) {
      return;
    }
    SnapshotFinalizeChildStateService.ChildState childState =
        childStateService.compactChildState(
            lease.accountId, lease.parentJobId, lease.jobId, snapshotTask.fileGroupCount());
    if (!childState.duplicateGroups().isEmpty()
        || !childState.invalidSucceededGroups().isEmpty()
        || !childState.failedGroups().isEmpty()
        || !childState.cancelledGroups().isEmpty()
        || !childState.pendingGroups().isEmpty()
        || !childState.missingGroups().isEmpty()) {
      throw Status.FAILED_PRECONDITION
          .withDescription("snapshot finalization child results are not ready")
          .asRuntimeException();
    }
  }

  public boolean persistFailure(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      String resultId,
      String message) {
    ReconcileJobStore.LeasedJob lease =
        requireLeasedSnapshotFinalizeJob(principalContext.getCorrelationId(), jobId, leaseEpoch);
    ReconcileSnapshotTask snapshotTask = requireSnapshotTask(lease);
    ResourceId tableId = tableId(lease, snapshotTask);
    String requiredResultId = requireResultId(resultId);
    String effectiveMessage = message == null ? "" : message;
    byte[] requestBytes = failurePayload(requiredResultId, effectiveMessage).toByteArray();
    return runIdempotentCreate(
            () ->
                MutationOps.createProto(
                    principalContext.getAccountId(),
                    "SubmitLeasedSnapshotFinalizeResult",
                    resultIdempotencyKey(jobId, requiredResultId),
                    () -> requestBytes,
                    () ->
                        new IdempotencyGuard.CreateResult<>(
                            SubmitLeasedSnapshotFinalizeResultResponse.newBuilder()
                                .setAccepted(true)
                                .build(),
                            tableId),
                    ignored -> MutationMeta.getDefaultInstance(),
                    idempotencyStore,
                    nowTs(),
                    idempotencyTtlSeconds(),
                    principalContext::getCorrelationId,
                    SubmitLeasedSnapshotFinalizeResultResponse::parseFrom))
        .body
        .getAccepted();
  }

  private static void requireAcceptedLeaseOutcome(boolean accepted, String jobId) {
    if (!accepted) {
      throw ReconcileLeaseGrpcStatus.leasePreconditionFailed(
          "reconcile lease is no longer valid for job " + jobId);
    }
  }

  private ReconcileJobStore.LeasedJob requireLeasedSnapshotFinalizeJob(
      String corr, String jobId, String leaseEpoch) {
    if (!jobs.renewLease(jobId, leaseEpoch)) {
      throw Status.FAILED_PRECONDITION
          .withDescription("reconcile lease is no longer valid")
          .asRuntimeException();
    }
    ReconcileJobStore.ReconcileJob job =
        jobs.getCompactLeaseView(jobId)
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
          .withDescription("reconcile job is no longer active for lease " + jobId)
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

  private static ReconcileSnapshotTask requireSnapshotTask(ReconcileJobStore.LeasedJob lease) {
    ReconcileSnapshotTask snapshotTask =
        lease == null || lease.snapshotTask == null
            ? ReconcileSnapshotTask.empty()
            : lease.snapshotTask;
    if (snapshotTask.isEmpty()
        || snapshotTask.tableId().isBlank()
        || snapshotTask.snapshotId() < 0L) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "snapshot task is required for FINALIZE_SNAPSHOT_CAPTURE result submission")
          .asRuntimeException();
    }
    return snapshotTask;
  }

  private static ResourceId tableId(
      ReconcileJobStore.LeasedJob lease, ReconcileSnapshotTask snapshotTask) {
    return ResourceId.newBuilder()
        .setAccountId(lease.accountId)
        .setKind(ResourceKind.RK_TABLE)
        .setId(snapshotTask.tableId())
        .build();
  }

  private static SubmitLeasedSnapshotFinalizeResultRequest.Success successPayload(
      String resultId, SnapshotCaptureManifestDescriptor descriptor) {
    return SubmitLeasedSnapshotFinalizeResultRequest.Success.newBuilder()
        .setResultId(resultId)
        .setManifestDescriptor(descriptor)
        .build();
  }

  private static SubmitLeasedSnapshotFinalizeResultRequest.Failure failurePayload(
      String resultId, String message) {
    return SubmitLeasedSnapshotFinalizeResultRequest.Failure.newBuilder()
        .setResultId(resultId)
        .setMessage(message == null ? "" : message)
        .build();
  }

  private static String resultIdempotencyKey(String jobId, String resultId) {
    return (jobId == null ? "" : jobId.trim()) + ":" + resultId;
  }

  private static String requireResultId(String resultId) {
    if (resultId == null || resultId.isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("result_id is required for snapshot finalize result submission")
          .asRuntimeException();
    }
    return resultId.trim();
  }

  private static boolean isActiveLeasedState(String state) {
    return "JS_RUNNING".equals(state) || "JS_CANCELLING".equals(state);
  }
}
