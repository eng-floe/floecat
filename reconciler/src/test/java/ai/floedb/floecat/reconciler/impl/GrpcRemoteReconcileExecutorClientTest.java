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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.auth.ReconcileWorkerAuthProvider;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotSelection;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.SnapshotPlanManifestIds;
import ai.floedb.floecat.reconciler.rpc.GetLeasedPlanConnectorInputResponse;
import ai.floedb.floecat.reconciler.rpc.GetLeasedPlanTableInputResponse;
import ai.floedb.floecat.reconciler.rpc.LeasedPlanConnectorInput;
import ai.floedb.floecat.reconciler.rpc.LeasedPlanTableInput;
import ai.floedb.floecat.reconciler.rpc.ReconcileExecutorControlGrpc;
import ai.floedb.floecat.reconciler.rpc.RenewReconcileLeaseResponse;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedFileGroupExecutionResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedFileGroupExecutionResultResponse;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanSnapshotResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanSnapshotResultResponse;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class GrpcRemoteReconcileExecutorClientTest {

  @Test
  void metadataIncludesExplicitWorkerAuthorizationHeader() {
    GrpcRemoteReconcileExecutorClient client =
        new GrpcRemoteReconcileExecutorClient(
            "authorization", () -> java.util.Optional.of("Bearer worker-token"));

    Metadata metadata = client.metadata("corr-1");

    assertThat(metadata.get(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("Bearer worker-token");
    assertThat(metadata.get(Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("corr-1");
  }

  @Test
  void oidcModeRequiresWorkerAuthorizationHeader() {
    GrpcRemoteReconcileExecutorClient client =
        new GrpcRemoteReconcileExecutorClient("authorization", java.util.Optional::empty);

    IllegalStateException ex =
        assertThrows(IllegalStateException.class, () -> client.metadata("corr-2"));

    assertThat(ex).hasMessageContaining("Reconcile worker authorization header is required");
  }

  @Test
  void workerAuthCanBeExplicitlyDisabled() {
    GrpcRemoteReconcileExecutorClient client =
        new GrpcRemoteReconcileExecutorClient("authorization", false, java.util.Optional::empty);

    Metadata metadata = client.metadata("corr-disabled");

    assertThat(metadata.get(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)))
        .isNull();
    assertThat(metadata.get(Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("corr-disabled");
  }

  @Test
  void workerCallsUseSessionHeaderWhenConfigured() {
    GrpcRemoteReconcileExecutorClient client =
        new GrpcRemoteReconcileExecutorClient(
            "x-floe-session", () -> java.util.Optional.of("Bearer worker-token"));

    Metadata metadata = client.metadata("corr-3");

    assertThat(metadata.keys()).contains("x-correlation-id");
    assertThat(metadata.get(Metadata.Key.of("x-floe-session", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("Bearer worker-token");
  }

  @Test
  void planConnectorInputPreservesSnapshotSelection() {
    GrpcRemoteReconcileExecutorClient client =
        new GrpcRemoteReconcileExecutorClient(
            "authorization", () -> java.util.Optional.of("Bearer worker-token"));
    client.executorControl =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    when(client.executorControl.withInterceptors(any())).thenReturn(client.executorControl);
    when(client.executorControl.getLeasedPlanConnectorInput(any()))
        .thenReturn(
            GetLeasedPlanConnectorInputResponse.newBuilder()
                .setInput(
                    LeasedPlanConnectorInput.newBuilder()
                        .setJobId("job-1")
                        .setLeaseEpoch("lease-1")
                        .setConnectorId(connectorId())
                        .setScope(
                            ai.floedb.floecat.reconciler.rpc.CaptureScope.newBuilder()
                                .setSnapshotSelection(
                                    ai.floedb.floecat.reconciler.rpc.SnapshotSelection.newBuilder()
                                        .setKind(
                                            ai.floedb.floecat.reconciler.rpc.SnapshotSelectionKind
                                                .SSK_CURRENT)
                                        .build())
                                .build())
                        .build())
                .build());

    StandalonePlanConnectorPayload payload = client.getPlanConnectorInput(remoteLease());

    assertEquals(
        ReconcileSnapshotSelection.Kind.CURRENT, payload.scope().snapshotSelection().kind());
  }

  @Test
  void planTableInputPreservesSnapshotSelection() {
    GrpcRemoteReconcileExecutorClient client =
        new GrpcRemoteReconcileExecutorClient(
            "authorization", () -> java.util.Optional.of("Bearer worker-token"));
    client.executorControl =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    when(client.executorControl.withInterceptors(any())).thenReturn(client.executorControl);
    when(client.executorControl.getLeasedPlanTableInput(any()))
        .thenReturn(
            GetLeasedPlanTableInputResponse.newBuilder()
                .setInput(
                    LeasedPlanTableInput.newBuilder()
                        .setJobId("job-2")
                        .setLeaseEpoch("lease-2")
                        .setConnectorId(connectorId())
                        .setScope(
                            ai.floedb.floecat.reconciler.rpc.CaptureScope.newBuilder()
                                .setDestinationTableId("table-1")
                                .setSnapshotSelection(
                                    ai.floedb.floecat.reconciler.rpc.SnapshotSelection.newBuilder()
                                        .setKind(
                                            ai.floedb.floecat.reconciler.rpc.SnapshotSelectionKind
                                                .SSK_CURRENT)
                                        .build())
                                .build())
                        .build())
                .build());

    StandalonePlanTablePayload payload = client.getPlanTableInput(remoteLease());

    assertEquals(
        ReconcileSnapshotSelection.Kind.CURRENT, payload.scope().snapshotSelection().kind());
    assertEquals("table-1", payload.scope().destinationTableId());
  }

  @Test
  void submitPlanSnapshotSuccessOmitsDuplicateSnapshotFileGroupsWhenFileGroupJobsArePresent() {
    GrpcRemoteReconcileExecutorClient client =
        new GrpcRemoteReconcileExecutorClient(
            "authorization", () -> java.util.Optional.of("Bearer worker-token"));
    client.executorControl =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.snapshotPlanBlobStore = mock(SnapshotPlanBlobStore.class);
    when(client.executorControl.withInterceptors(any())).thenReturn(client.executorControl);
    when(client.executorControl.submitLeasedPlanSnapshotResult(any()))
        .thenReturn(SubmitLeasedPlanSnapshotResultResponse.newBuilder().setAccepted(true).build());

    ReconcileFileGroupTask fileGroupTask =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet"));
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            "table-1",
            55L,
            "db",
            "events",
            List.of(fileGroupTask),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS);
    ReconcileSnapshotTask persistedSnapshotTask =
        ReconcileSnapshotTask.of(
            "table-1",
            55L,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            SnapshotPlanManifestIds.manifestBlobUri("acct", "job-lease", List.of(fileGroupTask)),
            1);
    when(client.snapshotPlanBlobStore.persistPlan(any(), any(), any(), any()))
        .thenReturn(persistedSnapshotTask);

    client.submitPlanSnapshotSuccess(
        remotePlanSnapshotLease(),
        snapshotTask,
        List.of(new PlannedFileGroupJob(ReconcileScope.empty(), fileGroupTask)),
        List.of());

    ArgumentCaptor<SubmitLeasedPlanSnapshotResultRequest> requestCaptor =
        ArgumentCaptor.forClass(SubmitLeasedPlanSnapshotResultRequest.class);
    verify(client.executorControl).submitLeasedPlanSnapshotResult(requestCaptor.capture());
    SubmitLeasedPlanSnapshotResultRequest.Success success = requestCaptor.getValue().getSuccess();
    assertThat(success.getSnapshotTask().getTableId()).isEqualTo("table-1");
    assertThat(success.getSnapshotTask().getSnapshotId()).isEqualTo(55L);
    assertThat(success.getSnapshotTask().getSourceNamespace()).isEqualTo("db");
    assertThat(success.getSnapshotTask().getSourceTable()).isEqualTo("events");
    assertThat(success.getSnapshotTask().getFileGroupPlanRecorded()).isTrue();
    assertThat(success.getSnapshotTask().getCompletionMode().name()).isEqualTo("RSCM_FILE_GROUPS");
    assertThat(success.getSnapshotTask().getFileGroupPlanBlobUri())
        .isEqualTo(
            SnapshotPlanManifestIds.manifestBlobUri("acct", "job-lease", List.of(fileGroupTask)));
    assertThat(success.getSnapshotTask().getFileGroupCount()).isEqualTo(1);
    assertThat(success.getSnapshotTask().getFileGroupsCount()).isZero();
  }

  @Test
  void renewRetriesOnceOnTransportFailure() {
    TransportLoggingClient client =
        new TransportLoggingClient(
            "authorization", () -> java.util.Optional.of("Bearer worker-token"));
    client.executorControl =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    when(client.executorControl.withInterceptors(any())).thenReturn(client.executorControl);
    when(client.executorControl.renewReconcileLease(any()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE))
        .thenReturn(
            RenewReconcileLeaseResponse.newBuilder()
                .setRenewed(true)
                .setCancellationRequested(false)
                .build());

    RemoteReconcileExecutorClient.LeaseHeartbeat heartbeat = client.renew(remoteLease());

    assertThat(heartbeat.leaseValid()).isTrue();
    assertThat(heartbeat.cancellationRequested()).isFalse();
    verify(client.executorControl, org.mockito.Mockito.times(2)).renewReconcileLease(any());
    assertThat(client.transportFailureLogs()).containsExactly("renewReconcileLease#1");
  }

  @Test
  void closeWorkerControlChannelUsesGracefulShutdownForReset() throws Exception {
    GrpcRemoteReconcileExecutorClient client =
        new GrpcRemoteReconcileExecutorClient(
            "authorization", () -> java.util.Optional.of("Bearer worker-token"));
    ManagedChannel channel = mock(ManagedChannel.class);

    client.closeWorkerControlChannel(channel, false);

    verify(channel).shutdown();
    org.mockito.Mockito.verify(channel, org.mockito.Mockito.never()).shutdownNow();
    org.mockito.Mockito.verify(channel, org.mockito.Mockito.never())
        .awaitTermination(anyLong(), any(TimeUnit.class));
  }

  @Test
  void closeWorkerControlChannelUsesForcedShutdownForDestroy() throws Exception {
    GrpcRemoteReconcileExecutorClient client =
        new GrpcRemoteReconcileExecutorClient(
            "authorization", () -> java.util.Optional.of("Bearer worker-token"));
    ManagedChannel channel = mock(ManagedChannel.class);
    when(channel.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);

    client.closeWorkerControlChannel(channel, true);

    verify(channel).shutdownNow();
    verify(channel).awaitTermination(5, TimeUnit.SECONDS);
    org.mockito.Mockito.verify(channel, org.mockito.Mockito.never()).shutdown();
  }

  @Test
  void submitFileGroupSuccessRetriesWhenResultIdIsStable() {
    GrpcRemoteReconcileExecutorClient client =
        new GrpcRemoteReconcileExecutorClient(
            "authorization", () -> java.util.Optional.of("Bearer worker-token"));
    client.executorControl =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    when(client.executorControl.withInterceptors(any())).thenReturn(client.executorControl);
    when(client.executorControl.submitLeasedFileGroupExecutionResult(any()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE))
        .thenReturn(
            SubmitLeasedFileGroupExecutionResultResponse.newBuilder().setAccepted(true).build());

    boolean accepted =
        client.submitSuccess(
            remoteFileGroupLease(), StandaloneFileGroupExecutionResult.empty("result-1"));

    assertThat(accepted).isTrue();
    verify(client.executorControl, org.mockito.Mockito.times(2))
        .submitLeasedFileGroupExecutionResult(any());
  }

  @Test
  void submitFileGroupSuccessDoesNotRetryWhenResultIdIsBlank() {
    GrpcRemoteReconcileExecutorClient client =
        new GrpcRemoteReconcileExecutorClient(
            "authorization", () -> java.util.Optional.of("Bearer worker-token"));
    client.executorControl =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    when(client.executorControl.withInterceptors(any())).thenReturn(client.executorControl);
    when(client.executorControl.submitLeasedFileGroupExecutionResult(any()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            client.submitSuccess(
                remoteFileGroupLease(), StandaloneFileGroupExecutionResult.empty("  ")));

    verify(client.executorControl).submitLeasedFileGroupExecutionResult(any());
  }

  @Test
  void submitFileGroupSuccessSendsUploadedArtifactManifestWithoutInlineContent() {
    GrpcRemoteReconcileExecutorClient client =
        new GrpcRemoteReconcileExecutorClient(
            "authorization", () -> java.util.Optional.of("Bearer worker-token"));
    client.executorControl =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    when(client.executorControl.withInterceptors(any())).thenReturn(client.executorControl);
    when(client.executorControl.submitLeasedFileGroupExecutionResult(any()))
        .thenReturn(
            SubmitLeasedFileGroupExecutionResultResponse.newBuilder().setAccepted(true).build());

    var record =
        ai.floedb.floecat.catalog.rpc.IndexArtifactRecord.newBuilder()
            .setArtifactUri("s3://bucket/artifacts/file-1.parquet.idx")
            .build();
    var result =
        new StandaloneFileGroupExecutionResult(
            "result-1",
            List.of(),
            StandaloneFileGroupExecutionResult.FileStatsBlobManifest.empty(),
            List.of(),
            List.of(
                new StandaloneFileGroupExecutionResult.PreUploadedIndexArtifact(
                    record, "application/x-parquet", "s3://bucket/artifacts/file-1.parquet.idx")));

    assertThat(client.submitSuccess(remoteFileGroupLease(), result)).isTrue();

    ArgumentCaptor<SubmitLeasedFileGroupExecutionResultRequest> requestCaptor =
        ArgumentCaptor.forClass(SubmitLeasedFileGroupExecutionResultRequest.class);
    verify(client.executorControl).submitLeasedFileGroupExecutionResult(requestCaptor.capture());
    var artifact = requestCaptor.getValue().getSuccess().getIndexArtifacts(0);
    assertThat(artifact.getUploadedArtifactUri())
        .isEqualTo("s3://bucket/artifacts/file-1.parquet.idx");
    assertThat(artifact.getContent()).isEmpty();
  }

  @Test
  void submitFileGroupSuccessSendsFileStatsBlobManifestWithoutInlineStats() {
    GrpcRemoteReconcileExecutorClient client =
        new GrpcRemoteReconcileExecutorClient(
            "authorization", () -> java.util.Optional.of("Bearer worker-token"));
    client.executorControl =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    when(client.executorControl.withInterceptors(any())).thenReturn(client.executorControl);
    when(client.executorControl.submitLeasedFileGroupExecutionResult(any()))
        .thenReturn(
            SubmitLeasedFileGroupExecutionResultResponse.newBuilder().setAccepted(true).build());

    var result =
        new StandaloneFileGroupExecutionResult(
            "result-1",
            List.of(),
            new StandaloneFileGroupExecutionResult.FileStatsBlobManifest(
                "/accounts/acct/reconcile/jobs/job-1/file-group-stats/result.json", 7),
            List.of(),
            List.of());

    assertThat(client.submitSuccess(remoteFileGroupLease(), result)).isTrue();

    ArgumentCaptor<SubmitLeasedFileGroupExecutionResultRequest> requestCaptor =
        ArgumentCaptor.forClass(SubmitLeasedFileGroupExecutionResultRequest.class);
    verify(client.executorControl).submitLeasedFileGroupExecutionResult(requestCaptor.capture());
    var success = requestCaptor.getValue().getSuccess();
    assertThat(success.getFileStatsBlobUri())
        .isEqualTo("/accounts/acct/reconcile/jobs/job-1/file-group-stats/result.json");
    assertThat(success.getFileStatsRecordCount()).isEqualTo(7);
    assertThat(success.getStatsRecordsCount()).isZero();
  }

  private static ResourceId connectorId() {
    return ResourceId.newBuilder()
        .setAccountId("acct")
        .setKind(ResourceKind.RK_CONNECTOR)
        .setId("connector-1")
        .build();
  }

  private static RemoteLeasedJob remoteLease() {
    return new RemoteLeasedJob(
        new ReconcileJobStore.LeasedJob(
            "job-lease",
            "acct",
            "connector-1",
            false,
            ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            null,
            "lease-epoch",
            "",
            "",
            ReconcileJobKind.PLAN_TABLE,
            null,
            null,
            null,
            null,
            ""));
  }

  private static RemoteLeasedJob remotePlanSnapshotLease() {
    return new RemoteLeasedJob(
        new ReconcileJobStore.LeasedJob(
            "job-lease",
            "acct",
            "connector-1",
            false,
            ReconcilerService.CaptureMode.CAPTURE_ONLY,
            ReconcileScope.empty(),
            null,
            "lease-epoch",
            "",
            "",
            ReconcileJobKind.PLAN_SNAPSHOT,
            null,
            null,
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
            null,
            ""));
  }

  private static RemoteLeasedJob remoteFileGroupLease() {
    return new RemoteLeasedJob(
        new ReconcileJobStore.LeasedJob(
            "job-lease",
            "acct",
            "connector-1",
            false,
            ReconcilerService.CaptureMode.CAPTURE_ONLY,
            ReconcileScope.empty(),
            null,
            "lease-epoch",
            "",
            "",
            ReconcileJobKind.EXEC_FILE_GROUP,
            null,
            null,
            null,
            ReconcileFileGroupTask.of(
                "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/file.parquet")),
            ""));
  }

  private static final class TransportLoggingClient extends GrpcRemoteReconcileExecutorClient {
    private final java.util.List<String> transportFailureLogs = new java.util.ArrayList<>();

    private TransportLoggingClient(
        String workerAuthHeaderName, ReconcileWorkerAuthProvider reconcileWorkerAuthProvider) {
      super(workerAuthHeaderName, reconcileWorkerAuthProvider);
    }

    @Override
    void logWorkerControlTransportFailure(String operation, int attempt, RuntimeException error) {
      transportFailureLogs.add(operation + "#" + attempt);
    }

    private java.util.List<String> transportFailureLogs() {
      return transportFailureLogs;
    }
  }
}
