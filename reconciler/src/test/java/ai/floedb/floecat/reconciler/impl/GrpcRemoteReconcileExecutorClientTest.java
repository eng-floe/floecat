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
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotSelection;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.SnapshotPlanManifestIds;
import ai.floedb.floecat.reconciler.rpc.CompleteLeasedReconcileJobResponse;
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
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanTableResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanTableResultResponse;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class GrpcRemoteReconcileExecutorClientTest {

  @Test
  void metadataIncludesExplicitWorkerAuthorizationHeader() {
    GrpcRemoteReconcileExecutorClient client =
        new GrpcRemoteReconcileExecutorClient(
            "authorization", ignored -> java.util.Optional.of("Bearer worker-token"));

    Metadata metadata = client.metadata("corr-1", "acct-1");

    assertThat(metadata.get(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("Bearer worker-token");
    assertThat(metadata.get(Metadata.Key.of("x-floe-account", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("acct-1");
    assertThat(metadata.get(Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("corr-1");
  }

  @Test
  void oidcModeRequiresWorkerAuthorizationHeader() {
    GrpcRemoteReconcileExecutorClient client =
        new GrpcRemoteReconcileExecutorClient(
            "authorization", ignored -> java.util.Optional.empty());

    IllegalStateException ex =
        assertThrows(IllegalStateException.class, () -> client.metadata("corr-2", null));

    assertThat(ex).hasMessageContaining("Reconcile worker authorization header is required");
  }

  @Test
  void workerAuthCanBeExplicitlyDisabled() {
    GrpcRemoteReconcileExecutorClient client =
        new GrpcRemoteReconcileExecutorClient(
            "authorization", false, ignored -> java.util.Optional.empty());

    Metadata metadata = client.metadata("corr-disabled", null);

    assertThat(metadata.get(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)))
        .isNull();
    assertThat(metadata.get(Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("corr-disabled");
  }

  @Test
  void workerCallsUseSessionHeaderWhenConfigured() {
    GrpcRemoteReconcileExecutorClient client =
        new GrpcRemoteReconcileExecutorClient(
            "x-floe-session", ignored -> java.util.Optional.of("Bearer worker-token"));

    Metadata metadata = client.metadata("corr-3", null);

    assertThat(metadata.keys()).contains("x-correlation-id");
    assertThat(metadata.get(Metadata.Key.of("x-floe-session", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("Bearer worker-token");
  }

  @Test
  void metadataUsesAccountScopedWorkerAuthorizationWithoutLeakage() {
    GrpcRemoteReconcileExecutorClient client =
        new GrpcRemoteReconcileExecutorClient(
            "authorization",
            accountId -> java.util.Optional.of("Bearer worker-token-" + accountId));

    Metadata accountOne = client.metadata("corr-1", "acct-1");
    Metadata accountTwo = client.metadata("corr-2", "acct-2");

    assertThat(accountOne.get(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("Bearer worker-token-acct-1");
    assertThat(accountTwo.get(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("Bearer worker-token-acct-2");
    assertThat(accountOne.get(Metadata.Key.of("x-floe-account", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("acct-1");
    assertThat(accountTwo.get(Metadata.Key.of("x-floe-account", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("acct-2");
  }

  @Test
  void requireWorkerControlHostRejectsBlankValue() {
    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class,
            () -> GrpcRemoteReconcileExecutorClient.requireWorkerControlHost(" "));

    assertThat(ex).hasMessageContaining("Worker-control gRPC host must be configured");
  }

  @Test
  void planConnectorInputPreservesSnapshotSelection() {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel, stub);
    when(stub.withInterceptors(any())).thenReturn(stub);
    when(stub.getLeasedPlanConnectorInput(any()))
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
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel, stub);
    when(stub.withInterceptors(any())).thenReturn(stub);
    when(stub.getLeasedPlanTableInput(any()))
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
  void submitPlanSnapshotSuccessOmitsDuplicateSnapshotFileGroupsWhenFileGroupJobsArePresent()
      throws Exception {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel chunkChannel = mock(ManagedChannel.class);
    ManagedChannel successChannel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub chunkStub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub successStub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.snapshotPlanBlobStore = mock(SnapshotPlanBlobStore.class);
    client.enqueueTransport(chunkChannel, chunkStub);
    client.enqueueTransport(successChannel, successStub);
    when(chunkStub.submitLeasedPlanSnapshotResult(any()))
        .thenReturn(SubmitLeasedPlanSnapshotResultResponse.newBuilder().setAccepted(true).build());
    when(successStub.submitLeasedPlanSnapshotResult(any()))
        .thenReturn(SubmitLeasedPlanSnapshotResultResponse.newBuilder().setAccepted(true).build());
    when(chunkChannel.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);
    when(successChannel.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);

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
    verify(chunkStub).submitLeasedPlanSnapshotResult(requestCaptor.capture());
    SubmitLeasedPlanSnapshotResultRequest chunkRequest = requestCaptor.getValue();
    assertThat(chunkRequest.hasChunk()).isTrue();
    assertThat(chunkRequest.getChunk().getChunkIndex()).isZero();
    assertThat(chunkRequest.getChunk().getFileGroupJobsCount()).isEqualTo(1);
    assertThat(chunkRequest.getChunk().getFileGroupJobs(0).getFileGroupTask().getGroupId())
        .isEqualTo("group-1");

    verify(successStub).submitLeasedPlanSnapshotResult(requestCaptor.capture());
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
    assertThat(success.getChunkCount()).isEqualTo(1);
  }

  @Test
  void submitPlanTableSuccessSplitsSnapshotJobsByChildCount() throws Exception {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel firstChunkChannel = mock(ManagedChannel.class);
    ManagedChannel secondChunkChannel = mock(ManagedChannel.class);
    ManagedChannel successChannel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub firstChunkStub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub secondChunkStub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub successStub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(firstChunkChannel, firstChunkStub);
    client.enqueueTransport(secondChunkChannel, secondChunkStub);
    client.enqueueTransport(successChannel, successStub);
    when(firstChunkStub.submitLeasedPlanTableResult(any()))
        .thenReturn(SubmitLeasedPlanTableResultResponse.newBuilder().setAccepted(true).build());
    when(secondChunkStub.submitLeasedPlanTableResult(any()))
        .thenReturn(SubmitLeasedPlanTableResultResponse.newBuilder().setAccepted(true).build());
    when(successStub.submitLeasedPlanTableResult(any()))
        .thenReturn(SubmitLeasedPlanTableResultResponse.newBuilder().setAccepted(true).build());
    when(firstChunkChannel.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);
    when(secondChunkChannel.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);
    when(successChannel.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);

    List<PlannedSnapshotJob> snapshotJobs = new ArrayList<>();
    for (int i = 0; i < 9; i++) {
      snapshotJobs.add(
          new PlannedSnapshotJob(
              ReconcileScope.empty(), ReconcileSnapshotTask.of("table-1", i, "db", "events")));
    }

    assertThat(client.submitPlanTableSuccess(remoteLease(), snapshotJobs, 1L, 1L, 0L, 9L, 0L))
        .isTrue();

    ArgumentCaptor<SubmitLeasedPlanTableResultRequest> firstChunkCaptor =
        ArgumentCaptor.forClass(SubmitLeasedPlanTableResultRequest.class);
    ArgumentCaptor<SubmitLeasedPlanTableResultRequest> secondChunkCaptor =
        ArgumentCaptor.forClass(SubmitLeasedPlanTableResultRequest.class);
    ArgumentCaptor<SubmitLeasedPlanTableResultRequest> successCaptor =
        ArgumentCaptor.forClass(SubmitLeasedPlanTableResultRequest.class);
    verify(firstChunkStub).submitLeasedPlanTableResult(firstChunkCaptor.capture());
    verify(secondChunkStub).submitLeasedPlanTableResult(secondChunkCaptor.capture());
    verify(successStub).submitLeasedPlanTableResult(successCaptor.capture());

    assertThat(firstChunkCaptor.getValue().hasChunk()).isTrue();
    assertThat(firstChunkCaptor.getValue().getChunk().getChunkIndex()).isZero();
    assertThat(firstChunkCaptor.getValue().getChunk().getSnapshotJobsCount()).isEqualTo(8);
    assertThat(secondChunkCaptor.getValue().hasChunk()).isTrue();
    assertThat(secondChunkCaptor.getValue().getChunk().getChunkIndex()).isEqualTo(1);
    assertThat(secondChunkCaptor.getValue().getChunk().getSnapshotJobsCount()).isEqualTo(1);
    assertThat(successCaptor.getValue().hasSuccess()).isTrue();
    assertThat(successCaptor.getValue().getSuccess().getChunkCount()).isEqualTo(2);
  }

  @Test
  void submitPlanSnapshotFailureMapsFailedPreconditionToLeasePreconditionException()
      throws Exception {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel, stub);
    when(stub.submitLeasedPlanSnapshotResult(any()))
        .thenThrow(
            Status.FAILED_PRECONDITION
                .withDescription("server text is intentionally not inspected")
                .asRuntimeException());
    when(channel.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);

    assertThrows(
        RemoteLeasePreconditionFailedException.class,
        () ->
            client.submitPlanSnapshotFailure(
                remotePlanSnapshotLease(),
                ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL,
                ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE,
                ReconcileExecutor.ExecutionResult.RetryClass.TRANSIENT_ERROR,
                "failed"));
  }

  @Test
  void renewRetriesOnceOnTransportFailure() {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel1 = mock(ManagedChannel.class);
    ManagedChannel channel2 = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub1 =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub2 =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel1, stub1);
    client.enqueueTransport(channel2, stub2);
    when(stub1.withInterceptors(any())).thenReturn(stub1);
    when(stub2.withInterceptors(any())).thenReturn(stub2);
    when(stub1.renewReconcileLease(any()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE));
    when(stub2.renewReconcileLease(any()))
        .thenReturn(
            RenewReconcileLeaseResponse.newBuilder()
                .setRenewed(true)
                .setCancellationRequested(false)
                .build());

    RemoteReconcileExecutorClient.LeaseHeartbeat heartbeat = client.renew(remoteLease());

    assertThat(heartbeat.leaseValid()).isTrue();
    assertThat(heartbeat.cancellationRequested()).isFalse();
    verify(stub1).renewReconcileLease(any());
    verify(stub2).renewReconcileLease(any());
    assertThat(client.transportFailureLogs()).containsExactly("renewReconcileLease@cached#1");
  }

  @Test
  void renewRetriesOnceOnCancelledTransportFailure() {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel1 = mock(ManagedChannel.class);
    ManagedChannel channel2 = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub1 =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub2 =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel1, stub1);
    client.enqueueTransport(channel2, stub2);
    when(stub1.withInterceptors(any())).thenReturn(stub1);
    when(stub2.withInterceptors(any())).thenReturn(stub2);
    when(stub1.renewReconcileLease(any())).thenThrow(new StatusRuntimeException(Status.CANCELLED));
    when(stub2.renewReconcileLease(any()))
        .thenReturn(
            RenewReconcileLeaseResponse.newBuilder()
                .setRenewed(true)
                .setCancellationRequested(false)
                .build());

    RemoteReconcileExecutorClient.LeaseHeartbeat heartbeat = client.renew(remoteLease());

    assertThat(heartbeat.leaseValid()).isTrue();
    assertThat(heartbeat.cancellationRequested()).isFalse();
    verify(stub1).renewReconcileLease(any());
    verify(stub2).renewReconcileLease(any());
    assertThat(client.transportFailureLogs()).containsExactly("renewReconcileLease@cached#1");
  }

  @Test
  void closeWorkerControlChannelUsesGracefulShutdownForReset() throws Exception {
    GrpcRemoteReconcileExecutorClient client =
        new GrpcRemoteReconcileExecutorClient(
            "authorization", ignored -> java.util.Optional.of("Bearer worker-token"));
    ManagedChannel channel = mock(ManagedChannel.class);
    when(channel.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);

    client.closeWorkerControlChannel(channel, false);

    verify(channel).shutdown();
    verify(channel).awaitTermination(5, TimeUnit.SECONDS);
    org.mockito.Mockito.verify(channel, org.mockito.Mockito.never()).shutdownNow();
  }

  @Test
  void closeWorkerControlChannelUsesForcedShutdownForDestroy() throws Exception {
    GrpcRemoteReconcileExecutorClient client =
        new GrpcRemoteReconcileExecutorClient(
            "authorization", ignored -> java.util.Optional.of("Bearer worker-token"));
    ManagedChannel channel = mock(ManagedChannel.class);
    when(channel.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);

    client.closeWorkerControlChannel(channel, true);

    verify(channel).shutdownNow();
    verify(channel).awaitTermination(5, TimeUnit.SECONDS);
    org.mockito.Mockito.verify(channel, org.mockito.Mockito.never()).shutdown();
  }

  @Test
  void completeUsesDedicatedFreshChannel() throws Exception {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel, stub);
    when(stub.withInterceptors(any())).thenReturn(stub);
    when(stub.completeLeasedReconcileJob(any()))
        .thenReturn(CompleteLeasedReconcileJobResponse.newBuilder().setAccepted(true).build());
    when(channel.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);

    RemoteReconcileExecutorClient.CompletionResult result =
        client.complete(
            remoteLease(),
            RemoteLeasedJob.CompletionState.SUCCEEDED,
            ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE,
            ReconcileExecutor.ExecutionResult.RetryClass.NONE,
            1L,
            2L,
            3L,
            4L,
            5L,
            6L,
            7L,
            "done");

    assertThat(result.accepted()).isTrue();
    verify(stub).completeLeasedReconcileJob(any());
    verify(channel).shutdown();
    verify(channel).awaitTermination(5, TimeUnit.SECONDS);
    org.mockito.Mockito.verify(channel, org.mockito.Mockito.never()).shutdownNow();
  }

  @Test
  void completeTransportFailureDoesNotRetryAndClosesDedicatedChannel() throws Exception {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel, stub);
    when(stub.withInterceptors(any())).thenReturn(stub);
    when(stub.completeLeasedReconcileJob(any()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE));
    when(channel.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);

    assertThrows(
        StatusRuntimeException.class,
        () ->
            client.complete(
                remoteLease(),
                RemoteLeasedJob.CompletionState.SUCCEEDED,
                ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE,
                ReconcileExecutor.ExecutionResult.RetryClass.NONE,
                1L,
                2L,
                3L,
                4L,
                5L,
                6L,
                7L,
                "done"));

    verify(stub).completeLeasedReconcileJob(any());
    verify(channel).shutdown();
    verify(channel).awaitTermination(5, TimeUnit.SECONDS);
    assertThat(client.transportFailureLogs())
        .containsExactly("completeLeasedReconcileJob@dedicated#1");
  }

  @Test
  void submitPlanSnapshotTransportFailureDoesNotRetryAndClosesDedicatedChannel() throws Exception {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.snapshotPlanBlobStore = mock(SnapshotPlanBlobStore.class);
    client.enqueueTransport(channel, stub);
    when(stub.withInterceptors(any())).thenReturn(stub);
    when(stub.submitLeasedPlanSnapshotResult(any()))
        .thenThrow(new StatusRuntimeException(Status.INTERNAL));
    when(channel.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);
    when(client.snapshotPlanBlobStore.persistPlan(any(), any(), any(), any()))
        .thenReturn(ReconcileSnapshotTask.of("table-1", 55L, "db", "events"));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            client.submitPlanSnapshotSuccess(
                remotePlanSnapshotLease(),
                ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
                List.of(),
                List.of()));

    verify(stub).submitLeasedPlanSnapshotResult(any());
    verify(channel).shutdown();
    verify(channel).awaitTermination(5, TimeUnit.SECONDS);
    assertThat(client.transportFailureLogs())
        .containsExactly("submitLeasedPlanSnapshotResult@dedicated#1");
  }

  @Test
  void submitFileGroupSuccessRetriesWhenResultIdIsStable() {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel1 = mock(ManagedChannel.class);
    ManagedChannel channel2 = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub1 =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub2 =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel1, stub1);
    client.enqueueTransport(channel2, stub2);
    when(stub1.withInterceptors(any())).thenReturn(stub1);
    when(stub2.withInterceptors(any())).thenReturn(stub2);
    when(stub1.submitLeasedFileGroupExecutionResult(any()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE));
    when(stub2.submitLeasedFileGroupExecutionResult(any()))
        .thenReturn(
            SubmitLeasedFileGroupExecutionResultResponse.newBuilder().setAccepted(true).build());

    boolean accepted =
        client.submitSuccess(
            remoteFileGroupLease(),
            fileGroupPayload("s3://bucket/file.parquet"),
            StandaloneFileGroupExecutionResult.empty("result-1"));

    assertThat(accepted).isTrue();
    verify(stub1).submitLeasedFileGroupExecutionResult(any());
    verify(stub2).submitLeasedFileGroupExecutionResult(any());
  }

  @Test
  void submitFileGroupSuccessDoesNotRetryWhenResultIdIsBlank() throws Exception {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel, stub);
    when(stub.withInterceptors(any())).thenReturn(stub);
    when(stub.submitLeasedFileGroupExecutionResult(any()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE));
    when(channel.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);

    assertThrows(
        StatusRuntimeException.class,
        () ->
            client.submitSuccess(
                remoteFileGroupLease(),
                fileGroupPayload("s3://bucket/file.parquet"),
                StandaloneFileGroupExecutionResult.empty("  ")));

    verify(stub).submitLeasedFileGroupExecutionResult(any());
    verify(channel).shutdown();
    verify(channel).awaitTermination(5, TimeUnit.SECONDS);
    assertThat(client.transportFailureLogs())
        .containsExactly("submitLeasedFileGroupExecutionResult@cached#1");
  }

  @Test
  void submitFileGroupSuccessSendsInlineManifestFields() throws Exception {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel, stub);
    when(stub.withInterceptors(any())).thenReturn(stub);
    when(stub.submitLeasedFileGroupExecutionResult(any()))
        .thenReturn(
            SubmitLeasedFileGroupExecutionResultResponse.newBuilder().setAccepted(true).build());
    when(channel.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);

    var result = new StandaloneFileGroupExecutionResult("result-1", List.of(), List.of());

    assertThat(
            client.submitSuccess(
                remoteFileGroupLease(), fileGroupPayload("s3://bucket/file.parquet"), result))
        .isTrue();

    ArgumentCaptor<SubmitLeasedFileGroupExecutionResultRequest> requestCaptor =
        ArgumentCaptor.forClass(SubmitLeasedFileGroupExecutionResultRequest.class);
    verify(stub).submitLeasedFileGroupExecutionResult(requestCaptor.capture());
    var success = requestCaptor.getValue().getSuccess();
    assertThat(success.getResultId()).isEqualTo("result-1");
    assertThat(success.getFileResultsCount()).isEqualTo(1);
    assertThat(success.getFileResults(0).getFilePath()).isEqualTo("s3://bucket/file.parquet");
  }

  @Test
  void submitFileGroupSuccessSendsChunkedFileStats() throws Exception {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel, stub);
    when(stub.withInterceptors(any())).thenReturn(stub);
    when(stub.submitLeasedFileGroupExecutionResult(any()))
        .thenReturn(
            SubmitLeasedFileGroupExecutionResultResponse.newBuilder().setAccepted(true).build());
    when(channel.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);

    var result =
        new StandaloneFileGroupExecutionResult(
            "result-1",
            List.of(
                ai.floedb.floecat.stats.identity.TargetStatsRecords.fileRecord(
                    ResourceId.newBuilder()
                        .setAccountId("acct")
                        .setKind(ResourceKind.RK_TABLE)
                        .setId("table-1")
                        .build(),
                    55L,
                    ai.floedb.floecat.catalog.rpc.FileTargetStats.newBuilder()
                        .setFilePath("s3://bucket/data/file-1.parquet")
                        .setRowCount(3L)
                        .build(),
                    null)),
            List.of());

    assertThat(
            client.submitSuccess(
                remoteFileGroupLease(),
                fileGroupPayload("s3://bucket/data/file-1.parquet"),
                result))
        .isTrue();

    ArgumentCaptor<SubmitLeasedFileGroupExecutionResultRequest> requestCaptor =
        ArgumentCaptor.forClass(SubmitLeasedFileGroupExecutionResultRequest.class);
    verify(stub, org.mockito.Mockito.atLeastOnce())
        .submitLeasedFileGroupExecutionResult(requestCaptor.capture());
    assertThat(
            requestCaptor.getAllValues().stream()
                .anyMatch(SubmitLeasedFileGroupExecutionResultRequest::hasChunk))
        .isTrue();
    var success =
        requestCaptor.getAllValues().stream()
            .filter(SubmitLeasedFileGroupExecutionResultRequest::hasSuccess)
            .findFirst()
            .orElseThrow()
            .getSuccess();
    assertThat(success.getResultId()).isEqualTo("result-1");
    assertThat(success.getChunkCount()).isEqualTo(1);
  }

  @Test
  void submitFileGroupSuccessSplitsLargeResultsIntoMultipleChunks() throws Exception {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel, stub);
    when(stub.withInterceptors(any())).thenReturn(stub);
    when(stub.submitLeasedFileGroupExecutionResult(any()))
        .thenReturn(
            SubmitLeasedFileGroupExecutionResultResponse.newBuilder().setAccepted(true).build());
    when(channel.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);

    String largeFilePath = "s3://bucket/" + "x".repeat(16 * 1024) + ".parquet";
    var record =
        ai.floedb.floecat.stats.identity.TargetStatsRecords.fileRecord(
            ResourceId.newBuilder()
                .setAccountId("acct")
                .setKind(ResourceKind.RK_TABLE)
                .setId("table-1")
                .build(),
            55L,
            ai.floedb.floecat.catalog.rpc.FileTargetStats.newBuilder()
                .setFilePath(largeFilePath)
                .setRowCount(3L)
                .build(),
            null);
    var result =
        new StandaloneFileGroupExecutionResult(
            "result-1", java.util.Collections.nCopies(12, record), List.of());

    assertThat(
            client.submitSuccess(remoteFileGroupLease(), fileGroupPayload(largeFilePath), result))
        .isTrue();

    ArgumentCaptor<SubmitLeasedFileGroupExecutionResultRequest> requestCaptor =
        ArgumentCaptor.forClass(SubmitLeasedFileGroupExecutionResultRequest.class);
    verify(stub, org.mockito.Mockito.atLeast(3))
        .submitLeasedFileGroupExecutionResult(requestCaptor.capture());
    long chunkCount =
        requestCaptor.getAllValues().stream()
            .filter(SubmitLeasedFileGroupExecutionResultRequest::hasChunk)
            .count();
    assertThat(chunkCount).isGreaterThan(1L);
    var success =
        requestCaptor.getAllValues().stream()
            .filter(SubmitLeasedFileGroupExecutionResultRequest::hasSuccess)
            .findFirst()
            .orElseThrow()
            .getSuccess();
    assertThat(success.getChunkCount()).isEqualTo((int) chunkCount);
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
            ReconcileFileGroupTask.of("plan-1", "group-1", "table-1", 55L, List.of()),
            ""));
  }

  private static StandaloneFileGroupExecutionPayload fileGroupPayload(String... filePaths) {
    return new StandaloneFileGroupExecutionPayload(
        "job-lease",
        "lease-epoch",
        "",
        ai.floedb.floecat.connector.rpc.Connector.getDefaultInstance(),
        "db",
        "events",
        "s3://bucket/path",
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("table-1")
            .build(),
        55L,
        "plan-1",
        "group-1",
        List.of(filePaths),
        ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy.empty());
  }

  private static final class ExplicitTransportClient extends GrpcRemoteReconcileExecutorClient {
    private final List<String> transportFailureLogs = new ArrayList<>();
    private final Deque<ManagedChannel> channels = new ArrayDeque<>();
    private final Deque<ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub> stubs =
        new ArrayDeque<>();

    private ExplicitTransportClient() {
      super(
          "authorization",
          "worker-host",
          9100,
          ignored -> java.util.Optional.of("Bearer worker-token"));
    }

    private void enqueueTransport(
        ManagedChannel channel,
        ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub) {
      when(stub.withInterceptors(any())).thenReturn(stub);
      when(stub.withDeadlineAfter(anyLong(), any())).thenReturn(stub);
      channels.addLast(channel);
      stubs.addLast(stub);
    }

    @Override
    ManagedChannel newWorkerControlChannel() {
      return channels.removeFirst();
    }

    @Override
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub workerControlStub(
        ManagedChannel channel) {
      return stubs.removeFirst();
    }

    @Override
    void logWorkerControlTransportFailure(
        String operation, String path, int attempt, RuntimeException error) {
      transportFailureLogs.add(operation + "@" + path + "#" + attempt);
    }

    private List<String> transportFailureLogs() {
      return transportFailureLogs;
    }
  }
}
