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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.IndexArtifactState;
import ai.floedb.floecat.catalog.rpc.IndexFileTarget;
import ai.floedb.floecat.catalog.rpc.IndexTarget;
import ai.floedb.floecat.common.rpc.BlobHeader;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileExecutionPlan;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupResultDescriptor;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotSelection;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReusableArtifactBundleSelection;
import ai.floedb.floecat.reconciler.jobs.SnapshotPlanManifestIds;
import ai.floedb.floecat.reconciler.rpc.CommitLeasedFileGroupResultRequest;
import ai.floedb.floecat.reconciler.rpc.CommitLeasedFileGroupResultResponse;
import ai.floedb.floecat.reconciler.rpc.CompleteLeasedReconcileJobResponse;
import ai.floedb.floecat.reconciler.rpc.FileGroupResultPayload;
import ai.floedb.floecat.reconciler.rpc.GetLeasedPlanConnectorInputResponse;
import ai.floedb.floecat.reconciler.rpc.GetLeasedPlanTableInputResponse;
import ai.floedb.floecat.reconciler.rpc.GetLeasedSnapshotFinalizeInputResponse;
import ai.floedb.floecat.reconciler.rpc.LeasedPlanConnectorInput;
import ai.floedb.floecat.reconciler.rpc.LeasedPlanTableInput;
import ai.floedb.floecat.reconciler.rpc.LeasedSnapshotFinalizeInput;
import ai.floedb.floecat.reconciler.rpc.ListLeasedSnapshotFileGroupResultsResponse;
import ai.floedb.floecat.reconciler.rpc.ReconcileExecutorControlGrpc;
import ai.floedb.floecat.reconciler.rpc.RenewReconcileLeaseResponse;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifest;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifestDescriptor;
import ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanSnapshotResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanSnapshotResultResponse;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanTableResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanTableResultResponse;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedSnapshotFinalizeResultResponse;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
  void snapshotFinalizeInputAcceptsExplicitEmptyCoverage() {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel, stub);
    when(stub.getLeasedSnapshotFinalizeInput(any()))
        .thenReturn(
            GetLeasedSnapshotFinalizeInputResponse.newBuilder()
                .setInput(
                    LeasedSnapshotFinalizeInput.newBuilder()
                        .setJobId("finalize-job")
                        .setLeaseEpoch("lease-epoch")
                        .setParentJobId("snapshot-job")
                        .setTableId(tableId())
                        .setSnapshotId(55L)
                        .setFinalizeMode(
                            LeasedSnapshotFinalizeInput.FinalizeMode.FZM_EXPLICIT_EMPTY)
                        .setFileGroupCount(0)
                        .setSourceFileCount(0)
                        .setStatsObjectPrefix("/stats.pb")
                        .setCaptureManifestUri("/manifest.pb")
                        .setIndexPredecessor(
                            ai.floedb.floecat.reconciler.rpc.IndexGenerationPredecessor.newBuilder()
                                .setGenerationId("generation-1")
                                .setActivePointerVersion(7L)
                                .setCaptureManifestUri("/capture-1.pb")
                                .setCaptureManifestPointerVersion(9L))
                        .build())
                .build());

    StandaloneSnapshotFinalizeExecutionPayload payload =
        client.getSnapshotFinalizeInput(remoteSnapshotFinalizeLease());

    assertEquals(0, payload.fileGroupCount());
    assertEquals(0, payload.sourceFileCount());
    assertEquals("/manifest.pb", payload.captureManifestUri());
    assertEquals("generation-1", payload.indexPredecessor().generationId());
  }

  @Test
  void snapshotFinalizeInputAcceptsAppendOnlyDeltaGroups() {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel, stub);
    when(stub.getLeasedSnapshotFinalizeInput(any()))
        .thenReturn(
            GetLeasedSnapshotFinalizeInputResponse.newBuilder()
                .setInput(
                    LeasedSnapshotFinalizeInput.newBuilder()
                        .setJobId("finalize-job")
                        .setLeaseEpoch("lease-epoch")
                        .setParentJobId("snapshot-job")
                        .setTableId(tableId())
                        .setSnapshotId(55L)
                        .setFinalizeMode(LeasedSnapshotFinalizeInput.FinalizeMode.FZM_APPEND_ONLY)
                        .setFileGroupCount(111)
                        .setSourceFileCount(14_092)
                        .setSnapshotPlanUri("/snapshot-plan.json")
                        .setStatsObjectPrefix("/stats/")
                        .setCaptureManifestUri("/manifest.pb")
                        .build())
                .build());

    StandaloneSnapshotFinalizeExecutionPayload payload =
        client.getSnapshotFinalizeInput(remoteSnapshotFinalizeLease());

    assertEquals(111, payload.fileGroupCount());
    assertEquals(14_092, payload.sourceFileCount());
  }

  @Test
  void submitSnapshotFinalizeSuccessPublishesZeroGroupManifest() throws Exception {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel, stub);
    when(stub.submitLeasedSnapshotFinalizeResult(any()))
        .thenReturn(
            SubmitLeasedSnapshotFinalizeResultResponse.newBuilder().setAccepted(true).build());

    RemoteLeasedJob lease = remoteSnapshotFinalizeLease();
    var prepared =
        client.prepareSnapshotFinalizeSuccess(
            lease,
            "result-1",
            "/stats/",
            "/manifest.pb",
            0,
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            null);

    assertThat(client.submitSnapshotFinalizeSuccess(lease, prepared)).isTrue();

    ArgumentCaptor<byte[]> manifestBytes = ArgumentCaptor.forClass(byte[].class);
    verify(client.blobStore)
        .put(eq("/manifest.pb"), manifestBytes.capture(), eq("application/x-protobuf"));
    SnapshotCaptureManifest manifest = SnapshotCaptureManifest.parseFrom(manifestBytes.getValue());
    assertEquals(0, manifest.getFileGroupsCount());
    assertEquals(0, manifest.getSourceFileCount());
    assertThat(manifest.getReusableArtifactBundlesComplete()).isTrue();
  }

  @Test
  void snapshotFinalizeManifestCountsUniqueStatsTargetsAcrossGroups() throws Exception {
    ExplicitTransportClient client = new ExplicitTransportClient();
    byte[] payloadSha256 = new byte[32];
    payloadSha256[0] = 1;
    StatsObjectDescriptor sharedDeleteStats =
        StatsObjectDescriptor.newBuilder()
            .setTargetStorageId("file-delete")
            .setPayloadUri("/stats/delete.pb")
            .setPayloadBytes(12L)
            .setPayloadSha256(ByteString.copyFrom(payloadSha256))
            .build();

    client.prepareSnapshotFinalizeSuccess(
        remoteSnapshotFinalizeLease(2),
        "result-1",
        "/stats/",
        "/manifest.pb",
        2,
        List.of(
            fileGroupResultDescriptor("group-a", 1, null),
            fileGroupResultDescriptor("group-b", 1, null)),
        List.of(sharedDeleteStats),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        null);

    ArgumentCaptor<byte[]> manifestBytes = ArgumentCaptor.forClass(byte[].class);
    verify(client.blobStore)
        .put(eq("/manifest.pb"), manifestBytes.capture(), eq("application/x-protobuf"));
    SnapshotCaptureManifest manifest = SnapshotCaptureManifest.parseFrom(manifestBytes.getValue());
    assertThat(manifest.getFileStatsRecordCount()).isEqualTo(1);
    assertThat(manifest.getFileGroupsList())
        .allMatch(group -> group.getFileStatsRecordCount() == 1);
  }

  @Test
  void submitSnapshotFinalizeSuccessPublishesZeroGroupIndexPredecessor() throws Exception {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel, stub);
    when(stub.submitLeasedSnapshotFinalizeResult(any()))
        .thenReturn(
            SubmitLeasedSnapshotFinalizeResultResponse.newBuilder().setAccepted(true).build());

    RemoteLeasedJob lease = remoteSnapshotFinalizeLease(0, indexCapturePolicy());
    var prepared =
        client.prepareSnapshotFinalizeSuccess(
            lease,
            "result-1",
            "/stats/",
            "/manifest.pb",
            0,
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            indexPredecessor());

    assertThat(client.submitSnapshotFinalizeSuccess(lease, prepared)).isTrue();

    ArgumentCaptor<byte[]> manifestBytes = ArgumentCaptor.forClass(byte[].class);
    verify(client.blobStore)
        .put(eq("/manifest.pb"), manifestBytes.capture(), eq("application/x-protobuf"));
    SnapshotCaptureManifest manifest = SnapshotCaptureManifest.parseFrom(manifestBytes.getValue());
    assertThat(manifest.getFileGroupsCount()).isZero();
    assertThat(manifest.getIndexPredecessor().getGenerationId()).isEqualTo("generation-1");
  }

  @Test
  void submitSnapshotFinalizeSuccessPropagatesConsistentIndexPredecessor() throws Exception {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel, stub);
    when(stub.submitLeasedSnapshotFinalizeResult(any()))
        .thenReturn(
            SubmitLeasedSnapshotFinalizeResultResponse.newBuilder().setAccepted(true).build());

    RemoteLeasedJob lease = remoteSnapshotFinalizeLease(1, indexCapturePolicy());
    var prepared =
        client.prepareSnapshotFinalizeSuccess(
            lease,
            "result-1",
            "/stats/",
            "/manifest.pb",
            1,
            List.of(fileGroupResultDescriptor(indexPredecessor())),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of("customer_id"),
            List.of("customer_id"),
            null);

    assertThat(client.submitSnapshotFinalizeSuccess(lease, prepared)).isTrue();

    ArgumentCaptor<byte[]> manifestBytes = ArgumentCaptor.forClass(byte[].class);
    verify(client.blobStore)
        .put(eq("/manifest.pb"), manifestBytes.capture(), eq("application/x-protobuf"));
    SnapshotCaptureManifest manifest = SnapshotCaptureManifest.parseFrom(manifestBytes.getValue());
    assertThat(manifest.hasIndexPredecessor()).isTrue();
    assertThat(manifest.getIndexPredecessor().getGenerationId()).isEqualTo("generation-1");
    assertThat(manifest.getRealizedIndexSelectorsList()).containsExactly("customer_id");
    assertThat(manifest.getFileGroups(0).getIndexPredecessor())
        .isEqualTo(manifest.getIndexPredecessor());
  }

  @Test
  void submitSnapshotFinalizeSuccessRejectsInconsistentIndexPredecessors() {
    ExplicitTransportClient client = new ExplicitTransportClient();
    var otherPredecessor =
        new ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor(
            "generation-2", 8L, "/capture-2.pb", 10L);

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                client.prepareSnapshotFinalizeSuccess(
                    remoteSnapshotFinalizeLease(2, indexCapturePolicy()),
                    "result-1",
                    "/stats/",
                    "/manifest.pb",
                    2,
                    List.of(
                        fileGroupResultDescriptor(indexPredecessor()),
                        fileGroupResultDescriptor(otherPredecessor)),
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of(),
                    null));

    assertThat(error).hasMessageContaining("inconsistent predecessors");
    verify(client.blobStore, never()).put(any(), any(), any());
  }

  @Test
  void submitSnapshotFinalizeSuccessClassifiesUnknownRpcOutcomeAsStateUncertain() {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel1 = mock(ManagedChannel.class);
    ManagedChannel channel2 = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub1 =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub2 =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel1, stub1);
    client.enqueueTransport(channel2, stub2);
    when(stub1.submitLeasedSnapshotFinalizeResult(any()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE));
    when(stub2.submitLeasedSnapshotFinalizeResult(any()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE));
    var prepared =
        new RemoteSnapshotFinalizeWorkerClient.PreparedSnapshotFinalizeSuccess(
            "result-1", SnapshotCaptureManifestDescriptor.getDefaultInstance());

    ReconcileFailureException error =
        assertThrows(
            ReconcileFailureException.class,
            () -> client.submitSnapshotFinalizeSuccess(remoteSnapshotFinalizeLease(), prepared));

    assertThat(error.retryDisposition())
        .isEqualTo(ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE);
    assertThat(error.retryClass())
        .isEqualTo(ReconcileExecutor.ExecutionResult.RetryClass.STATE_UNCERTAIN);
    verify(stub1).submitLeasedSnapshotFinalizeResult(any());
    verify(stub2).submitLeasedSnapshotFinalizeResult(any());
  }

  @Test
  void snapshotFileGroupResultPagingRejectsRepeatedTokens() {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel, stub);
    when(stub.listLeasedSnapshotFileGroupResults(any()))
        .thenReturn(
            ListLeasedSnapshotFileGroupResultsResponse.newBuilder()
                .setNextPageToken("repeat")
                .build(),
            ListLeasedSnapshotFileGroupResultsResponse.newBuilder()
                .setNextPageToken("repeat")
                .build());

    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () -> client.listSnapshotFileGroupResults(remoteSnapshotFinalizeLease(1)));

    assertThat(error).hasMessageContaining("repeated token");
    verify(stub, times(2)).listLeasedSnapshotFileGroupResults(any());
  }

  @Test
  void snapshotFileGroupResultPagingIsBoundedByThePlannedCount() {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel, stub);
    when(stub.listLeasedSnapshotFileGroupResults(any()))
        .thenReturn(
            ListLeasedSnapshotFileGroupResultsResponse.newBuilder()
                .addDescriptors(
                    ai.floedb.floecat.reconciler.rpc.FileGroupResultDescriptor.getDefaultInstance())
                .build());

    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () -> client.listSnapshotFileGroupResults(remoteSnapshotFinalizeLease(0)));

    assertThat(error).hasMessageContaining("exceeds planned count 0");
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
            "plan-1",
            "group-1",
            "table-1",
            55L,
            1,
            "",
            0,
            List.of("s3://bucket/data/file-1.parquet"),
            List.of(),
            List.of(),
            "{\"type\":\"struct\",\"schema-id\":1,\"fields\":[]}",
            List.of(
                ReconcileFileExecutionPlan.of(
                        "s3://bucket/data/file-1.parquet",
                        100L,
                        "{}",
                        null,
                        "PARQUET",
                        3,
                        List.of(
                            new ReconcileFileExecutionPlan.IcebergDeleteFile(
                                "s3://bucket/data/delete-1.parquet",
                                10L,
                                ReconcileFileExecutionPlan.IcebergDeleteContent.EQUALITY,
                                3,
                                List.of(7),
                                "iceberg-delete-v1:8:2")),
                        "iceberg-data-v1:7:10")
                    .withReuseBundleSelections(
                        "source-fingerprint",
                        "index-fingerprint",
                        "stats-signature",
                        "index-signature",
                        Map.of("s3://bucket/data/delete-1.parquet", "delete-fingerprint"),
                        List.of(
                            new ReusableArtifactBundleSelection(
                                "bundle:abc",
                                "s3://artifacts/reuse-bundle.pb",
                                321L,
                                new byte[32],
                                List.of("s3://bucket/data/file-1.parquet"),
                                List.of("s3://bucket/data/file-1.parquet"))))));
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
    verify(successStub, never()).submitLeasedPlanSnapshotResult(any());
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
    assertThat(success.getChunkCount()).isZero();
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
  void submitPlanSnapshotFailureMapsLeaseFailedPreconditionToLeasePreconditionException()
      throws Exception {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel, stub);
    when(stub.submitLeasedPlanSnapshotResult(any()))
        .thenThrow(
            ReconcileLeaseGrpcStatus.leasePreconditionFailed("reconcile lease is no longer valid"));
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
  void submitPlanSnapshotFailurePropagatesIntegrityFailedPrecondition() throws Exception {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel, stub);
    when(stub.submitLeasedPlanSnapshotResult(any()))
        .thenThrow(
            Status.FAILED_PRECONDITION
                .withDescription(
                    "snapshot plan declared file_group_count=1 but staged 0 file group job(s)")
                .asRuntimeException());
    when(channel.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                client.submitPlanSnapshotFailure(
                    remotePlanSnapshotLease(),
                    ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL,
                    ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE,
                    ReconcileExecutor.ExecutionResult.RetryClass.TRANSIENT_ERROR,
                    "failed"));

    assertThat(ex.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
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
    when(stub1.commitLeasedFileGroupResult(any()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE));
    when(stub2.commitLeasedFileGroupResult(any()))
        .thenReturn(CommitLeasedFileGroupResultResponse.newBuilder().setAccepted(true).build());

    boolean accepted =
        client.submitSuccess(
            remoteFileGroupLease(),
            fileGroupPayload("s3://bucket/file.parquet"),
            StandaloneFileGroupExecutionResult.empty("result-1"));

    assertThat(accepted).isTrue();
    verify(stub1).commitLeasedFileGroupResult(any());
    verify(stub2).commitLeasedFileGroupResult(any());
  }

  @Test
  void submitFileGroupSuccessRejectsBlankResultIdBeforeRpc() {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel, stub);
    when(stub.withInterceptors(any())).thenReturn(stub);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            client.submitSuccess(
                remoteFileGroupLease(),
                fileGroupPayload("s3://bucket/file.parquet"),
                StandaloneFileGroupExecutionResult.empty("  ")));

    verify(stub, org.mockito.Mockito.never()).commitLeasedFileGroupResult(any());
    assertThat(client.transportFailureLogs()).isEmpty();
  }

  @Test
  void submitFileGroupSuccessSendsResultDescriptor() throws Exception {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel, stub);
    when(stub.withInterceptors(any())).thenReturn(stub);
    when(stub.commitLeasedFileGroupResult(any()))
        .thenReturn(CommitLeasedFileGroupResultResponse.newBuilder().setAccepted(true).build());
    when(channel.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);

    var result =
        new StandaloneFileGroupExecutionResult("result-1", List.of(), List.of(), List.of());

    assertThat(
            client.submitSuccess(
                remoteFileGroupLease(), fileGroupPayload("s3://bucket/file.parquet"), result))
        .isTrue();

    ArgumentCaptor<CommitLeasedFileGroupResultRequest> requestCaptor =
        ArgumentCaptor.forClass(CommitLeasedFileGroupResultRequest.class);
    verify(stub).commitLeasedFileGroupResult(requestCaptor.capture());
    var success = requestCaptor.getValue().getSuccess();
    assertThat(success.getResultId()).isEqualTo("result-1");
    assertThat(success.getResultDescriptor().getPlannedFileCount()).isEqualTo(1);
    assertThat(success.getResultDescriptor().getSucceededFileCount()).isEqualTo(1);
    assertThat(success.getResultDescriptor().getPayloadUri()).isEqualTo("/result.pb");
  }

  @Test
  void submitFileGroupSuccessPropagatesIndexPredecessor() throws Exception {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel, stub);
    when(stub.commitLeasedFileGroupResult(any()))
        .thenReturn(CommitLeasedFileGroupResultResponse.newBuilder().setAccepted(true).build());
    var predecessor =
        new StandaloneFileGroupExecutionPayload.IndexGenerationPredecessor(
            "generation-1", 7L, "/capture-1.pb", 9L);
    var payload = indexFileGroupPayload(predecessor);

    assertThat(
            client.submitSuccess(
                remoteFileGroupLease(),
                payload,
                StandaloneFileGroupExecutionResult.empty("result-1")))
        .isTrue();

    ArgumentCaptor<CommitLeasedFileGroupResultRequest> requestCaptor =
        ArgumentCaptor.forClass(CommitLeasedFileGroupResultRequest.class);
    verify(stub).commitLeasedFileGroupResult(requestCaptor.capture());
    var descriptorPredecessor =
        requestCaptor.getValue().getSuccess().getResultDescriptor().getIndexPredecessor();
    assertThat(descriptorPredecessor.getGenerationId()).isEqualTo("generation-1");
    assertThat(descriptorPredecessor.getActivePointerVersion()).isEqualTo(7L);

    ArgumentCaptor<byte[]> payloadBytes = ArgumentCaptor.forClass(byte[].class);
    verify(client.blobStore)
        .put(eq("/result.pb"), payloadBytes.capture(), eq("application/x-protobuf"));
    FileGroupResultPayload packedPayload =
        FileGroupResultPayload.parseFrom(payloadBytes.getValue());
    assertThat(packedPayload.getIndexPredecessor()).isEqualTo(descriptorPredecessor);
  }

  @Test
  void submitFileGroupSuccessRejectsDefaultIndexArtifactWithoutResolvedColumns() {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(),
            Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX),
            ReconcileCapturePolicy.DefaultColumnScope.ALL,
            32);
    var payload = indexFileGroupPayload("s3://bucket/file.parquet", policy);
    var result =
        new StandaloneFileGroupExecutionResult(
            "result-1",
            List.of(),
            List.of(),
            List.of(indexArtifact("s3://bucket/file.parquet", List.of())));

    assertThrows(
        IllegalArgumentException.class,
        () -> client.submitSuccess(remoteFileGroupLease(), payload, result));
  }

  @Test
  void explicitIndexArtifactMustReportRequestedSelector() {
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("#1", false, true)),
            Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX),
            ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY,
            32);
    var payload = indexFileGroupPayload("s3://bucket/file.parquet", policy);
    var result =
        new StandaloneFileGroupExecutionResult(
            "result-1",
            List.of(),
            List.of(),
            List.of(indexArtifact("s3://bucket/file.parquet", List.of())));

    assertThrows(
        IllegalArgumentException.class,
        () -> GrpcRemoteReconcileExecutorClient.validateIndexArtifactCoverage(payload, result));
  }

  @Test
  void explicitIndexArtifactCoverageUsesNormalizedSelectorIdentity() {
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("#1", false, true)),
            Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX),
            ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY,
            32);
    var payload = indexFileGroupPayload("s3://bucket/file.parquet", policy);
    var result =
        new StandaloneFileGroupExecutionResult(
            "result-1",
            List.of(),
            List.of(),
            List.of(indexArtifact("s3://bucket/file.parquet", List.of("customer_id", "id", "#1"))));

    GrpcRemoteReconcileExecutorClient.validateIndexArtifactCoverage(payload, result);
  }

  @Test
  void explicitIndexArtifactCoveragePreservesExactLogicalSelector() {
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("customer_id", false, true)),
            Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX),
            ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY,
            32);
    var payload = indexFileGroupPayload("s3://bucket/file.parquet", policy);
    var result =
        new StandaloneFileGroupExecutionResult(
            "result-1",
            List.of(),
            List.of(),
            List.of(
                indexArtifact(
                    "s3://bucket/file.parquet", List.of("customer_id", "physical_id", "#1"))));

    GrpcRemoteReconcileExecutorClient.validateIndexArtifactCoverage(payload, result);
  }

  @Test
  void explicitIndexArtifactCoverageDoesNotDropNamedRequirements() {
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(
                new ReconcileCapturePolicy.Column("#1", false, true),
                new ReconcileCapturePolicy.Column("customer_email", false, true)),
            Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX),
            ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY,
            32);
    var payload = indexFileGroupPayload("s3://bucket/file.parquet", policy);
    var result =
        new StandaloneFileGroupExecutionResult(
            "result-1",
            List.of(),
            List.of(),
            List.of(indexArtifact("s3://bucket/file.parquet", List.of("#1"))));

    assertThrows(
        IllegalArgumentException.class,
        () -> GrpcRemoteReconcileExecutorClient.validateIndexArtifactCoverage(payload, result));
  }

  @Test
  void submitFileGroupSuccessRejectsDefaultIndexArtifactExceedingFirstNLimit() {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(),
            Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX),
            ReconcileCapturePolicy.DefaultColumnScope.FIRST_N,
            2);
    var payload = indexFileGroupPayload("s3://bucket/file.parquet", policy);
    var result =
        new StandaloneFileGroupExecutionResult(
            "result-1",
            List.of(),
            List.of(),
            List.of(indexArtifact("s3://bucket/file.parquet", List.of("a", "b", "c"))));

    assertThrows(
        IllegalArgumentException.class,
        () -> client.submitSuccess(remoteFileGroupLease(), payload, result));
  }

  @Test
  void defaultIndexCoverageCountsStableIdAndNameAsOneColumn() {
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(),
            Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX),
            ReconcileCapturePolicy.DefaultColumnScope.FIRST_N,
            1);
    var payload = indexFileGroupPayload("s3://bucket/file.parquet", policy);
    var result =
        new StandaloneFileGroupExecutionResult(
            "result-1",
            List.of(),
            List.of(),
            List.of(indexArtifact("s3://bucket/file.parquet", List.of("#1", "customer_id", "id"))));

    GrpcRemoteReconcileExecutorClient.validateIndexArtifactCoverage(payload, result);
  }

  @Test
  void indexCoverageTreatsConnectorNativeCommaAsPartOfOneSelector() {
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(),
            Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX),
            ReconcileCapturePolicy.DefaultColumnScope.FIRST_N,
            1);
    var payload = indexFileGroupPayload("s3://bucket/file.parquet", policy);
    var result =
        new StandaloneFileGroupExecutionResult(
            "result-1",
            List.of(),
            List.of(),
            List.of(indexArtifact("s3://bucket/file.parquet", List.of("a,b"))));

    GrpcRemoteReconcileExecutorClient.validateIndexArtifactCoverage(payload, result);
  }

  @Test
  void progressiveFileStatsPublicationWritesObjectBeforeTerminalDescriptorSubmission()
      throws Exception {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel, stub);
    when(stub.withInterceptors(any())).thenReturn(stub);
    when(stub.commitLeasedFileGroupResult(any()))
        .thenReturn(CommitLeasedFileGroupResultResponse.newBuilder().setAccepted(true).build());
    when(channel.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);

    var payload = fileGroupPayload("s3://bucket/data/file-1.parquet");
    var record =
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
                null)
            .toBuilder()
            .putProperties(
                FileArtifactReuse.REALIZED_STATS_SELECTORS_PROPERTY,
                FileArtifactReuse.encodeSelectors(List.of("a,b")))
            .build();
    var result =
        new StandaloneFileGroupExecutionResult("result-1", List.of(), List.of(record), List.of());

    assertThat(client.submitSuccess(remoteFileGroupLease(), payload, result)).isTrue();

    ArgumentCaptor<CommitLeasedFileGroupResultRequest> requestCaptor =
        ArgumentCaptor.forClass(CommitLeasedFileGroupResultRequest.class);
    verify(stub).commitLeasedFileGroupResult(requestCaptor.capture());
    var success = requestCaptor.getValue().getSuccess();
    assertThat(success.getResultId()).isEqualTo("result-1");
    assertThat(success.getResultDescriptor().getFileStatsRecordCount()).isEqualTo(1);
    assertThat(success.getResultDescriptor().getStatsObjectPrefix()).isEqualTo("/stats/");
    ArgumentCaptor<String> blobUris = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<byte[]> blobBytes = ArgumentCaptor.forClass(byte[].class);
    verify(client.blobStore, times(2))
        .put(blobUris.capture(), blobBytes.capture(), eq("application/x-protobuf"));
    assertThat(blobUris.getAllValues()).contains("/result.pb");
    assertThat(blobUris.getAllValues())
        .anyMatch(uri -> uri.startsWith("/stats/reuse-bundles/") && uri.endsWith(".pb"));
    int resultPayloadIndex = blobUris.getAllValues().indexOf("/result.pb");
    assertThat(
            FileGroupResultPayload.parseFrom(blobBytes.getAllValues().get(resultPayloadIndex))
                .getReusableArtifactBundle()
                .getFileStats(0)
                .getRealizedStatsSelectorsList())
        .containsExactly("a,b");
  }

  @Test
  void submitFileGroupSuccessDoesNotSendLargeStatsOverGrpc() throws Exception {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel, stub);
    when(stub.withInterceptors(any())).thenReturn(stub);
    when(stub.commitLeasedFileGroupResult(any()))
        .thenReturn(CommitLeasedFileGroupResultResponse.newBuilder().setAccepted(true).build());
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
    var payload = fileGroupPayload(largeFilePath);
    var result =
        new StandaloneFileGroupExecutionResult("result-1", List.of(), List.of(record), List.of());

    assertThat(client.submitSuccess(remoteFileGroupLease(), payload, result)).isTrue();

    ArgumentCaptor<CommitLeasedFileGroupResultRequest> requestCaptor =
        ArgumentCaptor.forClass(CommitLeasedFileGroupResultRequest.class);
    verify(stub).commitLeasedFileGroupResult(requestCaptor.capture());
    var success = requestCaptor.getValue().getSuccess();
    assertThat(success.getResultDescriptor().getFileStatsRecordCount()).isEqualTo(1);
    assertThat(success.hasArtifactBundle()).isTrue();
    assertThat(success.getArtifactBundle().getFileStatsTargetStorageIdsCount()).isEqualTo(1);
    assertThat(success.getSerializedSize()).isLessThan(4 * 1024);
  }

  @Test
  void submitFileGroupSuccessPreservesIndexDestinationAndStableSelectorAliases() throws Exception {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel, stub);
    when(stub.commitLeasedFileGroupResult(any()))
        .thenReturn(CommitLeasedFileGroupResultResponse.newBuilder().setAccepted(true).build());
    String filePath = "s3://source/file.parquet";
    String artifactUri = "s3://bucket/file.parquet.idx";
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("#1", false, true)),
            Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
    ReconcileFileExecutionPlan plan =
        ReconcileFileExecutionPlan.of(
            filePath, 1L, "", null, "PARQUET", 0, List.of(), "content-identity");
    StandaloneFileGroupExecutionPayload base = indexFileGroupPayload(filePath, policy);
    StandaloneFileGroupExecutionPayload payload =
        new StandaloneFileGroupExecutionPayload(
            base.jobId(),
            base.leaseEpoch(),
            base.parentJobId(),
            base.sourceConnector(),
            base.sourceNamespace(),
            base.sourceTable(),
            base.storageLocation(),
            base.tableId(),
            base.snapshotId(),
            base.planId(),
            base.groupId(),
            base.resultPayloadUri(),
            base.statsObjectPrefix(),
            base.plannedFilePaths(),
            base.executionSchemaJson(),
            List.of(plan),
            base.capturePolicy(),
            base.indexPredecessor(),
            base.predecessorIndexArtifacts());
    when(client.blobStore.head(artifactUri))
        .thenReturn(
            java.util.Optional.of(
                BlobHeader.newBuilder().setContentLength(3).setEtag("etag-1").build()));
    var artifact =
        new ReconcilerBackend.StagedIndexArtifact(
            IndexArtifactRecord.newBuilder()
                .setTarget(
                    IndexTarget.newBuilder()
                        .setFile(IndexFileTarget.newBuilder().setFilePath(filePath).build())
                        .build())
                .setArtifactUri(artifactUri)
                .setArtifactFormat("parquet")
                .setArtifactFormatVersion(1)
                .setState(IndexArtifactState.IAS_READY)
                .putProperties(
                    FileArtifactReuse.INDEXED_COLUMNS_PROPERTY,
                    FileArtifactReuse.encodeSelectors(List.of("#1", "customer_id")))
                .putProperties(
                    FileArtifactReuse.SOURCE_FINGERPRINT_PROPERTY,
                    FileArtifactReuse.indexSourceFingerprint(plan))
                .putProperties(
                    FileArtifactReuse.INDEX_SIGNATURE_PROPERTY,
                    FileArtifactReuse.indexCaptureSignature(policy, ""))
                .build(),
            null,
            "application/x-parquet");
    var result =
        new StandaloneFileGroupExecutionResult("result-1", List.of(), List.of(), List.of(artifact));

    assertThat(client.submitSuccess(remoteFileGroupLease(), payload, result)).isTrue();

    verify(client.blobStore, times(1)).head(artifactUri);
    ArgumentCaptor<CommitLeasedFileGroupResultRequest> requestCaptor =
        ArgumentCaptor.forClass(CommitLeasedFileGroupResultRequest.class);
    verify(stub).commitLeasedFileGroupResult(requestCaptor.capture());
    String payloadUri = requestCaptor.getValue().getSuccess().getResultDescriptor().getPayloadUri();
    ArgumentCaptor<String> uriCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<byte[]> bytesCaptor = ArgumentCaptor.forClass(byte[].class);
    verify(client.blobStore, times(2))
        .put(uriCaptor.capture(), bytesCaptor.capture(), eq("application/x-protobuf"));
    int metadataIndex =
        java.util.stream.IntStream.range(0, uriCaptor.getAllValues().size())
            .filter(
                index -> uriCaptor.getAllValues().get(index).startsWith("/stats/reuse-bundles/"))
            .findFirst()
            .orElseThrow();
    assertThat(uriCaptor.getAllValues().get(metadataIndex)).startsWith("/stats/reuse-bundles/");
    assertThat(
            ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundlePayload.parseFrom(
                    bytesCaptor.getAllValues().get(metadataIndex))
                .getIndexArtifacts(0)
                .getArtifactUri())
        .isEqualTo(artifactUri);
    int resultPayloadIndex = uriCaptor.getAllValues().indexOf(payloadUri);
    assertThat(resultPayloadIndex).isNotNegative();
    var packedResult =
        FileGroupResultPayload.parseFrom(bytesCaptor.getAllValues().get(resultPayloadIndex));
    assertThat(
            packedResult
                .getReusableArtifactBundle()
                .getIndexArtifacts(0)
                .getRealizedIndexSelectorsList())
        .containsExactly("#1", "customer_id");
    assertThat(packedResult.getRealizedIndexSelectorsList()).containsExactly("#1", "customer_id");

    ReconcileFileExecutionPlan enriched =
        RemoteFileArtifactReusePlanner.enrichFromBundles(
                "", List.of(plan), policy, false, List.of(packedResult.getReusableArtifactBundle()))
            .getFirst();
    assertThat(enriched.reusesIndexArtifact()).isTrue();
  }

  @Test
  void submitFileGroupSuccessVerifiesReusedIndexSidecar() {
    ExplicitTransportClient client = new ExplicitTransportClient();
    ManagedChannel channel = mock(ManagedChannel.class);
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub stub =
        mock(ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub.class);
    client.enqueueTransport(channel, stub);
    when(stub.withInterceptors(any())).thenReturn(stub);
    when(stub.commitLeasedFileGroupResult(any()))
        .thenReturn(CommitLeasedFileGroupResultResponse.newBuilder().setAccepted(true).build());
    String filePath = "s3://bucket/file.parquet";
    String artifactUri = "s3://sidecars/reused.parquet";
    ReconcileCapturePolicy policy = indexCapturePolicy();
    StandaloneFileGroupExecutionPayload base = indexFileGroupPayload(filePath, policy);
    ReconcileFileExecutionPlan plan =
        ReconcileFileExecutionPlan.of(
                filePath, 1L, "", null, "PARQUET", 0, List.of(), "content-identity")
            .withReuseBundleSelections(
                "stats-source",
                "index-source",
                "stats-signature",
                "index-signature",
                Map.of(),
                List.of(
                    new ReusableArtifactBundleSelection(
                        "reuse-bundle:prior",
                        "/stats/reuse-bundles/prior.pb",
                        1L,
                        new byte[32],
                        List.of(),
                        List.of(filePath))));
    StandaloneFileGroupExecutionPayload payload =
        new StandaloneFileGroupExecutionPayload(
            base.jobId(),
            base.leaseEpoch(),
            base.parentJobId(),
            base.sourceConnector(),
            base.sourceNamespace(),
            base.sourceTable(),
            base.storageLocation(),
            base.tableId(),
            base.snapshotId(),
            base.planId(),
            base.groupId(),
            base.resultPayloadUri(),
            base.statsObjectPrefix(),
            base.plannedFilePaths(),
            base.executionSchemaJson(),
            List.of(plan),
            base.capturePolicy(),
            base.indexPredecessor(),
            base.predecessorIndexArtifacts());
    var record =
        IndexArtifactRecord.newBuilder()
            .setTarget(
                IndexTarget.newBuilder()
                    .setFile(IndexFileTarget.newBuilder().setFilePath(filePath)))
            .setState(IndexArtifactState.IAS_READY)
            .setArtifactUri(artifactUri)
            .setContentEtag("prior-etag")
            .putProperties(
                FileArtifactReuse.INDEXED_COLUMNS_PROPERTY,
                FileArtifactReuse.encodeSelectors(List.of("#1")))
            .putProperties(FileArtifactReuse.SOURCE_FINGERPRINT_PROPERTY, "index-source")
            .putProperties(FileArtifactReuse.INDEX_SIGNATURE_PROPERTY, "index-signature")
            .build();
    var result =
        new StandaloneFileGroupExecutionResult(
            "result-1",
            List.of(),
            List.of(),
            List.of(new ReconcilerBackend.StagedIndexArtifact(record, null, "")));
    when(client.blobStore.head(artifactUri))
        .thenReturn(
            Optional.of(
                BlobHeader.newBuilder().setContentLength(128L).setEtag("prior-etag").build()));

    assertThat(client.submitSuccess(remoteFileGroupLease(), payload, result)).isTrue();

    verify(client.blobStore).head(artifactUri);
    verify(client.blobStore, never()).put(eq(artifactUri), any(byte[].class), any(String.class));

    when(client.blobStore.head(artifactUri)).thenReturn(Optional.empty());
    assertThrows(
        IllegalArgumentException.class,
        () -> client.submitSuccess(remoteFileGroupLease(), payload, result));

    when(client.blobStore.head(artifactUri))
        .thenReturn(
            Optional.of(
                BlobHeader.newBuilder()
                    .setContentLength(128L)
                    .setEtag("replacement-etag")
                    .build()));
    assertThrows(
        IllegalArgumentException.class,
        () -> client.submitSuccess(remoteFileGroupLease(), payload, result));
  }

  private static ResourceId connectorId() {
    return ResourceId.newBuilder()
        .setAccountId("acct")
        .setKind(ResourceKind.RK_CONNECTOR)
        .setId("connector-1")
        .build();
  }

  private static ResourceId tableId() {
    return ResourceId.newBuilder()
        .setAccountId("acct")
        .setKind(ResourceKind.RK_TABLE)
        .setId("table-1")
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

  private static RemoteLeasedJob remoteSnapshotFinalizeLease() {
    return remoteSnapshotFinalizeLease(0);
  }

  private static RemoteLeasedJob remoteSnapshotFinalizeLease(int fileGroupCount) {
    return remoteSnapshotFinalizeLease(fileGroupCount, ReconcileCapturePolicy.empty());
  }

  private static RemoteLeasedJob remoteSnapshotFinalizeLease(
      int fileGroupCount, ReconcileCapturePolicy capturePolicy) {
    return new RemoteLeasedJob(
        new ReconcileJobStore.LeasedJob(
            "finalize-job",
            "acct",
            "connector-1",
            false,
            ReconcilerService.CaptureMode.CAPTURE_ONLY,
            ReconcileScope.of(List.of(), "table-1", List.of(), capturePolicy),
            null,
            "lease-epoch",
            "",
            "",
            ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE,
            null,
            null,
            ReconcileSnapshotTask.of(
                "table-1",
                55L,
                "db",
                "events",
                List.of(),
                true,
                ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                "",
                fileGroupCount),
            null,
            "snapshot-job"));
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
        "/result.pb",
        "/stats/",
        List.of(filePaths),
        "",
        List.of(),
        ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy.empty());
  }

  private static StandaloneFileGroupExecutionPayload indexFileGroupPayload(
      StandaloneFileGroupExecutionPayload.IndexGenerationPredecessor predecessor) {
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
        "/result.pb",
        "/stats/",
        List.of(),
        "",
        List.of(),
        indexCapturePolicy(),
        predecessor,
        List.of());
  }

  private static StandaloneFileGroupExecutionPayload indexFileGroupPayload(
      String filePath, ReconcileCapturePolicy policy) {
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
        "/result.pb",
        "/stats/",
        List.of(filePath),
        "",
        List.of(),
        policy,
        new StandaloneFileGroupExecutionPayload.IndexGenerationPredecessor(
            "generation-1", 7L, "/capture-1.pb", 9L),
        List.of());
  }

  private static ReconcilerBackend.StagedIndexArtifact indexArtifact(
      String filePath, List<String> indexedColumns) {
    IndexArtifactRecord.Builder record =
        IndexArtifactRecord.newBuilder()
            .setTarget(
                IndexTarget.newBuilder()
                    .setFile(IndexFileTarget.newBuilder().setFilePath(filePath)))
            .setState(IndexArtifactState.IAS_READY)
            .setArtifactUri(filePath + ".idx");
    if (indexedColumns != null && !indexedColumns.isEmpty()) {
      record.putProperties(
          FileArtifactReuse.INDEXED_COLUMNS_PROPERTY,
          FileArtifactReuse.encodeSelectors(indexedColumns));
    }
    return new ReconcilerBackend.StagedIndexArtifact(
        record.build(), new byte[] {1}, "application/x-parquet");
  }

  private static ReconcileCapturePolicy indexCapturePolicy() {
    return ReconcileCapturePolicy.of(
        List.of(), Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
  }

  private static ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor indexPredecessor() {
    return new ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor(
        "generation-1", 7L, "/capture-1.pb", 9L);
  }

  private static ReconcileFileGroupResultDescriptor fileGroupResultDescriptor(
      ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor predecessor) {
    return fileGroupResultDescriptor("group-1", 0, predecessor);
  }

  private static ReconcileFileGroupResultDescriptor fileGroupResultDescriptor(
      String groupId,
      int fileStatsRecordCount,
      ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor predecessor) {
    return new ReconcileFileGroupResultDescriptor(
        1,
        "acct",
        "connector-1",
        "snapshot-job",
        "file-group-job-" + groupId,
        "plan-1",
        groupId,
        "table-1",
        55L,
        "file-group-lease-" + groupId,
        "file-group-result-" + groupId,
        "/result-" + groupId + ".pb",
        1L,
        java.util.Base64.getEncoder().encodeToString(new byte[32]),
        1,
        1,
        0,
        0,
        0,
        0,
        "/stats/",
        fileStatsRecordCount,
        ai.floedb.floecat.reconciler.jobs.ArtifactReferenceDigest.sha256(List.of(), List.of()),
        predecessor,
        1L);
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
      blobStore = mock(BlobStore.class);
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
