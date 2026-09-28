/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.statistics.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.BeginOwnerPublicationRequest;
import ai.floedb.floecat.catalog.rpc.CompleteOwnerPublicationRequest;
import ai.floedb.floecat.catalog.rpc.OwnerPublicationManifestRef;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotReuseManifestKind;
import ai.floedb.floecat.catalog.rpc.SnapshotReuseManifestRef;
import ai.floedb.floecat.catalog.rpc.SnapshotSpec;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.rpc.CaptureOutput;
import ai.floedb.floecat.reconciler.rpc.CapturePolicy;
import ai.floedb.floecat.reconciler.rpc.DefaultColumnScope;
import ai.floedb.floecat.reconciler.rpc.OwnerArtifactObjectReference;
import ai.floedb.floecat.reconciler.rpc.OwnerArtifactRegistrationManifestRef;
import ai.floedb.floecat.reconciler.rpc.ReusableCoverageManifestRef;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifest;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifestKind;
import ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor;
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import ai.floedb.floecat.service.catalog.impl.CurrentSnapshotPointerService;
import ai.floedb.floecat.service.reconciler.impl.SnapshotFinalizePersistenceService;
import ai.floedb.floecat.service.repo.impl.IndexArtifactRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.service.testsupport.TestNodes;
import ai.floedb.floecat.service.testsupport.TestPrincipals;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.types.Hashing;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class OwnerPublicationServiceImplTest {
  private static final long SNAPSHOT = 42L;
  private static final String CALLER_SUBJECT = "owner-service-account";
  private static final java.util.Map<String, byte[]> EXTERNAL_OBJECTS =
      new java.util.concurrent.ConcurrentHashMap<>();

  @Test
  void beginReservesGenerationAndCreatesFloecatManifest() {
    var service = service();
    when(service.statsStore.statsGenerationExists(any(), anyLong(), anyString())).thenReturn(false);

    var response = service.beginOwnerPublication(begin()).await().indefinitely();

    assertTrue(response.getPublicationId().startsWith("owner-"));
    assertEquals(response.getPublicationId(), response.getGenerationId());
    assertTrue(response.getExecutorObjectPrefix().endsWith("/worker-uploads/"));
    assertTrue(response.getOwnerObjectPrefix().endsWith("/finalizer-outputs/"));
    assertTrue(response.getManifestObjectPrefix().endsWith("/index-artifacts/capture-manifests/"));
    assertTrue(response.getReusableObjectPrefix().endsWith("/reusable-artifacts/"));
    assertTrue(response.getReuseLeaseExpiresAtEpochMs() > 0L);
    assertEquals(
        OwnerArtifactRegistrationManifest.DEFAULT_READ_BYTES,
        response.getExternalManifestChunkMaxBytes());
    assertEquals(
        OwnerArtifactRegistrationManifest.DEFAULT_MAX_OBJECTS,
        response.getRegistrationChunkMaxObjects());
    assertEquals(
        OwnerArtifactRegistrationManifest.DEFAULT_MAX_TARGETS,
        response.getRegistrationChunkMaxTargets());
    verify(service.statsStore)
        .beginStatsGeneration(tableId(), SNAPSHOT, response.getGenerationId());
    verify(service.statsStore)
        .prepareStatsGenerationManifest(tableId(), SNAPSHOT, response.getGenerationId());
    verify(service.snapshots, never()).getById(any(), anyLong());
  }

  @Test
  void beginRetryReturnsTheExistingPublicationWithoutRestagingIt() {
    var service = service();

    var first = service.beginOwnerPublication(begin()).await().indefinitely();
    var retry = service.beginOwnerPublication(begin()).await().indefinitely();

    assertEquals(first.getPublicationId(), retry.getPublicationId());
    assertEquals(first.getGenerationId(), retry.getGenerationId());
    assertTrue(retry.getReuseLeaseExpiresAtEpochMs() >= first.getReuseLeaseExpiresAtEpochMs());
    verify(service.statsStore, times(2))
        .statsGenerationExists(tableId(), SNAPSHOT, first.getGenerationId());
    verify(service.statsStore, never()).beginStatsGeneration(any(), anyLong(), anyString());
    verify(service.statsStore, times(2))
        .prepareStatsGenerationManifest(tableId(), SNAPSHOT, first.getGenerationId());
  }

  @Test
  void beginLeasesTheSelectedReuseManifest() {
    var service = service();
    var sourceRef =
        SnapshotReuseManifestRef.newBuilder()
            .setFormatVersion(1)
            .setKind(SnapshotReuseManifestKind.SRMK_OWNER_V2)
            .setUri("/source/capture.pb")
            .setPayloadBytes(32)
            .setPayloadSha256(ByteString.copyFrom(new byte[32]))
            .build();
    when(service.snapshots.getByIdConsistent(tableId(), 41L))
        .thenReturn(
            Optional.of(
                Snapshot.newBuilder()
                    .setTableId(tableId())
                    .setSnapshotId(41L)
                    .setReuseManifestRef(sourceRef)
                    .build()));
    var request = begin().toBuilder().setReuseSourceSnapshotId(41L).build();

    var response = service.beginOwnerPublication(request).await().indefinitely();

    assertTrue(response.getReuseSourceLeased());
    verify(service.reuseLeases)
        .acquire(
            tableId(),
            response.getGenerationId(),
            Keys.snapshotIndexArtifactCaptureManifestBlobPrefix(
                tableId().getAccountId(), tableId().getId(), SNAPSHOT),
            sourceRef);
  }

  @Test
  void beginDoesNotLeaseAReconcilerReuseManifest() {
    var service = service();
    var sourceRef =
        SnapshotReuseManifestRef.newBuilder()
            .setFormatVersion(1)
            .setKind(SnapshotReuseManifestKind.SRMK_RECONCILER)
            .setUri("/source/capture.pb")
            .setPayloadBytes(32)
            .setPayloadSha256(ByteString.copyFrom(new byte[32]))
            .build();
    when(service.snapshots.getByIdConsistent(tableId(), 41L))
        .thenReturn(
            Optional.of(
                Snapshot.newBuilder()
                    .setTableId(tableId())
                    .setSnapshotId(41L)
                    .setReuseManifestRef(sourceRef)
                    .build()));

    var response =
        service
            .beginOwnerPublication(begin().toBuilder().setReuseSourceSnapshotId(41L).build())
            .await()
            .indefinitely();

    assertFalse(response.getReuseSourceLeased());
    verify(service.reuseLeases)
        .acquire(
            tableId(),
            response.getGenerationId(),
            Keys.snapshotIndexArtifactCaptureManifestBlobPrefix(
                tableId().getAccountId(), tableId().getId(), SNAPSHOT),
            null);
  }

  @Test
  void completeRejectsPublicationThatWasNotBegun() {
    var service = service();
    when(service.statsStore.statsGenerationExists(any(), anyLong(), anyString())).thenReturn(false);

    var error =
        assertThrows(
            StatusRuntimeException.class,
            () -> service.completeOwnerPublication(complete(service)).await().indefinitely());

    assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
    verify(service.persistence, never())
        .publishPreparedStatsGeneration(any(), anyLong(), anyString(), any(), any(), any());
  }

  @Test
  void completeRejectsAManifestLargerThanItsDescriptorWithOneBoundedRead() {
    var service = service();
    var request = complete(service);
    int maximumRead = Math.toIntExact(request.getManifest().getManifestBytes() + 1L);
    byte[] manifest =
        service.blobStore.getRangeAtMost(request.getManifest().getManifestUri(), 0L, maximumRead);
    when(service.blobStore.getRangeAtMost(request.getManifest().getManifestUri(), 0L, maximumRead))
        .thenReturn(java.util.Arrays.copyOf(manifest, maximumRead));
    clearInvocations(service.blobStore);

    var error =
        assertThrows(
            StatusRuntimeException.class,
            () -> service.completeOwnerPublication(request).await().indefinitely());

    assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
    verify(service.blobStore, times(1))
        .getRangeAtMost(request.getManifest().getManifestUri(), 0L, maximumRead);
    verify(service.reuseLeases, never()).renew(any(), anyString(), any());
  }

  @Test
  void completeRejectsAProtectionGapInsteadOfReacquiringTheLease() {
    var service = service();
    when(service.reuseLeases.renew(any(), anyString(), any()))
        .thenThrow(
            new OwnerReuseLeaseRepository.LeaseContinuityException(
                "publication protection was lost"));

    var error =
        assertThrows(
            StatusRuntimeException.class,
            () -> service.completeOwnerPublication(complete(service)).await().indefinitely());

    assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
    verify(service.statsStore, never())
        .registerPrewrittenStatsReferencesInGeneration(any(), anyLong(), anyString(), any());
  }

  @Test
  void completeRejectsPublicationReservedByAnotherPrincipal() {
    var service = service();
    var request = complete(service);
    when(service.principal.get().getSubject()).thenReturn("different-owner-service-account");

    var error =
        assertThrows(
            StatusRuntimeException.class,
            () -> service.completeOwnerPublication(request).await().indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, error.getStatus().getCode());
    verify(service.blobStore, never()).get(anyString());
  }

  @Test
  void completeRegistersCanonicalReferencesAndActivatesOnce() {
    var service = service();

    var response = service.completeOwnerPublication(complete(service)).await().indefinitely();

    assertTrue(response.getActivated());
    assertEquals(2, response.getAggregateStatsPublished());
    var references =
        ArgumentCaptor.<java.util.List<StatsStore.PrewrittenTargetStatsReference>>captor();
    verify(service.statsStore)
        .registerPrewrittenStatsReferencesInGeneration(
            any(), anyLong(), anyString(), references.capture());
    assertEquals(
        java.util.List.of("column-0000000000000000001", "table"),
        references.getValue().stream()
            .map(StatsStore.PrewrittenTargetStatsReference::targetStorageId)
            .toList());
    verify(service.persistence).clearPrewrittenArtifactProtections(any(), anyLong(), anyString());
    verify(service.currentSnapshots).maybeAdvance(any(), any(Snapshot.class), anyString());
    var publishedSnapshot = ArgumentCaptor.<Snapshot>captor();
    verify(service.snapshots).prepareCreatePublicationUpdates(publishedSnapshot.capture());
    SnapshotReuseManifestRef reuseManifest = publishedSnapshot.getValue().getReuseManifestRef();
    verify(service.reuseLeases)
        .renew(
            tableId(),
            OwnerPublicationServiceImpl.generationId(begin(), CALLER_SUBJECT),
            reuseManifest);
    verify(service.reuseLeases)
        .release(tableId(), OwnerPublicationServiceImpl.generationId(begin(), CALLER_SUBJECT));
    var publicationOrder =
        org.mockito.Mockito.inOrder(service.currentSnapshots, service.reuseLeases);
    publicationOrder
        .verify(service.currentSnapshots)
        .maybeAdvance(any(), any(Snapshot.class), anyString());
    publicationOrder
        .verify(service.reuseLeases)
        .release(tableId(), OwnerPublicationServiceImpl.generationId(begin(), CALLER_SUBJECT));
    assertEquals(1, reuseManifest.getFormatVersion());
    assertEquals(SnapshotReuseManifestKind.SRMK_OWNER_V2, reuseManifest.getKind());
    assertEquals(32, reuseManifest.getPayloadSha256().size());
  }

  @Test
  void completeRegistersAnUnboundedManifestInDurableBatches() {
    var service = service();
    var publication = begin();
    String generationId = OwnerPublicationServiceImpl.generationId(publication, CALLER_SUBJECT);
    var manifest = baseManifest(generationId).clearFinalStats().setFinalStatsRecordCount(0);
    int recordCount = 300;
    String largeSegment = "finalizer-outputs/" + "x".repeat(32 * 1024);
    for (int index = 0; index < recordCount; index++) {
      manifest.addFinalStats(
          reference(
              generationId, largeSegment, String.format("column-%019d", index + 1L), (byte) index));
    }
    manifest.setFinalStatsRecordCount(recordCount);
    var firstRequest = complete(service, publication, manifest.build());

    var first = service.completeOwnerPublication(firstRequest).await().indefinitely();

    assertFalse(first.getActivated());
    assertEquals(Long.MAX_VALUE, first.getReuseLeaseExpiresAtEpochMs());
    assertFalse(first.getNextCompletionCursor().isBlank());
    var nextProgress = ArgumentCaptor.<OwnerReuseLeaseRepository.RegistrationProgress>captor();
    verify(service.reuseLeases)
        .advanceProgress(
            eq(tableId()),
            eq(generationId),
            anyString(),
            eq(OwnerReuseLeaseRepository.RegistrationProgress.initial()),
            nextProgress.capture());
    assertEquals(1L, nextProgress.getValue().registrationChunk());
    verify(service.persistence, never())
        .publishPreparedStatsGeneration(any(), anyLong(), anyString(), any(), any(), any());

    when(service.reuseLeases.progress(any(), anyString(), anyString()))
        .thenReturn(nextProgress.getValue());
    var secondRequest =
        firstRequest.toBuilder().setCompletionCursor(first.getNextCompletionCursor()).build();
    var second = service.completeOwnerPublication(secondRequest).await().indefinitely();

    assertTrue(second.getActivated());
    verify(service.statsStore, times(2))
        .registerPrewrittenStatsReferencesInGeneration(any(), anyLong(), anyString(), any());
    verify(service.persistence)
        .publishPreparedStatsGeneration(any(), anyLong(), anyString(), any(), any(), any());
  }

  @Test
  void completeVerifiesCommittedCoverageBeforeActivation() {
    var service = service();
    String generationId = OwnerPublicationServiceImpl.generationId(begin(), CALLER_SUBJECT);
    byte[] coverage = reusableManifest(5);
    var manifest =
        baseManifest(generationId).setReusableCoverageManifest(reuseDescriptor(coverage)).build();
    var request = complete(service, begin(), manifest);
    var progress = ArgumentCaptor.<OwnerReuseLeaseRepository.RegistrationProgress>captor();

    var registration = service.completeOwnerPublication(request).await().indefinitely();
    assertFalse(registration.getActivated());
    verify(service.reuseLeases)
        .advanceProgress(any(), anyString(), anyString(), any(), progress.capture());
    when(service.reuseLeases.progress(any(), anyString(), anyString()))
        .thenReturn(progress.getValue());

    var coverageResponse =
        service
            .completeOwnerPublication(
                request.toBuilder()
                    .setCompletionCursor(registration.getNextCompletionCursor())
                    .build())
            .await()
            .indefinitely();
    assertFalse(coverageResponse.getActivated());
    verify(service.reuseLeases, times(2))
        .advanceProgress(any(), anyString(), anyString(), any(), progress.capture());
    when(service.reuseLeases.progress(any(), anyString(), anyString()))
        .thenReturn(progress.getAllValues().getLast());

    var activated =
        service
            .completeOwnerPublication(
                request.toBuilder()
                    .setCompletionCursor(coverageResponse.getNextCompletionCursor())
                    .build())
            .await()
            .indefinitely();

    assertTrue(activated.getActivated());
    verify(service.blobStore)
        .getRange(
            org.mockito.ArgumentMatchers.eq(manifest.getReusableCoverageManifest().getUri()),
            eq(0L),
            eq(Math.toIntExact(manifest.getReusableCoverageManifest().getPayloadBytes())));
  }

  @Test
  void completeRejectsACorruptCommitmentIndexBeforeRegisteringArtifacts() throws Exception {
    var service = service();
    var request = complete(service);
    SnapshotCaptureManifest stored =
        SnapshotCaptureManifest.parseFrom(
            service.blobStore.getRangeAtMost(
                request.getManifest().getManifestUri(),
                0L,
                Math.toIntExact(request.getManifest().getManifestBytes() + 1L)));
    String indexUri = stored.getOwnerArtifactRegistrationManifest().getCommitmentIndex().getUri();
    when(service.blobStore.get(indexUri)).thenReturn(new byte[] {1, 2, 3});

    var error =
        assertThrows(
            StatusRuntimeException.class,
            () -> service.completeOwnerPublication(request).await().indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    verify(service.statsStore, never())
        .registerPrewrittenStatsReferencesInGeneration(any(), anyLong(), anyString(), any());
    verify(service.persistence, never())
        .publishPreparedStatsGeneration(any(), anyLong(), anyString(), any(), any(), any());
  }

  @Test
  void completeRejectsACorruptCoverageChunkBeforeActivation() {
    var service = service();
    String generationId = OwnerPublicationServiceImpl.generationId(begin(), CALLER_SUBJECT);
    byte[] coverage = reusableManifest(5);
    var manifest =
        baseManifest(generationId).setReusableCoverageManifest(reuseDescriptor(coverage)).build();
    var request = complete(service, begin(), manifest);
    var progress = ArgumentCaptor.<OwnerReuseLeaseRepository.RegistrationProgress>captor();
    var registration = service.completeOwnerPublication(request).await().indefinitely();
    verify(service.reuseLeases)
        .advanceProgress(any(), anyString(), anyString(), any(), progress.capture());
    when(service.reuseLeases.progress(any(), anyString(), anyString()))
        .thenReturn(progress.getValue());
    byte[] corrupt = EXTERNAL_OBJECTS.get(manifest.getReusableCoverageManifest().getUri()).clone();
    corrupt[0] ^= 1;
    when(service.blobStore.getRange(
            manifest.getReusableCoverageManifest().getUri(), 0L, corrupt.length))
        .thenReturn(corrupt);

    var error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .completeOwnerPublication(
                        request.toBuilder()
                            .setCompletionCursor(registration.getNextCompletionCursor())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    verify(service.persistence, never())
        .publishPreparedStatsGeneration(any(), anyLong(), anyString(), any(), any(), any());
  }

  @Test
  void finalCompleteResponseCanBeRetriedAfterLeaseRelease() {
    var service = service();
    var request = complete(service);
    var progress = ArgumentCaptor.<OwnerReuseLeaseRepository.RegistrationProgress>captor();
    var published = ArgumentCaptor.<Snapshot>captor();

    var first = service.completeOwnerPublication(request).await().indefinitely();
    assertTrue(first.getActivated());
    verify(service.reuseLeases)
        .advanceProgress(any(), anyString(), anyString(), any(), progress.capture());
    verify(service.snapshots).prepareCreatePublicationUpdates(published.capture());
    when(service.reuseLeases.progress(any(), anyString(), anyString()))
        .thenReturn(progress.getValue());
    when(service.snapshots.getByIdConsistent(tableId(), SNAPSHOT))
        .thenReturn(Optional.of(published.getValue()));
    when(service.statsStore.validatePreparedStatsGenerationRetry(
            any(), anyLong(), anyString(), any()))
        .thenReturn(true);
    when(service.reuseLeases.renew(any(), anyString(), any()))
        .thenThrow(new OwnerReuseLeaseRepository.LeaseContinuityException("lease was released"));

    var replay = service.completeOwnerPublication(request).await().indefinitely();

    assertTrue(replay.getActivated());
    assertEquals(0L, replay.getReuseLeaseExpiresAtEpochMs());
    verify(service.persistence, times(1))
        .publishPreparedStatsGeneration(any(), anyLong(), anyString(), any(), any(), any());
  }

  @Test
  void completeCreatesSnapshotInThePublicationFence() {
    var service = service();
    var fence = ArgumentCaptor.<StatsStore.PublicationFence>captor();

    service.completeOwnerPublication(complete(service)).await().indefinitely();

    verify(service.persistence)
        .publishPreparedStatsGeneration(
            any(), anyLong(), anyString(), any(), any(), fence.capture());
    assertTrue(
        fence.getValue().pointerUpdates().stream()
            .anyMatch(update -> update.pointerKey().equals("/snapshots/by-id/42")));
  }

  @Test
  void completeUpdatesAnExistingSnapshotsReuseRootInThePublicationFence() {
    var service = service();
    Snapshot stored =
        Snapshot.newBuilder()
            .setTableId(tableId())
            .setSnapshotId(SNAPSHOT)
            .setUpstreamCreatedAt(com.google.protobuf.Timestamp.newBuilder().setSeconds(1))
            .setIngestedAt(com.google.protobuf.Timestamp.newBuilder().setSeconds(2))
            .setSchemaJson("{}")
            .build();
    when(service.snapshots.getByIdConsistent(tableId(), SNAPSHOT)).thenReturn(Optional.of(stored));
    var snapshotUpdate =
        new StatsStore.PublicationPointerUpdate(
            "/snapshots/by-id/42", 7L, Pointer.getDefaultInstance());
    when(service.snapshots.prepareReuseManifestPublication(any(), anyLong(), any()))
        .thenAnswer(
            invocation -> {
              SnapshotReuseManifestRef reuse =
                  invocation.getArgument(2, SnapshotReuseManifestRef.class);
              return new SnapshotRepository.PreparedReuseManifestPublication(
                  stored.toBuilder().setReuseManifestRef(reuse).build(),
                  java.util.List.of(snapshotUpdate));
            });
    var fence = ArgumentCaptor.<StatsStore.PublicationFence>captor();

    var response = service.completeOwnerPublication(complete(service)).await().indefinitely();

    assertTrue(response.getActivated());
    verify(service.persistence)
        .publishPreparedStatsGeneration(
            any(), anyLong(), anyString(), any(), any(), fence.capture());
    assertTrue(fence.getValue().pointerUpdates().contains(snapshotUpdate));
    verify(service.snapshots, never()).recordReuseManifest(any(), anyLong(), any());
  }

  @Test
  void aLostFenceIsReportedAndNotSilentlyRebased() {
    var service = service();
    when(service.persistence.publishPreparedStatsGeneration(
            any(), anyLong(), anyString(), any(), any(), any()))
        .thenReturn(false);

    var response = service.completeOwnerPublication(complete(service)).await().indefinitely();

    assertFalse(response.getActivated());
    verify(service.persistence)
        .publishPreparedStatsGeneration(any(), anyLong(), anyString(), any(), any(), any());
    verify(service.persistence, never())
        .clearPrewrittenArtifactProtections(any(), anyLong(), anyString());
    verify(service.currentSnapshots, never()).maybeAdvance(any(), any(Snapshot.class), anyString());
  }

  @Test
  void completePublishesOptionalFileStatsAndIndexesInTheSameActivation() {
    var service = service();
    var indexPredecessor = new IndexArtifactRepository.GenerationPredecessor("", 0, "", 0);
    when(service.indexes.captureGenerationInput(any(), anyLong(), any()))
        .thenReturn(
            new IndexArtifactRepository.GenerationInput(indexPredecessor, java.util.List.of()));
    var indexPointerUpdate =
        new StatsStore.PublicationPointerUpdate(
            "/indexes/active/42", 0, Pointer.getDefaultInstance());
    var publicationFence = new StatsStore.PublicationFence(java.util.List.of(indexPointerUpdate));
    var preparedIndexes =
        new IndexArtifactRepository.PreparedActivation(null, publicationFence, false);
    when(service.indexes.prepareGenerationActivation(
            any(), anyLong(), anyString(), any(), any(), anyBoolean()))
        .thenReturn(preparedIndexes);

    var publication = begin().toBuilder().setPublishFileStats(true).setPublishIndexes(true).build();
    String generationId = OwnerPublicationServiceImpl.generationId(publication, CALLER_SUBJECT);
    var manifest =
        baseManifest(generationId)
            .addOwnerArtifactObjects(fileStatsObject(fileStatsTarget("a"), (byte) 3))
            .addOwnerArtifactObjects(fileStatsObject(fileStatsTarget("b"), (byte) 4))
            .addOwnerArtifactObjects(fileStatsObject(fileStatsTarget("c"), (byte) 5))
            .addOwnerArtifactObjects(
                indexBundleObject(generationId, "file:s3://bucket/file.parquet", (byte) 6))
            .setFileStatsRecordCount(3)
            .setFinalStatsRecordCount(2)
            .setIndexArtifactCount(1)
            .setSourceFileCount(1)
            .setCapturePolicy(ownerPolicy(true, true))
            .build();
    var request = complete(service, publication, manifest);
    var response = service.completeOwnerPublication(request).await().indefinitely();

    assertTrue(response.getActivated());
    assertEquals(3, response.getFileStatsPublished());
    assertEquals(1, response.getIndexArtifactsPublished());
    verify(service.indexes)
        .registerTrustedOwnerIndexArtifactReferencesInGeneration(
            any(), anyLong(), anyString(), anyString(), any());
    verify(service.blobStore, times(1))
        .getRangeAtMost(
            request.getManifest().getManifestUri(),
            0L,
            Math.toIntExact(request.getManifest().getManifestBytes() + 1L));
    var activationFence = ArgumentCaptor.<StatsStore.PublicationFence>captor();
    verify(service.persistence)
        .publishPreparedStatsGeneration(
            any(), anyLong(), anyString(), any(), any(), activationFence.capture());
    assertEquals(
        java.util.List.of("/indexes/active/42", "/snapshots/by-id/42"),
        activationFence.getValue().pointerUpdates().stream()
            .map(StatsStore.PublicationPointerUpdate::pointerKey)
            .toList());
    verify(service.indexes)
        .completePreparedGenerationActivation(tableId(), SNAPSHOT, preparedIndexes);
    verify(service.blobStore, times(1)).get(anyString());
  }

  @Test
  void requestedIndexesActivateAnEmptyGeneration() {
    var service = service();
    var indexPredecessor = new IndexArtifactRepository.GenerationPredecessor("", 0, "", 0);
    when(service.indexes.captureGenerationInput(any(), anyLong(), any()))
        .thenReturn(
            new IndexArtifactRepository.GenerationInput(indexPredecessor, java.util.List.of()));
    var indexPointerUpdate =
        new StatsStore.PublicationPointerUpdate(
            "/indexes/active/42", 0, Pointer.getDefaultInstance());
    var preparedIndexes =
        new IndexArtifactRepository.PreparedActivation(
            null, new StatsStore.PublicationFence(java.util.List.of(indexPointerUpdate)), false);
    when(service.indexes.prepareGenerationActivation(
            any(), anyLong(), anyString(), any(), any(), anyBoolean()))
        .thenReturn(preparedIndexes);
    var publication = begin().toBuilder().setPublishIndexes(true).build();
    String generationId = OwnerPublicationServiceImpl.generationId(publication, CALLER_SUBJECT);
    var manifest = baseManifest(generationId).setCapturePolicy(ownerPolicy(false, true)).build();

    var response =
        service
            .completeOwnerPublication(complete(service, publication, manifest))
            .await()
            .indefinitely();

    assertTrue(response.getActivated());
    verify(service.indexes, never())
        .registerTrustedOwnerIndexArtifactReferencesInGeneration(
            tableId(),
            SNAPSHOT,
            generationId,
            generationPrefix(generationId) + "worker-uploads/",
            java.util.List.of());
    verify(service.indexes)
        .prepareGenerationActivation(any(), anyLong(), anyString(), any(), any(), anyBoolean());
    verify(service.indexes)
        .completePreparedGenerationActivation(tableId(), SNAPSHOT, preparedIndexes);
  }

  @Test
  void rejectsUnsupportedManifestFormat() {
    var service = service();
    String generationId = OwnerPublicationServiceImpl.generationId(begin(), CALLER_SUBJECT);
    var manifest = baseManifest(generationId).setFormatVersion(2).build();

    var error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .completeOwnerPublication(complete(service, begin(), manifest))
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    verify(service.persistence, never())
        .publishPreparedStatsGeneration(any(), anyLong(), anyString(), any(), any(), any());
  }

  @Test
  void rejectsIndexArtifactsWhenPolicyDidNotRequestIndexes() {
    var service = service();
    String generationId = OwnerPublicationServiceImpl.generationId(begin(), CALLER_SUBJECT);
    var manifest =
        baseManifest(generationId)
            .addIndexArtifacts(
                reference(
                    generationId,
                    "worker-uploads/work-a/index-artifacts/",
                    "file:s3://bucket/file.parquet",
                    (byte) 6))
            .setIndexArtifactCount(1)
            .setSourceFileCount(1)
            .build();

    var error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .completeOwnerPublication(complete(service, begin(), manifest))
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
  }

  @Test
  void rejectsManifestThatOmitsAnOutputRequestedAtBegin() {
    var service = service();
    var publication = begin().toBuilder().setPublishIndexes(true).build();
    String generationId = OwnerPublicationServiceImpl.generationId(publication, CALLER_SUBJECT);
    var manifest = baseManifest(generationId).build();

    var error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .completeOwnerPublication(complete(service, publication, manifest))
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    verify(service.indexes, never())
        .prepareGenerationActivation(any(), anyLong(), anyString(), any(), any(), anyBoolean());
  }

  @Test
  void completeRetryAfterIndexActivationReportsTheCommittedPublication() throws Exception {
    var service = service();
    var indexPredecessor = new IndexArtifactRepository.GenerationPredecessor("", 0, "", 0);
    when(service.indexes.captureGenerationInput(any(), anyLong(), any()))
        .thenReturn(
            new IndexArtifactRepository.GenerationInput(indexPredecessor, java.util.List.of()));
    // An index generation that is already active at this capture manifest prepares no fence.
    var preparedIndexes =
        new IndexArtifactRepository.PreparedActivation(
            new IndexArtifactRepository.ActivationFence("/indexes/active/42", "gen", 1L),
            null,
            false);
    when(service.indexes.prepareGenerationActivation(
            any(), anyLong(), anyString(), any(), any(), anyBoolean()))
        .thenReturn(preparedIndexes);
    when(service.statsStore.validatePreparedStatsGenerationRetry(
            any(), anyLong(), anyString(), any()))
        .thenReturn(true);
    var publication = begin().toBuilder().setPublishIndexes(true).build();
    String generationId = OwnerPublicationServiceImpl.generationId(publication, CALLER_SUBJECT);
    var manifest = baseManifest(generationId).setCapturePolicy(ownerPolicy(false, true)).build();

    var request = complete(service, publication, manifest);
    var storedManifest =
        SnapshotCaptureManifest.parseFrom(
            service.blobStore.getRangeAtMost(
                request.getManifest().getManifestUri(),
                0L,
                Math.toIntExact(request.getManifest().getManifestBytes() + 1L)));
    when(service.reuseLeases.progress(any(), anyString(), anyString()))
        .thenReturn(completedProgress(storedManifest.getOwnerArtifactRegistrationManifest()));

    var response = service.completeOwnerPublication(request).await().indefinitely();

    assertTrue(response.getActivated());
    verify(service.statsStore, never())
        .protectPrewrittenStatsObjectsInGeneration(
            any(), anyLong(), anyString(), anyString(), any());
    var fence = ArgumentCaptor.<StatsStore.PublicationFence>captor();
    verify(service.persistence)
        .publishPreparedStatsGeneration(
            any(), anyLong(), anyString(), any(), any(), fence.capture());
    assertEquals(
        java.util.List.of("/snapshots/by-id/42"),
        fence.getValue().pointerUpdates().stream()
            .map(StatsStore.PublicationPointerUpdate::pointerKey)
            .toList());
  }

  @Test
  void completeProtectsIndexBundleAndAggregatesWithoutPerFileProtections() {
    var service = service();
    var indexPredecessor = new IndexArtifactRepository.GenerationPredecessor("", 0, "", 0);
    when(service.indexes.captureGenerationInput(any(), anyLong(), any()))
        .thenReturn(
            new IndexArtifactRepository.GenerationInput(indexPredecessor, java.util.List.of()));
    when(service.indexes.prepareGenerationActivation(
            any(), anyLong(), anyString(), any(), any(), anyBoolean()))
        .thenReturn(
            new IndexArtifactRepository.PreparedActivation(
                null,
                new StatsStore.PublicationFence(
                    java.util.List.of(
                        new StatsStore.PublicationPointerUpdate(
                            "/indexes/active/42", 0, Pointer.getDefaultInstance()))),
                false));
    var publication = begin().toBuilder().setPublishFileStats(true).setPublishIndexes(true).build();
    String generationId = OwnerPublicationServiceImpl.generationId(publication, CALLER_SUBJECT);
    var fileStats = fileStatsObject(fileStatsTarget("file.parquet"), (byte) 5);
    var indexArtifact = indexBundleObject(generationId, "file:s3://bucket/file.parquet", (byte) 6);
    var manifest =
        baseManifest(generationId)
            .addOwnerArtifactObjects(fileStats)
            .addOwnerArtifactObjects(indexArtifact)
            .setFileStatsRecordCount(1)
            .setIndexArtifactCount(1)
            .setSourceFileCount(1)
            .setCapturePolicy(ownerPolicy(true, true))
            .build();

    service
        .completeOwnerPublication(complete(service, publication, manifest))
        .await()
        .indefinitely();

    var protectedObjects =
        ArgumentCaptor.<java.util.List<StatsStore.PrewrittenStatsObject>>captor();
    verify(service.statsStore)
        .protectPrewrittenStatsObjectsInGeneration(
            any(), anyLong(), anyString(), anyString(), protectedObjects.capture());
    assertTrue(
        protectedObjects.getValue().stream()
            .anyMatch(object -> object.blobUri().equals(indexArtifact.getPayloadUri())));
    assertTrue(
        protectedObjects.getValue().stream()
            .noneMatch(object -> object.blobUri().equals(fileStats.getPayloadUri())));
    assertEquals(3, protectedObjects.getValue().size());
  }

  private static BeginOwnerPublicationRequest begin() {
    return BeginOwnerPublicationRequest.newBuilder()
        .setTableId(tableId())
        .setSnapshotId(SNAPSHOT)
        .setOwnerId("owner-a")
        .setOwnerGenerationId("generation-a")
        .build();
  }

  private static CompleteOwnerPublicationRequest complete(OwnerPublicationServiceImpl service) {
    return complete(
        service,
        begin(),
        baseManifest(OwnerPublicationServiceImpl.generationId(begin(), CALLER_SUBJECT)).build());
  }

  private static CompleteOwnerPublicationRequest complete(
      OwnerPublicationServiceImpl service,
      BeginOwnerPublicationRequest publication,
      SnapshotCaptureManifest manifest) {
    String publicationId = OwnerPublicationServiceImpl.generationId(publication, CALLER_SUBJECT);
    var objects = new java.util.ArrayList<>(manifest.getOwnerArtifactObjectsList());
    for (StatsObjectDescriptor aggregate : manifest.getFinalStatsList()) {
      objects.add(
          OwnerArtifactObjectReference.newBuilder()
              .setPayloadUri(aggregate.getPayloadUri())
              .setPayloadBytes(aggregate.getPayloadBytes())
              .setPayloadSha256(aggregate.getPayloadSha256())
              .addAggregateStatsTargetStorageIds(aggregate.getTargetStorageId())
              .build());
    }
    var registrationPayload = registrationManifest(objects);
    byte[] registrationBytes = registrationPayload.payload();
    byte[] registrationIndexBytes = registrationPayload.index().toByteArray();
    byte[] registrationIndexDigest = sha256(registrationIndexBytes);
    String registrationIndexUri =
        Keys.snapshotOwnerManifestCommitmentIndexBlobUri(
            tableId().getAccountId(),
            tableId().getId(),
            SNAPSHOT,
            "registration",
            java.util.HexFormat.of().formatHex(registrationIndexDigest));
    String registrationUri =
        Keys.snapshotOwnerRegistrationManifestBlobUri(
            tableId().getAccountId(),
            tableId().getId(),
            SNAPSHOT,
            java.util.HexFormat.of().formatHex(registrationIndexDigest));
    long fileStatsCount =
        objects.stream()
            .mapToLong(OwnerArtifactObjectReference::getFileStatsTargetStorageIdsCount)
            .sum();
    long indexCount =
        objects.stream()
            .mapToLong(OwnerArtifactObjectReference::getIndexTargetStorageIdsCount)
            .sum();
    long aggregateCount =
        objects.stream()
            .mapToLong(OwnerArtifactObjectReference::getAggregateStatsTargetStorageIdsCount)
            .sum();
    manifest =
        manifest.toBuilder()
            .clearFinalStats()
            .clearOwnerArtifactObjects()
            .setFinalStatsRecordCount(0)
            .setFileStatsRecordCount(0)
            .setIndexArtifactCount(0)
            .setOwnerFileStatsRecordCount(fileStatsCount)
            .setOwnerIndexArtifactCount(indexCount)
            .setOwnerAggregateStatsRecordCount(aggregateCount)
            .setOwnerArtifactRegistrationManifest(
                OwnerArtifactRegistrationManifestRef.newBuilder()
                    .setFormatVersion(OwnerArtifactRegistrationManifest.FORMAT_VERSION)
                    .setUri(registrationUri)
                    .setPayloadBytes(registrationBytes.length)
                    .setPayloadSha256(ByteString.copyFrom(sha256(registrationBytes)))
                    .setObjectCount(objects.size())
                    .setFileStatsTargetCount(fileStatsCount)
                    .setIndexTargetCount(indexCount)
                    .setAggregateStatsTargetCount(aggregateCount)
                    .setCommitmentIndex(
                        ai.floedb.floecat.reconciler.rpc.ExternalManifestCommitmentIndexRef
                            .newBuilder()
                            .setFormatVersion(1)
                            .setDomain(
                                ai.floedb.floecat.reconciler.rpc.ExternalManifestDomain
                                    .EMD_OWNER_ARTIFACT_REGISTRATION)
                            .setUri(registrationIndexUri)
                            .setPayloadBytes(registrationIndexBytes.length)
                            .setPayloadSha256(ByteString.copyFrom(registrationIndexDigest))
                            .setChunkCount(registrationPayload.index().getChunksCount())))
            .build();
    when(service.blobStore.get(registrationIndexUri)).thenReturn(registrationIndexBytes);
    for (var entry : EXTERNAL_OBJECTS.entrySet()) {
      when(service.blobStore.get(entry.getKey())).thenReturn(entry.getValue());
    }
    when(service.blobStore.getRange(anyString(), anyLong(), org.mockito.ArgumentMatchers.anyInt()))
        .thenAnswer(
            invocation -> {
              String uri = invocation.getArgument(0, String.class);
              long offset = invocation.getArgument(1, Long.class);
              int length = invocation.getArgument(2, Integer.class);
              byte[] external =
                  registrationUri.equals(uri) ? registrationBytes : EXTERNAL_OBJECTS.get(uri);
              if (external == null) {
                return null;
              }
              return java.util.Arrays.copyOfRange(
                  external, Math.toIntExact(offset), Math.toIntExact(offset + length));
            });
    byte[] manifestBytes = manifest.toByteArray();
    byte[] manifestDigest = sha256(manifestBytes);
    String manifestUri =
        "/accounts/"
            + tableId().getAccountId()
            + "/tables/"
            + tableId().getId()
            + "/snapshots/"
            + String.format("%019d", SNAPSHOT)
            + "/index-artifacts/capture-manifests/"
            + java.util.HexFormat.of().formatHex(manifestDigest)
            + ".pb";
    when(service.blobStore.getRangeAtMost(manifestUri, 0L, manifestBytes.length + 1))
        .thenReturn(manifestBytes);
    return CompleteOwnerPublicationRequest.newBuilder()
        .setTableId(tableId())
        .setSnapshotId(SNAPSHOT)
        .setPublicationId(publicationId)
        .setOwnerId(publication.getOwnerId())
        .setOwnerGenerationId(publication.getOwnerGenerationId())
        .setManifest(
            OwnerPublicationManifestRef.newBuilder()
                .setAccountId(tableId().getAccountId())
                .setTableId(tableId().getId())
                .setSnapshotId(SNAPSHOT)
                .setManifestUri(manifestUri)
                .setManifestBytes(manifestBytes.length)
                .setManifestSha256(ByteString.copyFrom(manifestDigest))
                .setStatsRecordCount(
                    manifest.getOwnerAggregateStatsRecordCount()
                        + manifest.getOwnerFileStatsRecordCount())
                .setIndexArtifactCount(manifest.getOwnerIndexArtifactCount()))
        .setSnapshot(snapshotSpec())
        .build();
  }

  private record ChunkedRegistration(
      byte[] payload, ai.floedb.floecat.reconciler.rpc.ExternalManifestCommitmentIndex index) {}

  private static ChunkedRegistration registrationManifest(
      java.util.List<OwnerArtifactObjectReference> objects) {
    objects = new java.util.ArrayList<>(objects);
    objects.sort(
        java.util.Comparator.comparing(OwnerPublicationServiceImplTest::registrationTargetKey));
    var chunks = new java.util.ArrayList<byte[]>();
    var commitments =
        new java.util.ArrayList<ai.floedb.floecat.reconciler.rpc.ExternalManifestChunkCommitment>();
    var pending = new java.util.ArrayList<OwnerArtifactObjectReference>();
    int pendingBytes = OwnerArtifactRegistrationManifest.CHUNK_HEADER_BYTES;
    int pendingTargets = 0;
    for (OwnerArtifactObjectReference object : objects) {
      int recordBytes = Integer.BYTES + object.getSerializedSize();
      int targets = objectTargets(object);
      if (!pending.isEmpty()
          && (pendingBytes + recordBytes > OwnerArtifactRegistrationManifest.DEFAULT_READ_BYTES
              || pending.size() >= OwnerArtifactRegistrationManifest.DEFAULT_MAX_OBJECTS
              || pendingTargets + targets
                  > OwnerArtifactRegistrationManifest.DEFAULT_MAX_TARGETS)) {
        appendRegistrationChunk(chunks, commitments, pending);
        pending = new java.util.ArrayList<>();
        pendingBytes = OwnerArtifactRegistrationManifest.CHUNK_HEADER_BYTES;
        pendingTargets = 0;
      }
      pending.add(object);
      pendingBytes += recordBytes;
      pendingTargets += targets;
    }
    if (!pending.isEmpty()) {
      appendRegistrationChunk(chunks, commitments, pending);
    }
    int payloadBytes = chunks.stream().mapToInt(value -> value.length).sum();
    var payload = java.nio.ByteBuffer.allocate(payloadBytes);
    chunks.forEach(payload::put);
    long fileTargets =
        objects.stream()
            .mapToLong(OwnerArtifactObjectReference::getFileStatsTargetStorageIdsCount)
            .sum();
    long indexTargets =
        objects.stream()
            .mapToLong(OwnerArtifactObjectReference::getIndexTargetStorageIdsCount)
            .sum();
    long aggregateTargets =
        objects.stream()
            .mapToLong(OwnerArtifactObjectReference::getAggregateStatsTargetStorageIdsCount)
            .sum();
    var index =
        ai.floedb.floecat.reconciler.rpc.ExternalManifestCommitmentIndex.newBuilder()
            .setFormatVersion(1)
            .setDomain(
                ai.floedb.floecat.reconciler.rpc.ExternalManifestDomain
                    .EMD_OWNER_ARTIFACT_REGISTRATION)
            .setPayloadBytes(payloadBytes)
            .setRecordCount(objects.size())
            .setChunkSizeLimit(OwnerArtifactRegistrationManifest.DEFAULT_READ_BYTES)
            .addAllChunks(commitments)
            .setFileStatsTargetCount(fileTargets)
            .setIndexTargetCount(indexTargets)
            .setAggregateStatsTargetCount(aggregateTargets)
            .build();
    return new ChunkedRegistration(payload.array(), index);
  }

  private static void appendRegistrationChunk(
      java.util.List<byte[]> chunks,
      java.util.List<ai.floedb.floecat.reconciler.rpc.ExternalManifestChunkCommitment> commitments,
      java.util.List<OwnerArtifactObjectReference> objects) {
    int size = OwnerArtifactRegistrationManifest.CHUNK_HEADER_BYTES;
    for (var object : objects) {
      size += Integer.BYTES + object.getSerializedSize();
    }
    long fileTargets =
        objects.stream()
            .mapToLong(OwnerArtifactObjectReference::getFileStatsTargetStorageIdsCount)
            .sum();
    long indexTargets =
        objects.stream()
            .mapToLong(OwnerArtifactObjectReference::getIndexTargetStorageIdsCount)
            .sum();
    long aggregateTargets =
        objects.stream()
            .mapToLong(OwnerArtifactObjectReference::getAggregateStatsTargetStorageIdsCount)
            .sum();
    var chunk = java.nio.ByteBuffer.allocate(size).order(java.nio.ByteOrder.BIG_ENDIAN);
    chunk.put("FLOREGC1".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
    chunk.putInt(OwnerArtifactRegistrationManifest.FORMAT_VERSION);
    chunk.putInt(OwnerArtifactRegistrationManifest.CHUNK_HEADER_BYTES);
    chunk.putLong(chunks.size());
    chunk.putLong(objects.size());
    chunk.putLong(fileTargets);
    chunk.putLong(indexTargets);
    chunk.putLong(aggregateTargets);
    chunk.putLong(0L);
    for (var object : objects) {
      byte[] record = object.toByteArray();
      chunk.putInt(record.length).put(record);
    }
    byte[] bytes = chunk.array();
    long offset = chunks.stream().mapToLong(value -> value.length).sum();
    commitments.add(
        ai.floedb.floecat.reconciler.rpc.ExternalManifestChunkCommitment.newBuilder()
            .setPayloadOffset(offset)
            .setPayloadBytes(bytes.length)
            .setRecordCount(objects.size())
            .setPayloadSha256(ByteString.copyFrom(sha256(bytes)))
            .setFileStatsTargetCount(fileTargets)
            .setIndexTargetCount(indexTargets)
            .setAggregateStatsTargetCount(aggregateTargets)
            .build());
    chunks.add(bytes);
  }

  private static int objectTargets(OwnerArtifactObjectReference object) {
    return object.getFileStatsTargetStorageIdsCount()
        + object.getIndexTargetStorageIdsCount()
        + object.getAggregateStatsTargetStorageIdsCount();
  }

  private static OwnerReuseLeaseRepository.RegistrationProgress completedProgress(
      OwnerArtifactRegistrationManifestRef descriptor) {
    return new OwnerReuseLeaseRepository.RegistrationProgress(
        descriptor.getCommitmentIndex().getChunkCount(),
        0L,
        0L,
        descriptor.getObjectCount(),
        descriptor.getFileStatsTargetCount(),
        descriptor.getIndexTargetCount(),
        descriptor.getAggregateStatsTargetCount());
  }

  private static String registrationTargetKey(OwnerArtifactObjectReference object) {
    if (object.getAggregateStatsTargetStorageIdsCount() > 0) {
      return object.getAggregateStatsTargetStorageIds(0);
    }
    if (object.getFileStatsTargetStorageIdsCount() > 0) {
      return object.getFileStatsTargetStorageIds(0);
    }
    if (object.getIndexTargetStorageIdsCount() > 0) {
      return object.getIndexTargetStorageIds(0);
    }
    return "";
  }

  private static SnapshotSpec snapshotSpec() {
    return SnapshotSpec.newBuilder()
        .setTableId(tableId())
        .setSnapshotId(SNAPSHOT)
        .setUpstreamCreatedAt(com.google.protobuf.Timestamp.newBuilder().setSeconds(1))
        .setSchemaJson("{}")
        .build();
  }

  private static SnapshotCaptureManifest.Builder baseManifest(String generationId) {
    byte[] reuse = reusableManifest();
    return SnapshotCaptureManifest.newBuilder()
        .setFormatVersion(1)
        .setAccountId(tableId().getAccountId())
        .setTableId(tableId().getId())
        .setSnapshotId(SNAPSHOT)
        .setPublicationGenerationId(generationId)
        .setManifestKind(SnapshotCaptureManifestKind.SCMK_OWNER_V2)
        .setReusableCoverageManifest(reuseDescriptor(reuse))
        .setCapturePolicy(ownerPolicy(false, false))
        .addFinalStats(reference(generationId, "finalizer-outputs/", "table", (byte) 2))
        .addFinalStats(
            reference(generationId, "finalizer-outputs/", "column-0000000000000000001", (byte) 1))
        .setFinalStatsRecordCount(2);
  }

  private static ReusableCoverageManifestRef reuseDescriptor(byte[] payload) {
    long coverageCount = payload.length / ReusableCoverageManifest.RECORD_BYTES;
    byte[] shardKey = new byte[32];
    shardKey[31] = 42;
    byte[] shardDigest = sha256(payload);
    byte[] shardIndex = new byte[0];
    if (payload.length > 0) {
      var indexRecord =
          java.nio.ByteBuffer.allocate(ReusableCoverageManifest.SHARD_INDEX_RECORD_BYTES)
              .order(java.nio.ByteOrder.BIG_ENDIAN);
      indexRecord.put(shardKey);
      indexRecord.put(shardDigest);
      indexRecord.putLong(payload.length);
      indexRecord.putLong(coverageCount);
      indexRecord.put(sha256(new byte[] {1}));
      indexRecord.putLong(1L);
      indexRecord.put(
          sha256("sidecar-formats-v1".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
      byte[] groupLayout = new byte[] {3};
      byte[] groupLayoutDigest = sha256(groupLayout);
      indexRecord.put(groupLayoutDigest);
      indexRecord.putLong(groupLayout.length);
      shardIndex = indexRecord.array();
      String shardUri =
          Keys.tableReusableArtifactBlobPrefix(tableId().getAccountId(), tableId().getId())
              + "coverage-shards/"
              + java.util.HexFormat.of().formatHex(shardKey)
              + "-"
              + java.util.HexFormat.of().formatHex(shardDigest)
              + ".bin";
      EXTERNAL_OBJECTS.put(shardUri, payload);
      String groupLayoutUri =
          Keys.tableReusableArtifactBlobPrefix(tableId().getAccountId(), tableId().getId())
              + "group-layouts/"
              + java.util.HexFormat.of().formatHex(shardKey)
              + "-"
              + java.util.HexFormat.of().formatHex(groupLayoutDigest)
              + ".bin";
      EXTERNAL_OBJECTS.put(groupLayoutUri, groupLayout);
    }
    long shardCount = payload.length == 0 ? 0L : 1L;
    var indexBuilder =
        ai.floedb.floecat.reconciler.rpc.ExternalManifestCommitmentIndex.newBuilder()
            .setFormatVersion(1)
            .setDomain(
                ai.floedb.floecat.reconciler.rpc.ExternalManifestDomain.EMD_REUSABLE_COVERAGE)
            .setPayloadBytes(shardIndex.length)
            .setRecordCount(shardCount)
            .setChunkSizeLimit(OwnerArtifactRegistrationManifest.DEFAULT_READ_BYTES)
            .setFixedRecordBytes(ReusableCoverageManifest.SHARD_INDEX_RECORD_BYTES);
    if (shardIndex.length > 0) {
      indexBuilder.addChunks(
          ai.floedb.floecat.reconciler.rpc.ExternalManifestChunkCommitment.newBuilder()
              .setPayloadOffset(0L)
              .setPayloadBytes(shardIndex.length)
              .setRecordCount(shardCount)
              .setPayloadSha256(ByteString.copyFrom(sha256(shardIndex))));
    }
    byte[] indexBytes = indexBuilder.build().toByteArray();
    byte[] indexDigest = sha256(indexBytes);
    String indexUri =
        Keys.snapshotOwnerManifestCommitmentIndexBlobUri(
            tableId().getAccountId(),
            tableId().getId(),
            SNAPSHOT,
            "coverage",
            java.util.HexFormat.of().formatHex(indexDigest));
    String payloadUri =
        "/accounts/"
            + tableId().getAccountId()
            + "/tables/"
            + tableId().getId()
            + "/snapshots/"
            + String.format("%019d", SNAPSHOT)
            + "/index-artifacts/capture-manifests/reuse-index-"
            + java.util.HexFormat.of().formatHex(indexDigest)
            + ".bin";
    EXTERNAL_OBJECTS.put(indexUri, indexBytes);
    EXTERNAL_OBJECTS.put(payloadUri, shardIndex);
    return ReusableCoverageManifestRef.newBuilder()
        .setFormatVersion(ReusableCoverageManifest.FORMAT_VERSION)
        .setUri(payloadUri)
        .setPayloadBytes(shardIndex.length)
        .setPayloadSha256(ByteString.copyFrom(sha256(shardIndex)))
        .setShardCount(shardCount)
        .setShardIndexRecordBytes(ReusableCoverageManifest.SHARD_INDEX_RECORD_BYTES)
        .setCoverageEntryCount(coverageCount)
        .setShardRecordBytes(ReusableCoverageManifest.RECORD_BYTES)
        .setCommitmentIndex(
            ai.floedb.floecat.reconciler.rpc.ExternalManifestCommitmentIndexRef.newBuilder()
                .setFormatVersion(1)
                .setDomain(
                    ai.floedb.floecat.reconciler.rpc.ExternalManifestDomain.EMD_REUSABLE_COVERAGE)
                .setUri(indexUri)
                .setPayloadBytes(indexBytes.length)
                .setPayloadSha256(ByteString.copyFrom(indexDigest))
                .setChunkCount(indexBuilder.getChunksCount()))
        .build();
  }

  private static byte[] reusableManifest(int... families) {
    var bytes =
        java.nio.ByteBuffer.allocate(families.length * ReusableCoverageManifest.RECORD_BYTES)
            .order(java.nio.ByteOrder.BIG_ENDIAN);
    for (int index = 0; index < families.length; index++) {
      byte[] coverage = new byte[32];
      coverage[31] = (byte) (index + 1);
      bytes.put(coverage);
      bytes.put(new byte[32]);
      bytes.putLong(1L);
      bytes.putInt(families[index]);
      bytes.putInt(0);
    }
    return bytes.array();
  }

  private static CapturePolicy ownerPolicy(boolean fileStats, boolean indexes) {
    var policy =
        CapturePolicy.newBuilder()
            .addOutputs(CaptureOutput.CO_TABLE_STATS)
            .addOutputs(CaptureOutput.CO_COLUMN_STATS)
            .setDefaultColumnScope(DefaultColumnScope.DCS_ALL);
    if (fileStats) {
      policy.addOutputs(CaptureOutput.CO_FILE_STATS);
    }
    if (indexes) {
      policy.addOutputs(CaptureOutput.CO_PARQUET_PAGE_INDEX);
    }
    return policy.build();
  }

  private static StatsObjectDescriptor reference(
      String generationId, String segment, String target, byte fill) {
    byte[] digest = new byte[32];
    java.util.Arrays.fill(digest, fill);
    String uri =
        generationPrefix(generationId)
            + segment
            + Hashing.sha256Hex(target)
            + "/"
            + java.util.HexFormat.of().formatHex(digest)
            + ".pb";
    return StatsObjectDescriptor.newBuilder()
        .setTargetStorageId(target)
        .setPayloadUri(uri)
        .setPayloadBytes(100)
        .setPayloadSha256(ByteString.copyFrom(digest))
        .build();
  }

  private static OwnerArtifactObjectReference fileStatsObject(String target, byte fill) {
    byte[] digest = new byte[32];
    java.util.Arrays.fill(digest, fill);
    String uri =
        "/accounts/"
            + tableId().getAccountId()
            + "/tables/"
            + tableId().getId()
            + "/reusable-artifacts/statistics/files/"
            + String.format("%064x", Byte.toUnsignedInt(fill))
            + ".pb";
    return OwnerArtifactObjectReference.newBuilder()
        .setPayloadUri(uri)
        .setPayloadBytes(100)
        .setPayloadSha256(ByteString.copyFrom(digest))
        .addFileStatsTargetStorageIds(target)
        .build();
  }

  private static OwnerArtifactObjectReference indexBundleObject(
      String generationId, String target, byte fill) {
    byte[] digest = new byte[32];
    java.util.Arrays.fill(digest, fill);
    String uri =
        generationPrefix(generationId)
            + "worker-uploads/reuse-bundles/"
            + java.util.HexFormat.of().formatHex(digest)
            + ".pb";
    return OwnerArtifactObjectReference.newBuilder()
        .setPayloadUri(uri)
        .setPayloadBytes(100)
        .setPayloadSha256(ByteString.copyFrom(digest))
        .addIndexTargetStorageIds(target)
        .build();
  }

  private static String generationPrefix(String generationId) {
    return "/accounts/"
        + tableId().getAccountId()
        + "/tables/"
        + tableId().getId()
        + "/target-stats/"
        + String.format("%019d", SNAPSHOT)
        + "/generations/"
        + generationId
        + "/";
  }

  private static String fileStatsTarget(String source) {
    return "file-" + Hashing.sha256Hex("F\u001f" + source);
  }

  private static ResourceId tableId() {
    return ResourceId.newBuilder()
        .setAccountId(TestPrincipals.ACCOUNT_ID)
        .setId("table-a")
        .setKind(ResourceKind.RK_TABLE)
        .build();
  }

  private static byte[] sha256(byte[] value) {
    try {
      return java.security.MessageDigest.getInstance("SHA-256").digest(value);
    } catch (java.security.NoSuchAlgorithmException error) {
      throw new AssertionError(error);
    }
  }

  private static OwnerPublicationServiceImpl service() {
    var service = new OwnerPublicationServiceImpl();
    service.statsStore = org.mockito.Mockito.mock(StatsStore.class);
    service.persistence = org.mockito.Mockito.mock(SnapshotFinalizePersistenceService.class);
    service.indexes = org.mockito.Mockito.mock(IndexArtifactRepository.class);
    service.tables = org.mockito.Mockito.mock(TableRepository.class);
    service.snapshots = org.mockito.Mockito.mock(SnapshotRepository.class);
    service.currentSnapshots = org.mockito.Mockito.mock(CurrentSnapshotPointerService.class);
    service.graphView = org.mockito.Mockito.mock(CatalogGraphView.class);
    service.blobStore = org.mockito.Mockito.mock(BlobStore.class);
    service.reuseLeases = org.mockito.Mockito.mock(OwnerReuseLeaseRepository.class);
    when(service.reuseLeases.acquire(any(), anyString(), anyString(), any()))
        .thenReturn(Long.MAX_VALUE);
    when(service.reuseLeases.renew(any(), anyString(), any())).thenReturn(Long.MAX_VALUE);
    when(service.reuseLeases.progress(any(), anyString(), anyString()))
        .thenReturn(OwnerReuseLeaseRepository.RegistrationProgress.initial());
    service.principal = org.mockito.Mockito.mock(PrincipalProvider.class);
    service.authz = org.mockito.Mockito.mock(Authorizer.class);
    service.blobBucket = "floecat-test";
    service.storageAwsRegion = "us-east-1";
    service.storageAwsS3Endpoint = Optional.empty();
    service.storageAwsPathStyleAccess = true;
    service.registrationBatchBytes = OwnerArtifactRegistrationManifest.DEFAULT_READ_BYTES;
    service.registrationBatchObjects = OwnerArtifactRegistrationManifest.DEFAULT_MAX_OBJECTS;
    service.registrationBatchTargets = OwnerArtifactRegistrationManifest.DEFAULT_MAX_TARGETS;
    var principal = TestPrincipals.stubPrincipal(service.principal, service.authz);
    when(principal.getSubject()).thenReturn(CALLER_SUBJECT);
    when(service.tables.getById(any())).thenReturn(Optional.of(Table.getDefaultInstance()));
    when(service.snapshots.getByIdConsistent(any(), anyLong())).thenReturn(Optional.empty());
    when(service.snapshots.prepareCreatePublicationUpdates(any()))
        .thenReturn(
            java.util.List.of(
                new StatsStore.PublicationPointerUpdate(
                    "/snapshots/by-id/42", 0, Pointer.getDefaultInstance())));
    when(service.graphView.resolve(any()))
        .thenReturn(Optional.of(TestNodes.tableNode(tableId(), "{}")));
    when(service.statsStore.statsGenerationExists(any(), anyLong(), anyString())).thenReturn(true);
    when(service.persistence.prepareStatsGenerationForPublication(
            any(), anyLong(), anyString(), anyBoolean()))
        .thenReturn(new StatsStore.StatsGenerationPredecessor("", 0));
    when(service.persistence.publishPreparedStatsGeneration(
            any(), anyLong(), anyString(), any(), any(), any()))
        .thenReturn(true);
    return service;
  }
}
