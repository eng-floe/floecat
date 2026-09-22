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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.BeginOwnerPublicationRequest;
import ai.floedb.floecat.catalog.rpc.CompleteOwnerPublicationRequest;
import ai.floedb.floecat.catalog.rpc.OwnerPublicationManifestRef;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotSpec;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.rpc.CaptureOutput;
import ai.floedb.floecat.reconciler.rpc.CapturePolicy;
import ai.floedb.floecat.reconciler.rpc.DefaultColumnScope;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifest;
import ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor;
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import ai.floedb.floecat.service.catalog.impl.CurrentSnapshotPointerService;
import ai.floedb.floecat.service.reconciler.impl.SnapshotFinalizePersistenceService;
import ai.floedb.floecat.service.repo.impl.IndexArtifactRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
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

    assertEquals(first, retry);
    verify(service.statsStore, times(2))
        .statsGenerationExists(tableId(), SNAPSHOT, first.getGenerationId());
    verify(service.statsStore, never()).beginStatsGeneration(any(), anyLong(), anyString());
    verify(service.statsStore, times(2))
        .prepareStatsGenerationManifest(tableId(), SNAPSHOT, first.getGenerationId());
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
    verify(service.persistence)
        .publishPreparedStatsGeneration(
            any(), anyLong(), anyString(), references.capture(), any(), any());
    assertEquals(
        java.util.List.of("column-0000000000000000001", "table"),
        references.getValue().stream()
            .map(StatsStore.PrewrittenTargetStatsReference::targetStorageId)
            .toList());
    verify(service.persistence).clearPrewrittenArtifactProtections(any(), anyLong(), anyString());
    verify(service.currentSnapshots).maybeAdvance(any(), any(Snapshot.class), anyString());
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
            .addFileStats(
                reference(
                    generationId,
                    "worker-uploads/work-a/file-stats/",
                    fileStatsTarget("a"),
                    (byte) 3))
            .addFileStats(
                reference(
                    generationId,
                    "worker-uploads/work-a/file-stats/",
                    fileStatsTarget("b"),
                    (byte) 4))
            .addFileStats(
                reference(
                    generationId,
                    "worker-uploads/work-a/file-stats/",
                    fileStatsTarget("c"),
                    (byte) 5))
            .addIndexArtifacts(
                reference(
                    generationId,
                    "worker-uploads/work-a/index-artifacts/",
                    "file:s3://bucket/file.parquet",
                    (byte) 6))
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
    verify(service.blobStore, times(1)).get(request.getManifest().getManifestUri());
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
    verify(service.indexes)
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
  void rejectsOversizedManifestBeforeReadingIt() {
    var service = service();
    var request = complete(service).toBuilder();
    request.setManifest(
        request.getManifest().toBuilder()
            .setManifestBytes(OwnerPublicationServiceImpl.MAX_MANIFEST_BYTES + 1));

    var error =
        assertThrows(
            StatusRuntimeException.class,
            () -> service.completeOwnerPublication(request.build()).await().indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    verify(service.blobStore, never()).get(anyString());
  }

  @Test
  void completeRetryAfterIndexActivationReportsTheCommittedPublication() {
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

    var response =
        service
            .completeOwnerPublication(complete(service, publication, manifest))
            .await()
            .indefinitely();

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
  void completeProtectsIndexArtifactPayloadsAlongsideTheStatsPayloads() {
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
    var publication = begin().toBuilder().setPublishIndexes(true).build();
    String generationId = OwnerPublicationServiceImpl.generationId(publication, CALLER_SUBJECT);
    var indexArtifact =
        reference(
            generationId,
            "worker-uploads/work-a/index-artifacts/",
            "file:s3://bucket/file.parquet",
            (byte) 6);
    var manifest =
        baseManifest(generationId)
            .addIndexArtifacts(indexArtifact)
            .setIndexArtifactCount(1)
            .setSourceFileCount(1)
            .setCapturePolicy(ownerPolicy(false, true))
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
    when(service.blobStore.get(manifestUri)).thenReturn(manifestBytes);
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
                .setStatsRecordCount(manifest.getFinalStatsCount() + manifest.getFileStatsCount())
                .setIndexArtifactCount(manifest.getIndexArtifactsCount()))
        .setSnapshot(snapshotSpec())
        .build();
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
    return SnapshotCaptureManifest.newBuilder()
        .setFormatVersion(1)
        .setAccountId(tableId().getAccountId())
        .setTableId(tableId().getId())
        .setSnapshotId(SNAPSHOT)
        .setPublicationGenerationId(generationId)
        .setCapturePolicy(ownerPolicy(false, false))
        .addFinalStats(reference(generationId, "finalizer-outputs/", "table", (byte) 2))
        .addFinalStats(
            reference(generationId, "finalizer-outputs/", "column-0000000000000000001", (byte) 1))
        .setFinalStatsRecordCount(2);
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
    service.principal = org.mockito.Mockito.mock(PrincipalProvider.class);
    service.authz = org.mockito.Mockito.mock(Authorizer.class);
    service.blobBucket = "floecat-test";
    service.storageAwsRegion = "us-east-1";
    service.storageAwsS3Endpoint = Optional.empty();
    service.storageAwsPathStyleAccess = true;
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
