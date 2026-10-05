/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.statistics.impl;

import ai.floedb.floecat.catalog.rpc.BeginOwnerPublicationRequest;
import ai.floedb.floecat.catalog.rpc.BeginOwnerPublicationResponse;
import ai.floedb.floecat.catalog.rpc.CompleteOwnerPublicationRequest;
import ai.floedb.floecat.catalog.rpc.CompleteOwnerPublicationResponse;
import ai.floedb.floecat.catalog.rpc.OwnerPublicationManifestRef;
import ai.floedb.floecat.catalog.rpc.OwnerPublicationService;
import ai.floedb.floecat.catalog.rpc.PublishOwnerReuseManifestRequest;
import ai.floedb.floecat.catalog.rpc.PublishOwnerReuseManifestResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotReuseManifestKind;
import ai.floedb.floecat.catalog.rpc.SnapshotReuseManifestRef;
import ai.floedb.floecat.catalog.rpc.SnapshotSpec;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.jobs.ReusableArtifactBundleUris;
import ai.floedb.floecat.reconciler.rpc.CaptureOutput;
import ai.floedb.floecat.reconciler.rpc.CapturePolicy;
import ai.floedb.floecat.reconciler.rpc.DefaultColumnScope;
import ai.floedb.floecat.reconciler.rpc.ExternalManifestCommitmentIndex;
import ai.floedb.floecat.reconciler.rpc.ExternalManifestDomain;
import ai.floedb.floecat.reconciler.rpc.OwnerArtifactObjectReference;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifest;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifestKind;
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import ai.floedb.floecat.service.catalog.impl.CurrentSnapshotPointerService;
import ai.floedb.floecat.service.catalog.impl.surface.CatalogSurfaceWritePolicy;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.common.PersistedSecretPropertyValidator;
import ai.floedb.floecat.service.error.impl.GeneratedErrorMessages;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.reconciler.impl.SnapshotFinalizePersistenceService;
import ai.floedb.floecat.service.repo.impl.IndexArtifactRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.security.RolePermissions;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.types.Hashing;
import com.google.protobuf.InvalidProtocolBufferException;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/** Publishes one complete Owner-produced snapshot generation with a single activation. */
@GrpcService
public class OwnerPublicationServiceImpl extends BaseServiceImpl
    implements OwnerPublicationService {
  static final String OWNER_GENERATION_PREFIX = "owner-";
  static final String EXECUTOR_SEGMENT = "worker-uploads/";
  static final String OWNER_SEGMENT = "finalizer-outputs/";
  static final long MAX_MANIFEST_BYTES = 64L * 1024L * 1024L;

  private static final Logger LOG = Logger.getLogger(OwnerPublicationService.class);

  @Inject StatsStore statsStore;
  @Inject SnapshotFinalizePersistenceService persistence;
  @Inject IndexArtifactRepository indexes;
  @Inject TableRepository tables;
  @Inject SnapshotRepository snapshots;
  @Inject CurrentSnapshotPointerService currentSnapshots;
  @Inject CatalogGraphView graphView;
  @Inject BlobStore blobStore;
  @Inject ExternalManifestCommitmentCache manifestCommitments;
  @Inject OwnerReuseLeaseRepository reuseLeases;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;

  @ConfigProperty(name = "floecat.blob.s3.bucket")
  String blobBucket;

  @ConfigProperty(name = "floecat.storage.aws.region", defaultValue = "us-east-1")
  String storageAwsRegion;

  @ConfigProperty(name = "floecat.storage.aws.s3.endpoint")
  java.util.Optional<String> storageAwsS3Endpoint;

  @ConfigProperty(name = "floecat.storage.aws.s3.path-style-access", defaultValue = "true")
  boolean storageAwsPathStyleAccess;

  @ConfigProperty(
      name = "floecat.owner-publication.registration-batch-bytes",
      defaultValue = "8388608")
  int registrationBatchBytes;

  @ConfigProperty(
      name = "floecat.owner-publication.registration-batch-objects",
      defaultValue = "10000")
  int registrationBatchObjects;

  @ConfigProperty(
      name = "floecat.owner-publication.registration-batch-targets",
      defaultValue = "10000")
  int registrationBatchTargets;

  @Override
  public Uni<BeginOwnerPublicationResponse> beginOwnerPublication(
      BeginOwnerPublicationRequest request) {
    var log = LogHelper.start(LOG, "BeginOwnerPublication");
    return mapFailures(
            run(
                () -> {
                  ResourceId tableId = authorizedTable(request.getTableId());
                  long snapshotId = requireSnapshotId(request.getSnapshotId());
                  new CatalogSurfaceWritePolicy(graphView, catalogContext())
                      .requireWritableTable(tableId, correlationId());
                  String generationId = generationId(request, requireCallerSubject());
                  boolean reuseSourceLeased = false;
                  SnapshotReuseManifestRef source = null;
                  if (request.hasReuseSourceSnapshotId()) {
                    source =
                        snapshots
                            .getByIdConsistent(tableId, request.getReuseSourceSnapshotId())
                            .filter(Snapshot::hasReuseManifestRef)
                            .map(Snapshot::getReuseManifestRef)
                            .filter(
                                reference ->
                                    reference.getKind() == SnapshotReuseManifestKind.SRMK_OWNER_V2)
                            .orElse(null);
                    if (source != null) {
                      reuseSourceLeased = true;
                    }
                  }
                  // Every publication leases the reusable namespace before returning upload
                  // prefixes. A reuse source additionally roots its exact capture manifest.
                  OwnerReuseLeaseRepository.AcquireResult lease;
                  try {
                    lease =
                        reuseLeases.acquireWithCandidates(
                            tableId,
                            generationId,
                            Keys.snapshotIndexArtifactCaptureManifestBlobPrefix(
                                tableId.getAccountId(), tableId.getId(), snapshotId),
                            source,
                            () -> {
                              if (!statsStore.statsGenerationExists(
                                  tableId, snapshotId, generationId)) {
                                statsStore.beginStatsGeneration(tableId, snapshotId, generationId);
                              }
                              // This marker is tiny and Floecat-owned. Requiring the Owner to
                              // manufacture it would add an upload while also making the existing
                              // prepared-generation API fail every publication that did not know
                              // about this repository detail. This also repairs a Begin retry
                              // interrupted after reserving the generation.
                              statsStore.prepareStatsGenerationManifest(
                                  tableId, snapshotId, generationId);
                            });
                  } catch (OwnerReuseLeaseRepository.LeaseContinuityException error) {
                    throw GrpcErrors.preconditionFailed(
                        correlationId(),
                        GeneratedErrorMessages.MessageKey.PUBLICATION_NOT_BEGUN,
                        Map.of("publication_id", generationId));
                  }
                  return BeginOwnerPublicationResponse.newBuilder()
                      .setPublicationId(generationId)
                      .setGenerationId(generationId)
                      .setExecutorObjectPrefix(
                          generationPrefix(tableId, snapshotId, generationId) + EXECUTOR_SEGMENT)
                      .setOwnerObjectPrefix(
                          generationPrefix(tableId, snapshotId, generationId) + OWNER_SEGMENT)
                      .setManifestObjectPrefix(
                          Keys.snapshotIndexArtifactCaptureManifestBlobPrefix(
                              tableId.getAccountId(), tableId.getId(), snapshotId))
                      .setReusableObjectPrefix(
                          Keys.tableReusableArtifactBlobPrefix(
                              tableId.getAccountId(), tableId.getId()))
                      .setReuseSourceLeased(reuseSourceLeased)
                      .setReuseLeaseExpiresAtEpochMs(lease.expiresAtEpochMs())
                      .addAllInProgressReuseManifestRefs(lease.inProgressManifests())
                      .setExternalManifestChunkMaxBytes(registrationBatchBytes)
                      .setRegistrationChunkMaxObjects(registrationBatchObjects)
                      .setRegistrationChunkMaxTargets(registrationBatchTargets)
                      .setArtifactStorageUriRoot("s3://" + requireBlobBucket())
                      .putArtifactStorageProperties("s3.region", storageAwsRegion)
                      .putArtifactStorageProperties(
                          "s3.path-style-access", Boolean.toString(storageAwsPathStyleAccess))
                      .putAllArtifactStorageProperties(artifactEndpointProperty())
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(log::fail)
        .onItem()
        .invoke(log::ok);
  }

  @Override
  public Uni<PublishOwnerReuseManifestResponse> publishOwnerReuseManifest(
      PublishOwnerReuseManifestRequest request) {
    var log = LogHelper.start(LOG, "PublishOwnerReuseManifest");
    return mapFailures(
            run(
                () -> {
                  ResourceId tableId = authorizedTable(request.getTableId());
                  long snapshotId = requireSnapshotId(request.getSnapshotId());
                  new CatalogSurfaceWritePolicy(graphView, catalogContext())
                      .requireWritableTable(tableId, correlationId());
                  String publicationId =
                      requirePublicationCapability(
                          tableId,
                          snapshotId,
                          request.getPublicationId(),
                          request.getOwnerId(),
                          request.getOwnerGenerationId());
                  if (!request.hasManifest()) {
                    throw GrpcErrors.invalidArgument(
                        correlationId(), null, Map.of("field", "manifest"));
                  }
                  SnapshotReuseManifestRef manifest = request.getManifest();
                  validateInProgressManifest(tableId, snapshotId, manifest);
                  long expiresAt =
                      reuseLeases.publishInProgressManifest(tableId, publicationId, manifest);
                  return PublishOwnerReuseManifestResponse.newBuilder()
                      .setManifest(manifest)
                      .setReuseLeaseExpiresAtEpochMs(expiresAt)
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(log::fail)
        .onItem()
        .invoke(log::ok);
  }

  @Override
  public Uni<CompleteOwnerPublicationResponse> completeOwnerPublication(
      CompleteOwnerPublicationRequest request) {
    var log = LogHelper.start(LOG, "CompleteOwnerPublication");
    return mapFailures(run(() -> complete(request)), correlationId())
        .onFailure()
        .invoke(log::fail)
        .onItem()
        .invoke(log::ok);
  }

  private CompleteOwnerPublicationResponse complete(CompleteOwnerPublicationRequest request) {
    ResourceId tableId = authorizedTable(request.getTableId());
    long snapshotId = requireSnapshotId(request.getSnapshotId());
    new CatalogSurfaceWritePolicy(graphView, catalogContext())
        .requireWritableTable(tableId, correlationId());
    Snapshot snapshot = publicationSnapshot(request, tableId, snapshotId);
    PublicationId publication = requirePublicationId(request.getPublicationId());
    String generationId = publication.value();
    String expectedGenerationId =
        generationId(
            request.getOwnerId(),
            request.getOwnerGenerationId(),
            publication.publishFileStats(),
            publication.publishIndexes(),
            requireCallerSubject());
    if (!MessageDigest.isEqual(
        generationId.getBytes(java.nio.charset.StandardCharsets.UTF_8),
        expectedGenerationId.getBytes(java.nio.charset.StandardCharsets.UTF_8))) {
      throw GrpcErrors.permissionDenied(correlationId(), null, null);
    }
    if (!statsStore.statsGenerationExists(tableId, snapshotId, generationId)) {
      throw GrpcErrors.preconditionFailed(
          correlationId(),
          GeneratedErrorMessages.MessageKey.PUBLICATION_NOT_BEGUN,
          Map.of("publication_id", generationId));
    }
    if (!request.hasManifest()) {
      throw GrpcErrors.invalidArgument(correlationId(), null, Map.of("field", "manifest"));
    }
    var descriptor = request.getManifest();
    byte[] digest = descriptor.getManifestSha256().toByteArray();
    requireDigest(digest, "manifest.manifest_sha256");
    String manifestUri = descriptor.getManifestUri();
    String requiredManifestPrefix =
        Keys.snapshotIndexArtifactCaptureManifestBlobPrefix(
            tableId.getAccountId(), tableId.getId(), snapshotId);
    if (!manifestUri.startsWith(requiredManifestPrefix)) {
      throw GrpcErrors.invalidArgument(
          correlationId(), null, Map.of("field", "manifest.manifest_uri"));
    }
    if (descriptor.getManifestBytes() == 0 || descriptor.getManifestBytes() > MAX_MANIFEST_BYTES) {
      throw GrpcErrors.invalidArgument(
          correlationId(),
          null,
          Map.of(
              "field", "manifest.manifest_bytes", "max_bytes", Long.toString(MAX_MANIFEST_BYTES)));
    }
    byte[] manifestBytes =
        blobStore.getRangeAtMost(
            manifestUri, 0L, Math.toIntExact(descriptor.getManifestBytes() + 1L));
    if (manifestBytes == null
        || manifestBytes.length != descriptor.getManifestBytes()
        || !MessageDigest.isEqual(digest, sha256(manifestBytes))
        || !manifestUri.equals(
            Keys.snapshotIndexArtifactCaptureManifestBlobUri(
                tableId.getAccountId(),
                tableId.getId(),
                snapshotId,
                Hashing.sha256Hex(manifestBytes)))) {
      throw GrpcErrors.preconditionFailed(
          correlationId(),
          GeneratedErrorMessages.MessageKey.PUBLICATION_MANIFEST_UNREADABLE,
          Map.of("manifest_uri", manifestUri));
    }
    SnapshotCaptureManifest manifest;
    try {
      manifest = SnapshotCaptureManifest.parseFrom(manifestBytes);
    } catch (InvalidProtocolBufferException error) {
      throw GrpcErrors.invalidArgument(correlationId(), null, Map.of("field", "manifest"));
    }
    validateManifest(tableId, snapshotId, publication, descriptor, manifest);
    SnapshotReuseManifestRef reuseManifestRef =
        SnapshotReuseManifestRef.newBuilder()
            .setFormatVersion(1)
            .setKind(SnapshotReuseManifestKind.SRMK_OWNER_V2)
            .setUri(manifestUri)
            .setPayloadBytes(descriptor.getManifestBytes())
            .setPayloadSha256(descriptor.getManifestSha256())
            .setStatsGenerationManifestUri(
                Keys.snapshotTargetStatsManifestBlobUri(
                    tableId.getAccountId(), tableId.getId(), snapshotId, generationId))
            .build();
    snapshot = snapshot.toBuilder().setReuseManifestRef(reuseManifestRef).build();
    if (manifest.getOwnerAggregateStatsRecordCount() == 0L) {
      throw invalidManifest();
    }
    boolean publishesIndexes =
        requests(manifest.getCapturePolicy(), CaptureOutput.CO_PARQUET_PAGE_INDEX);
    var registration = manifest.getOwnerArtifactRegistrationManifest();
    var coverage = manifest.getReusableCoverageManifest();
    String manifestIdentity =
        HexFormat.of().formatHex(descriptor.getManifestSha256().toByteArray());
    OwnerReuseLeaseRepository.RegistrationProgress progress =
        reuseLeases.progress(tableId, generationId, manifestIdentity);
    if (progress.equals(OwnerReuseLeaseRepository.RegistrationProgress.initial())
        && publicationAlreadyCommitted(
            tableId, snapshotId, generationId, snapshot, reuseManifestRef)) {
      return completedResponse(manifest, manifestIdentity, progress, true, 0L);
    }
    validateCompletionCursor(request.getCompletionCursor(), manifestIdentity, progress);
    // Replace the source-generation lease with the completed generation's manifest before any
    // reusable objects are registered. The manifest roots each coverage-addressed file-stat and
    // sidecar object as one unit, avoiding one transient protection pointer per source file while
    // still closing the upload-to-activation GC window.
    long leaseExpiresAt;
    try {
      leaseExpiresAt = reuseLeases.renew(tableId, generationId, reuseManifestRef);
    } catch (OwnerReuseLeaseRepository.LeaseContinuityException error) {
      if (publicationAlreadyCommitted(
          tableId, snapshotId, generationId, snapshot, reuseManifestRef)) {
        return completedResponse(manifest, manifestIdentity, progress, true, 0L);
      }
      throw GrpcErrors.preconditionFailed(
          correlationId(),
          GeneratedErrorMessages.MessageKey.PUBLICATION_NOT_BEGUN,
          Map.of("publication_id", generationId));
    }
    if (progress.registrationChunk() < registration.getCommitmentIndex().getChunkCount()) {
      OwnerArtifactRegistrationManifest.Batch batch;
      ExternalManifestCommitmentIndex commitments;
      try {
        commitments =
            ExternalManifestCommitments.load(
                manifestCommitments,
                blobStore,
                registration.getCommitmentIndex(),
                ExternalManifestDomain.EMD_OWNER_ARTIFACT_REGISTRATION,
                tableId.getAccountId(),
                tableId.getId(),
                snapshotId,
                registration.getPayloadBytes(),
                registration.getObjectCount(),
                registration.getFileStatsTargetCount(),
                registration.getIndexTargetCount(),
                registration.getAggregateStatsTargetCount(),
                0,
                registrationBatchBytes,
                registrationBatchObjects,
                registrationBatchTargets);
        batch =
            OwnerArtifactRegistrationManifest.readChunk(
                blobStore,
                registration,
                progress.registrationChunk(),
                commitments.getChunks(Math.toIntExact(progress.registrationChunk())));
      } catch (IllegalArgumentException error) {
        throw invalidManifest();
      }
      OwnerReuseLeaseRepository.RegistrationProgress nextProgress;
      try {
        nextProgress =
            new OwnerReuseLeaseRepository.RegistrationProgress(
                progress.registrationChunk() + 1L,
                progress.coverageChunk(),
                progress.coverageRecordCount(),
                Math.addExact(progress.objectCount(), batch.objectCount()),
                Math.addExact(progress.fileStatsTargetCount(), batch.fileStatsTargetCount()),
                Math.addExact(progress.indexTargetCount(), batch.indexTargetCount()),
                Math.addExact(
                    progress.aggregateStatsTargetCount(), batch.aggregateStatsTargetCount()));
        if (nextProgress.registrationChunk() == registration.getCommitmentIndex().getChunkCount()) {
          requireRegistrationTotals(registration, nextProgress);
        }
      } catch (ArithmeticException | IllegalArgumentException error) {
        throw invalidManifest();
      }
      registerOwnerArtifactBatch(
          tableId, snapshotId, generationId, publishesIndexes, batch.objects());
      reuseLeases.advanceProgress(tableId, generationId, manifestIdentity, progress, nextProgress);
      if (nextProgress.registrationChunk() < registration.getCommitmentIndex().getChunkCount()
          || coverage.getCommitmentIndex().getChunkCount() > 0L) {
        return pendingCompletion(manifest, manifestIdentity, nextProgress, leaseExpiresAt);
      }
      progress = nextProgress;
    }

    if (progress.coverageChunk() < coverage.getCommitmentIndex().getChunkCount()) {
      ReusableCoverageManifest.Batch batch;
      try {
        ExternalManifestCommitmentIndex commitments =
            ReusableCoverageManifest.loadIndex(
                manifestCommitments,
                blobStore,
                coverage,
                tableId.getAccountId(),
                tableId.getId(),
                snapshotId);
        batch =
            ReusableCoverageManifest.readCommittedChunk(
                blobStore,
                coverage,
                commitments.getChunks(Math.toIntExact(progress.coverageChunk())));
      } catch (IllegalArgumentException error) {
        throw invalidManifest();
      }
      OwnerReuseLeaseRepository.RegistrationProgress nextProgress;
      try {
        nextProgress =
            new OwnerReuseLeaseRepository.RegistrationProgress(
                progress.registrationChunk(),
                Math.addExact(progress.coverageChunk(), 1L),
                Math.addExact(progress.coverageRecordCount(), batch.coverageEntryCount()),
                progress.objectCount(),
                progress.fileStatsTargetCount(),
                progress.indexTargetCount(),
                progress.aggregateStatsTargetCount());
      } catch (ArithmeticException error) {
        throw invalidManifest();
      }
      if (nextProgress.coverageChunk() == coverage.getCommitmentIndex().getChunkCount()) {
        requireCoverageTotals(coverage, nextProgress);
      }
      reuseLeases.advanceProgress(tableId, generationId, manifestIdentity, progress, nextProgress);
      return pendingCompletion(manifest, manifestIdentity, nextProgress, leaseExpiresAt);
    }

    // Keep these checks at the activation boundary as a final invariant. In particular, a
    // malformed descriptor must never bypass them by declaring no chunks.
    requireRegistrationTotals(registration, progress);
    requireCoverageTotals(coverage, progress);

    statsStore.validatePreparedStatsGenerationRetry(tableId, snapshotId, generationId, List.of());

    IndexArtifactRepository.PreparedActivation preparedIndexes = null;
    if (publishesIndexes) {
      var predecessor =
          indexes.captureGenerationInput(tableId, snapshotId, List.of()).predecessor();
      preparedIndexes =
          indexes.prepareGenerationActivation(
              tableId, snapshotId, generationId, manifestBytes, predecessor, false);
    }

    List<StatsStore.PublicationPointerUpdate> publicationUpdates = new ArrayList<>();
    // A null fence means the index generation is already active at this exact capture manifest —
    // a replay of a Complete that committed. There is nothing left to CAS for indexes, and the
    // publication below still reconciles the stats generation and the snapshot.
    if (preparedIndexes != null && preparedIndexes.publicationFence() != null) {
      publicationUpdates.addAll(preparedIndexes.publicationFence().pointerUpdates());
    }
    Snapshot publishedSnapshot = snapshot;
    var existingSnapshot = snapshots.getByIdConsistent(tableId, snapshotId);
    if (existingSnapshot.isPresent()) {
      if (!sameOwnerSnapshot(existingSnapshot.get(), snapshot)
          || (existingSnapshot.get().hasReuseManifestRef()
              && !existingSnapshot.get().getReuseManifestRef().equals(reuseManifestRef))) {
        throw GrpcErrors.preconditionFailed(
            correlationId(),
            GeneratedErrorMessages.MessageKey.PUBLICATION_SNAPSHOT_CONFLICT,
            Map.of("table_id", tableId.getId(), "snapshot_id", Long.toString(snapshotId)));
      }
      try {
        var preparedSnapshot =
            snapshots.prepareReuseManifestPublication(tableId, snapshotId, reuseManifestRef);
        publishedSnapshot = preparedSnapshot.snapshot();
        publicationUpdates.addAll(preparedSnapshot.pointerUpdates());
      } catch (BaseResourceRepository.NameConflictException error) {
        throw GrpcErrors.preconditionFailed(
            correlationId(),
            GeneratedErrorMessages.MessageKey.PUBLICATION_SNAPSHOT_CONFLICT,
            Map.of("table_id", tableId.getId(), "snapshot_id", Long.toString(snapshotId)));
      }
    } else {
      publicationUpdates.addAll(snapshots.prepareCreatePublicationUpdates(snapshot));
    }
    StatsStore.PublicationFence publicationFence =
        publicationUpdates.isEmpty() ? null : new StatsStore.PublicationFence(publicationUpdates);

    StatsStore.StatsGenerationPredecessor predecessor =
        persistence.prepareStatsGenerationForPublication(tableId, snapshotId, generationId, false);
    boolean activated =
        persistence.publishPreparedStatsGeneration(
            tableId, snapshotId, generationId, List.of(), predecessor, publicationFence);
    if (activated) {
      if (preparedIndexes != null) {
        indexes.completePreparedGenerationActivation(tableId, snapshotId, preparedIndexes);
      }
      persistence.clearPrewrittenArtifactProtections(tableId, snapshotId, generationId);
      // The publication fence committed both a new snapshot and an existing snapshot's reuse-root
      // update atomically with stats/index activation. maybeAdvance re-commits the activated
      // generation when the current-snapshot pointer moves onto this snapshot.
      currentSnapshots.maybeAdvance(tableId, publishedSnapshot, correlationId());
      reuseLeases.release(tableId, generationId);
    }
    return completedResponse(manifest, manifestIdentity, progress, activated, leaseExpiresAt);
  }

  private CompleteOwnerPublicationResponse pendingCompletion(
      SnapshotCaptureManifest manifest,
      String manifestIdentity,
      OwnerReuseLeaseRepository.RegistrationProgress progress,
      long leaseExpiresAt) {
    return CompleteOwnerPublicationResponse.newBuilder()
        .setAggregateStatsPublished(manifest.getOwnerAggregateStatsRecordCount())
        .setFileStatsPublished(manifest.getOwnerFileStatsRecordCount())
        .setIndexArtifactsPublished(manifest.getOwnerIndexArtifactCount())
        .setActivated(false)
        .setNextCompletionCursor(completionCursor(manifestIdentity, progress))
        .setReuseLeaseExpiresAtEpochMs(leaseExpiresAt)
        .build();
  }

  private CompleteOwnerPublicationResponse completedResponse(
      SnapshotCaptureManifest manifest,
      String manifestIdentity,
      OwnerReuseLeaseRepository.RegistrationProgress progress,
      boolean activated,
      long leaseExpiresAt) {
    return CompleteOwnerPublicationResponse.newBuilder()
        .setAggregateStatsPublished(manifest.getOwnerAggregateStatsRecordCount())
        .setFileStatsPublished(manifest.getOwnerFileStatsRecordCount())
        .setIndexArtifactsPublished(manifest.getOwnerIndexArtifactCount())
        .setActivated(activated)
        .setNextCompletionCursor(completionCursor(manifestIdentity, progress))
        .setReuseLeaseExpiresAtEpochMs(activated ? 0L : leaseExpiresAt)
        .build();
  }

  private boolean publicationAlreadyCommitted(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      Snapshot expectedSnapshot,
      SnapshotReuseManifestRef reuseManifestRef) {
    Optional<Snapshot> stored = snapshots.getByIdConsistent(tableId, snapshotId);
    if (stored.isEmpty()
        || !sameOwnerSnapshot(stored.orElseThrow(), expectedSnapshot)
        || !stored.orElseThrow().hasReuseManifestRef()
        || !stored.orElseThrow().getReuseManifestRef().equals(reuseManifestRef)) {
      return false;
    }
    return statsStore.validatePreparedStatsGenerationRetry(
        tableId, snapshotId, generationId, List.of());
  }

  private void validateCompletionCursor(
      String requested,
      String manifestIdentity,
      OwnerReuseLeaseRepository.RegistrationProgress progress) {
    if (progress.equals(OwnerReuseLeaseRepository.RegistrationProgress.initial())) {
      if (!requested.isEmpty()) {
        throw GrpcErrors.invalidArgument(
            correlationId(), null, Map.of("field", "completion_cursor"));
      }
      return;
    }
    String current = completionCursor(manifestIdentity, progress);
    OwnerReuseLeaseRepository.RegistrationProgress predecessor;
    if (progress.coverageChunk() > 0L) {
      predecessor =
          new OwnerReuseLeaseRepository.RegistrationProgress(
              progress.registrationChunk(),
              progress.coverageChunk() - 1L,
              progress.coverageRecordCount(),
              progress.objectCount(),
              progress.fileStatsTargetCount(),
              progress.indexTargetCount(),
              progress.aggregateStatsTargetCount());
    } else {
      predecessor =
          new OwnerReuseLeaseRepository.RegistrationProgress(
              progress.registrationChunk() - 1L,
              0L,
              progress.coverageRecordCount(),
              progress.objectCount(),
              progress.fileStatsTargetCount(),
              progress.indexTargetCount(),
              progress.aggregateStatsTargetCount());
    }
    String previous =
        predecessor.registrationChunk() == 0L && predecessor.coverageChunk() == 0L
            ? ""
            : completionCursor(manifestIdentity, predecessor);
    if (!MessageDigest.isEqual(
            requested.getBytes(StandardCharsets.UTF_8), current.getBytes(StandardCharsets.UTF_8))
        && !MessageDigest.isEqual(
            requested.getBytes(StandardCharsets.UTF_8),
            previous.getBytes(StandardCharsets.UTF_8))) {
      throw GrpcErrors.invalidArgument(correlationId(), null, Map.of("field", "completion_cursor"));
    }
  }

  private static String completionCursor(
      String manifestIdentity, OwnerReuseLeaseRepository.RegistrationProgress progress) {
    byte[] digest =
        sha256(
            ("owner-complete-v1\n"
                    + manifestIdentity
                    + "\n"
                    + Long.toUnsignedString(progress.registrationChunk())
                    + "\n"
                    + Long.toUnsignedString(progress.coverageChunk()))
                .getBytes(StandardCharsets.UTF_8));
    return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
  }

  private void requireRegistrationTotals(
      ai.floedb.floecat.reconciler.rpc.OwnerArtifactRegistrationManifestRef descriptor,
      OwnerReuseLeaseRepository.RegistrationProgress progress) {
    if (progress.objectCount() != descriptor.getObjectCount()
        || progress.fileStatsTargetCount() != descriptor.getFileStatsTargetCount()
        || progress.indexTargetCount() != descriptor.getIndexTargetCount()
        || progress.aggregateStatsTargetCount() != descriptor.getAggregateStatsTargetCount()) {
      throw invalidManifest();
    }
  }

  private void requireCoverageTotals(
      ai.floedb.floecat.reconciler.rpc.ReusableCoverageManifestRef descriptor,
      OwnerReuseLeaseRepository.RegistrationProgress progress) {
    if (progress.coverageRecordCount() != descriptor.getCoverageEntryCount()) {
      throw invalidManifest();
    }
  }

  private void registerOwnerArtifactBatch(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      boolean publishesIndexes,
      List<OwnerArtifactObjectReference> objects) {
    OwnerArtifactReferences references =
        ownerArtifactReferences(
            generationPrefix(tableId, snapshotId, generationId) + OWNER_SEGMENT,
            Keys.tableReusableArtifactBlobPrefix(tableId.getAccountId(), tableId.getId()),
            generationPrefix(tableId, snapshotId, generationId) + EXECUTOR_SEGMENT,
            publishesIndexes,
            objects);
    List<StatsStore.PrewrittenTargetStatsReference> aggregateStats = references.aggregateStats();
    List<StatsStore.PrewrittenTargetStatsReference> fileStats = references.fileStats();
    if (aggregateStats.stream()
            .anyMatch(value -> !isAggregateTargetStorageId(value.targetStorageId()))
        || fileStats.stream()
            .anyMatch(value -> !isFileStatsTargetStorageId(value.targetStorageId()))) {
      throw invalidManifest();
    }
    List<StatsStore.PrewrittenTargetStatsReference> stats =
        new ArrayList<>(aggregateStats.size() + fileStats.size());
    stats.addAll(aggregateStats);
    stats.addAll(fileStats);
    if (!stats.isEmpty()) {
      statsStore.registerPrewrittenStatsReferencesInGeneration(
          tableId, snapshotId, generationId, stats);
    }
    List<IndexArtifactRepository.PrewrittenIndexArtifactReference> indexReferences =
        references.indexes();
    List<StatsStore.PrewrittenStatsObject> protections =
        protectedObjects(aggregateStats, indexReferences);
    if (!protections.isEmpty()) {
      statsStore.protectPrewrittenStatsObjectsInGeneration(
          tableId, snapshotId, generationId, generationId, protections);
    }
    if (!indexReferences.isEmpty()) {
      indexes.registerTrustedOwnerIndexArtifactReferencesInGeneration(
          tableId,
          snapshotId,
          generationId,
          generationPrefix(tableId, snapshotId, generationId) + EXECUTOR_SEGMENT,
          indexReferences);
    }
  }

  private Snapshot publicationSnapshot(
      CompleteOwnerPublicationRequest request, ResourceId tableId, long snapshotId) {
    if (!request.hasSnapshot()) {
      throw GrpcErrors.invalidArgument(correlationId(), null, Map.of("field", "snapshot"));
    }
    SnapshotSpec spec = request.getSnapshot();
    if (!spec.hasTableId()
        || !tableId.equals(spec.getTableId())
        || snapshotId != spec.getSnapshotId()
        || !spec.hasUpstreamCreatedAt()) {
      throw GrpcErrors.invalidArgument(correlationId(), null, Map.of("field", "snapshot"));
    }
    PersistedSecretPropertyValidator.validateNoGeneralMetadataSecretKeys(
        spec.getSummaryMap(), correlationId(), "snapshot.summary");
    var builder =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setUpstreamCreatedAt(spec.getUpstreamCreatedAt())
            .setIngestedAt(nowTs());
    if (spec.hasParentSnapshotId()) {
      builder.setParentSnapshotId(spec.getParentSnapshotId());
    }
    if (spec.hasSchemaJson()) {
      builder.setSchemaJson(spec.getSchemaJson());
    }
    if (spec.hasPartitionSpec()) {
      builder.setPartitionSpec(spec.getPartitionSpec());
    }
    if (spec.hasSequenceNumber()) {
      builder.setSequenceNumber(spec.getSequenceNumber());
    }
    if (spec.hasManifestList()) {
      builder.setManifestList(spec.getManifestList());
    }
    builder.putAllSummary(spec.getSummaryMap());
    if (spec.hasSchemaId()) {
      builder.setSchemaId(spec.getSchemaId());
    }
    if (spec.hasMetadataLocation()) {
      builder.setMetadataLocation(spec.getMetadataLocation());
    }
    return builder.build();
  }

  private static boolean sameOwnerSnapshot(Snapshot stored, Snapshot incoming) {
    return stored.toBuilder()
        .clearIngestedAt()
        .clearReuseManifestRef()
        .build()
        .equals(incoming.toBuilder().clearIngestedAt().clearReuseManifestRef().build());
  }

  private void validateManifest(
      ResourceId tableId,
      long snapshotId,
      PublicationId publication,
      OwnerPublicationManifestRef descriptor,
      SnapshotCaptureManifest manifest) {
    long fileStatsCount = manifest.getOwnerFileStatsRecordCount();
    long aggregateStatsCount = manifest.getOwnerAggregateStatsRecordCount();
    if (fileStatsCount < 0L
        || aggregateStatsCount < 0L
        || manifest.getOwnerIndexArtifactCount() < 0L
        || descriptor.getStatsRecordCount() < 0L
        || descriptor.getIndexArtifactCount() < 0L
        || fileStatsCount > Long.MAX_VALUE - aggregateStatsCount
        || manifest.getFormatVersion() != 1
        || !manifest.hasCapturePolicy()
        || !tableId.getAccountId().equals(manifest.getAccountId())
        || !tableId.getId().equals(manifest.getTableId())
        || snapshotId != manifest.getSnapshotId()
        || !publication.value().equals(manifest.getPublicationGenerationId())
        || !tableId.getAccountId().equals(descriptor.getAccountId())
        || !tableId.getId().equals(descriptor.getTableId())
        || snapshotId != descriptor.getSnapshotId()
        || !manifest.hasOwnerArtifactRegistrationManifest()
        || descriptor.getStatsRecordCount() != fileStatsCount + aggregateStatsCount
        || descriptor.getIndexArtifactCount() != manifest.getOwnerIndexArtifactCount()
        || manifest.getFileStatsCount() != 0
        || manifest.getIndexArtifactsCount() != 0
        || manifest.getFinalStatsCount() != 0
        || manifest.getOwnerArtifactObjectsCount() != 0) {
      throw GrpcErrors.invalidArgument(correlationId(), null, Map.of("field", "manifest"));
    }
    validateCapturePolicy(publication, manifest);
    validateReusableCoverageManifestDescriptor(tableId, manifest);
    validateRegistrationManifestDescriptor(tableId, manifest);
  }

  private void validateRegistrationManifestDescriptor(
      ResourceId tableId, SnapshotCaptureManifest manifest) {
    var descriptor = manifest.getOwnerArtifactRegistrationManifest();
    if (descriptor.getFormatVersion() != OwnerArtifactRegistrationManifest.FORMAT_VERSION
        || descriptor.getPayloadBytes() < OwnerArtifactRegistrationManifest.CHUNK_HEADER_BYTES
        || descriptor.getPayloadSha256().size() != 32
        || !descriptor.hasCommitmentIndex()
        || descriptor.getObjectCount() <= 0L
        || descriptor.getFileStatsTargetCount() < 0L
        || descriptor.getIndexTargetCount() < 0L
        || descriptor.getAggregateStatsTargetCount() < 0L
        || !OwnerArtifactRegistrationManifest.hasContentAddressedUri(
            descriptor, tableId.getAccountId(), tableId.getId(), manifest.getSnapshotId())
        || descriptor.getFileStatsTargetCount() != manifest.getOwnerFileStatsRecordCount()
        || descriptor.getIndexTargetCount() != manifest.getOwnerIndexArtifactCount()
        || descriptor.getAggregateStatsTargetCount()
            != manifest.getOwnerAggregateStatsRecordCount()) {
      throw invalidManifest();
    }
  }

  private void validateReusableCoverageManifestDescriptor(
      ResourceId tableId, SnapshotCaptureManifest manifest) {
    if (manifest.getManifestKind() != SnapshotCaptureManifestKind.SCMK_OWNER_V2
        || !manifest.hasReusableCoverageManifest()) {
      throw invalidManifest();
    }
    var descriptor = manifest.getReusableCoverageManifest();
    String requiredPrefix =
        Keys.snapshotIndexArtifactCaptureManifestBlobPrefix(
            tableId.getAccountId(), tableId.getId(), manifest.getSnapshotId());
    if (!descriptor.hasCommitmentIndex()
        || !ReusableCoverageManifest.hasValidDescriptor(descriptor)
        || !ReusableCoverageManifest.hasContentAddressedUri(descriptor, requiredPrefix)) {
      throw invalidManifest();
    }
  }

  private void validateCapturePolicy(PublicationId publication, SnapshotCaptureManifest manifest) {
    CapturePolicy policy = manifest.getCapturePolicy();
    Set<Integer> outputValues = new HashSet<>();
    for (int value : policy.getOutputsValueList()) {
      if (CaptureOutput.forNumber(value) == null
          || value == CaptureOutput.CO_UNSPECIFIED_VALUE
          || !outputValues.add(value)) {
        throw invalidManifest();
      }
    }
    if (!requests(policy, CaptureOutput.CO_TABLE_STATS)
        || !requests(policy, CaptureOutput.CO_COLUMN_STATS)) {
      throw invalidManifest();
    }
    boolean fileStats = requests(policy, CaptureOutput.CO_FILE_STATS);
    boolean indexes = requests(policy, CaptureOutput.CO_PARQUET_PAGE_INDEX);
    if (fileStats != publication.publishFileStats()
        || indexes != publication.publishIndexes()
        || (!fileStats && manifest.getOwnerFileStatsRecordCount() != 0)
        || (fileStats
            && manifest.getSourceFileCount() > 0
            && manifest.getOwnerFileStatsRecordCount() < manifest.getSourceFileCount())
        || (!indexes && manifest.getOwnerIndexArtifactCount() != 0)
        || (indexes && manifest.getOwnerIndexArtifactCount() != manifest.getSourceFileCount())) {
      throw invalidManifest();
    }
    Set<String> selectors = new HashSet<>();
    for (var column : policy.getColumnsList()) {
      String selector = column.getSelector().trim();
      if (selector.isBlank()
          || (!column.getCaptureStats() && !column.getCaptureIndex())
          || (column.getCaptureStats() && !requests(policy, CaptureOutput.CO_COLUMN_STATS))
          || (column.getCaptureIndex() && !indexes)
          || !selectors.add(selector)) {
        throw invalidManifest();
      }
    }
    if (policy.getDefaultColumnScope() == DefaultColumnScope.DCS_UNSPECIFIED
        || policy.getDefaultColumnScope() == DefaultColumnScope.UNRECOGNIZED
        || (policy.getDefaultColumnScope() == DefaultColumnScope.DCS_EXPLICIT_ONLY
            && selectors.isEmpty())) {
      throw invalidManifest();
    }
  }

  private static boolean requests(CapturePolicy policy, CaptureOutput output) {
    return policy.getOutputsList().contains(output);
  }

  private record OwnerArtifactReferences(
      List<StatsStore.PrewrittenTargetStatsReference> aggregateStats,
      List<StatsStore.PrewrittenTargetStatsReference> fileStats,
      List<IndexArtifactRepository.PrewrittenIndexArtifactReference> indexes) {}

  private OwnerArtifactReferences ownerArtifactReferences(
      String aggregatePrefix,
      String reusablePrefix,
      String indexPrefix,
      boolean publishesIndexes,
      List<OwnerArtifactObjectReference> objects) {
    LinkedHashMap<String, StatsStore.PrewrittenTargetStatsReference> aggregate =
        new LinkedHashMap<>();
    LinkedHashMap<String, StatsStore.PrewrittenTargetStatsReference> files = new LinkedHashMap<>();
    LinkedHashMap<String, IndexArtifactRepository.PrewrittenIndexArtifactReference> indexes =
        new LinkedHashMap<>();
    for (OwnerArtifactObjectReference object : objects) {
      validateOwnerObject(object);
      if (object.getAggregateStatsTargetStorageIdsCount() > 0) {
        if (object.getFileStatsTargetStorageIdsCount() != 0
            || object.getIndexTargetStorageIdsCount() != 0
            || !object.getPayloadUri().startsWith(aggregatePrefix)) {
          throw invalidManifest();
        }
        for (String target : object.getAggregateStatsTargetStorageIdsList()) {
          if (!isAggregateTargetStorageId(target)
              || aggregate.putIfAbsent(target, statsReference(target, object)) != null) {
            throw invalidManifest();
          }
        }
      } else if (object.getFileStatsTargetStorageIdsCount() > 0) {
        if (object.getIndexTargetStorageIdsCount() != 0
            || !Keys.isOwnerReusableArtifactBlobUri(
                reusablePrefix, "statistics/files", ".pb", object.getPayloadUri())) {
          throw invalidManifest();
        }
        for (String target : object.getFileStatsTargetStorageIdsList()) {
          if (!isFileStatsTargetStorageId(target)
              || files.putIfAbsent(target, statsReference(target, object)) != null) {
            throw invalidManifest();
          }
        }
      } else if (publishesIndexes) {
        if (!object.getPayloadUri().startsWith(indexPrefix)
            || !ReusableArtifactBundleUris.isBundleUri(object.getPayloadUri())
            || !ReusableArtifactBundleUris.matchesDigest(
                object.getPayloadUri(), object.getPayloadSha256().toByteArray())) {
          throw invalidManifest();
        }
        for (String target : object.getIndexTargetStorageIdsList()) {
          var reference =
              new IndexArtifactRepository.PrewrittenIndexArtifactReference(
                  target,
                  object.getPayloadUri(),
                  object.getPayloadBytes(),
                  object.getPayloadSha256().toByteArray());
          if (!target.startsWith("file:")
              || target.length() == "file:".length()
              || indexes.putIfAbsent(target, reference) != null) {
            throw invalidManifest();
          }
        }
      }
    }
    List<StatsStore.PrewrittenTargetStatsReference> sortedFiles = new ArrayList<>(files.values());
    sortedFiles.sort(
        Comparator.comparing(StatsStore.PrewrittenTargetStatsReference::targetStorageId));
    return new OwnerArtifactReferences(
        List.copyOf(aggregate.values()), List.copyOf(sortedFiles), List.copyOf(indexes.values()));
  }

  private static StatsStore.PrewrittenTargetStatsReference statsReference(
      String target, OwnerArtifactObjectReference object) {
    return new StatsStore.PrewrittenTargetStatsReference(
        target,
        object.getPayloadUri(),
        object.getPayloadBytes(),
        object.getPayloadSha256().toByteArray());
  }

  private void validateOwnerObject(OwnerArtifactObjectReference object) {
    if (object.getPayloadUri().isBlank()
        || object.getPayloadBytes() == 0
        || object.getPayloadSha256().size() != 32
        || (object.getFileStatsTargetStorageIdsCount() == 0
            && object.getIndexTargetStorageIdsCount() == 0
            && object.getAggregateStatsTargetStorageIdsCount() == 0)) {
      throw invalidManifest();
    }
  }

  private RuntimeException invalidManifest() {
    return GrpcErrors.invalidArgument(correlationId(), null, Map.of("field", "manifest"));
  }

  /**
   * Collapses the Owner-written stats and index payloads to one protection record per object. A
   * reusable bundle carries several targets at one URI, so the same object can appear more than
   * once across the two families.
   */
  private static List<StatsStore.PrewrittenStatsObject> protectedObjects(
      List<StatsStore.PrewrittenTargetStatsReference> statsReferences,
      List<IndexArtifactRepository.PrewrittenIndexArtifactReference> indexReferences) {
    LinkedHashMap<String, StatsStore.PrewrittenStatsObject> unique = new LinkedHashMap<>();
    for (StatsStore.PrewrittenTargetStatsReference reference : statsReferences) {
      unique.putIfAbsent(
          reference.blobUri(),
          new StatsStore.PrewrittenStatsObject(
              reference.blobUri(), reference.blobBytes(), reference.blobSha256()));
    }
    for (IndexArtifactRepository.PrewrittenIndexArtifactReference reference : indexReferences) {
      unique.putIfAbsent(
          reference.blobUri(),
          new StatsStore.PrewrittenStatsObject(
              reference.blobUri(), reference.blobBytes(), reference.blobSha256()));
    }
    return List.copyOf(unique.values());
  }

  private static boolean isAggregateTargetStorageId(String value) {
    if ("table".equals(value)) {
      return true;
    }
    String prefix = "column-";
    if (!value.startsWith(prefix) || value.length() != prefix.length() + 19) {
      return false;
    }
    for (int index = prefix.length(); index < value.length(); index++) {
      if (!Character.isDigit(value.charAt(index))) {
        return false;
      }
    }
    return true;
  }

  private static boolean isFileStatsTargetStorageId(String value) {
    String prefix = "file-";
    if (!value.startsWith(prefix) || value.length() != prefix.length() + 64) {
      return false;
    }
    for (int index = prefix.length(); index < value.length(); index++) {
      char digit = value.charAt(index);
      if (!((digit >= '0' && digit <= '9') || (digit >= 'a' && digit <= 'f'))) {
        return false;
      }
    }
    return true;
  }

  private ResourceId authorizedTable(ResourceId requested) {
    var context = principal.get();
    authz.require(context, RolePermissions.RECONCILE_EXECUTOR_CONTROL_INTERNAL);
    ensureKind(requested, ResourceKind.RK_TABLE, "table_id", correlationId());
    ResourceId tableId = requested.toBuilder().setAccountId(context.getAccountId()).build();
    if (tables.getById(tableId).isEmpty()) {
      throw GrpcErrors.notFound(
          correlationId(), GeneratedErrorMessages.MessageKey.TABLE, Map.of("id", tableId.getId()));
    }
    return tableId;
  }

  private long requireSnapshotId(long snapshotId) {
    if (snapshotId < 0) {
      throw GrpcErrors.invalidArgument(correlationId(), null, Map.of("field", "snapshot_id"));
    }
    return snapshotId;
  }

  static String generationId(BeginOwnerPublicationRequest request, String callerSubject) {
    return generationId(
        request.getOwnerId(),
        request.getOwnerGenerationId(),
        request.getPublishFileStats(),
        request.getPublishIndexes(),
        callerSubject);
  }

  private static String generationId(
      String ownerId,
      String ownerGenerationId,
      boolean publishFileStats,
      boolean publishIndexes,
      String callerSubject) {
    if (ownerId.isBlank() || ownerGenerationId.isBlank() || callerSubject.isBlank()) {
      throw new IllegalArgumentException("owner identity is required");
    }
    String identity =
        callerSubject
            + '\0'
            + ownerId
            + '\0'
            + ownerGenerationId
            + '\0'
            + publishFileStats
            + '\0'
            + publishIndexes;
    return OWNER_GENERATION_PREFIX
        + (publishFileStats ? '1' : '0')
        + (publishIndexes ? '1' : '0')
        + '-'
        + Hashing.sha256Hex(identity);
  }

  private String requireCallerSubject() {
    String subject = principal.get().getSubject();
    if (subject == null || subject.isBlank()) {
      throw GrpcErrors.permissionDenied(correlationId(), null, null);
    }
    return subject;
  }

  private PublicationId requirePublicationId(String publicationId) {
    if (publicationId == null
        || !publicationId.startsWith(OWNER_GENERATION_PREFIX)
        || publicationId.length() != OWNER_GENERATION_PREFIX.length() + 3 + 64
        || (publicationId.charAt(OWNER_GENERATION_PREFIX.length()) != '0'
            && publicationId.charAt(OWNER_GENERATION_PREFIX.length()) != '1')
        || (publicationId.charAt(OWNER_GENERATION_PREFIX.length() + 1) != '0'
            && publicationId.charAt(OWNER_GENERATION_PREFIX.length() + 1) != '1')
        || publicationId.charAt(OWNER_GENERATION_PREFIX.length() + 2) != '-') {
      throw GrpcErrors.invalidArgument(correlationId(), null, Map.of("field", "publication_id"));
    }
    return new PublicationId(
        publicationId,
        publicationId.charAt(OWNER_GENERATION_PREFIX.length()) == '1',
        publicationId.charAt(OWNER_GENERATION_PREFIX.length() + 1) == '1');
  }

  private String requirePublicationCapability(
      ResourceId tableId,
      long snapshotId,
      String publicationId,
      String ownerId,
      String ownerGenerationId) {
    PublicationId publication = requirePublicationId(publicationId);
    String expected =
        generationId(
            ownerId,
            ownerGenerationId,
            publication.publishFileStats(),
            publication.publishIndexes(),
            requireCallerSubject());
    if (!MessageDigest.isEqual(
        publication.value().getBytes(StandardCharsets.UTF_8),
        expected.getBytes(StandardCharsets.UTF_8))) {
      throw GrpcErrors.permissionDenied(correlationId(), null, null);
    }
    if (!statsStore.statsGenerationExists(tableId, snapshotId, publication.value())) {
      throw GrpcErrors.preconditionFailed(
          correlationId(),
          GeneratedErrorMessages.MessageKey.PUBLICATION_NOT_BEGUN,
          Map.of("publication_id", publication.value()));
    }
    return publication.value();
  }

  private void validateInProgressManifest(
      ResourceId tableId, long snapshotId, SnapshotReuseManifestRef manifest) {
    byte[] digest = manifest.getPayloadSha256().toByteArray();
    String expectedUri =
        Keys.snapshotIndexArtifactCaptureManifestBlobPrefix(
                tableId.getAccountId(), tableId.getId(), snapshotId)
            + HexFormat.of().formatHex(digest)
            + ".pb";
    if (manifest.getFormatVersion() != 1
        || manifest.getKind() != SnapshotReuseManifestKind.SRMK_OWNER_V2_PARTIAL
        || manifest.getPayloadBytes() <= 0L
        || digest.length != 32
        || !manifest.getStatsGenerationManifestUri().isBlank()
        || !expectedUri.equals(manifest.getUri())) {
      throw GrpcErrors.invalidArgument(correlationId(), null, Map.of("field", "manifest"));
    }
  }

  private record PublicationId(String value, boolean publishFileStats, boolean publishIndexes) {}

  private String generationPrefix(ResourceId tableId, long snapshotId, String generationId) {
    return Keys.snapshotTargetStatsGenerationBlobPrefix(
        tableId.getAccountId(), tableId.getId(), snapshotId, generationId);
  }

  private void requireDigest(byte[] digest, String field) {
    if (digest.length != 32) {
      throw GrpcErrors.invalidArgument(correlationId(), null, Map.of("field", field));
    }
  }

  private String requireBlobBucket() {
    if (blobBucket == null || blobBucket.isBlank()) {
      throw new IllegalStateException("floecat.blob.s3.bucket is required");
    }
    return blobBucket.trim();
  }

  private Map<String, String> artifactEndpointProperty() {
    if (storageAwsS3Endpoint == null) {
      return Map.of();
    }
    return storageAwsS3Endpoint
        .map(String::trim)
        .filter(value -> !value.isEmpty())
        .map(value -> Map.of("s3.endpoint", value))
        .orElseGet(Map::of);
  }

  private static byte[] sha256(byte[] value) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(value);
    } catch (NoSuchAlgorithmException error) {
      throw new IllegalStateException("SHA-256 unavailable", error);
    }
  }
}
