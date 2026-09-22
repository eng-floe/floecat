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
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotSpec;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.rpc.CaptureOutput;
import ai.floedb.floecat.reconciler.rpc.CapturePolicy;
import ai.floedb.floecat.reconciler.rpc.DefaultColumnScope;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifest;
import ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor;
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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
  static final int MAX_AGGREGATE_REFERENCES = 100_000;
  // A single family at this cardinality fits below the independent 64 MiB encoded-manifest cap;
  // combinations remain bounded by that cap and report manifest_bytes explicitly.
  static final int MAX_FILE_STAT_REFERENCES = 100_000;
  static final int MAX_INDEX_REFERENCES = 100_000;
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

  @Override
  public Uni<BeginOwnerPublicationResponse> beginOwnerPublication(
      BeginOwnerPublicationRequest request) {
    var log = LogHelper.start(LOG, "BeginOwnerPublication");
    return mapFailures(
            run(
                () -> {
                  ResourceId tableId = authorizedTable(request.getTableId());
                  long snapshotId = requireSnapshotId(request.getSnapshotId());
                  new CatalogSurfaceWritePolicy(graphView)
                      .requireWritableTable(tableId, correlationId());
                  String generationId = generationId(request, requireCallerSubject());
                  if (!statsStore.statsGenerationExists(tableId, snapshotId, generationId)) {
                    statsStore.beginStatsGeneration(tableId, snapshotId, generationId);
                  }
                  // This marker is tiny and Floecat-owned. Requiring the Owner to manufacture it
                  // would add an upload while also making the existing prepared-generation API
                  // fail every publication that did not know about this repository detail. This
                  // call also repairs a Begin retry interrupted after reserving the generation.
                  statsStore.prepareStatsGenerationManifest(tableId, snapshotId, generationId);
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
    new CatalogSurfaceWritePolicy(graphView).requireWritableTable(tableId, correlationId());
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
    byte[] manifestBytes = blobStore.get(manifestUri);
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
    requireReferenceCount(
        "manifest.final_stats", manifest.getFinalStatsCount(), MAX_AGGREGATE_REFERENCES, false);
    requireReferenceCount(
        "manifest.file_stats", manifest.getFileStatsCount(), MAX_FILE_STAT_REFERENCES, true);
    requireReferenceCount(
        "manifest.index_artifacts", manifest.getIndexArtifactsCount(), MAX_INDEX_REFERENCES, true);

    List<StatsStore.PrewrittenTargetStatsReference> aggregateStats =
        statsReferences(
            generationPrefix(tableId, snapshotId, generationId) + OWNER_SEGMENT,
            manifest.getFinalStatsList());
    List<StatsStore.PrewrittenTargetStatsReference> fileStats =
        statsReferences(
            generationPrefix(tableId, snapshotId, generationId) + EXECUTOR_SEGMENT,
            manifest.getFileStatsList());
    if (aggregateStats.stream()
            .anyMatch(value -> !isAggregateTargetStorageId(value.targetStorageId()))
        || fileStats.stream()
            .anyMatch(value -> !isFileStatsTargetStorageId(value.targetStorageId()))) {
      throw invalidManifest();
    }
    List<StatsStore.PrewrittenTargetStatsReference> allStats =
        new ArrayList<>(aggregateStats.size() + fileStats.size());
    allStats.addAll(aggregateStats);
    allStats.addAll(fileStats);
    boolean publishesIndexes =
        requests(manifest.getCapturePolicy(), CaptureOutput.CO_PARQUET_PAGE_INDEX);
    List<IndexArtifactRepository.PrewrittenIndexArtifactReference> indexReferences =
        publishesIndexes
            ? indexReferences(
                generationPrefix(tableId, snapshotId, generationId) + EXECUTOR_SEGMENT,
                manifest.getIndexArtifactsList())
            : List.of();
    // Index wrappers are protected alongside the stats payloads: both families are Owner-written
    // objects under this generation's prefix, and GC must not reclaim either between the upload
    // and the activation that roots them.
    List<StatsStore.PrewrittenStatsObject> protectedObjects =
        protectedObjects(allStats, indexReferences);
    boolean retry =
        statsStore.validatePreparedStatsGenerationRetry(
            tableId, snapshotId, generationId, allStats);
    if (!retry) {
      statsStore.protectPrewrittenStatsObjectsInGeneration(
          tableId, snapshotId, generationId, generationId, protectedObjects);
    }

    IndexArtifactRepository.PreparedActivation preparedIndexes = null;
    if (publishesIndexes) {
      indexes.registerTrustedOwnerIndexArtifactReferencesInGeneration(
          tableId,
          snapshotId,
          generationId,
          generationPrefix(tableId, snapshotId, generationId) + EXECUTOR_SEGMENT,
          indexReferences);
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
      if (!sameOwnerSnapshot(existingSnapshot.get(), snapshot)) {
        throw GrpcErrors.preconditionFailed(
            correlationId(),
            GeneratedErrorMessages.MessageKey.PUBLICATION_SNAPSHOT_CONFLICT,
            Map.of("table_id", tableId.getId(), "snapshot_id", Long.toString(snapshotId)));
      }
      publishedSnapshot = existingSnapshot.get();
    } else {
      publicationUpdates.addAll(snapshots.prepareCreatePublicationUpdates(snapshot));
    }
    StatsStore.PublicationFence publicationFence =
        publicationUpdates.isEmpty() ? null : new StatsStore.PublicationFence(publicationUpdates);

    StatsStore.StatsGenerationPredecessor predecessor =
        persistence.prepareStatsGenerationForPublication(tableId, snapshotId, generationId, false);
    boolean activated =
        persistence.publishPreparedStatsGeneration(
            tableId, snapshotId, generationId, allStats, predecessor, publicationFence);
    if (activated) {
      if (preparedIndexes != null) {
        indexes.completePreparedGenerationActivation(tableId, snapshotId, preparedIndexes);
      }
      persistence.clearPrewrittenArtifactProtections(tableId, snapshotId, generationId);
      // publishPreparedStatsGeneration already committed the activated generation onto the table
      // root, and maybeAdvance re-commits it when the current-snapshot pointer moves onto this
      // snapshot. No third commit is needed here.
      currentSnapshots.maybeAdvance(tableId, publishedSnapshot, correlationId());
    }
    return CompleteOwnerPublicationResponse.newBuilder()
        .setAggregateStatsPublished(aggregateStats.size())
        .setFileStatsPublished(fileStats.size())
        .setIndexArtifactsPublished(manifest.getIndexArtifactsCount())
        .setActivated(activated)
        .build();
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
    if (manifest.getFormatVersion() != 1
        || !manifest.hasCapturePolicy()
        || !tableId.getAccountId().equals(manifest.getAccountId())
        || !tableId.getId().equals(manifest.getTableId())
        || snapshotId != manifest.getSnapshotId()
        || !publication.value().equals(manifest.getPublicationGenerationId())
        || !tableId.getAccountId().equals(descriptor.getAccountId())
        || !tableId.getId().equals(descriptor.getTableId())
        || snapshotId != descriptor.getSnapshotId()
        || manifest.getFileStatsCount() != manifest.getFileStatsRecordCount()
        || manifest.getFinalStatsCount() != manifest.getFinalStatsRecordCount()
        || manifest.getIndexArtifactsCount() != manifest.getIndexArtifactCount()
        || descriptor.getStatsRecordCount()
            != manifest.getFileStatsCount() + manifest.getFinalStatsCount()
        || descriptor.getIndexArtifactCount() != manifest.getIndexArtifactsCount()) {
      throw GrpcErrors.invalidArgument(correlationId(), null, Map.of("field", "manifest"));
    }
    validateCapturePolicy(publication, manifest);
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
        || (!fileStats && manifest.getFileStatsCount() != 0)
        || (fileStats
            && manifest.getSourceFileCount() > 0
            && manifest.getFileStatsCount() < manifest.getSourceFileCount())
        || (!indexes && manifest.getIndexArtifactsCount() != 0)
        || (indexes && manifest.getIndexArtifactsCount() != manifest.getSourceFileCount())) {
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

  private List<StatsStore.PrewrittenTargetStatsReference> statsReferences(
      String prefix, List<StatsObjectDescriptor> values) {
    LinkedHashMap<String, StatsStore.PrewrittenTargetStatsReference> unique = new LinkedHashMap<>();
    for (StatsObjectDescriptor value : values) {
      validateReference(prefix, value);
      var reference =
          new StatsStore.PrewrittenTargetStatsReference(
              value.getTargetStorageId(),
              value.getPayloadUri(),
              value.getPayloadBytes(),
              value.getPayloadSha256().toByteArray());
      if (unique.putIfAbsent(value.getTargetStorageId(), reference) != null) {
        throw invalidManifest();
      }
    }
    List<StatsStore.PrewrittenTargetStatsReference> result = new ArrayList<>(unique.values());
    result.sort(Comparator.comparing(StatsStore.PrewrittenTargetStatsReference::targetStorageId));
    return List.copyOf(result);
  }

  private List<IndexArtifactRepository.PrewrittenIndexArtifactReference> indexReferences(
      String prefix, List<StatsObjectDescriptor> values) {
    LinkedHashMap<String, IndexArtifactRepository.PrewrittenIndexArtifactReference> unique =
        new LinkedHashMap<>();
    for (StatsObjectDescriptor value : values) {
      validateReference(prefix, value);
      if (!value.getTargetStorageId().startsWith("file:")
          || value.getTargetStorageId().length() == "file:".length()) {
        throw invalidManifest();
      }
      var reference =
          new IndexArtifactRepository.PrewrittenIndexArtifactReference(
              value.getTargetStorageId(),
              value.getPayloadUri(),
              value.getPayloadBytes(),
              value.getPayloadSha256().toByteArray());
      if (unique.putIfAbsent(value.getTargetStorageId(), reference) != null) {
        throw invalidManifest();
      }
    }
    return List.copyOf(unique.values());
  }

  private void validateReference(String prefix, StatsObjectDescriptor value) {
    if (value.getTargetStorageId().isBlank()
        || value.getPayloadUri().isBlank()
        || !value.getPayloadUri().startsWith(prefix)
        || value.getPayloadBytes() == 0) {
      throw GrpcErrors.invalidArgument(correlationId(), null, Map.of("field", "references"));
    }
    requireDigest(value.getPayloadSha256().toByteArray(), "payload_sha256");
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

  private void requireReferenceCount(String field, int count, int maximum, boolean emptyAllowed) {
    if ((!emptyAllowed && count == 0) || count > maximum) {
      throw GrpcErrors.invalidArgument(
          correlationId(), null, Map.of("field", field, "max_count", Integer.toString(maximum)));
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
