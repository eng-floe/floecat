/*
 * Copyright 2026 Yellowbrick Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package ai.floedb.floecat.service.statistics.impl;

import ai.floedb.floecat.catalog.rpc.OwnerPublicationLease;
import ai.floedb.floecat.catalog.rpc.SnapshotReuseManifestKind;
import ai.floedb.floecat.catalog.rpc.SnapshotReuseManifestRef;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.TableBlobReachabilityGuard;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.types.Hashing;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.TreeSet;
import java.util.function.LongSupplier;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * One namespace-and-manifest GC lease held by an in-flight Owner publication.
 *
 * <p>Each surviving lease pins the table's reusable namespace plus its exact source/successor
 * capture manifests. Begin and Complete renew the bounded lease; GC ignores it after expiry so an
 * abandoned publication cannot stop reclamation indefinitely.
 */
@ApplicationScoped
public class OwnerReuseLeaseRepository {
  private static final String PROGRESS_PREFIX = "registration:v1:";

  public record RegistrationProgress(
      long registrationChunk,
      long coverageChunk,
      long coverageRecordCount,
      long objectCount,
      long fileStatsTargetCount,
      long indexTargetCount,
      long aggregateStatsTargetCount) {
    static RegistrationProgress initial() {
      return new RegistrationProgress(0L, 0L, 0L, 0L, 0L, 0L, 0L);
    }
  }

  @Inject PointerStore pointers;
  @Inject BlobStore blobs;
  @Inject TableBlobReachabilityGuard reachability;

  @ConfigProperty(name = "floecat.owner-publication.reuse-lease-ttl-ms", defaultValue = "86400000")
  long leaseTtlMs = 24L * 60L * 60L * 1000L;

  LongSupplier nowMillis = System::currentTimeMillis;

  public static final class LeaseContinuityException extends IllegalStateException {
    public LeaseContinuityException(String message) {
      super(message);
    }
  }

  public long acquire(
      ResourceId tableId,
      String publicationId,
      String captureManifestPrefix,
      SnapshotReuseManifestRef source) {
    return acquire(tableId, publicationId, captureManifestPrefix, source, () -> {});
  }

  public long acquire(
      ResourceId tableId,
      String publicationId,
      String captureManifestPrefix,
      SnapshotReuseManifestRef source,
      Runnable initializePublication) {
    return updateLease(
        tableId,
        publicationId,
        captureManifestPrefix,
        source,
        false,
        java.util.Objects.requireNonNull(initializePublication, "initializePublication"));
  }

  public long renew(ResourceId tableId, String publicationId, SnapshotReuseManifestRef successor) {
    return updateLease(tableId, publicationId, null, successor, true, () -> {});
  }

  private long updateLease(
      ResourceId tableId,
      String publicationId,
      String captureManifestPrefix,
      SnapshotReuseManifestRef source,
      boolean requireContinuousLease,
      Runnable initializePublication) {
    if (leaseTtlMs <= 0L) {
      throw new IllegalStateException("Owner reuse lease TTL must be positive");
    }
    long now = nowMillis.getAsLong();
    long expiresAt;
    try {
      expiresAt = Math.addExact(now, leaseTtlMs);
    } catch (ArithmeticException error) {
      throw new IllegalStateException("Owner reuse lease TTL is too large", error);
    }
    if (source != null
        && (source.getFormatVersion() != 1
            || source.getKind() != SnapshotReuseManifestKind.SRMK_OWNER_V2
            || source.getUri().isBlank()
            || source.getPayloadBytes() <= 0L
            || source.getPayloadSha256().size() != 32)) {
      throw new IllegalArgumentException("complete reuse source manifest metadata is required");
    }
    if (!requireContinuousLease
        && (captureManifestPrefix == null
            || !captureManifestPrefix.startsWith(
                Keys.tableSnapshotBlobPrefix(tableId.getAccountId(), tableId.getId()))
            || !captureManifestPrefix.endsWith(Keys.SEG_INDEX_CAPTURE_MANIFESTS))) {
      throw new IllegalArgumentException("snapshot capture-manifest prefix is required");
    }
    reachability.publishing(
        tableId,
        () -> {
          String key =
              Keys.tableOwnerReuseLeasePointer(
                  tableId.getAccountId(), tableId.getId(), publicationId);
          for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
            Pointer current = pointers.get(key).orElse(null);
            TreeSet<String> protectedManifests = new TreeSet<>();
            TreeSet<String> protectedManifestPrefixes = new TreeSet<>();
            boolean reclaimExpired = false;
            if (current == null && requireContinuousLease) {
              throw new LeaseContinuityException(
                  "Owner reuse lease is missing; publication protection was lost");
            }
            if (current != null) {
              byte[] currentBytes = blobs.get(current.getBlobUri());
              if (currentBytes == null) {
                throw new BaseResourceRepository.CorruptionException(
                    "Owner reuse lease object is missing", null);
              }
              OwnerPublicationLease currentLease;
              try {
                currentLease = OwnerPublicationLease.parseFrom(currentBytes);
              } catch (com.google.protobuf.InvalidProtocolBufferException error) {
                throw new BaseResourceRepository.CorruptionException(
                    "Owner reuse lease object is malformed", error);
              }
              validateLease(
                  tableId, publicationId, currentLease, current.getBlobUri(), currentBytes);
              if (currentLease.getExpiresAtEpochMs() <= now) {
                if (requireContinuousLease) {
                  throw new LeaseContinuityException(
                      "Owner reuse lease expired; publication protection was lost");
                }
                reclaimExpired = true;
              } else {
                protectedManifests.addAll(currentLease.getProtectedCaptureManifestUrisList());
                protectedManifestPrefixes.addAll(
                    currentLease.getProtectedCaptureManifestPrefixesList());
              }
            }
            if (source != null) {
              protectedManifests.add(source.getUri());
            }
            if (captureManifestPrefix != null) {
              protectedManifestPrefixes.add(captureManifestPrefix);
            }
            OwnerPublicationLease lease =
                OwnerPublicationLease.newBuilder()
                    .setFormatVersion(1)
                    .setAccountId(tableId.getAccountId())
                    .setTableId(tableId.getId())
                    .setPublicationId(publicationId)
                    .setReusableNamespacePrefix(
                        Keys.tableReusableArtifactBlobPrefix(
                            tableId.getAccountId(), tableId.getId()))
                    .addAllProtectedCaptureManifestUris(protectedManifests)
                    .addAllProtectedCaptureManifestPrefixes(protectedManifestPrefixes)
                    .setExpiresAtEpochMs(expiresAt)
                    .build();
            byte[] leaseBytes = lease.toByteArray();
            String leaseUri =
                Keys.ownerPublicationLeaseBlobUri(
                    tableId.getAccountId(),
                    tableId.getId(),
                    publicationId,
                    Hashing.sha256Hex(leaseBytes));
            if (current != null && leaseUri.equals(current.getBlobUri())) {
              initializePublication.run();
              return null;
            }
            blobs.putImmutable(leaseUri, leaseBytes, "application/x-protobuf");
            long expected = current == null ? 0L : current.getVersion();
            Pointer next =
                PointerReferences.blobPointer(key, leaseUri, expected + 1L, leaseBytes.length);
            if (pointers.compareAndSet(key, expected, next)) {
              if (reclaimExpired) {
                clearProgress(tableId, publicationId);
              }
              initializePublication.run();
              return null;
            }
          }
          throw new BaseResourceRepository.AbortRetryableException(
              "could not acquire Owner reuse lease " + publicationId);
        });
    return expiresAt;
  }

  private static void validateLease(
      ResourceId tableId,
      String publicationId,
      OwnerPublicationLease lease,
      String leaseUri,
      byte[] leaseBytes) {
    String expectedUri =
        Keys.ownerPublicationLeaseBlobUri(
            tableId.getAccountId(), tableId.getId(), publicationId, Hashing.sha256Hex(leaseBytes));
    if (lease.getFormatVersion() != 1
        || !tableId.getAccountId().equals(lease.getAccountId())
        || !tableId.getId().equals(lease.getTableId())
        || !publicationId.equals(lease.getPublicationId())
        || lease.getExpiresAtEpochMs() <= 0L
        || !Keys.tableReusableArtifactBlobPrefix(tableId.getAccountId(), tableId.getId())
            .equals(lease.getReusableNamespacePrefix())
        || !expectedUri.equals(leaseUri)) {
      throw new BaseResourceRepository.CorruptionException("invalid Owner reuse lease", null);
    }
  }

  public void release(ResourceId tableId, String publicationId) {
    reachability.publishing(
        tableId,
        () -> {
          deletePointer(
              Keys.tableOwnerReuseLeasePointer(
                  tableId.getAccountId(), tableId.getId(), publicationId),
              "Owner reuse lease " + publicationId);
          clearProgress(tableId, publicationId);
          return null;
        });
  }

  private void clearProgress(ResourceId tableId, String publicationId) {
    deletePointer(
        Keys.tableOwnerPublicationProgressPointer(
            tableId.getAccountId(), tableId.getId(), publicationId),
        "Owner publication progress " + publicationId);
  }

  private void deletePointer(String key, String description) {
    for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
      Pointer current = pointers.get(key).orElse(null);
      if (current == null || pointers.compareAndDelete(key, current.getVersion())) {
        return;
      }
    }
    throw new BaseResourceRepository.AbortRetryableException("could not release " + description);
  }

  public RegistrationProgress progress(
      ResourceId tableId, String publicationId, String manifestSha256) {
    Pointer progress =
        pointers
            .get(
                Keys.tableOwnerPublicationProgressPointer(
                    tableId.getAccountId(), tableId.getId(), publicationId))
            .orElse(null);
    if (progress == null) {
      return RegistrationProgress.initial();
    }
    String prefix = PROGRESS_PREFIX + manifestSha256 + ":";
    if (!progress.getBlobUri().startsWith(prefix)) {
      throw new IllegalArgumentException("Owner publication manifest changed while resuming");
    }
    String[] fields = progress.getBlobUri().substring(prefix.length()).split(":", -1);
    if (fields.length != 7) {
      throw new IllegalArgumentException("Owner publication progress is malformed");
    }
    try {
      return new RegistrationProgress(
          Long.parseUnsignedLong(fields[0]),
          Long.parseUnsignedLong(fields[1]),
          Long.parseUnsignedLong(fields[2]),
          Long.parseUnsignedLong(fields[3]),
          Long.parseUnsignedLong(fields[4]),
          Long.parseUnsignedLong(fields[5]),
          Long.parseUnsignedLong(fields[6]));
    } catch (IllegalArgumentException error) {
      throw new IllegalArgumentException("Owner publication progress is malformed", error);
    }
  }

  public void advanceProgress(
      ResourceId tableId,
      String publicationId,
      String manifestSha256,
      RegistrationProgress expected,
      RegistrationProgress nextProgress) {
    if (nextProgress.registrationChunk() < expected.registrationChunk()
        || nextProgress.coverageChunk() < expected.coverageChunk()
        || (nextProgress.registrationChunk() == expected.registrationChunk()
            && nextProgress.coverageChunk() == expected.coverageChunk())) {
      throw new IllegalArgumentException("publication progress must advance");
    }
    String key =
        Keys.tableOwnerPublicationProgressPointer(
            tableId.getAccountId(), tableId.getId(), publicationId);
    for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
      Pointer current = pointers.get(key).orElse(null);
      RegistrationProgress currentProgress =
          current == null
              ? RegistrationProgress.initial()
              : progress(tableId, publicationId, manifestSha256);
      if (currentProgress.equals(nextProgress)) {
        return;
      }
      if (!currentProgress.equals(expected)) {
        throw new BaseResourceRepository.AbortRetryableException(
            "Owner publication cursor advanced concurrently");
      }
      long version = current == null ? 1L : current.getVersion() + 1L;
      Pointer next =
          PointerReferences.opaqueMarkerPointer(
              key,
              PROGRESS_PREFIX
                  + manifestSha256
                  + ":"
                  + Long.toUnsignedString(nextProgress.registrationChunk())
                  + ":"
                  + Long.toUnsignedString(nextProgress.coverageChunk())
                  + ":"
                  + Long.toUnsignedString(nextProgress.coverageRecordCount())
                  + ":"
                  + Long.toUnsignedString(nextProgress.objectCount())
                  + ":"
                  + Long.toUnsignedString(nextProgress.fileStatsTargetCount())
                  + ":"
                  + Long.toUnsignedString(nextProgress.indexTargetCount())
                  + ":"
                  + Long.toUnsignedString(nextProgress.aggregateStatsTargetCount()),
              version);
      if (pointers.compareAndSet(key, current == null ? 0L : current.getVersion(), next)) {
        return;
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "could not advance Owner publication cursor " + publicationId);
  }
}
