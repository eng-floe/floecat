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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * One namespace-and-manifest GC lease held by an in-flight Owner publication.
 *
 * <p>Each surviving lease pins the table's reusable namespace plus its exact source/successor
 * capture manifests. Begin and Complete renew the bounded lease; GC ignores it after expiry so an
 * abandoned publication cannot stop reclamation indefinitely.
 */
@ApplicationScoped
public class OwnerReuseLeaseRepository {
  private static final Logger LOG = Logger.getLogger(OwnerReuseLeaseRepository.class);
  private static final String PROGRESS_PREFIX = "registration:v1:";
  private static final int ACTIVE_REUSE_MANIFEST_LIMIT = 16;
  private static final int LEASE_SCAN_PAGE_SIZE = 256;
  // Pointer order is publication-id order, not recency order. This deliberately bounds a
  // best-effort cross-publication optimization; the direct own-lease probe below preserves the
  // restart path even when the current publication falls outside this window.
  private static final int LEASE_SCAN_LIMIT = 1024;
  static final int LEASE_SCAN_PAGE_LIMIT =
      (LEASE_SCAN_LIMIT + LEASE_SCAN_PAGE_SIZE - 1) / LEASE_SCAN_PAGE_SIZE + 1;

  public record AcquireResult(
      long expiresAtEpochMs, List<SnapshotReuseManifestRef> inProgressManifests) {}

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
        null,
        false,
        null,
        false,
        java.util.Objects.requireNonNull(initializePublication, "initializePublication"));
  }

  public AcquireResult acquireWithCandidates(
      ResourceId tableId,
      String publicationId,
      String captureManifestPrefix,
      SnapshotReuseManifestRef source,
      Runnable initializePublication) {
    AtomicReference<List<SnapshotReuseManifestRef>> candidates = new AtomicReference<>(List.of());
    long expiresAt =
        updateLease(
            tableId,
            publicationId,
            captureManifestPrefix,
            source,
            null,
            true,
            candidates,
            false,
            java.util.Objects.requireNonNull(initializePublication, "initializePublication"));
    return new AcquireResult(expiresAt, candidates.get());
  }

  public long renew(ResourceId tableId, String publicationId, SnapshotReuseManifestRef successor) {
    return updateLease(tableId, publicationId, null, successor, null, false, null, true, () -> {});
  }

  public long publishInProgressManifest(
      ResourceId tableId, String publicationId, SnapshotReuseManifestRef manifest) {
    return updateLease(tableId, publicationId, null, null, manifest, false, null, true, () -> {});
  }

  private long updateLease(
      ResourceId tableId,
      String publicationId,
      String captureManifestPrefix,
      SnapshotReuseManifestRef source,
      SnapshotReuseManifestRef inProgressManifest,
      boolean discoverCandidates,
      AtomicReference<List<SnapshotReuseManifestRef>> candidatesOut,
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
    if (inProgressManifest != null && !isValidPartialManifest(tableId, inProgressManifest)) {
      throw new IllegalArgumentException("partial Owner reuse manifest metadata is required");
    }
    if (!requireContinuousLease
        && (captureManifestPrefix == null
            || !Keys.isSnapshotIndexArtifactCaptureManifestBlobPrefix(
                tableId.getAccountId(), tableId.getId(), captureManifestPrefix))) {
      throw new IllegalArgumentException("snapshot capture-manifest prefix is required");
    }
    reachability.publishing(
        tableId,
        () -> {
          List<SnapshotReuseManifestRef> candidates =
              discoverCandidates
                  ? activeInProgressManifests(tableId, publicationId, now)
                  : List.of();
          if (candidatesOut != null) {
            candidatesOut.set(candidates);
          }
          String key =
              Keys.tableOwnerReuseLeasePointer(
                  tableId.getAccountId(), tableId.getId(), publicationId);
          for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
            Pointer current = pointers.get(key).orElse(null);
            TreeSet<String> protectedManifests = new TreeSet<>();
            TreeSet<String> protectedManifestPrefixes = new TreeSet<>();
            TreeSet<String> discoveredManifestPrefixes = new TreeSet<>();
            boolean reclaimExpired = false;
            String currentReuseSourceManifestUri = "";
            SnapshotReuseManifestRef currentInProgressManifest = null;
            long currentInProgressManifestUpdatedAt = 0L;
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
                discoveredManifestPrefixes.addAll(
                    currentLease.getDiscoveredCaptureManifestPrefixesList());
                currentReuseSourceManifestUri = currentLease.getReuseSourceCaptureManifestUri();
                if (currentLease.hasInProgressReuseManifestRef()) {
                  currentInProgressManifest = currentLease.getInProgressReuseManifestRef();
                  currentInProgressManifestUpdatedAt =
                      currentLease.getInProgressReuseManifestUpdatedAtEpochMs();
                }
              }
            }
            if (discoverCandidates) {
              // Begin replaces only the prior discovered planning inputs. Source and self-published
              // roots have independent lifetimes and must survive a repeated Begin.
              protectedManifestPrefixes.removeAll(discoveredManifestPrefixes);
              discoveredManifestPrefixes.clear();
            }
            if (!requireContinuousLease && source != null) {
              if (!currentReuseSourceManifestUri.isBlank()) {
                protectedManifests.remove(currentReuseSourceManifestUri);
              }
              currentReuseSourceManifestUri = source.getUri();
              protectedManifests.add(currentReuseSourceManifestUri);
            } else if (source != null) {
              protectedManifests.add(source.getUri());
            }
            if (captureManifestPrefix != null) {
              protectedManifestPrefixes.add(captureManifestPrefix);
            }
            for (SnapshotReuseManifestRef candidate : candidates) {
              String candidatePrefix = partialManifestPrefix(tableId, candidate);
              boolean alreadyOwned =
                  candidatePrefix.equals(captureManifestPrefix)
                      || protectedManifestPrefixes.contains(candidatePrefix);
              protectedManifestPrefixes.add(candidatePrefix);
              if (!alreadyOwned) {
                discoveredManifestPrefixes.add(candidatePrefix);
              }
            }
            if (inProgressManifest != null) {
              protectedManifestPrefixes.add(partialManifestPrefix(tableId, inProgressManifest));
              currentInProgressManifest = inProgressManifest;
              currentInProgressManifestUpdatedAt = now;
            }
            OwnerPublicationLease.Builder leaseBuilder =
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
                    .addAllDiscoveredCaptureManifestPrefixes(discoveredManifestPrefixes)
                    .setExpiresAtEpochMs(expiresAt);
            if (!currentReuseSourceManifestUri.isBlank()) {
              leaseBuilder.setReuseSourceCaptureManifestUri(currentReuseSourceManifestUri);
            }
            if (currentInProgressManifest != null) {
              leaseBuilder
                  .setInProgressReuseManifestRef(currentInProgressManifest)
                  .setInProgressReuseManifestUpdatedAtEpochMs(currentInProgressManifestUpdatedAt);
            }
            OwnerPublicationLease lease = leaseBuilder.build();
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

  private List<SnapshotReuseManifestRef> activeInProgressManifests(
      ResourceId tableId, String publicationId, long now) {
    String prefix = Keys.tableOwnerReuseLeasePointerPrefix(tableId.getAccountId(), tableId.getId());
    String token = "";
    Set<String> seenTokens = new HashSet<>();
    Map<String, OwnerPublicationLease> active = new HashMap<>();
    int scanned = 0;
    int pages = 0;
    int unreadable = 0;
    Throwable firstReadFailure = null;
    do {
      StringBuilder next = new StringBuilder();
      int pageSize = Math.min(LEASE_SCAN_PAGE_SIZE, LEASE_SCAN_LIMIT - scanned);
      for (Pointer pointer : pointers.listPointersByPrefix(prefix, pageSize, token, next)) {
        scanned++;
        try {
          byte[] leaseBytes = blobs.get(pointer.getBlobUri());
          if (leaseBytes == null) {
            throw new BaseResourceRepository.CorruptionException(
                "Owner reuse lease object is missing", null);
          }
          OwnerPublicationLease lease = OwnerPublicationLease.parseFrom(leaseBytes);
          String canonicalPointer =
              Keys.tableOwnerReuseLeasePointer(
                  tableId.getAccountId(), tableId.getId(), lease.getPublicationId());
          if (!canonicalPointer.equals(pointer.getKey())) {
            throw new BaseResourceRepository.CorruptionException(
                "Owner reuse lease pointer identity is inconsistent", null);
          }
          validateLease(tableId, lease.getPublicationId(), lease, pointer.getBlobUri(), leaseBytes);
          if (lease.getExpiresAtEpochMs() > now && lease.hasInProgressReuseManifestRef()) {
            active.put(lease.getPublicationId(), lease);
          }
        } catch (RuntimeException | com.google.protobuf.InvalidProtocolBufferException error) {
          unreadable++;
          if (firstReadFailure == null) {
            firstReadFailure = error;
          }
        }
        if (scanned >= LEASE_SCAN_LIMIT) {
          break;
        }
      }
      pages++;
      String nextToken = next.toString();
      if (!nextToken.isBlank() && !seenTokens.add(nextToken)) {
        LOG.warnf(
            "stopped Owner reuse candidate discovery for publication %s after pointer pagination repeated token %s",
            publicationId, nextToken);
        token = "";
      } else {
        token = nextToken;
      }
    } while (!token.isBlank() && scanned < LEASE_SCAN_LIMIT && pages < LEASE_SCAN_PAGE_LIMIT);
    if (!token.isBlank() && pages >= LEASE_SCAN_PAGE_LIMIT) {
      LOG.warnf(
          "stopped Owner reuse candidate discovery for publication %s after %d pointer pages",
          publicationId, pages);
    }
    boolean truncated = !token.isBlank() && scanned >= LEASE_SCAN_LIMIT;
    if (unreadable > 0) {
      LOG.warnf(
          "ignored %d unreadable Owner reuse candidate lease(s) while beginning publication %s; first failure: %s",
          unreadable, publicationId, firstReadFailure.getMessage());
    }
    if (!active.containsKey(publicationId)) {
      String ownKey =
          Keys.tableOwnerReuseLeasePointer(tableId.getAccountId(), tableId.getId(), publicationId);
      Pointer ownPointer = pointers.get(ownKey).orElse(null);
      if (ownPointer != null) {
        try {
          byte[] leaseBytes = blobs.get(ownPointer.getBlobUri());
          if (leaseBytes == null) {
            throw new BaseResourceRepository.CorruptionException(
                "Owner reuse lease object is missing", null);
          }
          OwnerPublicationLease lease = OwnerPublicationLease.parseFrom(leaseBytes);
          validateLease(tableId, publicationId, lease, ownPointer.getBlobUri(), leaseBytes);
          if (lease.getExpiresAtEpochMs() > now && lease.hasInProgressReuseManifestRef()) {
            active.put(publicationId, lease);
          }
        } catch (RuntimeException | com.google.protobuf.InvalidProtocolBufferException error) {
          LOG.warnf(
              error,
              "could not discover the current Owner reuse lease %s while beginning publication",
              ownKey);
        }
      }
    }
    List<OwnerPublicationLease> selected =
        active.values().stream()
            .sorted(
                Comparator.comparingLong(
                        OwnerPublicationLease::getInProgressReuseManifestUpdatedAtEpochMs)
                    .reversed())
            .limit(ACTIVE_REUSE_MANIFEST_LIMIT)
            .collect(java.util.stream.Collectors.toCollection(ArrayList::new));
    OwnerPublicationLease own = active.get(publicationId);
    if (own != null
        && selected.stream().noneMatch(lease -> publicationId.equals(lease.getPublicationId()))) {
      selected.set(selected.size() - 1, own);
      selected.sort(
          Comparator.comparingLong(
                  OwnerPublicationLease::getInProgressReuseManifestUpdatedAtEpochMs)
              .reversed());
    }
    LOG.infof(
        "Owner reuse candidate discovery table=%s publication=%s leases_scanned=%d live_with_manifest=%d returned=%d truncated=%s",
        tableId.getId(), publicationId, scanned, active.size(), selected.size(), truncated);
    return selected.stream().map(OwnerPublicationLease::getInProgressReuseManifestRef).toList();
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
        || (lease.hasInProgressReuseManifestRef()
            && (lease.getInProgressReuseManifestUpdatedAtEpochMs() <= 0L
                || !isValidPartialManifest(tableId, lease.getInProgressReuseManifestRef())
                || !lease
                    .getProtectedCaptureManifestPrefixesList()
                    .contains(
                        partialManifestPrefix(tableId, lease.getInProgressReuseManifestRef()))))
        || !new HashSet<>(lease.getProtectedCaptureManifestPrefixesList())
            .containsAll(lease.getDiscoveredCaptureManifestPrefixesList())
        || (!lease.getReuseSourceCaptureManifestUri().isBlank()
            && !lease
                .getProtectedCaptureManifestUrisList()
                .contains(lease.getReuseSourceCaptureManifestUri()))
        || !Keys.tableReusableArtifactBlobPrefix(tableId.getAccountId(), tableId.getId())
            .equals(lease.getReusableNamespacePrefix())
        || !expectedUri.equals(leaseUri)) {
      throw new BaseResourceRepository.CorruptionException("invalid Owner reuse lease", null);
    }
  }

  private static String partialManifestPrefix(
      ResourceId tableId, SnapshotReuseManifestRef manifest) {
    return Keys.snapshotIndexArtifactCaptureManifestBlobPrefixForUri(
            tableId.getAccountId(), tableId.getId(), manifest.getUri())
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "canonical partial Owner reuse manifest URI is required"));
  }

  private static boolean isValidPartialManifest(
      ResourceId tableId, SnapshotReuseManifestRef manifest) {
    return manifest.getFormatVersion() == 1
        && manifest.getKind() == SnapshotReuseManifestKind.SRMK_OWNER_V2_PARTIAL
        && manifest.getPayloadBytes() > 0L
        && manifest.getPayloadSha256().size() == 32
        && manifest.getStatsGenerationManifestUri().isBlank()
        && Keys.snapshotIndexArtifactCaptureManifestBlobPrefixForUri(
                tableId.getAccountId(), tableId.getId(), manifest.getUri())
            .isPresent();
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
