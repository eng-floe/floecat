/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.statistics.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import ai.floedb.floecat.catalog.rpc.OwnerPublicationLease;
import ai.floedb.floecat.catalog.rpc.SnapshotReuseManifestKind;
import ai.floedb.floecat.catalog.rpc.SnapshotReuseManifestRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.util.TableBlobReachabilityGuard;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.types.Hashing;
import com.google.protobuf.ByteString;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OwnerReuseLeaseRepositoryTest {
  private static final ResourceId TABLE =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("table")
          .setKind(ResourceKind.RK_TABLE)
          .build();
  private static final String PUBLICATION = "owner-generation";
  private static final String CAPTURE_PREFIX = capturePrefix(42L);

  private InMemoryPointerStore pointers;
  private InMemoryBlobStore blobs;
  private OwnerReuseLeaseRepository repository;

  @BeforeEach
  void setUp() {
    pointers = new InMemoryPointerStore();
    blobs = new InMemoryBlobStore();
    repository = new OwnerReuseLeaseRepository();
    repository.pointers = pointers;
    repository.blobs = blobs;
    repository.reachability = new TableBlobReachabilityGuard();
  }

  @Test
  void acquireProtectsTheNamespaceAndNeverRegressesAnExistingManifest() throws Exception {
    repository.acquire(TABLE, PUBLICATION, CAPTURE_PREFIX, null);

    OwnerPublicationLease initial = currentLease();
    assertThat(initial.getReusableNamespacePrefix())
        .isEqualTo(Keys.tableReusableArtifactBlobPrefix("acct", "table"));
    assertThat(initial.getExpiresAtEpochMs()).isGreaterThan(System.currentTimeMillis());
    assertThat(initial.getProtectedCaptureManifestUrisList()).isEmpty();
    assertThat(initial.getProtectedCaptureManifestPrefixesList()).containsExactly(CAPTURE_PREFIX);

    var source =
        SnapshotReuseManifestRef.newBuilder()
            .setFormatVersion(1)
            .setKind(SnapshotReuseManifestKind.SRMK_OWNER_V2)
            .setUri("/capture/source.pb")
            .setPayloadBytes(10)
            .setPayloadSha256(ByteString.copyFrom(new byte[32]))
            .build();
    repository.acquire(TABLE, PUBLICATION, CAPTURE_PREFIX, source);

    assertThat(currentLease().getProtectedCaptureManifestUrisList())
        .containsExactly("/capture/source.pb");

    repository.acquire(TABLE, PUBLICATION, CAPTURE_PREFIX, null);

    assertThat(currentLease().getProtectedCaptureManifestUrisList())
        .containsExactly("/capture/source.pb");

    var replacement = source.toBuilder().setUri("/capture/replacement.pb").build();
    repository.acquire(TABLE, PUBLICATION, CAPTURE_PREFIX, replacement);

    assertThat(currentLease().getProtectedCaptureManifestUrisList())
        .containsExactly("/capture/replacement.pb");
    assertThat(currentLease().getReuseSourceCaptureManifestUri())
        .isEqualTo("/capture/replacement.pb");
  }

  @Test
  void acquirePublishesLeaseBeforeInitializingGeneration() throws Exception {
    var initialized = new java.util.concurrent.atomic.AtomicBoolean();

    repository.acquire(
        TABLE,
        PUBLICATION,
        CAPTURE_PREFIX,
        null,
        () -> {
          assertThat(pointers.get(Keys.tableOwnerReuseLeasePointer("acct", "table", PUBLICATION)))
              .isPresent();
          initialized.set(true);
        });

    assertThat(initialized).isTrue();
    assertThat(currentLease().getPublicationId()).isEqualTo(PUBLICATION);
  }

  @Test
  void acquireReturnsNewestLivePartialManifestsAndPinsThem() throws Exception {
    var now = new AtomicLong(1_000L);
    repository.nowMillis = now::get;
    repository.leaseTtlMs = 1_000L;
    repository.acquire(TABLE, "publication-a", capturePrefix(40L), null);
    var older = partialManifest(40L, (byte) 1);
    repository.publishInProgressManifest(TABLE, "publication-a", older);

    now.set(1_001L);
    repository.acquire(TABLE, "publication-b", capturePrefix(41L), null);
    var newer = partialManifest(41L, (byte) 2);
    repository.publishInProgressManifest(TABLE, "publication-b", newer);

    now.set(1_002L);
    var acquired =
        repository.acquireWithCandidates(TABLE, "publication-c", CAPTURE_PREFIX, null, () -> {});

    assertThat(acquired.inProgressManifests()).containsExactly(newer, older);
    assertThat(currentLease("publication-c").getProtectedCaptureManifestUrisList()).isEmpty();
    assertThat(currentLease("publication-c").getProtectedCaptureManifestPrefixesList())
        .containsExactlyInAnyOrder(CAPTURE_PREFIX, capturePrefix(40L), capturePrefix(41L));
  }

  @Test
  void acquireReturnsOwnPartialManifestAfterRestart() throws Exception {
    repository.acquire(TABLE, PUBLICATION, CAPTURE_PREFIX, null);
    var partial = partialManifest(42L, (byte) 3);
    repository.publishInProgressManifest(TABLE, PUBLICATION, partial);

    var reacquired =
        repository.acquireWithCandidates(TABLE, PUBLICATION, CAPTURE_PREFIX, null, () -> {});

    assertThat(reacquired.inProgressManifests()).containsExactly(partial);
    assertThat(currentLease().getInProgressReuseManifestRef()).isEqualTo(partial);
    assertThat(currentLease().getProtectedCaptureManifestUrisList()).isEmpty();
    assertThat(currentLease().getProtectedCaptureManifestPrefixesList())
        .containsExactly(CAPTURE_PREFIX);
    assertThat(currentLease().getDiscoveredCaptureManifestPrefixesList()).isEmpty();
  }

  @Test
  void acquireKeepsOwnPartialWhenNewerCandidatesFillTheLimit() {
    var now = new AtomicLong(1_000L);
    repository.nowMillis = now::get;
    repository.leaseTtlMs = 10_000L;
    repository.acquire(TABLE, PUBLICATION, CAPTURE_PREFIX, null);
    var own = partialManifest(42L, (byte) 8);
    repository.publishInProgressManifest(TABLE, PUBLICATION, own);
    for (int index = 0; index < 16; index++) {
      now.incrementAndGet();
      String sibling = "publication-" + index;
      repository.acquire(TABLE, sibling, CAPTURE_PREFIX, null);
      repository.publishInProgressManifest(
          TABLE, sibling, partialManifest(42L, (byte) (index + 16)));
    }

    now.incrementAndGet();
    var reacquired =
        repository.acquireWithCandidates(TABLE, PUBLICATION, CAPTURE_PREFIX, null, () -> {});

    assertThat(reacquired.inProgressManifests()).hasSize(16).contains(own);
  }

  @Test
  void expiredPartialManifestIsNotReturned() {
    var now = new AtomicLong(1_000L);
    repository.nowMillis = now::get;
    repository.leaseTtlMs = 100L;
    repository.acquire(TABLE, "publication-a", capturePrefix(40L), null);
    repository.publishInProgressManifest(TABLE, "publication-a", partialManifest(40L, (byte) 4));
    now.set(1_100L);

    var acquired =
        repository.acquireWithCandidates(TABLE, "publication-b", CAPTURE_PREFIX, null, () -> {});

    assertThat(acquired.inProgressManifests()).isEmpty();
  }

  @Test
  void corruptSiblingLeaseDoesNotBlockBegin() throws Exception {
    repository.acquire(TABLE, "publication-a", capturePrefix(40L), null);
    repository.publishInProgressManifest(TABLE, "publication-a", partialManifest(40L, (byte) 5));
    var pointer =
        pointers
            .get(Keys.tableOwnerReuseLeasePointer("acct", "table", "publication-a"))
            .orElseThrow();
    blobs.delete(pointer.getBlobUri());

    var acquired =
        repository.acquireWithCandidates(TABLE, "publication-b", CAPTURE_PREFIX, null, () -> {});

    assertThat(acquired.inProgressManifests()).isEmpty();
    assertThat(currentLease("publication-b").getPublicationId()).isEqualTo("publication-b");
  }

  @Test
  void repeatedBeginReplacesStaleCandidatePins() throws Exception {
    repository.acquire(TABLE, "old-producer", capturePrefix(40L), null);
    var oldCandidate = partialManifest(40L, (byte) 6);
    repository.publishInProgressManifest(TABLE, "old-producer", oldCandidate);
    repository.acquireWithCandidates(TABLE, PUBLICATION, CAPTURE_PREFIX, null, () -> {});
    assertThat(currentLease().getProtectedCaptureManifestPrefixesList())
        .contains(capturePrefix(40L));

    repository.release(TABLE, "old-producer");
    repository.acquire(TABLE, "new-producer", capturePrefix(41L), null);
    var newCandidate = partialManifest(41L, (byte) 7);
    repository.publishInProgressManifest(TABLE, "new-producer", newCandidate);
    repository.acquireWithCandidates(TABLE, PUBLICATION, CAPTURE_PREFIX, null, () -> {});

    assertThat(currentLease().getProtectedCaptureManifestPrefixesList())
        .contains(CAPTURE_PREFIX, capturePrefix(41L))
        .doesNotContain(capturePrefix(40L));
    assertThat(currentLease().getDiscoveredCaptureManifestPrefixesList())
        .containsExactly(capturePrefix(41L));
  }

  @Test
  void repeatedBeginReturnsSupersedingPartialFromTheSameProducer() throws Exception {
    repository.acquire(TABLE, "producer", capturePrefix(40L), null);
    var oldCandidate = partialManifest(40L, (byte) 6);
    repository.publishInProgressManifest(TABLE, "producer", oldCandidate);

    var first =
        repository.acquireWithCandidates(TABLE, PUBLICATION, CAPTURE_PREFIX, null, () -> {});

    var newCandidate = partialManifest(40L, (byte) 7);
    repository.publishInProgressManifest(TABLE, "producer", newCandidate);
    var second =
        repository.acquireWithCandidates(TABLE, PUBLICATION, CAPTURE_PREFIX, null, () -> {});

    assertThat(first.inProgressManifests()).contains(oldCandidate);
    assertThat(second.inProgressManifests()).contains(newCandidate).doesNotContain(oldCandidate);
    assertThat(currentLease().getProtectedCaptureManifestPrefixesList())
        .containsExactlyInAnyOrder(CAPTURE_PREFIX, capturePrefix(40L));
  }

  @Test
  void malformedSiblingPartialIsIgnoredDuringCandidateDiscovery() throws Exception {
    String producer = "malformed-producer";
    repository.acquire(TABLE, producer, capturePrefix(40L), null);
    String key = Keys.tableOwnerReuseLeasePointer("acct", "table", producer);
    var pointer = pointers.get(key).orElseThrow();
    var malformed =
        OwnerPublicationLease.parseFrom(blobs.get(pointer.getBlobUri())).toBuilder()
            .setInProgressReuseManifestRef(partialManifest(40L, (byte) 6).toBuilder().setUri("bad"))
            .setInProgressReuseManifestUpdatedAtEpochMs(System.currentTimeMillis())
            .build();
    byte[] malformedBytes = malformed.toByteArray();
    String malformedUri =
        Keys.ownerPublicationLeaseBlobUri(
            "acct", "table", producer, Hashing.sha256Hex(malformedBytes));
    blobs.putImmutable(malformedUri, malformedBytes, "application/x-protobuf");
    assertThat(
            pointers.compareAndSet(
                key,
                pointer.getVersion(),
                PointerReferences.blobPointer(
                    key, malformedUri, pointer.getVersion() + 1L, malformedBytes.length)))
        .isTrue();

    var acquired =
        repository.acquireWithCandidates(TABLE, PUBLICATION, CAPTURE_PREFIX, null, () -> {});

    assertThat(acquired.inProgressManifests()).isEmpty();
  }

  @Test
  void repeatedBeginPreservesSourceWhileReplacingDiscoveredCandidates() throws Exception {
    var source =
        SnapshotReuseManifestRef.newBuilder()
            .setFormatVersion(1)
            .setKind(SnapshotReuseManifestKind.SRMK_OWNER_V2)
            .setUri("/capture/source.pb")
            .setPayloadBytes(10)
            .setPayloadSha256(ByteString.copyFrom(new byte[32]))
            .build();
    repository.acquire(TABLE, "old-producer", capturePrefix(40L), null);
    var oldCandidate = partialManifest(40L, (byte) 6);
    repository.publishInProgressManifest(TABLE, "old-producer", oldCandidate);
    repository.acquireWithCandidates(TABLE, PUBLICATION, CAPTURE_PREFIX, source, () -> {});

    repository.release(TABLE, "old-producer");
    repository.acquire(TABLE, "new-producer", capturePrefix(41L), null);
    var newCandidate = partialManifest(41L, (byte) 7);
    repository.publishInProgressManifest(TABLE, "new-producer", newCandidate);
    repository.acquireWithCandidates(TABLE, PUBLICATION, CAPTURE_PREFIX, null, () -> {});

    assertThat(currentLease().getProtectedCaptureManifestUrisList())
        .containsExactly(source.getUri());
    assertThat(currentLease().getProtectedCaptureManifestPrefixesList())
        .contains(CAPTURE_PREFIX, capturePrefix(41L))
        .doesNotContain(capturePrefix(40L));
  }

  @Test
  void candidateDiscoveryStopsWhenPaginationTokenRepeats() {
    var repeatingPointers = spy(new InMemoryPointerStore());
    doAnswer(
            invocation -> {
              invocation.getArgument(3, StringBuilder.class).append("repeated-token");
              return java.util.List.of();
            })
        .when(repeatingPointers)
        .listPointersByPrefix(anyString(), anyInt(), anyString(), any(StringBuilder.class));
    repository.pointers = repeatingPointers;

    repository.acquireWithCandidates(TABLE, PUBLICATION, CAPTURE_PREFIX, null, () -> {});

    verify(repeatingPointers, times(2))
        .listPointersByPrefix(anyString(), anyInt(), anyString(), any(StringBuilder.class));
  }

  @Test
  void progressIsDurableMonotonicAndBoundToOneCaptureManifest() {
    var initial = OwnerReuseLeaseRepository.RegistrationProgress.initial();
    var complete = new OwnerReuseLeaseRepository.RegistrationProgress(1L, 0L, 0L, 2L, 3L, 4L, 5L);
    assertThat(repository.progress(TABLE, PUBLICATION, "aa")).isEqualTo(initial);

    repository.advanceProgress(TABLE, PUBLICATION, "aa", initial, complete);
    repository.advanceProgress(TABLE, PUBLICATION, "aa", initial, complete);

    assertThat(repository.progress(TABLE, PUBLICATION, "aa")).isEqualTo(complete);
    assertThatThrownBy(() -> repository.progress(TABLE, PUBLICATION, "bb"))
        .isInstanceOf(IllegalArgumentException.class);
    var later = new OwnerReuseLeaseRepository.RegistrationProgress(2L, 0L, 0L, 3L, 4L, 5L, 6L);
    assertThatThrownBy(() -> repository.advanceProgress(TABLE, PUBLICATION, "aa", initial, later))
        .isInstanceOf(RuntimeException.class);
  }

  @Test
  void releaseRemovesLeaseAndProgress() {
    repository.acquire(TABLE, PUBLICATION, CAPTURE_PREFIX, null);
    var initial = OwnerReuseLeaseRepository.RegistrationProgress.initial();
    var complete = new OwnerReuseLeaseRepository.RegistrationProgress(1L, 0L, 0L, 2L, 3L, 4L, 5L);
    repository.advanceProgress(TABLE, PUBLICATION, "aa", initial, complete);

    repository.release(TABLE, PUBLICATION);

    assertThat(pointers.get(Keys.tableOwnerReuseLeasePointer("acct", "table", PUBLICATION)))
        .isEmpty();
    assertThat(repository.progress(TABLE, PUBLICATION, "aa"))
        .isEqualTo(OwnerReuseLeaseRepository.RegistrationProgress.initial());
  }

  @Test
  void expiredLeaseCannotBeRenewedButBeginCanReclaimIt() throws Exception {
    var now = new AtomicLong(1_000L);
    repository.nowMillis = now::get;
    repository.leaseTtlMs = 100L;
    repository.acquire(TABLE, PUBLICATION, CAPTURE_PREFIX, null);
    repository.advanceProgress(
        TABLE,
        PUBLICATION,
        "aa",
        OwnerReuseLeaseRepository.RegistrationProgress.initial(),
        new OwnerReuseLeaseRepository.RegistrationProgress(1L, 0L, 0L, 1L, 0L, 0L, 1L));
    now.set(1_100L);

    assertThatThrownBy(() -> repository.renew(TABLE, PUBLICATION, null))
        .isInstanceOf(OwnerReuseLeaseRepository.LeaseContinuityException.class);
    repository.acquire(TABLE, PUBLICATION, CAPTURE_PREFIX, null);

    assertThat(repository.progress(TABLE, PUBLICATION, "bb"))
        .isEqualTo(OwnerReuseLeaseRepository.RegistrationProgress.initial());
    assertThat(currentLease().getExpiresAtEpochMs()).isEqualTo(1_200L);
  }

  @Test
  void completeRenewalRequiresAnExistingLease() {
    assertThatThrownBy(() -> repository.renew(TABLE, PUBLICATION, null))
        .isInstanceOf(OwnerReuseLeaseRepository.LeaseContinuityException.class);
  }

  private OwnerPublicationLease currentLease() throws Exception {
    return currentLease(PUBLICATION);
  }

  private OwnerPublicationLease currentLease(String publicationId) throws Exception {
    var pointer =
        pointers
            .get(Keys.tableOwnerReuseLeasePointer("acct", "table", publicationId))
            .orElseThrow();
    return OwnerPublicationLease.parseFrom(blobs.get(pointer.getBlobUri()));
  }

  private static String capturePrefix(long snapshotId) {
    return Keys.snapshotIndexArtifactCaptureManifestBlobPrefix("acct", "table", snapshotId);
  }

  private static SnapshotReuseManifestRef partialManifest(long snapshotId, byte fill) {
    byte[] digest = new byte[32];
    java.util.Arrays.fill(digest, fill);
    return SnapshotReuseManifestRef.newBuilder()
        .setFormatVersion(1)
        .setKind(SnapshotReuseManifestKind.SRMK_OWNER_V2_PARTIAL)
        .setUri(capturePrefix(snapshotId) + java.util.HexFormat.of().formatHex(digest) + ".pb")
        .setPayloadBytes(10)
        .setPayloadSha256(ByteString.copyFrom(digest))
        .build();
  }
}
