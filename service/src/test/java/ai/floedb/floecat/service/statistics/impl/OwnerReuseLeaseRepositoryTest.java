/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.statistics.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.OwnerPublicationLease;
import ai.floedb.floecat.catalog.rpc.SnapshotReuseManifestKind;
import ai.floedb.floecat.catalog.rpc.SnapshotReuseManifestRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.TableBlobReachabilityGuard;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
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
  private static final String CAPTURE_PREFIX =
      Keys.snapshotIndexArtifactCaptureManifestBlobPrefix("acct", "table", 42L);

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
    repository.acquire(TABLE, "publication-a", CAPTURE_PREFIX, null);
    var older = partialManifest("/capture/older.pb", (byte) 1);
    repository.publishInProgressManifest(TABLE, "publication-a", older);

    now.set(1_001L);
    repository.acquire(TABLE, "publication-b", CAPTURE_PREFIX, null);
    var newer = partialManifest("/capture/newer.pb", (byte) 2);
    repository.publishInProgressManifest(TABLE, "publication-b", newer);

    now.set(1_002L);
    var acquired =
        repository.acquireWithCandidates(TABLE, "publication-c", CAPTURE_PREFIX, null, () -> {});

    assertThat(acquired.inProgressManifests()).containsExactly(newer, older);
    assertThat(currentLease("publication-c").getProtectedCaptureManifestUrisList())
        .containsExactlyInAnyOrder(newer.getUri(), older.getUri());
  }

  @Test
  void acquireReturnsOwnPartialManifestAfterRestart() throws Exception {
    repository.acquire(TABLE, PUBLICATION, CAPTURE_PREFIX, null);
    var partial = partialManifest("/capture/current.pb", (byte) 3);
    repository.publishInProgressManifest(TABLE, PUBLICATION, partial);

    var reacquired =
        repository.acquireWithCandidates(TABLE, PUBLICATION, CAPTURE_PREFIX, null, () -> {});

    assertThat(reacquired.inProgressManifests()).containsExactly(partial);
    assertThat(currentLease().getInProgressReuseManifestRef()).isEqualTo(partial);
  }

  @Test
  void acquireKeepsOwnPartialWhenNewerCandidatesFillTheLimit() {
    var now = new AtomicLong(1_000L);
    repository.nowMillis = now::get;
    repository.leaseTtlMs = 10_000L;
    repository.acquire(TABLE, PUBLICATION, CAPTURE_PREFIX, null);
    var own = partialManifest("/capture/own.pb", (byte) 8);
    repository.publishInProgressManifest(TABLE, PUBLICATION, own);
    for (int index = 0; index < 16; index++) {
      now.incrementAndGet();
      String sibling = "publication-" + index;
      repository.acquire(TABLE, sibling, CAPTURE_PREFIX, null);
      repository.publishInProgressManifest(
          TABLE,
          sibling,
          partialManifest("/capture/sibling-" + index + ".pb", (byte) (index + 16)));
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
    repository.acquire(TABLE, "publication-a", CAPTURE_PREFIX, null);
    repository.publishInProgressManifest(
        TABLE, "publication-a", partialManifest("/capture/expired.pb", (byte) 4));
    now.set(1_100L);

    var acquired =
        repository.acquireWithCandidates(TABLE, "publication-b", CAPTURE_PREFIX, null, () -> {});

    assertThat(acquired.inProgressManifests()).isEmpty();
  }

  @Test
  void corruptSiblingLeaseDoesNotBlockBegin() throws Exception {
    repository.acquire(TABLE, "publication-a", CAPTURE_PREFIX, null);
    repository.publishInProgressManifest(
        TABLE, "publication-a", partialManifest("/capture/corrupt.pb", (byte) 5));
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
    repository.acquire(TABLE, "producer", CAPTURE_PREFIX, null);
    var oldCandidate = partialManifest("/capture/old.pb", (byte) 6);
    repository.publishInProgressManifest(TABLE, "producer", oldCandidate);
    repository.acquireWithCandidates(TABLE, PUBLICATION, CAPTURE_PREFIX, null, () -> {});
    assertThat(currentLease().getProtectedCaptureManifestUrisList())
        .contains(oldCandidate.getUri());

    var newCandidate = partialManifest("/capture/new.pb", (byte) 7);
    repository.publishInProgressManifest(TABLE, "producer", newCandidate);
    repository.acquireWithCandidates(TABLE, PUBLICATION, CAPTURE_PREFIX, null, () -> {});

    assertThat(currentLease().getProtectedCaptureManifestUrisList())
        .contains(newCandidate.getUri())
        .doesNotContain(oldCandidate.getUri());
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

  private static SnapshotReuseManifestRef partialManifest(String uri, byte fill) {
    byte[] digest = new byte[32];
    java.util.Arrays.fill(digest, fill);
    return SnapshotReuseManifestRef.newBuilder()
        .setFormatVersion(1)
        .setKind(SnapshotReuseManifestKind.SRMK_OWNER_V2_PARTIAL)
        .setUri(uri)
        .setPayloadBytes(10)
        .setPayloadSha256(ByteString.copyFrom(digest))
        .build();
  }
}
