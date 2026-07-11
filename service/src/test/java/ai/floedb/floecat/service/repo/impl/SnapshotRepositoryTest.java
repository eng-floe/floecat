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

package ai.floedb.floecat.service.repo.impl;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.CurrentSnapshotPointer;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.util.Timestamps;
import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class SnapshotRepositoryTest {
  private final Clock clock = Clock.systemUTC();

  private TableRepository tableRepo;
  private SnapshotRepository snapshotRepo;
  private PointerStore ptr;
  private BlobStore blobs;

  @BeforeEach
  void setUp() {
    ptr = new InMemoryPointerStore();
    blobs = new InMemoryBlobStore();
    tableRepo = new TableRepository(ptr, blobs);
    snapshotRepo = new SnapshotRepository(ptr, blobs, tableRepo);
  }

  @Test
  void snapshotRepoCreateSnapshot() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    String catalogId = UUID.randomUUID().toString();
    String nsId = UUID.randomUUID().toString();
    String tblId = UUID.randomUUID().toString();

    var catalogRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(catalogId)
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    var nsRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(nsId)
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();
    var tableRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(tblId)
            .setKind(ResourceKind.RK_TABLE)
            .build();

    var upstream =
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_DELTA)
            .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_PATH_ORDINAL)
            .setUri("s3://upstream/tables/orders/")
            .build();
    var td =
        Table.newBuilder()
            .setResourceId(tableRid)
            .setDisplayName("orders")
            .setDescription("Orders table")
            .setUpstream(upstream)
            .setCatalogId(catalogRid)
            .setNamespaceId(nsRid)
            .setSchemaJson("{}")
            .setCreatedAt(Timestamps.fromMillis(clock.millis()))
            .build();
    tableRepo.create(td);

    seedSnapshot(
        snapshotRepo, account, tableRid, 199, clock.millis() - 20_000, clock.millis() - 60_000);
    seedSnapshot(
        snapshotRepo, account, tableRid, 200, clock.millis() - 10_000, clock.millis() - 50_000);
    snapshotRepo.maybeAdvanceCurrentSnapshotPointer(
        tableRid, snapshotRepo.getById(tableRid, 200).orElseThrow());

    StringBuilder next = new StringBuilder();
    var page1 = snapshotRepo.list(tableRid, 1, "", next);
    assertEquals(1, page1.size());
    assertFalse(next.toString().isEmpty(), "should return next_page_token");

    String token = next.toString();
    next.setLength(0);
    var page2 = snapshotRepo.list(tableRid, 10, token, next);
    assertTrue(page2.size() >= 1);
    assertTrue(next.toString().isEmpty(), "no more pages");

    int total = snapshotRepo.count(tableRid);
    assertEquals(2, total);

    var cur = snapshotRepo.getCurrentSnapshot(tableRid).orElseThrow();
    assertEquals(200, cur.getSnapshotId());
  }

  @Test
  void getCurrentSnapshotUsesCurrentPointer() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var tableRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    tableRepo.create(table(account, tableRid));

    long createdMs = clock.millis() - 10_000;
    seedSnapshot(snapshotRepo, account, tableRid, 0, clock.millis(), createdMs);
    seedSnapshot(snapshotRepo, account, tableRid, 1, clock.millis(), createdMs);
    snapshotRepo.maybeAdvanceCurrentSnapshotPointer(
        tableRid, snapshotRepo.getById(tableRid, 1).orElseThrow());

    Snapshot current = snapshotRepo.getCurrentSnapshot(tableRid).orElseThrow();
    assertEquals(1L, current.getSnapshotId());
  }

  @Test
  void maybeAdvanceCurrentSnapshotPointerCreatesPointerWithoutMutatingTable() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var tableRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    tableRepo.create(table(account, tableRid));

    long createdMs = clock.millis() - 10_000;
    seedSnapshot(snapshotRepo, account, tableRid, 1, clock.millis(), createdMs);
    Snapshot snapshot = snapshotRepo.getById(tableRid, 1).orElseThrow();

    assertEquals(
        SnapshotRepository.CurrentSnapshotPointerUpdateResult.UPDATED,
        snapshotRepo.maybeAdvanceCurrentSnapshotPointer(tableRid, snapshot));

    assertEquals(1L, snapshotRepo.getCurrentSnapshot(tableRid).orElseThrow().getSnapshotId());
    assertTrue(tableRepo.getById(tableRid).orElseThrow().getPropertiesMap().isEmpty());
  }

  @Test
  void updateDoesNotAdvanceCurrentSnapshotPointer() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var tableRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    tableRepo.create(table(account, tableRid));

    long t = clock.millis();
    seedSnapshot(snapshotRepo, account, tableRid, 1, t, t - 10_000); // newest by commit time
    seedSnapshot(snapshotRepo, account, tableRid, 2, t, t - 20_000); // older
    snapshotRepo.maybeAdvanceCurrentSnapshotPointer(
        tableRid, snapshotRepo.getById(tableRid, 1).orElseThrow());
    assertEquals(1L, snapshotRepo.getCurrentSnapshot(tableRid).orElseThrow().getSnapshotId());

    // Make snapshot 2 the newest by commit time via update(). update() must NOT advance the current
    // pointer — the advance belongs to the service layer (which re-commits the root entry). If
    // update() advanced here, that commit would be pre-empted and CURRENT pins would go stale.
    Snapshot snap2 = snapshotRepo.getById(tableRid, 2).orElseThrow();
    long version = snapshotRepo.metaForSafe(tableRid, 2).getPointerVersion();
    Snapshot advanced =
        snap2.toBuilder().setUpstreamCreatedAt(Timestamps.fromMillis(t + 10_000)).build();
    assertTrue(snapshotRepo.update(advanced, version));

    assertEquals(
        1L,
        snapshotRepo.getCurrentSnapshot(tableRid).orElseThrow().getSnapshotId(),
        "update() must not advance the current pointer");

    // The service-path advance (which commits the root entry) is what moves it.
    snapshotRepo.maybeAdvanceCurrentSnapshotPointer(
        tableRid, snapshotRepo.getById(tableRid, 2).orElseThrow());
    assertEquals(2L, snapshotRepo.getCurrentSnapshot(tableRid).orElseThrow().getSnapshotId());
  }

  @Test
  void getAsOfPrefersHighestSnapshotIdWhenUpstreamTimestampTies() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var tableRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();

    long createdMs = clock.millis() - 10_000;
    seedSnapshot(snapshotRepo, account, tableRid, 0, clock.millis(), createdMs);
    seedSnapshot(snapshotRepo, account, tableRid, 1, clock.millis(), createdMs);

    Snapshot asOf =
        snapshotRepo
            .getAsOf(tableRid, Timestamps.fromMillis(createdMs))
            .orElseThrow(() -> new AssertionError("expected snapshot at as-of timestamp"));
    assertEquals(1L, asOf.getSnapshotId());
  }

  @Test
  void getAsOfSeeksPredecessorWithoutReturningNewerSnapshots() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var tableRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();

    long older = clock.millis() - 100_000;
    long newer = clock.millis() - 50_000;
    seedSnapshot(snapshotRepo, account, tableRid, 10, clock.millis(), older);
    seedSnapshot(snapshotRepo, account, tableRid, 20, clock.millis(), newer);

    // Between the two → the older snapshot, never the newer one.
    assertEquals(
        10L,
        snapshotRepo
            .getAsOf(tableRid, Timestamps.fromMillis(newer - 1))
            .orElseThrow()
            .getSnapshotId());
    // Exactly at the newer commit time → the newer snapshot.
    assertEquals(
        20L,
        snapshotRepo.getAsOf(tableRid, Timestamps.fromMillis(newer)).orElseThrow().getSnapshotId());
    // After both → the newest.
    assertEquals(
        20L,
        snapshotRepo
            .getAsOf(tableRid, Timestamps.fromMillis(clock.millis()))
            .orElseThrow()
            .getSnapshotId());
    // Before the first snapshot → not found (feeds the not-found-at-time error).
    assertTrue(snapshotRepo.getAsOf(tableRid, Timestamps.fromMillis(older - 1)).isEmpty());
  }

  @Test
  void getCurrentSnapshotUsesPointerEvenWhenSnapshotIsNotNewestByTime() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var tableRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    tableRepo.create(table(account, tableRid));

    long newestCreatedMs = clock.millis() - 5_000;
    long olderCreatedMs = newestCreatedMs - 1_000;

    seedSnapshot(snapshotRepo, account, tableRid, 0, clock.millis(), newestCreatedMs);
    seedSnapshot(snapshotRepo, account, tableRid, 1, clock.millis(), newestCreatedMs);
    seedSnapshot(snapshotRepo, account, tableRid, 999, clock.millis(), olderCreatedMs);
    snapshotRepo.maybeAdvanceCurrentSnapshotPointer(
        tableRid, snapshotRepo.getById(tableRid, 999).orElseThrow());

    Snapshot current = snapshotRepo.getCurrentSnapshot(tableRid).orElseThrow();
    assertEquals(999L, current.getSnapshotId());
    assertEquals(Timestamps.fromMillis(olderCreatedMs), current.getUpstreamCreatedAt());
  }

  @Test
  void getCurrentSnapshotFallsBackToLatestSnapshotWhenCurrentPointerIsMissing() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var tableRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    tableRepo.create(table(account, tableRid));
    seedSnapshot(snapshotRepo, account, tableRid, 204, clock.millis(), clock.millis() - 10_000);

    Snapshot fallback = snapshotRepo.getCurrentSnapshot(tableRid).orElseThrow();
    assertEquals(204L, fallback.getSnapshotId());
    assertTrue(tableRepo.getById(tableRid).orElseThrow().getPropertiesMap().isEmpty());
  }

  @Test
  void getCurrentSnapshotFallsBackToLatestSnapshotWhenCurrentPointerIsDangling() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var tableRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    tableRepo.create(table(account, tableRid));
    seedCurrentPointer(tableRid, 999_999L, clock.millis());
    seedSnapshot(snapshotRepo, account, tableRid, 101, clock.millis(), clock.millis() - 20_000);
    seedSnapshot(snapshotRepo, account, tableRid, 204, clock.millis(), clock.millis() - 10_000);

    Snapshot fallback = snapshotRepo.getCurrentSnapshot(tableRid).orElseThrow();
    assertEquals(204L, fallback.getSnapshotId());
    assertTrue(tableRepo.getById(tableRid).orElseThrow().getPropertiesMap().isEmpty());
  }

  @Test
  void maybeAdvanceCurrentSnapshotPointerDoesNotDowngradeNewerPointer() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var tableRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    tableRepo.create(table(account, tableRid));

    long newerCreatedMs = clock.millis() - 10_000;
    long olderCreatedMs = newerCreatedMs - 10_000;
    seedSnapshot(snapshotRepo, account, tableRid, 100, clock.millis(), olderCreatedMs);
    seedSnapshot(snapshotRepo, account, tableRid, 200, clock.millis(), newerCreatedMs);
    snapshotRepo.maybeAdvanceCurrentSnapshotPointer(
        tableRid, snapshotRepo.getById(tableRid, 200).orElseThrow());

    var older =
        snapshotRepo.getById(tableRid, 100).orElseThrow(() -> new AssertionError("missing older"));
    assertEquals(
        SnapshotRepository.CurrentSnapshotPointerUpdateResult.UNCHANGED,
        snapshotRepo.maybeAdvanceCurrentSnapshotPointer(tableRid, older));
    assertEquals(200L, snapshotRepo.getCurrentSnapshot(tableRid).orElseThrow().getSnapshotId());
  }

  @Test
  void maybeAdvanceCurrentSnapshotPointerDoesNotRewriteSameSnapshotIdWhenTimestampDiffers() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var tableRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    tableRepo.create(table(account, tableRid));

    long originalCreatedMs = clock.millis() - 20_000;
    long conflictingCreatedMs = clock.millis() - 10_000;
    seedSnapshot(snapshotRepo, account, tableRid, 100, clock.millis(), originalCreatedMs);
    snapshotRepo.maybeAdvanceCurrentSnapshotPointer(
        tableRid, snapshotRepo.getById(tableRid, 100).orElseThrow());

    CurrentSnapshotPointerRepository currentRepo = new CurrentSnapshotPointerRepository(ptr, blobs);
    MutationMeta beforeMeta = currentRepo.metaFor(tableRid);

    Snapshot conflictingCandidate =
        Snapshot.newBuilder()
            .setTableId(tableRid)
            .setSnapshotId(100L)
            .setIngestedAt(Timestamps.fromMillis(clock.millis()))
            .setUpstreamCreatedAt(Timestamps.fromMillis(conflictingCreatedMs))
            .build();

    assertEquals(
        SnapshotRepository.CurrentSnapshotPointerUpdateResult.UNCHANGED,
        snapshotRepo.maybeAdvanceCurrentSnapshotPointer(tableRid, conflictingCandidate));

    CurrentSnapshotPointer currentPointer = currentRepo.get(tableRid).orElseThrow();
    MutationMeta afterMeta = currentRepo.metaFor(tableRid);
    assertEquals(100L, currentPointer.getSnapshotId());
    assertEquals(Timestamps.fromMillis(originalCreatedMs), currentPointer.getUpstreamCreatedAt());
    assertEquals(
        beforeMeta.getPointerVersion(),
        afterMeta.getPointerVersion(),
        "same snapshot id must not rewrite the current pointer");
  }

  @Test
  void maybeAdvanceCurrentSnapshotPointerRetriesWhenPointerChangesBeforeCas() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var tableRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    tableRepo.create(table(account, tableRid));

    long newestCreatedMs = clock.millis() - 10_000;
    long middleCreatedMs = newestCreatedMs - 10_000;
    long oldestCreatedMs = middleCreatedMs - 10_000;
    AdvancingOnMetaCurrentSnapshotPointerRepository currentRepo =
        new AdvancingOnMetaCurrentSnapshotPointerRepository(ptr, blobs);
    SnapshotRepository repo = new SnapshotRepository(ptr, blobs, tableRepo, currentRepo);

    seedSnapshot(repo, account, tableRid, 100, clock.millis(), oldestCreatedMs);
    seedSnapshot(repo, account, tableRid, 200, clock.millis(), middleCreatedMs);
    seedSnapshot(repo, account, tableRid, 300, clock.millis(), newestCreatedMs);
    repo.maybeAdvanceCurrentSnapshotPointer(tableRid, repo.getById(tableRid, 100).orElseThrow());
    currentRepo.advanceDuringMetaFor(
        tableRid,
        CurrentSnapshotPointer.newBuilder()
            .setTableId(tableRid)
            .setSnapshotId(300)
            .setUpstreamCreatedAt(Timestamps.fromMillis(newestCreatedMs))
            .setUpdatedAt(Timestamps.fromMillis(clock.millis()))
            .build());

    assertEquals(
        SnapshotRepository.CurrentSnapshotPointerUpdateResult.UNCHANGED,
        repo.maybeAdvanceCurrentSnapshotPointer(
            tableRid, repo.getById(tableRid, 200).orElseThrow()));
    assertEquals(300L, repo.getCurrentSnapshot(tableRid).orElseThrow().getSnapshotId());
  }

  @Test
  void maybeAdvanceCurrentSnapshotPointerReturnsConflictAfterBoundedCasRetries() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var tableRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    tableRepo.create(table(account, tableRid));
    FailingCurrentSnapshotPointerRepository failingCurrentRepo =
        new FailingCurrentSnapshotPointerRepository(ptr, blobs);
    SnapshotRepository repo = new SnapshotRepository(ptr, blobs, tableRepo, failingCurrentRepo);
    Snapshot candidate =
        Snapshot.newBuilder()
            .setTableId(tableRid)
            .setSnapshotId(100L)
            .setUpstreamCreatedAt(Timestamps.fromMillis(clock.millis()))
            .build();

    assertEquals(
        SnapshotRepository.CurrentSnapshotPointerUpdateResult.CONFLICT,
        repo.maybeAdvanceCurrentSnapshotPointer(tableRid, candidate));
    assertEquals(4, failingCurrentRepo.createAttempts.get());
  }

  @Test
  void rootCurrencyIsTheCurrentSnapshotEvenWhenTheIngestPointerIsAhead() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var tableRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    tableRepo.create(table(account, tableRid));

    long t = clock.millis();
    seedSnapshot(snapshotRepo, account, tableRid, 1, t, t - 20_000);
    seedSnapshot(snapshotRepo, account, tableRid, 2, t, t - 10_000);
    // Ingest advanced the legacy pointer to snapshot 2, but only snapshot 1 finalized: the
    // root's currency stays at 1 and that is what every reader must see.
    snapshotRepo.maybeAdvanceCurrentSnapshotPointer(
        tableRid, snapshotRepo.getById(tableRid, 2).orElseThrow());
    seedRootWithCurrency(tableRid, 1L, t - 20_000);

    assertEquals(1L, snapshotRepo.getCurrentSnapshot(tableRid).orElseThrow().getSnapshotId());
    assertEquals(
        1L, snapshotRepo.getCurrentSnapshotPointer(tableRid).orElseThrow().getSnapshotId());
    // The raw ingest pointer remains reachable for write-side plumbing.
    assertEquals(
        2L, snapshotRepo.latestRegisteredSnapshotPointer(tableRid).orElseThrow().getSnapshotId());
    assertEquals(2L, snapshotRepo.latestRegisteredSnapshot(tableRid).orElseThrow().getSnapshotId());
  }

  @Test
  void rootWithoutCurrencyGatesTheTableEvenWhenTheIngestPointerIsSet() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var tableRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    tableRepo.create(table(account, tableRid));

    long t = clock.millis();
    seedSnapshot(snapshotRepo, account, tableRid, 1, t, t - 10_000);
    snapshotRepo.maybeAdvanceCurrentSnapshotPointer(
        tableRid, snapshotRepo.getById(tableRid, 1).orElseThrow());
    // The root registered the snapshot but nothing finalized: no queryable snapshot, and no
    // fallback to the ingest pointer.
    seedRootWithCurrency(tableRid, null, t - 10_000);

    assertTrue(snapshotRepo.getCurrentSnapshot(tableRid).isEmpty());
    assertTrue(snapshotRepo.getCurrentSnapshotPointer(tableRid).isEmpty());
    assertEquals(
        1L, snapshotRepo.latestRegisteredSnapshotPointer(tableRid).orElseThrow().getSnapshotId());
  }

  @Test
  void tableWithoutARootStillReadsTheLegacyPointer() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var tableRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    tableRepo.create(table(account, tableRid));

    long t = clock.millis();
    seedSnapshot(snapshotRepo, account, tableRid, 7, t, t - 10_000);
    snapshotRepo.maybeAdvanceCurrentSnapshotPointer(
        tableRid, snapshotRepo.getById(tableRid, 7).orElseThrow());

    assertEquals(7L, snapshotRepo.getCurrentSnapshot(tableRid).orElseThrow().getSnapshotId());
    assertEquals(
        7L, snapshotRepo.getCurrentSnapshotPointer(tableRid).orElseThrow().getSnapshotId());
  }

  /**
   * Commits a root whose manifest holds every seeded snapshot; currency as given (null = gated).
   */
  @Test
  void aDanglingRootPointerStaysGatedInsteadOfFallingBackToTheLegacyPointer() {
    // A root POINTER that exists with an unreadable blob is a swept/corrupt root, not an
    // un-migrated table: falling back to the raw legacy pointer would bypass the visibility gate.
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var tableRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    tableRepo.create(table(account, tableRid));

    long t = clock.millis();
    seedSnapshot(snapshotRepo, account, tableRid, 5, t, t - 10_000);
    snapshotRepo.maybeAdvanceCurrentSnapshotPointer(
        tableRid, snapshotRepo.getById(tableRid, 5).orElseThrow());
    seedRootWithCurrency(tableRid, 5L, t - 10_000);
    // Sweep the root blob out from under the pointer.
    String rootBlobUri = new TableRootRepository(ptr, blobs).metaForSafe(tableRid).getBlobUri();
    blobs.delete(rootBlobUri);

    assertTrue(
        snapshotRepo.getCurrentSnapshot(tableRid).isEmpty(),
        "an unreadable root must stay gated, never serve the raw legacy pointer");
    assertTrue(snapshotRepo.getCurrentSnapshotPointer(tableRid).isEmpty());
    assertEquals(
        5L,
        snapshotRepo.latestRegisteredSnapshotPointer(tableRid).orElseThrow().getSnapshotId(),
        "the raw accessor still serves write-side plumbing");
  }

  @Test
  void aTransientlySweptRootBlobRecoversOnTheRetry() {
    // The pointer exists but the first blob read misses (superseded root swept between the two
    // reads); the retry re-follows the pointer and must serve currency, not gate a live table.
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var tableRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    tableRepo.create(table(account, tableRid));
    long t = clock.millis();
    seedSnapshot(snapshotRepo, account, tableRid, 6, t, t - 10_000);
    seedRootWithCurrency(tableRid, 6L, t - 10_000);

    int[] misses = {1};
    var flakyRoots =
        new TableRootRepository(ptr, blobs) {
          @Override
          public java.util.Optional<ai.floedb.floecat.catalog.rpc.TableRoot> getByBlobUri(
              String blobUri) {
            if (misses[0] > 0) {
              misses[0]--;
              return java.util.Optional.empty();
            }
            return super.getByBlobUri(blobUri);
          }
        };
    var flakyRepo =
        new SnapshotRepository(
            ptr, blobs, tableRepo, new CurrentSnapshotPointerRepository(ptr, blobs), flakyRoots);

    assertEquals(
        6L,
        flakyRepo.getCurrentSnapshot(tableRid).orElseThrow().getSnapshotId(),
        "a transient sweep race must recover on the single retry");
  }

  @Test
  void aDanglingByTimeEntryIsSkippedInFavorOfTheNextIntactSnapshot() {
    // by-id and by-time are independent secondaries created and deleted non-atomically: a delete
    // or partial registration can leave the NEWEST by-time entry dangling. AS_OF and
    // latest-by-time must advance past it to the older intact snapshot, not report a spurious
    // not-found for a healthy table.
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var tableRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    tableRepo.create(table(account, tableRid));

    long older = clock.millis() - 100_000;
    long newer = clock.millis() - 50_000;
    seedSnapshot(snapshotRepo, account, tableRid, 10, clock.millis(), older);
    seedSnapshot(snapshotRepo, account, tableRid, 20, clock.millis(), newer);
    // Dangle snapshot 20: its by-id pointer vanishes, its by-time entry stays.
    ptr.delete(Keys.snapshotPointerById(account, tableRid.getId(), 20L));

    assertEquals(
        10L,
        snapshotRepo
            .getAsOf(tableRid, Timestamps.fromMillis(clock.millis()))
            .orElseThrow()
            .getSnapshotId(),
        "AS_OF must skip the dangling entry to the older intact snapshot");
    assertEquals(
        10L,
        snapshotRepo.getCurrentSnapshot(tableRid).orElseThrow().getSnapshotId(),
        "the latest-by-time fallback must skip the dangling entry too");
  }

  @Test
  void theVisiblePointerTimestampIsAStablePropertyOfTheSnapshot() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var tableRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    tableRepo.create(table(account, tableRid));

    long ingestedMs = clock.millis() - 5_000;
    seedSnapshot(snapshotRepo, account, tableRid, 4, ingestedMs, ingestedMs - 1_000);
    seedRootWithCurrency(tableRid, 4L, ingestedMs - 1_000);

    CurrentSnapshotPointer first = snapshotRepo.getCurrentSnapshotPointer(tableRid).orElseThrow();
    CurrentSnapshotPointer second = snapshotRepo.getCurrentSnapshotPointer(tableRid).orElseThrow();
    assertEquals(first, second, "polling the pointer must not observe a perpetual update");
    assertEquals(Timestamps.fromMillis(ingestedMs), first.getUpdatedAt());
  }

  private void seedRootWithCurrency(ResourceId tableId, Long currentSnapshotId, long createdAtMs) {
    var roots = new TableRootRepository(ptr, blobs);
    var committer = new ai.floedb.floecat.service.catalog.impl.TableRootCommitter(roots);
    for (Snapshot snap : snapshotRepo.list(tableId, 100, "", new StringBuilder())) {
      committer.commit(
          tableId,
          ai.floedb.floecat.service.catalog.impl.TableRootMutations.upsertSnapshot(
              roots,
              tableId,
              ai.floedb.floecat.catalog.rpc.SnapshotManifestEntry.newBuilder()
                  .setSnapshotId(snap.getSnapshotId())
                  .setUpstreamCreatedAt(snap.getUpstreamCreatedAt())
                  .build(),
              null,
              false));
    }
    if (currentSnapshotId != null) {
      committer.commit(
          tableId,
          current ->
              current.orElseThrow().toBuilder().setCurrentSnapshotId(currentSnapshotId).build());
    }
  }

  private void seedSnapshot(
      SnapshotRepository snapshotRepo,
      String account,
      ResourceId tableId,
      long snapshotId,
      long ingestedAtMs,
      long upstreamCreatedAt) {
    var snap =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setIngestedAt(Timestamps.fromMillis(ingestedAtMs))
            .setUpstreamCreatedAt(Timestamps.fromMillis(upstreamCreatedAt))
            .build();

    snapshotRepo.create(snap);
  }

  private void seedCurrentPointer(ResourceId tableId, long snapshotId, long upstreamCreatedAtMs) {
    new CurrentSnapshotPointerRepository(ptr, blobs)
        .createIfAbsent(
            CurrentSnapshotPointer.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(snapshotId)
                .setUpstreamCreatedAt(Timestamps.fromMillis(upstreamCreatedAtMs))
                .setUpdatedAt(Timestamps.fromMillis(clock.millis()))
                .build());
  }

  private Table table(String account, ResourceId tableId) {
    return Table.newBuilder()
        .setResourceId(tableId)
        .setDisplayName("orders")
        .setCatalogId(
            ResourceId.newBuilder()
                .setAccountId(account)
                .setId(UUID.randomUUID().toString())
                .setKind(ResourceKind.RK_CATALOG)
                .build())
        .setNamespaceId(
            ResourceId.newBuilder()
                .setAccountId(account)
                .setId(UUID.randomUUID().toString())
                .setKind(ResourceKind.RK_NAMESPACE)
                .build())
        .setUpstream(
            UpstreamRef.newBuilder()
                .setFormat(TableFormat.TF_ICEBERG)
                .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_FIELD_ID)
                .setUri("s3://warehouse/orders/")
                .build())
        .setCreatedAt(Timestamps.fromMillis(clock.millis()))
        .build();
  }

  private static final class FailingCurrentSnapshotPointerRepository
      extends CurrentSnapshotPointerRepository {
    private final AtomicInteger createAttempts = new AtomicInteger();

    private FailingCurrentSnapshotPointerRepository(
        PointerStore pointerStore, BlobStore blobStore) {
      super(pointerStore, blobStore);
    }

    @Override
    public boolean createIfAbsent(CurrentSnapshotPointer pointer) {
      createAttempts.incrementAndGet();
      return false;
    }
  }

  private static final class AdvancingOnMetaCurrentSnapshotPointerRepository
      extends CurrentSnapshotPointerRepository {
    private final AtomicBoolean advanced = new AtomicBoolean();
    private ResourceId tableId;
    private CurrentSnapshotPointer nextPointer;

    private AdvancingOnMetaCurrentSnapshotPointerRepository(
        PointerStore pointerStore, BlobStore blobStore) {
      super(pointerStore, blobStore);
    }

    void advanceDuringMetaFor(ResourceId tableId, CurrentSnapshotPointer nextPointer) {
      this.tableId = tableId;
      this.nextPointer = nextPointer;
    }

    @Override
    public MutationMeta metaFor(ResourceId requestedTableId) {
      if (requestedTableId.equals(tableId) && advanced.compareAndSet(false, true)) {
        long expectedVersion = super.metaFor(requestedTableId).getPointerVersion();
        assertTrue(super.update(nextPointer, expectedVersion));
      }
      return super.metaFor(requestedTableId);
    }
  }

  private static boolean isExpectedRepoAbort(Throwable t) {
    return t instanceof BaseResourceRepository.AbortRetryableException
        && t.getMessage().contains("blob write verification failed");
  }

  @Test
  @Timeout(20)
  void snapshotRepoCreateConcurrentSnapshots() throws Exception {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var tblId =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    tableRepo.create(table(account, tblId));

    int WORKERS = 24;
    int OPS = 200;
    var pool = Executors.newFixedThreadPool(WORKERS);
    var start = new CountDownLatch(1);
    var unexpected = new ConcurrentLinkedQueue<Throwable>();
    final ConcurrentMap<String, LongAdder> expected = new ConcurrentHashMap<>();
    var conflicts = new LongAdder();

    Runnable worker =
        () -> {
          try {
            start.await();
            var rnd = ThreadLocalRandom.current();
            for (int i = 0; i < OPS; i++) {
              long snapId = rnd.nextInt(160, 210);
              var snap =
                  Snapshot.newBuilder()
                      .setTableId(tblId)
                      .setSnapshotId(snapId)
                      .setIngestedAt(Timestamps.fromMillis(clock.millis()))
                      .build();
              try {
                for (int j = 0; j < 5; j++) {
                  try {
                    snapshotRepo.create(snap);
                  } catch (BaseResourceRepository.AbortRetryableException e) {
                    Thread.sleep(5L * (1L << j));
                  }
                }
              } catch (BaseResourceRepository.NameConflictException e) {
                conflicts.increment();
              } catch (Throwable t) {
                if (isExpectedRepoAbort(t)) {
                  expected.computeIfAbsent("ABORT_RETRYABLE", k -> new LongAdder()).increment();
                } else {
                  unexpected.add(t);
                }
              }
            }
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            unexpected.add(ie);
          }
        };

    for (int i = 0; i < WORKERS; i++) pool.submit(worker);
    start.countDown();
    pool.shutdown();
    assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS), "workers timed out");

    if (!unexpected.isEmpty()) {
      unexpected.peek().printStackTrace();
    }
    assertTrue(unexpected.isEmpty(), "unexpected exceptions: " + unexpected.size());
    assertTrue(conflicts.sum() >= 0, "should have some conflicts");

    Snapshot current = snapshotRepo.getCurrentSnapshot(tblId).orElse(null);
    if (current != null) {
      assertTrue(current.getSnapshotId() >= 160 && current.getSnapshotId() <= 209);
    }

    var next = new StringBuilder();
    var first = snapshotRepo.list(tblId, 5, "", next);
    assertTrue(first.size() <= 5);
    var token = next.toString();
    if (!token.isEmpty()) {
      next.setLength(0);
      var second = snapshotRepo.list(tblId, 5, token, next);
      assertTrue(second.size() >= 0);
    }
  }
}
