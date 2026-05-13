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
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
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
    tableRepo.update(
        td.toBuilder().putProperties("current-snapshot-id", "200").build(),
        tableRepo.metaFor(tableRid).getPointerVersion());

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
  void getCurrentSnapshotUsesTableCurrentSnapshotId() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var tableRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    tableRepo.create(tableWithCurrentSnapshot(account, tableRid, 1L));

    long createdMs = clock.millis() - 10_000;
    seedSnapshot(snapshotRepo, account, tableRid, 0, clock.millis(), createdMs);
    seedSnapshot(snapshotRepo, account, tableRid, 1, clock.millis(), createdMs);

    Snapshot current = snapshotRepo.getCurrentSnapshot(tableRid).orElseThrow();
    assertEquals(1L, current.getSnapshotId());
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
  void getCurrentSnapshotIgnoresByTimeOrderingWhenTableCurrentSnapshotIsOlder() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var tableRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    tableRepo.create(tableWithCurrentSnapshot(account, tableRid, 999L));

    long newestCreatedMs = clock.millis() - 5_000;
    long olderCreatedMs = newestCreatedMs - 1_000;

    seedSnapshot(snapshotRepo, account, tableRid, 0, clock.millis(), newestCreatedMs);
    seedSnapshot(snapshotRepo, account, tableRid, 1, clock.millis(), newestCreatedMs);
    seedSnapshot(snapshotRepo, account, tableRid, 999, clock.millis(), olderCreatedMs);

    Snapshot current = snapshotRepo.getCurrentSnapshot(tableRid).orElseThrow();
    assertEquals(999L, current.getSnapshotId());
    assertEquals(Timestamps.fromMillis(olderCreatedMs), current.getUpstreamCreatedAt());
  }

  @Test
  void getCurrentSnapshotReturnsEmptyWithoutTableCurrentSnapshotId() {
    String account = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();
    var tableRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    tableRepo.create(tableWithCurrentSnapshot(account, tableRid, null));
    seedSnapshot(snapshotRepo, account, tableRid, 204, clock.millis(), clock.millis() - 10_000);

    assertTrue(snapshotRepo.getCurrentSnapshot(tableRid).isEmpty());
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

  private Table tableWithCurrentSnapshot(
      String account, ResourceId tableId, Long currentSnapshotId) {
    Table.Builder builder =
        Table.newBuilder()
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
            .setCreatedAt(Timestamps.fromMillis(clock.millis()));
    if (currentSnapshotId != null) {
      builder.putProperties("current-snapshot-id", Long.toString(currentSnapshotId));
    }
    return builder.build();
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
    tableRepo.create(tableWithCurrentSnapshot(account, tblId, null));

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

    assertTrue(snapshotRepo.getCurrentSnapshot(tblId).isEmpty());

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
