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

package ai.floedb.floecat.service.query.impl;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.PinKind;
import ai.floedb.floecat.query.rpc.RelationPinSet;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.service.query.QueryPins;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class QueryContextStoreImplTest {

  private QueryContextStoreImpl store;

  private static final ResourceId CATALOG =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("cat")
          .setKind(ResourceKind.RK_CATALOG)
          .build();

  @BeforeEach
  void setUp() {
    store = new QueryContextStoreImpl();
    store.defaultTtlMs = 60_000L;
    store.endedGraceMs = 15_000L;
    store.maxSize = 1000L;
    store.safetyExpiryMinutes = 10L;
    store.resolvingPinGraceMs = 60_000L;
    store.init();
  }

  @AfterEach
  void tearDown() {
    store.close();
  }

  private static ResourceId table(String id) {
    return ResourceId.newBuilder()
        .setAccountId("acct")
        .setId(id)
        .setKind(ResourceKind.RK_TABLE)
        .build();
  }

  private static byte[] pinBytes(String tableUri, String snapUri) {
    TablePin pin =
        TablePin.newBuilder()
            .setTableId(table("t1"))
            .setPinKind(PinKind.PIN_KIND_CURRENT)
            .setSnapshotId(7)
            .setTableBlobUri(tableUri)
            .setSnapshotBlobUri(snapUri)
            .build();
    return RelationPinSet.newBuilder().addPins(QueryPins.ofTable(pin)).build().toByteArray();
  }

  private QueryContext active(String queryId, String corrId, byte[] pins) {
    var principal = PrincipalContext.newBuilder().setAccountId("acct");
    if (corrId != null) {
      principal.setCorrelationId(corrId);
    }
    return QueryContext.newActive(
        queryId,
        principal.build(),
        new byte[0],
        pins,
        new byte[0],
        new byte[0],
        60_000L,
        1L,
        CATALOG);
  }

  @Test
  void referencedPinBlobUrisIncludesRootConstraintsAndFrozenScanGenerations() {
    TablePin pin =
        TablePin.newBuilder()
            .setTableId(table("t1"))
            .setPinKind(PinKind.PIN_KIND_CURRENT)
            .setSnapshotId(7)
            .setRootUri("s3://t1/root/abc.pb")
            .setTableBlobUri("s3://t1/table.pb")
            .setSnapshotBlobUri("s3://t1/snap-7.pb")
            .setConstraintsRefUri("s3://t1/constraints-7.pb")
            .build();
    store.put(
        QueryContext.newActive(
            "q-roots",
            PrincipalContext.newBuilder().setAccountId("acct").build(),
            new byte[0],
            RelationPinSet.newBuilder().addPins(QueryPins.ofTable(pin)).build().toByteArray(),
            new byte[0],
            new byte[0],
            60_000L,
            1L,
            CATALOG));
    // A live scan stream froze a stats generation: its manifest blob must be a GC root for the
    // scan's lifetime (generation-scoped pages load it), even after a replace-all superseded it.
    store.createScanSession(
        "corr",
        ScanSession.builder()
            .queryId("q-roots")
            .tableId(table("t1"))
            .snapshotId(7L)
            .statsGeneration("s3://t1/stats/7/manifest/gen-1.pb")
            .build());

    assertThat(store.referencedPinBlobUris())
        .contains(
            "s3://t1/root/abc.pb",
            "s3://t1/table.pb",
            "s3://t1/snap-7.pb",
            "s3://t1/constraints-7.pb",
            "s3://t1/stats/7/manifest/gen-1.pb");
  }

  @Test
  void referencedPinBlobUrisIncludesBothTableAndSnapshotBlobsOfLiveContexts() {
    store.put(active("q1", null, pinBytes("s3://t1/table.pb", "s3://t1/snap-7.pb")));

    assertThat(store.referencedPinBlobUris())
        .containsExactlyInAnyOrder("s3://t1/table.pb", "s3://t1/snap-7.pb");
  }

  @Test
  void referencedUrisUnionCommittedContextsAndResolvingPins() {
    store.put(active("q1", null, pinBytes("s3://t1/table.pb", "s3://t1/snap-7.pb")));
    // A committed context AND an in-flight resolution are both roots simultaneously.
    store.registerResolvingPinBlobs("corr-1", List.of("s3://t2/resolving.pb"));

    assertThat(store.referencedPinBlobUris())
        .containsExactlyInAnyOrder("s3://t1/table.pb", "s3://t1/snap-7.pb", "s3://t2/resolving.pb");
  }

  @Test
  void resolvingPinsAccumulatePerCorrelationIdAndAreReferenced() {
    store.registerResolvingPinBlobs("corr-1", List.of("s3://t1/a.pb"));
    store.registerResolvingPinBlobs("corr-1", List.of("s3://t1/b.pb")); // accumulates
    store.registerResolvingPinBlobs("corr-2", List.of("s3://t2/c.pb")); // separate id

    assertThat(store.referencedPinBlobUris())
        .containsExactlyInAnyOrder("s3://t1/a.pb", "s3://t1/b.pb", "s3://t2/c.pb");
  }

  @Test
  void committingAContextDropsItsResolvingPinRegistration() {
    // Resolve pins (register), then commit a context containing them: the transient registration is
    // released because the committed context now roots those blobs.
    store.registerResolvingPinBlobs("q1", List.of("s3://t1/table.pb", "s3://t1/snap-7.pb"));
    assertThat(store.referencedPinBlobUris())
        .containsExactlyInAnyOrder("s3://t1/table.pb", "s3://t1/snap-7.pb");

    // The committing context carries the same query id its pins were registered under, so the
    // commit
    // releases its own resolving registration (and only its own).
    store.put(active("q1", "corr-1", pinBytes("s3://t1/table.pb", "s3://t1/snap-7.pb")));

    // Still rooted — now via the committed context. Deleting the context leaves nothing, proving
    // the resolving registration was dropped (not double-counted / lingering).
    assertThat(store.referencedPinBlobUris())
        .containsExactlyInAnyOrder("s3://t1/table.pb", "s3://t1/snap-7.pb");
    store.delete("q1");
    assertThat(store.referencedPinBlobUris()).isEmpty();
  }

  @Test
  void committingReleasesResolvingRootByQueryIdNotTheStoredCorrelationId() {
    // Pins resolved on a later RPC (e.g. DescribeInputs) register under the stable query id, while
    // the committed context still carries the original BeginQuery correlation id. Releasing by the
    // query id — not the stored principal's correlation id — is what actually drops the transient
    // registration on commit instead of leaving it to linger until the fail-safe grace expires.
    store.registerResolvingPinBlobs("q1", List.of("s3://t1/table.pb", "s3://t1/snap-7.pb"));

    store.replace(
        active("q1", "begin-query-corr", pinBytes("s3://t1/table.pb", "s3://t1/snap-7.pb")));

    // Rooted only via the committed context now; deleting it leaves nothing, proving the resolving
    // registration was released on commit despite the differing correlation id.
    store.delete("q1");
    assertThat(store.referencedPinBlobUris()).isEmpty();
  }

  @Test
  void committingDropsOnlyTheRootedUrisAndKeepsTheRest() {
    // A resolution registered three blobs; a commit roots two of them. The third (not yet committed
    // — e.g. a later streaming chunk) must stay protected. URI-precise, so it also does not disturb
    // a different query.
    store.registerResolvingPinBlobs("q1", List.of("s3://a.pb", "s3://b.pb", "s3://c.pb"));

    store.put(active("q1", "corr-1", pinBytes("s3://a.pb", "s3://b.pb")));

    store.delete("q1"); // remove the committed-context roots, leaving only the resolving remainder
    assertThat(store.referencedPinBlobUris()).containsExactly("s3://c.pb");
  }

  @Test
  void committingDoesNotUnrootABlobStillBeingResolvedByAnotherQuery() {
    // Two concurrent queries pin the same physical blob: A has committed, B is still resolving. A's
    // commit must not strip the shared blob from B's resolving registration — otherwise A ending
    // before B commits would leave the blob rooted by neither.
    store.registerResolvingPinBlobs("qA", List.of("s3://shared/table.pb", "s3://shared/snap.pb"));
    store.registerResolvingPinBlobs("qB", List.of("s3://shared/table.pb", "s3://shared/snap.pb"));

    store.put(active("qA", "corr-A", pinBytes("s3://shared/table.pb", "s3://shared/snap.pb")));

    // A ends and is removed from the cache before B commits. B's resolving registration must still
    // root the shared blob.
    store.delete("qA");
    assertThat(store.referencedPinBlobUris())
        .containsExactlyInAnyOrder("s3://shared/table.pb", "s3://shared/snap.pb");
  }

  @Test
  void resolvingPinsExpireAndArePrunedFromTheRootSet() {
    // A non-positive grace makes the registration already expired, so the next read prunes it.
    store.resolvingPinGraceMs = -1L;
    store.registerResolvingPinBlobs("corr-1", List.of("s3://t1/stale.pb"));

    assertThat(store.referencedPinBlobUris()).isEmpty();
  }

  @Test
  void resolvingPinIsRootedWithinGraceAndExpiresAfterIt() {
    // Fail-safe path (never released, e.g. an abandoned resolution): rooted while within the grace,
    // pruned once the grace elapses. Uses a controllable clock to advance time deterministically.
    MutableClock clk = new MutableClock(Instant.ofEpochMilli(1_000_000L));
    store.clock = clk;
    store.resolvingPinGraceMs = 60_000L;

    store.registerResolvingPinBlobs("corr-1", List.of("s3://t1/x.pb"));
    clk.advance(Duration.ofMillis(59_000L));
    assertThat(store.referencedPinBlobUris()).containsExactly("s3://t1/x.pb");

    clk.advance(Duration.ofMillis(2_000L)); // now past the 60s grace
    assertThat(store.referencedPinBlobUris()).isEmpty();
  }

  @Test
  void registrationPrunesExpiredEntriesOnceTheMapExceedsMaxSize() {
    // With CAS blob GC disabled, the GC read path never prunes; registration itself must keep the
    // map bounded once it holds more entries than there can be live queries (maxSize).
    MutableClock clk = new MutableClock(Instant.ofEpochMilli(1_000_000L));
    store.clock = clk;
    store.resolvingPinGraceMs = 60_000L;
    store.maxSize = 2L;

    store.registerResolvingPinBlobs("q-dead-1", List.of("s3://t1/a.pb"));
    store.registerResolvingPinBlobs("q-dead-2", List.of("s3://t2/b.pb"));
    clk.advance(Duration.ofMillis(61_000L)); // both entries are now past the grace

    // The map is at the maxSize threshold, so this registration prunes the expired entries.
    store.registerResolvingPinBlobs("q-live", List.of("s3://t3/c.pb"));

    assertThat(store.resolvingPinEntryCount()).isEqualTo(1);
  }

  @Test
  void expiryTransitionNeverClobbersAConcurrentPinCommit() throws Exception {
    // get()'s lazy ACTIVE→EXPIRED transition races update() committing pins. The transition must be
    // atomic (computeIfPresent): a read-then-put could write back a stale pre-pin version, losing
    // pins whose transient resolving roots were already dropped. Race the two operations and assert
    // the committed pins always survive into whatever version wins.
    for (int i = 0; i < 200; i++) {
      String queryId = "q-race-" + i;
      // Zero TTL: past expiry by the time the racing threads run, so get() attempts the transition.
      store.put(
          QueryContext.newActive(
              queryId,
              PrincipalContext.newBuilder().setAccountId("acct").build(),
              new byte[0],
              new byte[0],
              new byte[0],
              new byte[0],
              0L,
              1L,
              CATALOG));

      byte[] pins = pinBytes("s3://race/table.pb", "s3://race/snap.pb");
      var latch = new java.util.concurrent.CountDownLatch(1);
      Thread committer =
          new Thread(
              () -> {
                try {
                  latch.await();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                store.update(queryId, ctx -> ctx.toBuilder().relationPins(pins).build());
              });
      Thread reader =
          new Thread(
              () -> {
                try {
                  latch.await();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                store.get(queryId);
              });
      committer.start();
      reader.start();
      latch.countDown();
      committer.join();
      reader.join();

      QueryContext stored = store.get(queryId).orElseThrow();
      assertThat(stored.getRelationPins()).isEqualTo(pins);
    }
  }

  /** A clock whose instant can be advanced, for deterministic grace-expiry tests. */
  private static final class MutableClock extends Clock {
    private volatile Instant now;

    MutableClock(Instant now) {
      this.now = now;
    }

    void advance(Duration by) {
      now = now.plus(by);
    }

    @Override
    public Instant instant() {
      return now;
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }
  }

  @Test
  void registerIgnoresBlankAndNullUris() {
    store.registerResolvingPinBlobs("corr-1", java.util.Arrays.asList("", null));
    assertThat(store.referencedPinBlobUris()).isEmpty();

    store.registerResolvingPinBlobs("corr-1", List.of("s3://t1/x.pb"));
    assertThat(store.referencedPinBlobUris()).containsExactly("s3://t1/x.pb");
  }

  @org.junit.jupiter.api.Test
  void createScanSessionForAMissingQueryLeavesNoBookkeepingBehind() {
    // The handle maps are installed before the context update; if the query is already gone the
    // update returns empty and the maps must be rolled back — otherwise a handleToQuery +
    // scanSessionCache entry leaks forever (never cleaned, since no context ever references it).
    var store = QueryContextStores.forTesting();
    try {
      org.junit.jupiter.api.Assertions.assertThrows(
          io.grpc.StatusRuntimeException.class,
          () ->
              store.createScanSession(
                  "corr",
                  ScanSession.builder()
                      .queryId("q-absent")
                      .tableId(table("t1"))
                      .snapshotId(1L)
                      .statsGeneration("s3://t1/stats/1/manifest/gen.pb")
                      .build()));

      // No leaked GC root from the rolled-back scan session.
      assertThat(store.referencedPinBlobUris()).doesNotContain("s3://t1/stats/1/manifest/gen.pb");
    } finally {
      store.close();
    }
  }

  @org.junit.jupiter.api.Test
  void activeContextsAreNotSizeEvictedSoTheirPinsStayRooted() {
    // A committed context is a durable GC root; size-evicting a live query's context would unroot
    // its pins and let GC delete a blob it is still reading. ACTIVE contexts weigh 0, so a cap of
    // 1 evicts none of them — all their pinned blobs stay rooted.
    var store = QueryContextStores.forTesting(1L);
    try {
      for (int i = 0; i < 50; i++) {
        var pin =
            ai.floedb.floecat.query.rpc.TablePin.newBuilder()
                .setTableId(
                    ai.floedb.floecat.common.rpc.ResourceId.newBuilder()
                        .setAccountId("acct")
                        .setId("t" + i)
                        .setKind(ai.floedb.floecat.common.rpc.ResourceKind.RK_TABLE))
                .setPinKind(ai.floedb.floecat.query.rpc.PinKind.PIN_KIND_CURRENT)
                .setSnapshotId(1L)
                .setRootUri("s3://t" + i + "/root.pb")
                .build();
        store.put(
            QueryContext.newActive(
                "q-" + i,
                PrincipalContext.newBuilder().setAccountId("acct").build(),
                new byte[0],
                RelationPinSet.newBuilder().addPins(QueryPins.ofTable(pin)).build().toByteArray(),
                new byte[0],
                new byte[0],
                60_000L,
                1L,
                CATALOG));
      }

      var roots = store.referencedPinBlobUris();
      for (int i = 0; i < 50; i++) {
        assertThat(roots)
            .as("active context q-%s must not be size-evicted", i)
            .contains("s3://t" + i + "/root.pb");
      }
    } finally {
      store.close();
    }
  }

  @Test
  void renewingANonActiveContextSurfacesNotFound() {
    // A renew against a context that has left ACTIVE (ended, or lazily expired) must NOT report a
    // false success carrying the stale lease — the caller treats empty as not-found.
    store.put(active("q-ended", null, pinBytes("s3://t1/table.pb", "s3://t1/snap-7.pb")));
    store.end("q-ended", true); // ENDED_COMMIT — no longer ACTIVE

    assertThat(store.extendLease("q-ended", Long.MAX_VALUE)).isEmpty();
  }
}
