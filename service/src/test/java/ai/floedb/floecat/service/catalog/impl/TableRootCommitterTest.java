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
package ai.floedb.floecat.service.catalog.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.BlobRef;
import ai.floedb.floecat.catalog.rpc.TableRoot;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class TableRootCommitterTest {

  private static final ResourceId TABLE =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("tbl")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  private TableRootRepository roots;
  private TableRootCommitter committer;

  @BeforeEach
  void setUp() {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    roots = new TableRootRepository(pointers, blobs);
    committer = new TableRootCommitter(roots);
  }

  private static BlobRef ref(String uri) {
    return BlobRef.newBuilder().setUri(uri).setVersion("v-" + uri).build();
  }

  @Test
  void commitCreatesTheFirstRootAndStampsSeqAndTimestamp() {
    TableRoot committed =
        committer
            .commit(
                TABLE,
                current ->
                    TableRoot.newBuilder().setDefinitionRef(ref("s3://tbl/def-1.pb")).build())
            .orElseThrow();

    assertEquals(1L, committed.getRootSeq());
    assertTrue(committed.hasCommittedAt());
    assertEquals("s3://tbl/def-1.pb", committed.getDefinitionRef().getUri());
    assertEquals(committed, roots.get(TABLE).orElseThrow());
  }

  @Test
  void commitAppliesTheMutatorToTheCurrentRootAndBumpsSeq() {
    committer.commit(
        TABLE,
        current -> TableRoot.newBuilder().setDefinitionRef(ref("s3://tbl/def-1.pb")).build());

    TableRoot second =
        committer
            .commit(
                TABLE,
                current -> current.orElseThrow().toBuilder().setCurrentSnapshotId(7L).build())
            .orElseThrow();

    assertEquals(2L, second.getRootSeq());
    assertEquals(7L, second.getCurrentSnapshotId());
    // The mutator built on the current root, so the definition ref carried over.
    assertEquals("s3://tbl/def-1.pb", second.getDefinitionRef().getUri());
  }

  @Test
  void lostCasRerunsTheMutatorAgainstTheWinnersRoot() {
    committer.commit(
        TABLE,
        current -> TableRoot.newBuilder().setDefinitionRef(ref("s3://tbl/def-1.pb")).build());

    // First mutator invocation triggers a competing commit AFTER its reads, so its CAS loses; the
    // retry re-runs the mutator against the winner's root and both mutations land.
    AtomicInteger calls = new AtomicInteger();
    TableRoot merged =
        committer
            .commit(
                TABLE,
                current -> {
                  if (calls.incrementAndGet() == 1) {
                    committer.commit(
                        TABLE, c -> c.orElseThrow().toBuilder().setCurrentSnapshotId(42L).build());
                  }
                  return current.orElseThrow().toBuilder()
                      .setDefinitionRef(ref("s3://tbl/def-2.pb"))
                      .build();
                })
            .orElseThrow();

    assertTrue(calls.get() >= 2, "mutator must re-run after the lost CAS");
    assertEquals(42L, merged.getCurrentSnapshotId(), "competitor's mutation preserved");
    assertEquals("s3://tbl/def-2.pb", merged.getDefinitionRef().getUri(), "our mutation applied");
    assertEquals(3L, merged.getRootSeq());
  }

  @Test
  void noOpMutatorCommitsNothing() {
    committer.commit(
        TABLE,
        current -> TableRoot.newBuilder().setDefinitionRef(ref("s3://tbl/def-1.pb")).build());
    long versionBefore = roots.metaForSafe(TABLE).getPointerVersion();

    Optional<TableRoot> result = committer.commit(TABLE, current -> null);

    assertEquals(1L, result.orElseThrow().getRootSeq());
    assertEquals(versionBefore, roots.metaForSafe(TABLE).getPointerVersion());
  }

  @Test
  void unchangedMutatorOutputCommitsNothing() {
    committer.commit(
        TABLE,
        current -> TableRoot.newBuilder().setDefinitionRef(ref("s3://tbl/def-1.pb")).build());
    long versionBefore = roots.metaForSafe(TABLE).getPointerVersion();

    committer.commit(TABLE, current -> current.orElseThrow());

    assertEquals(versionBefore, roots.metaForSafe(TABLE).getPointerVersion());
  }

  @Test
  void terminalRepositoryErrorFailsTheCommit() {
    var failing = mock(TableRootRepository.class);
    when(failing.metaForSafe(TABLE))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(3L).build());
    when(failing.get(TABLE))
        .thenReturn(Optional.of(TableRoot.newBuilder().setTableId(TABLE).build()));
    when(failing.update(any(), anyLong()))
        .thenThrow(new BaseResourceRepository.NotFoundException("pointer gone"));
    var failingCommitter = new TableRootCommitter(failing);

    assertThrows(
        TableRootCommitter.CommitFailedException.class,
        () ->
            failingCommitter.commit(
                TABLE,
                current -> current.orElseThrow().toBuilder().setCurrentSnapshotId(1L).build()));
  }

  @Test
  void exhaustedCasAttemptsFailTheCommit() {
    var contended = mock(TableRootRepository.class);
    when(contended.metaForSafe(TABLE))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(3L).build());
    when(contended.get(TABLE))
        .thenReturn(Optional.of(TableRoot.newBuilder().setTableId(TABLE).build()));
    when(contended.update(any(), anyLong())).thenReturn(false); // never wins
    var contendedCommitter = new TableRootCommitter(contended);

    assertThrows(
        TableRootCommitter.CommitFailedException.class,
        () ->
            contendedCommitter.commit(
                TABLE,
                current -> current.orElseThrow().toBuilder().setCurrentSnapshotId(1L).build()));
  }

  @Test
  @Timeout(30)
  void concurrentRegistrationsFinalizesAndDdlConvergeToOneLineage() throws Exception {
    // The PRD's convergence acceptance, at the unit level with REAL threads: concurrent snapshot
    // registrations, per-snapshot finalizes, and DDL on ONE table must merge with no lost
    // mutation. Callers retry on CommitFailedException — the documented contract ("the caller
    // retries; mutators re-derive from committed state, so retries converge").
    final int writers = 6;
    final int perWriter = 4;
    var pool = Executors.newFixedThreadPool(writers + 1);
    var start = new CountDownLatch(1);
    var failures = new ConcurrentLinkedQueue<Throwable>();
    List<Callable<Void>> jobs = new ArrayList<>();
    for (int w = 0; w < writers; w++) {
      final int wi = w;
      jobs.add(
          () -> {
            start.await();
            for (int i = 0; i < perWriter; i++) {
              long sid = wi * 100L + i;
              commitWithRetry(
                  TableRootMutations.upsertSnapshot(
                      roots,
                      TABLE,
                      ai.floedb.floecat.catalog.rpc.SnapshotManifestEntry.newBuilder()
                          .setSnapshotId(sid)
                          .setSnapshotRef(ref("s3://t/snap-" + sid + ".pb"))
                          .setUpstreamCreatedAt(
                              com.google.protobuf.util.Timestamps.fromMillis(1_000 + sid))
                          .build(),
                      null,
                      false));
              // No /snapshots/current in this unit harness: null exercises the ordering-rule
              // fallback, so currency still converges on the newest across concurrent finalizes.
              commitWithRetry(
                  TableRootMutations.setStatsGeneration(
                      roots, TABLE, sid, ref("s3://t/stats/" + sid + "/gen.pb"), null));
            }
            return null;
          });
    }
    jobs.add(
        () -> {
          start.await();
          for (int i = 0; i < 8; i++) {
            commitWithRetry(
                TableRootMutations.setDefinition(TABLE, ref("s3://t/def-" + i + ".pb")));
          }
          return null;
        });

    List<Future<Void>> futures = new ArrayList<>();
    for (Callable<Void> job : jobs) {
      futures.add(pool.submit(job));
    }
    start.countDown();
    for (Future<Void> f : futures) {
      try {
        f.get(20, TimeUnit.SECONDS);
      } catch (Exception e) {
        failures.add(e);
      }
    }
    pool.shutdown();
    assertTrue(failures.isEmpty(), "writers must all converge: " + failures);

    TableRoot root = roots.get(TABLE).orElseThrow();
    long newest = -1;
    for (int w = 0; w < writers; w++) {
      for (int i = 0; i < perWriter; i++) {
        long sid = w * 100L + i;
        var entry =
            ai.floedb.floecat.service.repo.impl.SnapshotManifests.findEntry(
                roots, root.getSnapshotManifestRef(), sid);
        assertTrue(entry.isPresent(), "no lost registration: snapshot " + sid);
        assertTrue(entry.get().hasStatsGenerationRef(), "no lost finalize: snapshot " + sid);
        newest = Math.max(newest, sid);
      }
    }
    // Every snapshot finalized, so the advance rule crowns the newest upstream time (== max sid).
    assertEquals(newest, root.getCurrentSnapshotId(), "currency follows the advance rule");
    assertTrue(
        root.getDefinitionRef().getUri().startsWith("s3://t/def-"),
        "the definition converged on one of the DDL writes");
  }

  private void commitWithRetry(TableRootCommitter.RootMutator mutator) {
    for (int attempt = 0; ; attempt++) {
      try {
        committer.commit(TABLE, mutator);
        return;
      } catch (TableRootCommitter.CommitFailedException e) {
        if (attempt >= 50) {
          throw e;
        }
      }
    }
  }
}
