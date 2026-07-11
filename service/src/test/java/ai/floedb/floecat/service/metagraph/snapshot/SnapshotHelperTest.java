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

package ai.floedb.floecat.service.metagraph.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.BlobRef;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotManifestEntry;
import ai.floedb.floecat.catalog.rpc.TableRoot;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.query.rpc.PinKind;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.service.catalog.impl.TableRootCommitter;
import ai.floedb.floecat.service.catalog.impl.TableRootMutations;
import ai.floedb.floecat.service.catalog.impl.TableRootSynthesizer;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.service.testsupport.SnapshotTestSupport;
import ai.floedb.floecat.service.testsupport.TestNodes;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import com.google.protobuf.Timestamp;
import io.grpc.StatusRuntimeException;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SnapshotHelperTest {

  private SnapshotHelper helper;
  private SnapshotTestSupport.FakeSnapshotRepository repository;
  private TableRepository tableRepo;
  private TableRootRepository roots;
  private TableRootCommitter committer;

  @BeforeEach
  void setUp() {
    repository = new SnapshotTestSupport.FakeSnapshotRepository();
    tableRepo = mock(TableRepository.class);
    when(tableRepo.metaForSafe(any()))
        .thenReturn(
            MutationMeta.newBuilder().setBlobUri("s3://tbl/table.pb").setEtag("etag-t").build());
    when(tableRepo.blobEtag("s3://tbl/table.pb")).thenReturn("etag-t");
    roots = new TableRootRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    committer = new TableRootCommitter(roots);
    helper = new SnapshotHelper(repository, roots, committer, null);
  }

  /** Seed a snapshot in the legacy fake so its blob identity resolves for validation. */
  private void seedSnapshot(ResourceId tableId, long snapshotId, String createdAt) {
    repository.put(
        tableId,
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setUpstreamCreatedAt(ts(createdAt))
            .build());
  }

  /**
   * Commit the snapshot's manifest entry onto the table root (the write path every snapshot write
   * funnels through), with refs matching the fake's synthesized blob identity.
   */
  private void commitEntry(ResourceId tableId, long snapshotId, String createdAt) {
    committer.commit(
        tableId,
        TableRootMutations.upsertSnapshot(
            roots,
            tableId,
            SnapshotManifestEntry.newBuilder()
                .setSnapshotId(snapshotId)
                .setSnapshotRef(
                    BlobRef.newBuilder()
                        .setUri("s3://" + tableId.getId() + "/snap-" + snapshotId + ".pb")
                        .setVersion("etag-s" + snapshotId))
                .setUpstreamCreatedAt(ts(createdAt))
                .build(),
            BlobRef.newBuilder().setUri("s3://tbl/table.pb").setVersion("etag-t").build(),
            true));
  }

  private void seedAndCommit(ResourceId tableId, long snapshotId, String createdAt) {
    seedSnapshot(tableId, snapshotId, createdAt);
    commitEntry(tableId, snapshotId, createdAt);
  }

  @Test
  void schemaJsonUsesSnapshotPayload() {
    ResourceId tableId = tableId("tbl");
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(41L)
            .setSchemaJson("{\"fields\":[]}")
            .setUpstreamCreatedAt(ts("2024-04-01T00:00:00Z"))
            .build();
    repository.put(tableId, snapshot);

    String schema =
        helper.schemaJsonFor(
            "corr",
            TestNodes.tableNode(tableId, "{}"),
            SnapshotRef.newBuilder().setSnapshotId(41L).build(),
            () -> "{}");

    assertThat(schema).contains("fields");
  }

  @Test
  void schemaJsonPrefersPinnedSnapshotBlobOverLivePointer() {
    ResourceId tableId = tableId("tbl");
    // The live (table, snapshot id) pointer carries a newer schema (as an in-place UpdateSnapshot
    // would leave it), but the pinned blob URI names the snapshot the query froze. The read must
    // serve the pinned blob's schema, never the live pointer's.
    Snapshot pinned =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(41L)
            .setSchemaJson("{\"fields\":[\"pinned\"]}")
            .setUpstreamCreatedAt(ts("2024-04-01T00:00:00Z"))
            .build();
    repository.put(tableId, pinned);

    String schema =
        helper.schemaJsonFor(
            "corr",
            TestNodes.tableNode(tableId, "{}"),
            SnapshotRef.newBuilder().setSnapshotId(41L).build(),
            "s3://tbl/snap-41.pb",
            () -> "{}");

    assertThat(schema).contains("pinned");
  }

  @Test
  void schemaJsonThrowsWhenPinnedSnapshotBlobMissing() {
    ResourceId tableId = tableId("tbl");
    // The pinned snapshot blob URI names a blob the fake cannot resolve: fail hard rather than
    // silently falling back to the live pointer.
    assertThatThrownBy(
            () ->
                helper.schemaJsonFor(
                    "corr",
                    TestNodes.tableNode(tableId, "{}"),
                    SnapshotRef.newBuilder().setSnapshotId(41L).build(),
                    "s3://tbl/snap-gone.pb",
                    () -> "{}"))
        .isInstanceOf(RuntimeException.class);
  }

  @Test
  void schemaJsonThrowsWhenSnapshotMissing() {
    ResourceId tableId = tableId("tbl");
    assertThatThrownBy(
            () ->
                helper.schemaJsonFor(
                    "corr",
                    TestNodes.tableNode(tableId, "{}"),
                    SnapshotRef.newBuilder().setSnapshotId(1L).build(),
                    () -> "{}"))
        .isInstanceOf(RuntimeException.class);
  }

  @Test
  void tablePinCurrentUsesTheRootsCurrentSnapshot() {
    ResourceId tableId = tableId("tbl");
    seedAndCommit(tableId, 142, "2024-05-01T00:00:00Z");

    TablePin pin = helper.tablePinFor("corr", tableId, null, Optional.empty());

    assertThat(pin.getPinKind()).isEqualTo(PinKind.PIN_KIND_CURRENT);
    assertThat(pin.getSnapshotId()).isEqualTo(142);
    assertThat(pin.getTableBlobUri()).isEqualTo("s3://tbl/table.pb");
    assertThat(pin.getSnapshotBlobUri()).isEqualTo("s3://tbl/snap-142.pb");
    assertThat(pin.getRootUri()).isNotEmpty();
    assertThat(pin.getRootVersion()).isNotEmpty();
    // The pinned root is the immutable object every read follows refs out of.
    TableRoot pinnedRoot = roots.getByBlobUri(pin.getRootUri()).orElseThrow();
    assertThat(pinnedRoot.getCurrentSnapshotId()).isEqualTo(142);
  }

  @Test
  void tablePinCurrentMaterializesALegacyTableAtFirstTouch() {
    // A table with pre-existing (legacy family) data but no root yet: pin construction runs the
    // committer's ensureRoot, which persists the synthesized history, then pins through it.
    ResourceId tableId = tableId("tbl");
    seedSnapshot(tableId, 142, "2024-05-01T00:00:00Z");
    TableRoot synthesized =
        TableRoot.newBuilder()
            .setTableId(tableId)
            .setDefinitionRef(BlobRef.newBuilder().setUri("s3://tbl/table.pb").setVersion("etag-t"))
            .setSnapshotManifestRef(
                ai.floedb.floecat.service.repo.impl.SnapshotManifests.upsert(
                    roots,
                    tableId,
                    null,
                    SnapshotManifestEntry.newBuilder()
                        .setSnapshotId(142)
                        .setSnapshotRef(
                            BlobRef.newBuilder()
                                .setUri("s3://tbl/snap-142.pb")
                                .setVersion("etag-s142"))
                        .setUpstreamCreatedAt(ts("2024-05-01T00:00:00Z"))
                        .build()))
            .setCurrentSnapshotId(142)
            .build();
    TableRootSynthesizer synthesizer = mock(TableRootSynthesizer.class);
    when(synthesizer.synthesize(tableId)).thenReturn(Optional.of(synthesized));
    helper =
        new SnapshotHelper(repository, roots, new TableRootCommitter(roots, synthesizer), null);

    TablePin pin = helper.tablePinFor("corr", tableId, null, Optional.empty());

    assertThat(pin.getSnapshotId()).isEqualTo(142);
    // The synthesized root was persisted: the table is migrated, not re-synthesized per read.
    assertThat(roots.get(tableId)).isPresent();
  }

  @Test
  void tablePinCurrentFailsNotFoundWhenTableHasNoCurrentSnapshot() {
    ResourceId tableId = tableId("tbl");
    // No root at all (fresh table with no trace): an expected, client-reachable state.
    assertThatThrownBy(() -> helper.tablePinFor("corr", tableId, null, Optional.empty()))
        .isInstanceOf(StatusRuntimeException.class)
        .satisfies(
            e ->
                assertThat(((StatusRuntimeException) e).getStatus().getCode())
                    .isEqualTo(io.grpc.Status.Code.NOT_FOUND));
  }

  @Test
  void tablePinCurrentFailsNotFoundWhenRootHasNoCurrency() {
    ResourceId tableId = tableId("tbl");
    // A root exists (definition committed by DDL) but the table has no current snapshot yet.
    committer.commit(
        tableId,
        TableRootMutations.setDefinition(
            tableId,
            BlobRef.newBuilder().setUri("s3://tbl/table.pb").setVersion("etag-t").build()));

    assertThatThrownBy(() -> helper.tablePinFor("corr", tableId, null, Optional.empty()))
        .isInstanceOf(StatusRuntimeException.class)
        .satisfies(
            e ->
                assertThat(((StatusRuntimeException) e).getStatus().getCode())
                    .isEqualTo(io.grpc.Status.Code.NOT_FOUND));
  }

  @Test
  void tablePinCurrentFailsInternalWhenCurrencyPointsAtAVanishedEntry() {
    ResourceId tableId = tableId("tbl");
    seedAndCommit(tableId, 142, "2024-05-01T00:00:00Z");
    // Force a broken root: currency names a snapshot the manifest does not carry. Removal clears
    // currency in the same commit, so this state is a violated invariant, never a client state.
    committer.commit(
        tableId, current -> current.orElseThrow().toBuilder().setCurrentSnapshotId(999).build());

    assertThatThrownBy(() -> helper.tablePinFor("corr", tableId, null, Optional.empty()))
        .isInstanceOf(StatusRuntimeException.class)
        .satisfies(
            e ->
                assertThat(((StatusRuntimeException) e).getStatus().getCode())
                    .isEqualTo(io.grpc.Status.Code.INTERNAL));
  }

  @Test
  void tablePinValidatesAgainstThePinnedRootsRefsNotLivePointers() {
    ResourceId tableId = tableId("tbl");
    seedSnapshot(tableId, 142, "2024-05-01T00:00:00Z");
    // The root's definition ref names an older blob that is still retrievable at its version,
    // while the live table pointer has advanced (an ALTER whose root commit has not landed yet).
    // The pin carries and validates the ROOT's ref, not the live pointer.
    when(tableRepo.blobEtag("s3://tbl/table-v1.pb")).thenReturn("etag-t1");
    when(tableRepo.metaForSafe(tableId))
        .thenReturn(
            MutationMeta.newBuilder()
                .setBlobUri("s3://tbl/table-v2.pb")
                .setEtag("etag-t2")
                .build());
    committer.commit(
        tableId,
        TableRootMutations.upsertSnapshot(
            roots,
            tableId,
            SnapshotManifestEntry.newBuilder()
                .setSnapshotId(142)
                .setSnapshotRef(
                    BlobRef.newBuilder().setUri("s3://tbl/snap-142.pb").setVersion("etag-s142"))
                .setUpstreamCreatedAt(ts("2024-05-01T00:00:00Z"))
                .build(),
            BlobRef.newBuilder().setUri("s3://tbl/table-v1.pb").setVersion("etag-t1").build(),
            true));

    TablePin pin = helper.tablePinFor("corr", tableId, null, Optional.empty());

    assertThat(pin.getTableBlobUri()).isEqualTo("s3://tbl/table-v1.pb");
    assertThat(pin.getTableBlobVersion()).isEqualTo("etag-t1");
  }

  @Test
  void tablePinCarriesTheRootsRefsWithoutPerBlobReValidation() {
    // The pin copies its refs out of the immutable root it just read; the single root leg is the
    // integrity contract, and a read that later loads a vanished copied blob fails loudly via
    // requirePinned*. Construction therefore succeeds even when a copied blob is unreadable.
    ResourceId tableId = tableId("tbl");
    seedSnapshot(tableId, 142, "2024-05-01T00:00:00Z");
    committer.commit(
        tableId,
        TableRootMutations.upsertSnapshot(
            roots,
            tableId,
            SnapshotManifestEntry.newBuilder()
                .setSnapshotId(142)
                .setSnapshotRef(
                    BlobRef.newBuilder().setUri("s3://tbl/snap-142.pb").setVersion("etag-s142"))
                .setUpstreamCreatedAt(ts("2024-05-01T00:00:00Z"))
                .build(),
            BlobRef.newBuilder().setUri("s3://tbl/gone.pb").setVersion("etag-x").build(),
            true));

    TablePin pin = helper.tablePinFor("corr", tableId, null, Optional.empty());

    assertThat(pin.getTableBlobUri()).isEqualTo("s3://tbl/gone.pb");
    assertThat(pin.getRootUri()).isNotEmpty();
  }

  @Test
  void tablePinExplicitSnapshotIdResolvesThroughTheManifest() {
    ResourceId tableId = tableId("tbl");
    seedAndCommit(tableId, 5, "2024-01-01T00:00:00Z");

    TablePin pin =
        helper.tablePinFor(
            "corr", tableId, SnapshotRef.newBuilder().setSnapshotId(5).build(), Optional.empty());

    assertThat(pin.getPinKind()).isEqualTo(PinKind.PIN_KIND_SNAPSHOT_ID);
    assertThat(pin.getSnapshotId()).isEqualTo(5);
    assertThat(pin.getTableBlobUri()).isEqualTo("s3://tbl/table.pb");
    assertThat(pin.getTableBlobVersion()).isEqualTo("etag-t");
    assertThat(pin.getSnapshotBlobUri()).isEqualTo("s3://tbl/snap-5.pb");
    assertThat(pin.getSnapshotBlobVersion()).isEqualTo("etag-s5");
    assertThat(pin.getRootUri()).isNotEmpty();
  }

  @Test
  void tablePinExplicitSnapshotIdNotFoundFails() {
    ResourceId tableId = tableId("tbl");
    seedAndCommit(tableId, 5, "2024-01-01T00:00:00Z");

    assertThatThrownBy(
            () ->
                helper.tablePinFor(
                    "corr",
                    tableId,
                    SnapshotRef.newBuilder().setSnapshotId(404).build(),
                    Optional.empty()))
        .isInstanceOf(StatusRuntimeException.class)
        .satisfies(
            e ->
                assertThat(((StatusRuntimeException) e).getStatus().getCode())
                    .isEqualTo(io.grpc.Status.Code.NOT_FOUND));
  }

  @Test
  void tablePinAsOfResolvesToThePredecessorEntry() {
    ResourceId tableId = tableId("tbl");
    seedAndCommit(tableId, 11, "2024-02-01T00:00:00Z");
    seedAndCommit(tableId, 12, "2024-03-01T00:00:00Z");
    Timestamp asOf = ts("2024-02-15T00:00:00Z");

    TablePin pin =
        helper.tablePinFor(
            "corr", tableId, SnapshotRef.newBuilder().setAsOf(asOf).build(), Optional.empty());

    assertThat(pin.getPinKind()).isEqualTo(PinKind.PIN_KIND_AS_OF);
    // Resolved once to the predecessor entry; timestamp kept only as provenance.
    assertThat(pin.getSnapshotId()).isEqualTo(11);
    assertThat(pin.getOriginalAsOf()).isEqualTo(asOf);
    assertThat(pin.getSnapshotBlobUri()).isEqualTo("s3://tbl/snap-11.pb");
    assertThat(pin.getSnapshotBlobVersion()).isEqualTo("etag-s11");
    assertThat(pin.getTableBlobVersion()).isEqualTo("etag-t");
  }

  @Test
  void tablePinAsOfWithNoSnapshotAtOrBeforeFails() {
    ResourceId tableId = tableId("tbl");
    seedAndCommit(tableId, 20, "2024-06-01T00:00:00Z");

    assertThatThrownBy(
            () ->
                helper.tablePinFor(
                    "corr",
                    tableId,
                    SnapshotRef.newBuilder().setAsOf(ts("2024-01-01T00:00:00Z")).build(),
                    Optional.empty()))
        .isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  void tablePinSpecialCurrentUsesTheRootsCurrency() {
    // An explicit SS_CURRENT selector resolves through the same CURRENT path as no override.
    ResourceId tableId = tableId("tbl");
    seedAndCommit(tableId, 142, "2024-05-01T00:00:00Z");

    TablePin pin =
        helper.tablePinFor(
            "corr",
            tableId,
            SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT).build(),
            Optional.empty());

    assertThat(pin.getPinKind()).isEqualTo(PinKind.PIN_KIND_CURRENT);
    assertThat(pin.getSnapshotId()).isEqualTo(142);
  }

  @Test
  void tablePinFreezesTheEntrysConstraintsRef() {
    ResourceId tableId = tableId("tbl");
    seedSnapshot(tableId, 7, "2024-05-01T00:00:00Z");
    committer.commit(
        tableId,
        TableRootMutations.upsertSnapshot(
            roots,
            tableId,
            SnapshotManifestEntry.newBuilder()
                .setSnapshotId(7)
                .setSnapshotRef(
                    BlobRef.newBuilder().setUri("s3://tbl/snap-7.pb").setVersion("etag-s7"))
                .setConstraintsRef(
                    BlobRef.newBuilder().setUri("s3://tbl/constraints-7.pb").setVersion("etag-c7"))
                .setUpstreamCreatedAt(ts("2024-05-01T00:00:00Z"))
                .build(),
            BlobRef.newBuilder().setUri("s3://tbl/table.pb").setVersion("etag-t").build(),
            true));

    TablePin pin = helper.tablePinFor("corr", tableId, null, Optional.empty());

    assertThat(pin.getConstraintsRefUri()).isEqualTo("s3://tbl/constraints-7.pb");
    assertThat(pin.getConstraintsRefVersion()).isEqualTo("etag-c7");

    // A pin over an entry with no bundle stays deterministically constraint-free.
    ResourceId bare = tableId("tbl-bare");
    seedAndCommit(bare, 3, "2024-05-01T00:00:00Z");
    assertThat(helper.tablePinFor("corr", bare, null, Optional.empty()).getConstraintsRefUri())
        .isEmpty();
  }

  @Test
  void tablePinRejectsUnsupportedSpecialSelector() {
    // A SPECIAL selector other than SS_CURRENT has no pin resolution and must be rejected rather
    // than silently pinning CURRENT.
    ResourceId tableId = tableId("tbl");
    assertThatThrownBy(
            () ->
                helper.tablePinFor(
                    "corr",
                    tableId,
                    SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_UNSPECIFIED).build(),
                    Optional.empty()))
        .isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  void tablePinFailsWhenTheRootHasNoDefinitionRef() {
    // The manifest resolves the snapshot, but the root carries no definition ref: table-scoped
    // reads cannot be served → catalog-integrity failure, not a walk to the live pointer.
    ResourceId tableId = tableId("tbl");
    seedSnapshot(tableId, 9, "2024-01-01T00:00:00Z");
    committer.commit(
        tableId,
        TableRootMutations.upsertSnapshot(
            roots,
            tableId,
            SnapshotManifestEntry.newBuilder()
                .setSnapshotId(9)
                .setSnapshotRef(
                    BlobRef.newBuilder().setUri("s3://tbl/snap-9.pb").setVersion("etag-s9"))
                .setUpstreamCreatedAt(ts("2024-01-01T00:00:00Z"))
                .build(),
            null,
            true));

    assertThatThrownBy(
            () ->
                helper.tablePinFor(
                    "corr",
                    tableId,
                    SnapshotRef.newBuilder().setSnapshotId(9).build(),
                    Optional.empty()))
        .isInstanceOf(StatusRuntimeException.class)
        .satisfies(
            e ->
                assertThat(((StatusRuntimeException) e).getStatus().getCode())
                    .isEqualTo(io.grpc.Status.Code.INTERNAL));
  }

  @Test
  void gatedPinsRejectUnfinalizedSnapshots() {
    // With a generation-tracking store, an entry without its generation ref is not query-ready:
    // registration happened, finalize has not. Explicit pins reject it loudly; AS_OF resolves
    // among finalized entries only; CURRENT never points at it (currency advances at finalize).
    var tracking = mock(ai.floedb.floecat.stats.spi.StatsStore.class);
    when(tracking.tracksStatsGenerations()).thenReturn(true);
    helper = new SnapshotHelper(repository, roots, committer, tracking);
    ResourceId tableId = tableId("tbl");
    seedSnapshot(tableId, 11, "2024-02-01T00:00:00Z");
    seedSnapshot(tableId, 12, "2024-03-01T00:00:00Z");
    // 11 finalized (generation ref present), 12 registered only.
    committer.commit(
        tableId,
        TableRootMutations.upsertSnapshot(
            roots,
            tableId,
            SnapshotManifestEntry.newBuilder()
                .setSnapshotId(11)
                .setSnapshotRef(
                    BlobRef.newBuilder().setUri("s3://tbl/snap-11.pb").setVersion("etag-s11"))
                .setStatsGenerationRef(BlobRef.newBuilder().setUri("s3://tbl/stats/11/gen.pb"))
                .setUpstreamCreatedAt(ts("2024-02-01T00:00:00Z"))
                .build(),
            BlobRef.newBuilder().setUri("s3://tbl/table.pb").setVersion("etag-t").build(),
            true));
    committer.commit(
        tableId,
        TableRootMutations.upsertSnapshot(
            roots,
            tableId,
            SnapshotManifestEntry.newBuilder()
                .setSnapshotId(12)
                .setSnapshotRef(
                    BlobRef.newBuilder().setUri("s3://tbl/snap-12.pb").setVersion("etag-s12"))
                .setUpstreamCreatedAt(ts("2024-03-01T00:00:00Z"))
                .build(),
            null,
            false));

    // Explicit pin on the unfinalized snapshot: FAILED_PRECONDITION, not NOT_FOUND — it exists,
    // it is just not query-ready yet.
    assertThatThrownBy(
            () ->
                helper.tablePinFor(
                    "corr",
                    tableId,
                    SnapshotRef.newBuilder().setSnapshotId(12).build(),
                    Optional.empty()))
        .isInstanceOf(StatusRuntimeException.class)
        .satisfies(
            e ->
                assertThat(((StatusRuntimeException) e).getStatus().getCode())
                    .isEqualTo(io.grpc.Status.Code.FAILED_PRECONDITION));

    // AS_OF after 12's upstream time still resolves to 11 — the newest FINALIZED entry.
    TablePin asOf =
        helper.tablePinFor(
            "corr",
            tableId,
            SnapshotRef.newBuilder().setAsOf(ts("2024-04-01T00:00:00Z")).build(),
            Optional.empty());
    assertThat(asOf.getSnapshotId()).isEqualTo(11);

    // CURRENT serves the finalized snapshot (12 never advanced currency at registration).
    TablePin current = helper.tablePinFor("corr", tableId, null, Optional.empty());
    assertThat(current.getSnapshotId()).isEqualTo(11);
  }

  private static ResourceId tableId(String id) {
    return ResourceId.newBuilder()
        .setAccountId("account")
        .setId(id)
        .setKind(ResourceKind.RK_TABLE)
        .build();
  }

  private static Timestamp ts(String instant) {
    Instant parsed = Instant.parse(instant);
    return Timestamp.newBuilder()
        .setSeconds(parsed.getEpochSecond())
        .setNanos(parsed.getNano())
        .build();
  }

  @Test
  void aRootSupersededBetweenThePointerAndBlobReadsIsReFollowedOnce() {
    // GC can sweep a superseded root blob between tablePinFor's pointer read and its blob read;
    // the pointer has necessarily moved on, so the helper must re-follow it once instead of
    // failing a pin on a live table.
    var ptr = new InMemoryPointerStore();
    var blobStore = new InMemoryBlobStore();
    int[] misses = {1};
    var flakyRoots =
        new TableRootRepository(ptr, blobStore) {
          @Override
          public java.util.Optional<ai.floedb.floecat.catalog.rpc.TableRoot> getByBlobUri(
              String blobUri) {
            if (misses[0] > 0) {
              misses[0]--;
              return java.util.Optional.empty(); // the swept superseded blob
            }
            return super.getByBlobUri(blobUri);
          }
        };
    ResourceId table = tableId("tbl-refollow");
    var flakyCommitter = new TableRootCommitter(flakyRoots);
    flakyCommitter.commit(
        table,
        TableRootMutations.upsertSnapshot(
            flakyRoots,
            table,
            SnapshotManifestEntry.newBuilder()
                .setSnapshotId(11)
                .setSnapshotRef(BlobRef.newBuilder().setUri("s3://t/snap-11.pb").setVersion("v11"))
                .setStatsGenerationRef(BlobRef.newBuilder().setUri("s3://t/stats/11/gen.pb"))
                .setUpstreamCreatedAt(com.google.protobuf.util.Timestamps.fromMillis(11_000))
                .build(),
            BlobRef.newBuilder().setUri("s3://t/table.pb").setVersion("vt").build(),
            true));
    var flakyHelper = new SnapshotHelper(repository, flakyRoots, flakyCommitter, null);

    TablePin pin = flakyHelper.tablePinFor("corr", table, null, Optional.empty());

    assertThat(pin.getSnapshotId()).isEqualTo(11);
  }

  @Test
  void anEntryWithoutASnapshotRefFailsThePinWithThePreciseError() {
    // Every writer records a snapshot ref; an entry without one is a broken root invariant. The
    // pin must fail naming that, not construct an empty-URI pin a downstream validator reports
    // as a generic internal error.
    ResourceId table = tableId("tbl-no-snap-ref");
    var localCommitter = new TableRootCommitter(roots);
    localCommitter.commit(
        table,
        TableRootMutations.upsertSnapshot(
            roots,
            table,
            SnapshotManifestEntry.newBuilder()
                .setSnapshotId(4)
                .setStatsGenerationRef(BlobRef.newBuilder().setUri("s3://t/stats/4/gen.pb"))
                .setUpstreamCreatedAt(com.google.protobuf.util.Timestamps.fromMillis(4_000))
                .build(),
            BlobRef.newBuilder().setUri("s3://t/table.pb").setVersion("vt").build(),
            true));
    var localHelper = new SnapshotHelper(repository, roots, localCommitter, null);

    assertThatThrownBy(() -> localHelper.tablePinFor("corr", table, null, Optional.empty()))
        .hasMessageContaining("INTERNAL");
  }

  @Test
  void currentRejectsASnapshotWhoseGenerationWasRemoved() {
    // Generation removal (deleteAllStats) clears stats_generation_ref WITHOUT clearing currency, so
    // currency can point at an unfinalized snapshot. CURRENT must reject it — consistent with the
    // explicit-id (reject) and AS_OF (skip) paths — not pin a snapshot that would then scan empty.
    var tracking = mock(ai.floedb.floecat.stats.spi.StatsStore.class);
    when(tracking.tracksStatsGenerations()).thenReturn(true);
    helper = new SnapshotHelper(repository, roots, committer, tracking);
    ResourceId tableId = tableId("tbl");
    seedSnapshot(tableId, 7, "2024-02-01T00:00:00Z");
    // Finalize 7 (generation ref present -> becomes current), then remove its generation.
    committer.commit(
        tableId,
        TableRootMutations.upsertSnapshot(
            roots,
            tableId,
            SnapshotManifestEntry.newBuilder()
                .setSnapshotId(7)
                .setSnapshotRef(
                    BlobRef.newBuilder().setUri("s3://tbl/snap-7.pb").setVersion("etag-s7"))
                .setStatsGenerationRef(BlobRef.newBuilder().setUri("s3://tbl/stats/7/gen.pb"))
                .setUpstreamCreatedAt(ts("2024-02-01T00:00:00Z"))
                .build(),
            BlobRef.newBuilder().setUri("s3://tbl/table.pb").setVersion("etag-t").build(),
            true));
    committer.commit(tableId, TableRootMutations.setStatsGeneration(roots, tableId, 7, null));

    var root = roots.get(tableId).orElseThrow();
    org.junit.jupiter.api.Assertions.assertEquals(
        7L, root.getCurrentSnapshotId(), "currency stays on 7 after generation removal");
    assertThatThrownBy(() -> helper.tablePinFor("corr", tableId, null, Optional.empty()))
        .hasMessageContaining("NOT_FOUND");
  }
}
