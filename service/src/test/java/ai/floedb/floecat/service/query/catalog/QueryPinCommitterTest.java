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

package ai.floedb.floecat.service.query.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.RelationPinSet;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport.TestQueryContextStore;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.testsupport.SnapshotTestSupport;
import ai.floedb.floecat.telemetry.PhaseDiagnostics;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Direct tests of {@link QueryPinCommitter}: the collect→commit pin-durability transaction the
 * {@link UserObjectBundleService} conductor drives per chunk. {@code accumulate} folds the
 * resolver's pins into the pending set; {@code commit} writes them durably to the QueryContext
 * exactly once and, on any failure arm, releases the transient GC roots registered during
 * resolution. The committer deliberately receives only immutable pin sets: resolution is owned by
 * the caller, not repeated here.
 */
class QueryPinCommitterTest {

  private static final String QID = "q-1";
  private static final String CID = "cid";

  private static final ResourceId CATALOG =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("catalog")
          .setKind(ResourceKind.RK_CATALOG)
          .build();

  private static final ResourceId TABLE_A =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("TABLE_A")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  private static final ResourceId TABLE_B =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("TABLE_B")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  private static final ResourceId TABLE_C =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("TABLE_C")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  private TimingAccumulator timings;

  @BeforeEach
  void setUp() {
    timings = new TimingAccumulator();
  }

  @Test
  void accumulateGrowsPendingPinCountAcrossRelations() {
    TestQueryContextStore store = seededStore();
    QueryPinCommitter committer = new QueryPinCommitter(store, ctx(), CID, timings);

    assertThat(committer.pendingPinCount()).isZero();

    committer.accumulate(
        SnapshotTestSupport.relationPins(
            SnapshotTestSupport.blobBackedPin(TABLE_A, 1),
            SnapshotTestSupport.blobBackedPin(TABLE_B, 1)),
        PhaseDiagnostics.NOOP);
    assertThat(committer.pendingPinCount()).isEqualTo(2);

    committer.accumulate(
        SnapshotTestSupport.relationPins(SnapshotTestSupport.blobBackedPin(TABLE_C, 1)),
        PhaseDiagnostics.NOOP);
    assertThat(committer.pendingPinCount()).isEqualTo(3);
  }

  @Test
  void commitWritesToQueryContextExactlyOnceAndIsDurable() {
    TestQueryContextStore store = seededStore();
    QueryPinCommitter committer = new QueryPinCommitter(store, ctx(), CID, timings);

    committer.accumulate(
        SnapshotTestSupport.relationPins(
            SnapshotTestSupport.blobBackedPin(TABLE_A, 1),
            SnapshotTestSupport.blobBackedPin(TABLE_B, 1)),
        PhaseDiagnostics.NOOP);
    committer.commit();

    // One durable write; the pending set is drained.
    assertThat(store.updateCount()).isEqualTo(1);
    assertThat(committer.pendingPinCount()).isZero();

    // The pins are durable on the stored context.
    QueryContext durable = store.get(QID).orElseThrow();
    RelationPinSet persisted = durable.parseRelationPins(CID);
    assertThat(persisted.getPinsCount()).isEqualTo(2);

    // A second commit with nothing pending does no further work.
    committer.commit();
    assertThat(store.updateCount()).isEqualTo(1);
  }

  @Test
  void commitFailureReleasesResolvingPinBlobs() {
    RecordingReleaseStore store = new RecordingReleaseStore();
    store.seed(ctx());
    store.failUpdateWith(new IllegalStateException("boom"));
    QueryPinCommitter committer = new QueryPinCommitter(store, ctx(), CID, timings);

    committer.accumulate(
        SnapshotTestSupport.relationPins(SnapshotTestSupport.blobBackedPin(TABLE_A, 1)),
        PhaseDiagnostics.NOOP);

    assertThatThrownBy(committer::commit).isInstanceOf(IllegalStateException.class);
    // The transient GC roots registered at resolution are released on the failure arm.
    assertThat(store.releasedQueryIds()).containsExactly(QID);
    assertThat(store.releasedBlobUris()).isNotEmpty();
  }

  @Test
  void cancellationBeforeCommitReleasesPendingRootsWithoutUpdatingContext() {
    RecordingReleaseStore store = new RecordingReleaseStore();
    store.seed(ctx());
    QueryPinCommitter committer = new QueryPinCommitter(store, ctx(), CID, timings);
    AtomicBoolean cancelled = new AtomicBoolean();

    committer.accumulate(
        SnapshotTestSupport.relationPins(SnapshotTestSupport.blobBackedPin(TABLE_A, 1)),
        PhaseDiagnostics.NOOP,
        cancelled::get);
    cancelled.set(true);

    assertThatThrownBy(() -> committer.commit(cancelled::get))
        .isInstanceOf(CancellationException.class);
    assertThat(store.updateCount()).isZero();
    assertThat(committer.pendingPinCount()).isZero();
    assertThat(store.releasedQueryIds()).containsExactly(QID);
    assertThat(store.releasedBlobUris()).isNotEmpty();
  }

  @Test
  void accumulateMergeFailureReleasesPriorAndIncomingPinBlobs() {
    RecordingReleaseStore store = new RecordingReleaseStore();
    store.seed(ctx());
    QueryPinCommitter committer = new QueryPinCommitter(store, ctx(), CID, timings);

    committer.accumulate(
        SnapshotTestSupport.relationPins(
            SnapshotTestSupport.blobBackedPin(TABLE_A, 1).toBuilder()
                .setPinKind(ai.floedb.floecat.query.rpc.PinKind.PIN_KIND_SNAPSHOT_ID)
                .build()),
        PhaseDiagnostics.NOOP);

    assertThatThrownBy(
            () ->
                committer.accumulate(
                    SnapshotTestSupport.relationPins(
                        SnapshotTestSupport.blobBackedPin(TABLE_A, 2).toBuilder()
                            .setPinKind(ai.floedb.floecat.query.rpc.PinKind.PIN_KIND_SNAPSHOT_ID)
                            .build()),
                    PhaseDiagnostics.NOOP))
        .isInstanceOf(RuntimeException.class);

    assertThat(committer.pendingPinCount()).isZero();
    assertThat(store.releasedQueryIds()).containsExactly(QID);
    assertThat(store.releasedBlobUris())
        .contains("s3://TABLE_A/snap-1.pb", "s3://TABLE_A/snap-2.pb");
  }

  @Test
  void emptyAccumulateThenCommitIsANoOp() {
    RecordingReleaseStore store = new RecordingReleaseStore();
    store.seed(ctx());
    QueryPinCommitter committer = new QueryPinCommitter(store, ctx(), CID, timings);

    committer.accumulate(RelationPinSet.getDefaultInstance(), PhaseDiagnostics.NOOP);
    assertThat(committer.pendingPinCount()).isZero();

    committer.commit();
    assertThat(store.updateCount()).isZero();
    assertThat(store.releasedQueryIds()).isEmpty();
  }

  private TestQueryContextStore seededStore() {
    TestQueryContextStore store = new TestQueryContextStore();
    store.seed(ctx());
    return store;
  }

  private static QueryContext ctx() {
    return QueryContext.builder()
        .queryId(QID)
        .principal(
            PrincipalContext.newBuilder()
                .setAccountId("acct")
                .setSubject("tester")
                .setCorrelationId(CID)
                .build())
        .relationPins(RelationPinSet.getDefaultInstance().toByteArray())
        .createdAtMs(1)
        .expiresAtMs(1000)
        .state(QueryContext.State.ACTIVE)
        .version(1)
        .queryDefaultCatalogId(CATALOG)
        .build();
  }

  /**
   * A store that records {@code releaseResolvingPinBlobs} calls and can fail {@code update}. Wraps
   * a {@link TestQueryContextStore} (which is final) by delegation.
   */
  private static final class RecordingReleaseStore implements QueryContextStore {
    private final TestQueryContextStore delegate = new TestQueryContextStore();
    private final List<String> releasedQueryIds = new ArrayList<>();
    private final List<String> releasedBlobUris = new ArrayList<>();
    private RuntimeException updateFailure;

    void seed(QueryContext ctx) {
      delegate.seed(ctx);
    }

    void failUpdateWith(RuntimeException failure) {
      this.updateFailure = failure;
    }

    int updateCount() {
      return delegate.updateCount();
    }

    List<String> releasedQueryIds() {
      return releasedQueryIds;
    }

    List<String> releasedBlobUris() {
      return releasedBlobUris;
    }

    @Override
    public java.util.Optional<QueryContext> update(String queryId, UnaryOperator<QueryContext> fn) {
      if (updateFailure != null) {
        throw updateFailure;
      }
      return delegate.update(queryId, fn);
    }

    @Override
    public void releaseResolvingPinBlobs(String queryId, Collection<String> blobUris) {
      releasedQueryIds.add(queryId);
      releasedBlobUris.addAll(blobUris);
    }

    @Override
    public java.util.Optional<QueryContext> get(String queryId) {
      return delegate.get(queryId);
    }

    @Override
    public void put(QueryContext ctx) {
      delegate.put(ctx);
    }

    @Override
    public boolean putIfAbsent(QueryContext ctx) {
      return delegate.putIfAbsent(ctx);
    }

    @Override
    public java.util.Optional<QueryContext> extendLease(String queryId, long requestedExpiresAtMs) {
      return delegate.extendLease(queryId, requestedExpiresAtMs);
    }

    @Override
    public java.util.Optional<QueryContext> end(String queryId, boolean commit) {
      return delegate.end(queryId, commit);
    }

    @Override
    public boolean delete(String queryId) {
      return delegate.delete(queryId);
    }

    @Override
    public long size() {
      return delegate.size();
    }

    @Override
    public java.util.Set<String> referencedPinBlobUris() {
      return delegate.referencedPinBlobUris();
    }

    @Override
    public void registerResolvingPinBlobs(
        String queryId, ResourceId tableId, Collection<String> blobUris) {
      delegate.registerResolvingPinBlobs(queryId, tableId, blobUris);
    }

    @Override
    public void replace(QueryContext ctx) {
      delegate.replace(ctx);
    }

    @Override
    public ai.floedb.floecat.query.rpc.ScanHandle createScanSession(
        String correlationId, ai.floedb.floecat.service.query.impl.ScanSession session) {
      return delegate.createScanSession(correlationId, session);
    }

    @Override
    public java.util.Optional<ai.floedb.floecat.service.query.impl.ScanSession> getScanSession(
        ai.floedb.floecat.query.rpc.ScanHandle handle) {
      return delegate.getScanSession(handle);
    }

    @Override
    public void removeScanSession(ai.floedb.floecat.query.rpc.ScanHandle handle) {
      delegate.removeScanSession(handle);
    }

    @Override
    public void close() {
      delegate.close();
    }
  }
}
