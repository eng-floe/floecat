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

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.query.rpc.RelationPinSet;
import ai.floedb.floecat.query.rpc.TableReferenceCandidate;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.catalog.UserObjectBundleService.ResolvedRelation;
import ai.floedb.floecat.service.query.catalog.UserObjectBundleService.TimingAccumulator;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport.FakeCatalogOverlay;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport.TestQueryContextStore;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport.TestQueryInputResolver;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.telemetry.PhaseDiagnostics;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Direct tests of {@link ChunkPinBarrier}: the collect→commit pin-durability transaction the {@link
 * UserObjectBundleService} conductor drives per chunk. {@code accumulate} folds the resolver's pins
 * into the pending set; {@code commit} writes them durably to the QueryContext exactly once and, on
 * any failure arm, releases the transient GC roots the resolver registered at resolution. Uses the
 * shared {@link TestQueryInputResolver} + {@link TestQueryContextStore} fakes over a real {@link
 * TimingAccumulator}.
 */
class ChunkPinBarrierTest {

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

  private final FakeCatalogOverlay overlay = new FakeCatalogOverlay();
  private TestQueryInputResolver resolver;
  private TimingAccumulator timings;

  @BeforeEach
  void setUp() {
    overlay.clear();
    registerTable(TABLE_A, "a");
    registerTable(TABLE_B, "b");
    registerTable(TABLE_C, "c");
    resolver = new TestQueryInputResolver();
    timings = new TimingAccumulator();
  }

  @Test
  void accumulateGrowsPendingPinCountAcrossRelations() {
    TestQueryContextStore store = seededStore();
    ChunkPinBarrier barrier = new ChunkPinBarrier(resolver, store, ctx(), CID, timings);

    assertThat(barrier.pendingPinCount()).isZero();

    barrier.accumulate(List.of(resolved(TABLE_A), resolved(TABLE_B)), PhaseDiagnostics.NOOP);
    // The fake resolver mints one pin per TABLE_ID input.
    assertThat(barrier.pendingPinCount()).isEqualTo(2);

    barrier.accumulate(List.of(resolved(TABLE_C)), PhaseDiagnostics.NOOP);
    assertThat(barrier.pendingPinCount()).isEqualTo(3);
  }

  @Test
  void commitWritesToQueryContextExactlyOnceAndIsDurable() {
    TestQueryContextStore store = seededStore();
    ChunkPinBarrier barrier = new ChunkPinBarrier(resolver, store, ctx(), CID, timings);

    barrier.accumulate(List.of(resolved(TABLE_A), resolved(TABLE_B)), PhaseDiagnostics.NOOP);
    barrier.commit();

    // One durable write; the pending set is drained.
    assertThat(store.updateCount()).isEqualTo(1);
    assertThat(barrier.pendingPinCount()).isZero();

    // The pins are durable on the stored context.
    QueryContext durable = store.get(QID).orElseThrow();
    RelationPinSet persisted = durable.parseRelationPins(CID);
    assertThat(persisted.getPinsCount()).isEqualTo(2);

    // A second commit with nothing pending does no further work.
    barrier.commit();
    assertThat(store.updateCount()).isEqualTo(1);
  }

  @Test
  void commitFailureReleasesResolvingPinBlobs() {
    RecordingReleaseStore store = new RecordingReleaseStore();
    store.seed(ctx());
    store.failUpdateWith(new IllegalStateException("boom"));
    ChunkPinBarrier barrier = new ChunkPinBarrier(resolver, store, ctx(), CID, timings);

    barrier.accumulate(List.of(resolved(TABLE_A)), PhaseDiagnostics.NOOP);

    assertThatThrownBy(barrier::commit).isInstanceOf(IllegalStateException.class);
    // The transient GC roots registered at resolution are released on the failure arm.
    assertThat(store.releasedQueryIds()).containsExactly(QID);
    assertThat(store.releasedBlobUris()).isNotEmpty();
  }

  @Test
  void emptyAccumulateThenCommitIsANoOp() {
    RecordingReleaseStore store = new RecordingReleaseStore();
    store.seed(ctx());
    ChunkPinBarrier barrier = new ChunkPinBarrier(resolver, store, ctx(), CID, timings);

    barrier.accumulate(List.of(), PhaseDiagnostics.NOOP);
    assertThat(barrier.pendingPinCount()).isZero();

    barrier.commit();
    assertThat(store.updateCount()).isZero();
    assertThat(store.releasedQueryIds()).isEmpty();
  }

  private void registerTable(ResourceId id, String name) {
    overlay.registerTable(
        id,
        UserObjectBundleTestSupport.schemaFor("id_" + name),
        NameRef.newBuilder().setCatalog("cat").setName(name).build());
  }

  private ResolvedRelation resolved(ResourceId table) {
    RelationNode node = (RelationNode) overlay.resolve(table).orElseThrow();
    return new ResolvedRelation(
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(table))
            .build(),
        table,
        node,
        QueryInput.newBuilder().setTableId(table).build());
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
    public void registerResolvingPinBlobs(String correlationId, Collection<String> blobUris) {
      delegate.registerResolvingPinBlobs(correlationId, blobUris);
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
