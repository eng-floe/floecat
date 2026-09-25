/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package ai.floedb.floecat.service.query.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.PinKind;
import ai.floedb.floecat.query.rpc.RelationPinSet;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport.FakeCatalogGraphView;
import com.google.protobuf.Timestamp;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Contract coverage for input resolution after snapshot selection stopped being a GC root. */
class QueryInputResolverBehaviorTest {
  private static final ResourceId TABLE =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("table")
          .setKind(ResourceKind.RK_TABLE)
          .build();
  private static final ResourceId OTHER_TABLE =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("other")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  private FakeCatalogGraphView graph;
  private QueryInputResolver resolver;

  @BeforeEach
  void setUp() {
    graph = new FakeCatalogGraphView();
    graph.registerTable(TABLE, List.of(), name("cat", "table"));
    graph.registerTable(OTHER_TABLE, List.of(), name("cat", "other"));
    resolver = new QueryInputResolver(graph);
  }

  @Test
  void resolvesNamesAndDirectIdsInRequestOrder() {
    var result =
        resolver.resolveInputs(
            "cid",
            List.of(
                QueryInput.newBuilder().setName(name("cat", "table")).build(),
                QueryInput.newBuilder().setTableId(OTHER_TABLE).build()),
            Optional.empty(),
            Optional.empty());

    assertThat(result.resolved()).containsExactly(TABLE, OTHER_TABLE);
    assertThat(result.relationPinSet().getPinsCount()).isEqualTo(2);
  }

  @Test
  void repeatedCurrentReferencesShareOneResolvedSelection() {
    var input = QueryInput.newBuilder().setTableId(TABLE).build();

    var result =
        resolver.resolveInputs("cid", List.of(input, input), Optional.empty(), Optional.empty());

    assertThat(result.resolved()).containsExactly(TABLE, TABLE);
    assertThat(result.relationPinSet().getPinsCount()).isEqualTo(1);
  }

  @Test
  void repeatedExplicitSnapshotsShareOneResolvedSelection() {
    QueryInput input =
        QueryInput.newBuilder()
            .setTableId(TABLE)
            .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(7).build())
            .build();

    var result =
        resolver.resolveInputs("cid", List.of(input, input), Optional.empty(), Optional.empty());

    assertThat(result.resolved()).containsExactly(TABLE, TABLE);
    assertThat(result.relationPinSet().getPinsCount()).isEqualTo(1);
    assertThat(result.relationPinSet().getPins(0).getTablePin().getSnapshotId()).isEqualTo(7);
  }

  @Test
  void repeatedAsOfSelectionsShareOneResolvedSelection() {
    Timestamp asOf = Timestamp.newBuilder().setSeconds(1_700_000_000L).build();
    QueryInput input =
        QueryInput.newBuilder()
            .setTableId(TABLE)
            .setSnapshot(SnapshotRef.newBuilder().setAsOf(asOf).build())
            .build();

    var result =
        resolver.resolveInputs("cid", List.of(input, input), Optional.empty(), Optional.empty());

    assertThat(result.relationPinSet().getPinsCount()).isEqualTo(1);
    assertThat(result.relationPinSet().getPins(0).getTablePin().getOriginalAsOf()).isEqualTo(asOf);
  }

  @Test
  void snapshotIdZeroIsARealResolvedSnapshot() {
    TablePin pin =
        resolve(
                QueryInput.newBuilder()
                    .setTableId(TABLE)
                    .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(0).build())
                    .build())
            .getPins(0)
            .getTablePin();

    assertThat(pin.getPinKind()).isEqualTo(PinKind.PIN_KIND_SNAPSHOT_ID);
    assertThat(pin.getSnapshotId()).isZero();
  }

  @Test
  void explicitSnapshotAndAsOfSelectionsPreserveTheirResolvedProvenance() {
    Timestamp asOf = Timestamp.newBuilder().setSeconds(1_700_000_000L).build();
    TablePin explicit =
        resolve(
                QueryInput.newBuilder()
                    .setTableId(TABLE)
                    .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(7))
                    .build())
            .getPins(0)
            .getTablePin();
    TablePin asOfPin =
        resolve(
                QueryInput.newBuilder()
                    .setTableId(OTHER_TABLE)
                    .setSnapshot(SnapshotRef.newBuilder().setAsOf(asOf))
                    .build())
            .getPins(0)
            .getTablePin();

    assertThat(explicit.getPinKind()).isEqualTo(PinKind.PIN_KIND_SNAPSHOT_ID);
    assertThat(explicit.getSnapshotId()).isEqualTo(7);
    assertThat(asOfPin.getPinKind()).isEqualTo(PinKind.PIN_KIND_AS_OF);
    assertThat(asOfPin.getOriginalAsOf()).isEqualTo(asOf);
  }

  @Test
  void currentOverrideTakesPrecedenceOverAsOfDefault() {
    Timestamp asOf = Timestamp.newBuilder().setSeconds(1_700_000_000L).build();
    TablePin pin =
        resolve(
                QueryInput.newBuilder()
                    .setTableId(TABLE)
                    .setSnapshot(
                        SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT).build())
                    .build(),
                Optional.of(asOf))
            .getPins(0)
            .getTablePin();

    assertThat(pin.getPinKind()).isEqualTo(PinKind.PIN_KIND_CURRENT);
    assertThat(pin.hasOriginalAsOf()).isFalse();
  }

  @Test
  void asOfDefaultAppliesWhenInputHasNoOverride() {
    Timestamp asOf = Timestamp.newBuilder().setSeconds(1_700_000_000L).build();
    TablePin pin =
        resolve(QueryInput.newBuilder().setTableId(TABLE).build(), Optional.of(asOf))
            .getPins(0)
            .getTablePin();

    assertThat(pin.getPinKind()).isEqualTo(PinKind.PIN_KIND_AS_OF);
    assertThat(pin.getOriginalAsOf()).isEqualTo(asOf);
  }

  @Test
  void emptySnapshotOverrideFallsBackToCurrent() {
    TablePin pin =
        resolve(
                QueryInput.newBuilder()
                    .setTableId(TABLE)
                    .setSnapshot(SnapshotRef.getDefaultInstance())
                    .build())
            .getPins(0)
            .getTablePin();

    assertThat(pin.getPinKind()).isEqualTo(PinKind.PIN_KIND_CURRENT);
  }

  @Test
  void snapshotOverrideUsesTheLastOneofFieldSet() {
    SnapshotRef ref =
        SnapshotRef.newBuilder()
            .setSnapshotId(555)
            .setAsOf(Timestamp.newBuilder().setSeconds(999).build())
            .build();

    assertThat(ref.getWhichCase()).isEqualTo(SnapshotRef.WhichCase.AS_OF);
    assertThat(ref.hasSnapshotId()).isFalse();
  }

  @Test
  void incompatibleSelectionsForOneTableFailInsteadOfSilentlyChangingSnapshot() {
    QueryInput first =
        QueryInput.newBuilder()
            .setTableId(TABLE)
            .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(7))
            .build();
    QueryInput second =
        QueryInput.newBuilder()
            .setTableId(TABLE)
            .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(8))
            .build();

    assertThatThrownBy(
            () ->
                resolver.resolveInputs(
                    "cid", List.of(first, second), Optional.empty(), Optional.empty()))
        .isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  void unresolvedNameFailsWithoutCreatingASelection() {
    QueryInput missing = QueryInput.newBuilder().setName(name("cat", "missing")).build();

    assertThatThrownBy(
            () ->
                resolver.resolveInputs("cid", List.of(missing), Optional.empty(), Optional.empty()))
        .isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  void viewBaseRelationGetsTheDefaultCatalogBeforeResolution() {
    ResourceId catalog =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("catalog")
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    graph.registerCatalog(catalog, "mycat");
    ResourceId base =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("customers")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    NameRef enriched =
        NameRef.newBuilder().setCatalog("mycat").addPath("sales").setName("customers").build();
    graph.registerTable(base, List.of(), enriched);
    ResourceId view =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("view")
            .setKind(ResourceKind.RK_VIEW)
            .build();
    graph.registerRelation(
        view,
        viewNode(
            view,
            List.of(NameRef.newBuilder().addPath("sales").setName("customers").build()),
            List.of()),
        List.of(),
        name("cat", "view"));

    var result =
        resolver.resolveInputs(
            "cid",
            List.of(QueryInput.newBuilder().setViewId(view).build()),
            Optional.empty(),
            Optional.of(catalog));

    assertThat(result.resolved()).containsExactly(view);
    assertThat(result.relationPinSet().getPins(0).getTablePin().getTableId()).isEqualTo(base);
  }

  @Test
  void viewBaseRelationGetsItsCreationSearchPathBeforeResolution() {
    ResourceId base =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("orders")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    NameRef enriched =
        NameRef.newBuilder().setCatalog("cat").addPath("reporting").setName("orders").build();
    graph.registerTable(base, List.of(), enriched);
    ResourceId view =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("view-path")
            .setKind(ResourceKind.RK_VIEW)
            .build();
    graph.registerRelation(
        view,
        viewNode(
            view,
            List.of(NameRef.newBuilder().setCatalog("cat").setName("orders").build()),
            List.of("reporting")),
        List.of(),
        name("cat", "view-path"));

    var result =
        resolver.resolveInputs(
            "cid",
            List.of(QueryInput.newBuilder().setViewId(view).build()),
            Optional.empty(),
            Optional.empty());

    assertThat(result.relationPinSet().getPins(0).getTablePin().getTableId()).isEqualTo(base);
  }

  @Test
  void viewBaseRelationWithResourceIdBypassesNameEnrichment() {
    ResourceId base =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("direct")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    graph.registerTable(base, List.of(), name("wrong", "name"));
    ResourceId view =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("view-direct")
            .setKind(ResourceKind.RK_VIEW)
            .build();
    graph.registerRelation(
        view,
        viewNode(
            view, List.of(NameRef.newBuilder().setResourceId(base).build()), List.of("reporting")),
        List.of(),
        name("cat", "view-direct"));

    var result =
        resolver.resolveInputs(
            "cid",
            List.of(QueryInput.newBuilder().setViewId(view).build()),
            Optional.empty(),
            Optional.empty());

    assertThat(result.relationPinSet().getPins(0).getTablePin().getTableId()).isEqualTo(base);
  }

  @Test
  void nestedViewsResolveTheirBaseTableOnce() {
    ResourceId inner =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("inner")
            .setKind(ResourceKind.RK_VIEW)
            .build();
    ResourceId outer =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("outer")
            .setKind(ResourceKind.RK_VIEW)
            .build();
    graph.registerRelation(
        inner,
        viewNode(inner, List.of(name("cat", "table")), List.of()),
        List.of(),
        name("cat", "inner"));
    graph.registerRelation(
        outer,
        viewNode(outer, List.of(NameRef.newBuilder().setResourceId(inner).build()), List.of()),
        List.of(),
        name("cat", "outer"));

    var result =
        resolver.resolveInputs(
            "cid",
            List.of(QueryInput.newBuilder().setViewId(outer).build()),
            Optional.empty(),
            Optional.empty());

    assertThat(result.resolved()).containsExactly(outer);
    assertThat(result.relationPinSet().getPins(0).getTablePin().getTableId()).isEqualTo(TABLE);
  }

  @Test
  void nestedNamePathsResolveThroughTheGraph() {
    ResourceId nested =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("nested-path")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    NameRef qualified =
        NameRef.newBuilder()
            .setCatalog("cat")
            .addPath("sales")
            .addPath("archive")
            .setName("nested")
            .build();
    graph.registerTable(nested, List.of(), qualified);

    var result =
        resolver.resolveInputs(
            "cid",
            List.of(QueryInput.newBuilder().setName(qualified).build()),
            Optional.empty(),
            Optional.empty());

    assertThat(result.resolved()).containsExactly(nested);
  }

  @Test
  void currentSelectionMemoIsReusableAcrossResolutionCalls() {
    AtomicInteger snapshotCalls = new AtomicInteger();
    FakeCatalogGraphView countingGraph =
        new FakeCatalogGraphView() {
          @Override
          public TablePin resolvedSnapshotFor(
              String correlationId,
              ResourceId tableId,
              SnapshotRef override,
              Optional<Timestamp> asOfDefault) {
            snapshotCalls.incrementAndGet();
            return super.resolvedSnapshotFor(correlationId, tableId, override, asOfDefault);
          }
        };
    countingGraph.registerTable(TABLE, List.of(), name("cat", "table"));
    QueryInputResolver memoResolver = new QueryInputResolver(countingGraph);
    var memo = new QueryInputResolver.SnapshotSelectionMemo();
    QueryInput input = QueryInput.newBuilder().setTableId(TABLE).build();

    memoResolver.resolveInputs(
        "q-1", "cid", List.of(input), Optional.empty(), Optional.empty(), memo, null);
    memoResolver.resolveInputs(
        "q-2", "cid", List.of(input), Optional.empty(), Optional.empty(), memo, null);

    assertThat(snapshotCalls).hasValue(1);
  }

  @Test
  void nonConcurrentGraphResolutionStaysOnTheCallerThread() {
    CopyOnWriteArrayList<Thread> threads = new CopyOnWriteArrayList<>();
    FakeCatalogGraphView threadCheckingGraph =
        new FakeCatalogGraphView() {
          @Override
          public TablePin resolvedSnapshotFor(
              String correlationId,
              ResourceId tableId,
              SnapshotRef override,
              Optional<Timestamp> asOfDefault) {
            threads.add(Thread.currentThread());
            return super.resolvedSnapshotFor(correlationId, tableId, override, asOfDefault);
          }
        };
    threadCheckingGraph.registerTable(TABLE, List.of(), name("cat", "table"));
    threadCheckingGraph.registerTable(OTHER_TABLE, List.of(), name("cat", "other"));
    QueryInputResolver threadCheckingResolver = new QueryInputResolver(threadCheckingGraph);
    Thread caller = Thread.currentThread();

    threadCheckingResolver.resolveInputs(
        "cid",
        List.of(
            QueryInput.newBuilder().setTableId(TABLE).build(),
            QueryInput.newBuilder().setTableId(OTHER_TABLE).build()),
        Optional.empty(),
        Optional.empty());

    assertThat(threads).hasSize(2).allMatch(captured -> captured == caller);
  }

  @Test
  void cancellationInterruptsConcurrentSnapshotResolution() throws Exception {
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    FakeCatalogGraphView blockingGraph =
        new FakeCatalogGraphView() {
          @Override
          public boolean supportsConcurrentResolution() {
            return true;
          }

          @Override
          public TablePin resolvedSnapshotFor(
              String correlationId,
              ResourceId tableId,
              SnapshotRef override,
              Optional<Timestamp> asOfDefault) {
            started.countDown();
            try {
              release.await();
            } catch (InterruptedException interrupted) {
              Thread.currentThread().interrupt();
              throw new CancellationException("snapshot resolution interrupted");
            }
            return super.resolvedSnapshotFor(correlationId, tableId, override, asOfDefault);
          }
        };
    blockingGraph.registerTable(TABLE, List.of(), name("cat", "table"));
    blockingGraph.registerTable(OTHER_TABLE, List.of(), name("cat", "other"));
    QueryInputResolver blockingResolver = new QueryInputResolver(blockingGraph);
    AtomicBoolean cancelled = new AtomicBoolean();
    CompletableFuture<?> resolution =
        CompletableFuture.runAsync(
            () ->
                blockingResolver.resolveInputs(
                    "",
                    "cid",
                    List.of(
                        QueryInput.newBuilder().setTableId(TABLE).build(),
                        QueryInput.newBuilder().setTableId(OTHER_TABLE).build()),
                    Optional.empty(),
                    Optional.empty(),
                    new QueryInputResolver.SnapshotSelectionMemo(),
                    null,
                    cancelled::get));
    try {
      assertThat(started.await(1, TimeUnit.SECONDS)).isTrue();
      cancelled.set(true);
      assertThatThrownBy(() -> resolution.get(1, TimeUnit.SECONDS))
          .hasCauseInstanceOf(CancellationException.class);
    } finally {
      release.countDown();
    }
  }

  @Test
  void viewSnapshotOverrideIsRejected() {
    ResourceId view =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("view-override")
            .setKind(ResourceKind.RK_VIEW)
            .build();
    graph.registerRelation(
        view,
        viewNode(view, List.of(name("cat", "table")), List.of()),
        List.of(),
        name("cat", "view-override"));

    assertThatThrownBy(
            () ->
                resolver.resolveInputs(
                    "cid",
                    List.of(
                        QueryInput.newBuilder()
                            .setViewId(view)
                            .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(7).build())
                            .build()),
                    Optional.empty(),
                    Optional.empty()))
        .isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  void viewAsOfOverrideAppliesToBaseTableSelection() {
    ResourceId view =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("view-asof")
            .setKind(ResourceKind.RK_VIEW)
            .build();
    graph.registerRelation(
        view,
        viewNode(view, List.of(name("cat", "table")), List.of()),
        List.of(),
        name("cat", "view-asof"));
    Timestamp asOf = Timestamp.newBuilder().setSeconds(202).build();

    var result =
        resolver.resolveInputs(
            "cid",
            List.of(
                QueryInput.newBuilder()
                    .setViewId(view)
                    .setSnapshot(SnapshotRef.newBuilder().setAsOf(asOf).build())
                    .build()),
            Optional.empty(),
            Optional.empty());

    assertThat(result.relationPinSet().getPins(0).getTablePin().getOriginalAsOf()).isEqualTo(asOf);
  }

  @Test
  void anEarlierSnapshotConflictWinsOverLaterMalformedInput() {
    QueryInput first =
        QueryInput.newBuilder()
            .setTableId(TABLE)
            .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(7).build())
            .build();
    QueryInput second =
        QueryInput.newBuilder()
            .setTableId(TABLE)
            .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(8).build())
            .build();

    assertThatThrownBy(
            () ->
                resolver.resolveInputs(
                    "cid",
                    List.of(first, second, QueryInput.getDefaultInstance()),
                    Optional.empty(),
                    Optional.empty()))
        .isInstanceOf(StatusRuntimeException.class)
        .extracting(error -> ((StatusRuntimeException) error).getStatus().getCode())
        .isEqualTo(io.grpc.Status.Code.FAILED_PRECONDITION);
  }

  private static ViewNode viewNode(ResourceId id, List<NameRef> bases, List<String> searchPath) {
    return new ViewNode(
        id,
        "blob://test/view",
        ResourceId.getDefaultInstance(),
        ResourceId.getDefaultInstance(),
        id.getId(),
        "SELECT 1",
        "test",
        List.of(),
        bases,
        searchPath,
        GraphNodeOrigin.USER,
        java.util.Map.of(),
        Optional.empty(),
        java.util.Map.of(),
        java.util.Map.of());
  }

  private RelationPinSet resolve(QueryInput input) {
    return resolve(input, Optional.empty());
  }

  private RelationPinSet resolve(QueryInput input, Optional<Timestamp> asOfDefault) {
    return resolver
        .resolveInputs("cid", List.of(input), asOfDefault, Optional.empty())
        .relationPinSet();
  }

  private static NameRef name(String catalog, String table) {
    return NameRef.newBuilder().setCatalog(catalog).setName(table).build();
  }
}
