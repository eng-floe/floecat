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

package ai.floedb.floecat.service.query.resolver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.PinKind;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.concurrent.UninterruptibleBlocker;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import com.google.protobuf.Timestamp;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class QueryInputResolverTest {

  QueryInputResolver resolver;
  FakeGraph metadataGraph;

  @BeforeEach
  void init() {
    metadataGraph = new FakeGraph();
    resolver = new QueryInputResolver(metadataGraph);
  }

  @Test
  void legacyMapOverloadDoesNotRewriteAnExistingCacheHit() {
    ResourceId tableId = rid("CACHED");
    TablePin cachedPin =
        metadataGraph.tablePinFor(
            "cid", tableId, SnapshotRef.getDefaultInstance(), Optional.empty());

    var result =
        resolver.resolveInputs(
            "",
            "cid",
            List.of(QueryInput.newBuilder().setTableId(tableId).build()),
            Optional.empty(),
            Optional.empty(),
            Map.of(tableId, cachedPin),
            null);

    assertEquals(tableId, result.resolved().getFirst());
    assertEquals(cachedPin, result.relationPinSet().getPins(0).getTablePin());
  }

  NameRef name(String cat, String... parts) {
    NameRef.Builder b = NameRef.newBuilder().setCatalog(cat);
    for (int i = 0; i < parts.length - 1; i++) {
      b.addPath(parts[i]);
    }
    b.setName(parts[parts.length - 1]);
    return b.build();
  }

  ResourceId rid(String id) {
    return ResourceId.newBuilder().setId(id).setKind(ResourceKind.RK_TABLE).build();
  }

  ResourceId viewRid(String id) {
    return ResourceId.newBuilder().setId(id).setKind(ResourceKind.RK_VIEW).build();
  }

  /** Runs a cancellable resolution without coupling the test to the caller's executor. */
  private static CompletableFuture<Throwable> resolveCancellable(Runnable resolution) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            resolution.run();
            return null;
          } catch (Throwable failure) {
            return failure;
          }
        });
  }

  /** Verifies cancellation returns promptly even when the active metadata operation ignores it. */
  private static void assertCancellationReturnsWhileBlocked(
      CompletableFuture<Throwable> resolution,
      java.util.concurrent.atomic.AtomicBoolean cancelled,
      UninterruptibleBlocker blocker)
      throws Exception {
    try {
      assertTrue(blocker.started.await(1, TimeUnit.SECONDS));
      cancelled.set(true);
      assertTrue(
          resolution.get(250, TimeUnit.MILLISECONDS)
              instanceof java.util.concurrent.CancellationException);
    } finally {
      blocker.release.countDown();
    }
  }

  @Test
  void typedCancellableOverloadDoesNotDispatchThroughLegacyMapOverride() {
    AtomicBoolean legacyOverrideCalled = new AtomicBoolean();
    QueryInputResolver legacySubclass =
        new QueryInputResolver(metadataGraph) {
          @Override
          public ResolutionResult resolveInputs(
              String queryId,
              String correlationId,
              List<QueryInput> inputs,
              Optional<Timestamp> asOfDefault,
              Optional<ResourceId> defaultCatalogId,
              Map<ResourceId, TablePin> currentSnapshotPinCache,
              ai.floedb.floecat.telemetry.PhaseDiagnostics diagnostics) {
            legacyOverrideCalled.set(true);
            return super.resolveInputs(
                queryId,
                correlationId,
                inputs,
                asOfDefault,
                defaultCatalogId,
                currentSnapshotPinCache,
                diagnostics);
          }
        };
    ResourceId tableId = rid("legacy-dispatch");

    var result =
        legacySubclass.resolveInputs(
            "q-legacy-dispatch",
            "cid",
            List.of(QueryInput.newBuilder().setTableId(tableId).build()),
            Optional.empty(),
            Optional.empty(),
            new ConcurrentHashMap<>(),
            null,
            () -> false);

    assertFalse(legacyOverrideCalled.get());
    assertEquals(tableId, result.resolved().getFirst());
  }

  /**
   * As each pin is constructed, the resolver registers its blobs as transient GC roots — the single
   * transparent seam that protects a resolved-but-not-yet-persisted blob on every resolve path.
   */
  @Test
  void registersResolvedPinBlobsAsTransientRoots() {
    var store = org.mockito.Mockito.mock(QueryContextStore.class);
    var withStore = new QueryInputResolver(metadataGraph, store);

    NameRef n = name("c", "ns", "t");
    metadataGraph.bind(n, rid("T1"));

    withStore.resolveInputs(
        "q1",
        "cid",
        List.of(QueryInput.newBuilder().setName(n).build()),
        Optional.empty(),
        Optional.empty(),
        new ConcurrentHashMap<ResourceId, CompletableFuture<TablePin>>(),
        null);

    // Registered under the stable query id (not the per-RPC correlation id), so the committing RPC
    // releases the roots by the same key regardless of which RPC resolved the pins.
    org.mockito.Mockito.verify(store)
        .registerResolvingPinBlobs(
            org.mockito.ArgumentMatchers.eq("q1"),
            org.mockito.ArgumentMatchers.any(ResourceId.class),
            org.mockito.ArgumentMatchers.argThat(
                uris -> uris.contains("s3://T1/table.pb") && uris.contains("s3://T1/snap.pb")));
  }

  @Test
  void registersPinRootsOnTheMetadataWorkerBeforeResultHandoff() {
    var store = org.mockito.Mockito.mock(QueryContextStore.class);
    var withStore = new QueryInputResolver(metadataGraph, store);
    List<Thread> registrationThreads = java.util.Collections.synchronizedList(new ArrayList<>());
    org.mockito.Mockito.doAnswer(
            ignored -> {
              registrationThreads.add(Thread.currentThread());
              return null;
            })
        .when(store)
        .registerResolvingPinBlobs(
            org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.any(ResourceId.class),
            org.mockito.ArgumentMatchers.anyCollection());

    withStore.resolveInputs(
        "q-worker-registration",
        "cid",
        List.of(QueryInput.newBuilder().setTableId(rid("T1")).build()),
        Optional.empty(),
        Optional.empty(),
        new ConcurrentHashMap<ResourceId, CompletableFuture<TablePin>>(),
        null);

    // Concurrent fan-out roots each pin on its off-request worker before handing the plan back to
    // the caller for ordered merge, rather than inline on the request thread after the fact. Those
    // workers are the shared virtual-thread fan-out pool, so registration runs off the caller.
    assertEquals(1, registrationThreads.size());
    assertTrue(registrationThreads.get(0).isVirtual());
    assertTrue(registrationThreads.get(0) != Thread.currentThread());
  }

  /** A completed parallel input roots its pin while another input is still resolving. */
  @Test
  void rootsCompletedParallelPinBeforeSlowerInputReturns() throws Exception {
    var blockingGraph = new BlockingPinGraph("SLOW");
    var store = org.mockito.Mockito.mock(QueryContextStore.class);
    var withStore = new QueryInputResolver(blockingGraph, store);
    ResourceId slow = rid("SLOW");
    ResourceId fast = rid("FAST");

    CompletableFuture<Void> resolution =
        CompletableFuture.runAsync(
            () ->
                withStore.resolveInputs(
                    "q-parallel",
                    "cid",
                    List.of(
                        QueryInput.newBuilder().setTableId(slow).build(),
                        QueryInput.newBuilder().setTableId(fast).build()),
                    Optional.empty(),
                    Optional.empty(),
                    new java.util.concurrent.ConcurrentHashMap<
                        ResourceId, CompletableFuture<TablePin>>(),
                    null));

    try {
      assertTrue(blockingGraph.slowPinStarted.await(1, TimeUnit.SECONDS));
      org.mockito.Mockito.verify(store, org.mockito.Mockito.timeout(1_000))
          .registerResolvingPinBlobs(
              org.mockito.ArgumentMatchers.eq("q-parallel"),
              org.mockito.ArgumentMatchers.eq(fast),
              org.mockito.ArgumentMatchers.argThat(uris -> uris.contains("s3://FAST/table.pb")));
    } finally {
      blockingGraph.allowSlowPin.countDown();
    }
    resolution.join();
  }

  @Test
  void cancellationInterruptsBlockedParallelPinResolution() throws Exception {
    var blockingGraph = new BlockingPinGraph("SLOW");
    var withStore = new QueryInputResolver(blockingGraph);
    var cancelled = new java.util.concurrent.atomic.AtomicBoolean();

    CompletableFuture<Throwable> resolution =
        resolveCancellable(
            () ->
                withStore.resolveInputs(
                    "q-cancel",
                    "cid",
                    List.of(
                        QueryInput.newBuilder().setTableId(rid("SLOW")).build(),
                        QueryInput.newBuilder().setTableId(rid("FAST")).build()),
                    Optional.empty(),
                    Optional.empty(),
                    new ConcurrentHashMap<ResourceId, CompletableFuture<TablePin>>(),
                    null,
                    cancelled::get));

    try {
      assertTrue(blockingGraph.slowPinStarted.await(1, TimeUnit.SECONDS));
      cancelled.set(true);
      assertTrue(
          resolution.get(250, TimeUnit.MILLISECONDS)
              instanceof java.util.concurrent.CancellationException);
    } finally {
      blockingGraph.allowSlowPin.countDown();
    }
  }

  @Test
  void cancellationReturnsWhileSingleInputPinLookupIgnoresInterrupts() throws Exception {
    var blockingGraph = new BlockingPinGraph("SLOW");
    var withStore = new QueryInputResolver(blockingGraph);
    var cancelled = new java.util.concurrent.atomic.AtomicBoolean();

    CompletableFuture<Throwable> resolution =
        resolveCancellable(
            () ->
                withStore.resolveInputs(
                    "q-cancel-single-pin",
                    "cid",
                    List.of(QueryInput.newBuilder().setTableId(rid("SLOW")).build()),
                    Optional.empty(),
                    Optional.empty(),
                    new ConcurrentHashMap<ResourceId, CompletableFuture<TablePin>>(),
                    null,
                    cancelled::get));

    assertCancellationReturnsWhileBlocked(resolution, cancelled, blockingGraph.slowPinBlocker);
  }

  @Test
  void cancellationAfterFinalThreadConfinedPinLookupDoesNotReturnSuccess() throws Exception {
    var blockingGraph = new BlockingThreadConfinedPinGraph();
    var withStore = new QueryInputResolver(blockingGraph);
    var cancelled = new AtomicBoolean();

    CompletableFuture<Throwable> resolution =
        resolveCancellable(
            () ->
                withStore.resolveInputs(
                    "q-cancel-thread-confined-pin",
                    "cid",
                    List.of(QueryInput.newBuilder().setTableId(rid("SLOW")).build()),
                    Optional.empty(),
                    Optional.empty(),
                    new ConcurrentHashMap<ResourceId, CompletableFuture<TablePin>>(),
                    null,
                    cancelled::get));

    assertTrue(blockingGraph.pinBlocker.started.await(1, TimeUnit.SECONDS));
    cancelled.set(true);
    blockingGraph.pinBlocker.release.countDown();
    assertTrue(resolution.get(250, TimeUnit.MILLISECONDS) instanceof CancellationException);
  }

  @Test
  void serialPlanningStaysOnTheCallerThreadWhenOverlayOptsOutOfConcurrency() {
    Thread callerThread = Thread.currentThread();
    var threadConfinedGraph = new CallerThreadOnlyGraph(callerThread);
    var threadConfinedResolver = new QueryInputResolver(threadConfinedGraph);

    threadConfinedResolver.resolveInputs(
        "cid",
        List.of(
            QueryInput.newBuilder().setTableId(rid("ONE")).build(),
            QueryInput.newBuilder().setTableId(rid("TWO")).build()),
        Optional.empty(),
        Optional.empty());

    assertEquals(List.of(callerThread, callerThread), threadConfinedGraph.planningThreads());
  }

  @Test
  void cancellableSerialPlanningStaysOnTheCallerThreadWhenOverlayOptsOutOfConcurrency() {
    Thread callerThread = Thread.currentThread();
    var threadConfinedGraph = new CallerThreadOnlyGraph(callerThread);
    var threadConfinedResolver = new QueryInputResolver(threadConfinedGraph);

    threadConfinedResolver.resolveInputs(
        "q-thread-confined",
        "cid",
        List.of(
            QueryInput.newBuilder().setTableId(rid("ONE")).build(),
            QueryInput.newBuilder().setTableId(rid("TWO")).build()),
        Optional.empty(),
        Optional.empty(),
        new ConcurrentHashMap<ResourceId, CompletableFuture<TablePin>>(),
        null,
        () -> false);

    assertEquals(List.of(callerThread, callerThread), threadConfinedGraph.planningThreads());
  }

  /** A failed parallel plan releases every provisional root it constructed. */
  @Test
  void releasesCompletedParallelPinWhenSiblingPlanningFails() {
    var currentSnapshotPinCache = new ConcurrentHashMap<ResourceId, CompletableFuture<TablePin>>();
    var failingGraph = new FailingAfterFastPinGraph(currentSnapshotPinCache);
    var store = org.mockito.Mockito.mock(QueryContextStore.class);
    var withStore = new QueryInputResolver(failingGraph, store);

    assertThrows(
        IllegalStateException.class,
        () ->
            withStore.resolveInputs(
                "q-failure",
                "cid",
                List.of(
                    QueryInput.newBuilder().setTableId(rid("FAIL")).build(),
                    QueryInput.newBuilder().setTableId(rid("FAST")).build()),
                Optional.empty(),
                Optional.empty(),
                currentSnapshotPinCache,
                null));

    org.mockito.Mockito.verify(store)
        .registerResolvingPinBlobs(
            org.mockito.ArgumentMatchers.eq("q-failure"),
            org.mockito.ArgumentMatchers.any(ResourceId.class),
            org.mockito.ArgumentMatchers.argThat(uris -> uris.contains("s3://FAST/table.pb")));
    org.mockito.Mockito.verify(store)
        .releaseResolvingPinBlobs(
            org.mockito.ArgumentMatchers.eq("q-failure"),
            org.mockito.ArgumentMatchers.argThat(uris -> uris.contains("s3://FAST/table.pb")));
  }

  /** Completed sibling work still contributes pin diagnostics when another parallel plan fails. */
  @Test
  void flushesParallelPinDiagnosticsWhenSiblingPlanningFails() {
    var currentSnapshotPinCache = new ConcurrentHashMap<ResourceId, CompletableFuture<TablePin>>();
    var failingGraph = new FailingAfterFastPinGraph(currentSnapshotPinCache);
    var diagnostics = org.mockito.Mockito.mock(ai.floedb.floecat.telemetry.PhaseDiagnostics.class);
    var withStore = new QueryInputResolver(failingGraph);

    assertThrows(
        IllegalStateException.class,
        () ->
            withStore.resolveInputs(
                "q-diagnostics-failure",
                "cid",
                List.of(
                    QueryInput.newBuilder().setTableId(rid("FAST")).build(),
                    QueryInput.newBuilder().setTableId(rid("FAIL")).build()),
                Optional.empty(),
                Optional.empty(),
                currentSnapshotPinCache,
                diagnostics));

    org.mockito.Mockito.verify(diagnostics).add("pin.snapshot_calls", 1L);
    org.mockito.Mockito.verify(diagnostics).add("pin.current_snapshot_cache_misses", 1L);
    org.mockito.Mockito.verify(diagnostics)
        .nanos(
            org.mockito.ArgumentMatchers.eq("pin.snapshot_lookup"),
            org.mockito.ArgumentMatchers.anyLong());
  }

  /** Resolving a name that maps only to a table should return the table id. */
  @Test
  void resolve_table_only() {
    NameRef n = name("c", "ns", "t");
    ResourceId tableId = rid("T1");
    metadataGraph.bind(n, tableId);

    var res =
        resolver.resolveInputs(
            "cid",
            List.of(QueryInput.newBuilder().setName(n).build()),
            Optional.empty(),
            Optional.empty());

    assertEquals("T1", res.resolved().get(0).getId());
  }

  /** Resolving a name that maps only to a view should return the view id. */
  @Test
  void resolve_view_only() {
    NameRef n = name("c", "ns", "v");
    ResourceId viewId = ResourceId.newBuilder().setId("V1").setKind(ResourceKind.RK_VIEW).build();
    metadataGraph.bind(n, viewId);

    var res =
        resolver.resolveInputs(
            "cid",
            List.of(QueryInput.newBuilder().setName(n).build()),
            Optional.empty(),
            Optional.empty());

    assertEquals("V1", res.resolved().get(0).getId());
  }

  /** If both table and view match the same name, resolution is ambiguous and must fail. */
  @Test
  void resolve_ambiguous() {
    NameRef n = name("c", "x", "y");
    metadataGraph.fail(n, new StatusRuntimeException(io.grpc.Status.INVALID_ARGUMENT));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            resolver.resolveInputs(
                "cid",
                List.of(QueryInput.newBuilder().setName(n).build()),
                Optional.empty(),
                Optional.empty()));
  }

  /** Name resolution should fail if no table or view exists for the given NameRef. */
  @Test
  void resolve_unresolved() {
    NameRef n = name("c", "a", "b");
    metadataGraph.fail(n, new StatusRuntimeException(io.grpc.Status.INVALID_ARGUMENT));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            resolver.resolveInputs(
                "cid",
                List.of(QueryInput.newBuilder().setName(n).build()),
                Optional.empty(),
                Optional.empty()));
  }

  /**
   * Two references to the same table's CURRENT snapshot in one batch resolve the snapshot once,
   * even though the inputs are resolved concurrently. Without single-flight the two references
   * could resolve independently and, if an ingest landed between them, freeze different snapshots.
   */
  @Test
  void currentSnapshotForRepeatedTableIsResolvedOnce() {
    ResourceId tableId = rid("T1");
    NameRef a = name("c", "ns", "a");
    NameRef b = name("c", "ns", "b");
    metadataGraph.bind(a, tableId);
    metadataGraph.bind(b, tableId);
    metadataGraph.setCurrentSnapshot(tableId, 55);

    var result =
        resolver.resolveInputs(
            "cid",
            List.of(
                QueryInput.newBuilder().setName(a).build(),
                QueryInput.newBuilder().setName(b).build()),
            Optional.empty(),
            Optional.empty());

    long pinsForT1 =
        result.snapshotSet().getPinsList().stream()
            .filter(p -> p.getTableId().getId().equals("T1"))
            .count();
    assertEquals(1, pinsForT1, "the two references must merge into one pin");

    long tablePinCallsForT1 =
        metadataGraph.pinCalls().stream().filter(c -> c.tableId().equals(tableId)).count();
    assertEquals(1, tablePinCallsForT1, "CURRENT snapshot for a table must be resolved once");
  }

  @Test
  void explicitCurrentSnapshotForRepeatedTableIsResolvedOnce() {
    ResourceId tableId = rid("EXPLICIT_CURRENT");
    metadataGraph.setCurrentSnapshot(tableId, 55);
    SnapshotRef current = SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT).build();
    Timestamp defaultAsOf = Timestamp.newBuilder().setSeconds(1_000).build();

    var result =
        resolver.resolveInputs(
            "cid",
            List.of(
                QueryInput.newBuilder().setTableId(tableId).setSnapshot(current).build(),
                QueryInput.newBuilder().setTableId(tableId).setSnapshot(current).build()),
            Optional.of(defaultAsOf),
            Optional.empty());

    assertEquals(1, result.snapshotSet().getPinsCount());
    assertEquals(55L, result.snapshotSet().getPins(0).getSnapshotId());
    List<FakeGraph.PinCall> calls =
        metadataGraph.pinCalls().stream().filter(call -> call.tableId().equals(tableId)).toList();
    assertEquals(1, calls.size());
    assertTrue(calls.get(0).asOfDefault().isEmpty());
  }

  /** A batch of distinct relations resolves every one, preserving request order. */
  @Test
  void batchResolvesEveryInputInRequestOrder() {
    List<QueryInput> inputs = new ArrayList<>();
    List<String> expectedOrder = new ArrayList<>();
    for (int i = 0; i < 12; i++) {
      NameRef n = name("c", "ns", "t" + i);
      ResourceId id = rid("T" + i);
      metadataGraph.bind(n, id);
      metadataGraph.setCurrentSnapshot(id, 100 + i);
      inputs.add(QueryInput.newBuilder().setName(n).build());
      expectedOrder.add("T" + i);
    }

    var result = resolver.resolveInputs("cid", inputs, Optional.empty(), Optional.empty());

    assertEquals(expectedOrder, result.resolved().stream().map(ResourceId::getId).toList());
    assertEquals(12, result.snapshotSet().getPinsCount());
  }

  /** When a QueryInput specifies a snapshot_id override, the resolver must use it verbatim. */
  @Test
  void snapshot_override_id() {
    NameRef n = name("c", "ns", "t2");
    ResourceId tableId = rid("T2");
    metadataGraph.bind(n, tableId);

    QueryInput qi =
        QueryInput.newBuilder()
            .setName(n)
            .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(777))
            .build();

    SnapshotPin p =
        resolver
            .resolveInputs("cid", List.of(qi), Optional.empty(), Optional.empty())
            .snapshotSet()
            .getPins(0);

    assertEquals("T2", p.getTableId().getId());
    assertEquals(777, p.getSnapshotId());
    FakeGraph.PinCall call = metadataGraph.pinCalls().get(metadataGraph.pinCalls().size() - 1);
    assertEquals(tableId, call.tableId());
    assertEquals(777, call.override().getSnapshotId());
    assertTrue(call.asOfDefault().isEmpty());
  }

  @Test
  void explicitSnapshotIdReusesTheExistingPinInsteadOfReResolving() {
    // A snapshot deleted mid-query keeps its pin's blobs GC-rooted; a client restating the pinned
    // id on a later DescribeInputs must get the pin back, not a hard NOT_FOUND from re-resolving
    // against the live root. Reuse short-circuits before tablePinFor is ever called.
    ResourceId tableId = rid("T2");
    NameRef n = name("c", "ns", "t2");
    metadataGraph.bind(n, tableId);

    var existingPin =
        ai.floedb.floecat.query.rpc.TablePin.newBuilder()
            .setTableId(tableId)
            .setPinKind(ai.floedb.floecat.query.rpc.PinKind.PIN_KIND_SNAPSHOT_ID)
            .setSnapshotId(777)
            .setTableBlobUri("s3://T2/pinned-table.pb")
            .setSnapshotBlobUri("s3://T2/pinned-snap.pb")
            .build();
    var committed =
        ai.floedb.floecat.service.query.impl.QueryContext.newActive(
            "q-reuse",
            ai.floedb.floecat.common.rpc.PrincipalContext.newBuilder().setAccountId("a").build(),
            new byte[0],
            ai.floedb.floecat.query.rpc.RelationPinSet.newBuilder()
                .addPins(ai.floedb.floecat.service.query.QueryPins.ofTable(existingPin))
                .build()
                .toByteArray(),
            new byte[0],
            new byte[0],
            60_000L,
            1L,
            rid("cat"));
    var store = org.mockito.Mockito.mock(QueryContextStore.class);
    org.mockito.Mockito.when(store.get("q-reuse")).thenReturn(java.util.Optional.of(committed));
    var withStore = new QueryInputResolver(metadataGraph, store);

    QueryInput qi =
        QueryInput.newBuilder()
            .setName(n)
            .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(777))
            .build();

    SnapshotPin p =
        withStore
            .resolveInputs(
                "q-reuse",
                "cid",
                List.of(qi),
                Optional.<com.google.protobuf.Timestamp>empty(),
                Optional.<ResourceId>empty(),
                new ConcurrentHashMap<ResourceId, CompletableFuture<TablePin>>(),
                null)
            .snapshotSet()
            .getPins(0);

    assertEquals(777, p.getSnapshotId());
    assertTrue(
        metadataGraph.pinCalls().isEmpty(),
        "reuse must short-circuit before re-resolving against the live root");
    org.mockito.Mockito.verify(store)
        .registerResolvingPinBlobs(
            org.mockito.ArgumentMatchers.eq("q-reuse"),
            org.mockito.ArgumentMatchers.any(ResourceId.class),
            org.mockito.ArgumentMatchers.argThat(
                uris ->
                    uris.contains("s3://T2/pinned-table.pb")
                        && uris.contains("s3://T2/pinned-snap.pb")));
  }

  @Test
  void asOfReusesTheExistingPinInsteadOfReResolving() {
    // Same first-touch-wins reuse as explicit snapshot_id, extended to AS_OF: a snapshot pinned
    // as-of a timestamp at BeginQuery keeps its blobs GC-rooted for the query's lifetime, so a
    // later
    // DescribeInputs restating the same as-of must get the pin back — not re-resolve against the
    // live root (which, if that snapshot has since expired, would resolve a DIFFERENT snapshot and
    // fail with a pin CONFLICT, or throw NOT_FOUND_AT_TIME).
    ResourceId tableId = rid("T4");
    NameRef n = name("c", "ns", "t4");
    metadataGraph.bind(n, tableId);
    com.google.protobuf.Timestamp asOf =
        com.google.protobuf.Timestamp.newBuilder().setSeconds(1_700_000_000L).build();

    var existingPin =
        ai.floedb.floecat.query.rpc.TablePin.newBuilder()
            .setTableId(tableId)
            .setPinKind(ai.floedb.floecat.query.rpc.PinKind.PIN_KIND_AS_OF)
            .setOriginalAsOf(asOf)
            .setSnapshotId(555)
            .setTableBlobUri("s3://T4/pinned-table.pb")
            .setSnapshotBlobUri("s3://T4/pinned-snap.pb")
            .build();
    var committed =
        ai.floedb.floecat.service.query.impl.QueryContext.newActive(
            "q-asof",
            ai.floedb.floecat.common.rpc.PrincipalContext.newBuilder().setAccountId("a").build(),
            new byte[0],
            ai.floedb.floecat.query.rpc.RelationPinSet.newBuilder()
                .addPins(ai.floedb.floecat.service.query.QueryPins.ofTable(existingPin))
                .build()
                .toByteArray(),
            new byte[0],
            new byte[0],
            60_000L,
            1L,
            rid("cat"));
    var store = org.mockito.Mockito.mock(QueryContextStore.class);
    org.mockito.Mockito.when(store.get("q-asof")).thenReturn(java.util.Optional.of(committed));
    var withStore = new QueryInputResolver(metadataGraph, store);

    QueryInput qi =
        QueryInput.newBuilder()
            .setName(n)
            .setSnapshot(SnapshotRef.newBuilder().setAsOf(asOf))
            .build();

    SnapshotPin p =
        withStore
            .resolveInputs(
                "q-asof",
                "cid",
                List.of(qi),
                Optional.<com.google.protobuf.Timestamp>empty(),
                Optional.<ResourceId>empty(),
                new ConcurrentHashMap<ResourceId, CompletableFuture<TablePin>>(),
                null)
            .snapshotSet()
            .getPins(0);

    assertEquals(555, p.getSnapshotId());
    assertTrue(
        metadataGraph.pinCalls().isEmpty(),
        "an AS_OF pin restated with the same timestamp must reuse, not re-resolve");
  }

  @Test
  void restatingADifferentSnapshotIdStillReResolves() {
    // Reuse triggers ONLY when the restated id matches the pinned one; a different id must
    // re-resolve (and reconcile) as before.
    ResourceId tableId = rid("T3");
    NameRef n = name("c", "ns", "t3");
    metadataGraph.bind(n, tableId);
    var existingPin =
        ai.floedb.floecat.query.rpc.TablePin.newBuilder()
            .setTableId(tableId)
            .setPinKind(ai.floedb.floecat.query.rpc.PinKind.PIN_KIND_SNAPSHOT_ID)
            .setSnapshotId(100)
            .build();
    var committed =
        ai.floedb.floecat.service.query.impl.QueryContext.newActive(
            "q-diff",
            ai.floedb.floecat.common.rpc.PrincipalContext.newBuilder().setAccountId("a").build(),
            new byte[0],
            ai.floedb.floecat.query.rpc.RelationPinSet.newBuilder()
                .addPins(ai.floedb.floecat.service.query.QueryPins.ofTable(existingPin))
                .build()
                .toByteArray(),
            new byte[0],
            new byte[0],
            60_000L,
            1L,
            rid("cat"));
    var store = org.mockito.Mockito.mock(QueryContextStore.class);
    org.mockito.Mockito.when(store.get("q-diff")).thenReturn(java.util.Optional.of(committed));
    var withStore = new QueryInputResolver(metadataGraph, store);

    QueryInput qi =
        QueryInput.newBuilder()
            .setName(n)
            .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(200))
            .build();

    withStore
        .resolveInputs(
            "q-diff",
            "cid",
            List.of(qi),
            Optional.<com.google.protobuf.Timestamp>empty(),
            Optional.<ResourceId>empty(),
            new ConcurrentHashMap<ResourceId, CompletableFuture<TablePin>>(),
            null)
        .snapshotSet();

    assertTrue(
        !metadataGraph.pinCalls().isEmpty(), "a different restated id must re-resolve, not reuse");
  }

  /** A snapshot_id override of zero is valid and must be preserved end-to-end. */
  /** A snapshot_id override of zero is valid and must be preserved end-to-end. */
  @Test
  void snapshot_override_id_zero() {
    NameRef n = name("c", "ns", "t2z");
    ResourceId tableId = rid("T2Z");
    metadataGraph.bind(n, tableId);

    QueryInput qi =
        QueryInput.newBuilder()
            .setName(n)
            .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(0))
            .build();

    SnapshotPin p =
        resolver
            .resolveInputs("cid", List.of(qi), Optional.empty(), Optional.empty())
            .snapshotSet()
            .getPins(0);

    assertEquals("T2Z", p.getTableId().getId());
    assertTrue(p.hasSnapshotId());
    assertEquals(0, p.getSnapshotId());
    FakeGraph.PinCall call = metadataGraph.pinCalls().get(metadataGraph.pinCalls().size() - 1);
    assertEquals(tableId, call.tableId());
    assertTrue(call.override().hasSnapshotId());
    assertEquals(0, call.override().getSnapshotId());
  }

  /**
   * When a QueryInput specifies an explicit AS OF timestamp, the resolver must interpret it as a
   * timestamp pin (asOf set, without forcing an explicit snapshot_id pin).
   */
  @Test
  void snapshot_override_asof() {
    NameRef n = name("c", "ns", "t3");
    ResourceId tableId = rid("T3");
    metadataGraph.bind(n, tableId);
    metadataGraph.setAsOfSnapshot(tableId, 321L);
    Timestamp ts = Timestamp.newBuilder().setSeconds(100).build();

    QueryInput qi =
        QueryInput.newBuilder()
            .setName(n)
            .setSnapshot(SnapshotRef.newBuilder().setAsOf(ts))
            .build();

    SnapshotPin p =
        resolver
            .resolveInputs("cid", List.of(qi), Optional.empty(), Optional.empty())
            .snapshotSet()
            .getPins(0);

    // AS_OF resolves to a concrete snapshot; the projected pin names it, not the raw timestamp.
    assertTrue(p.hasSnapshotId());
    assertEquals(321L, p.getSnapshotId());
    // The timestamp is still what the resolver hands tablePinFor to resolve against.
    FakeGraph.PinCall call = metadataGraph.pinCalls().get(metadataGraph.pinCalls().size() - 1);
    assertEquals(Optional.empty(), call.asOfDefault());
    assertEquals(ts, call.override().getAsOf());
  }

  /**
   * If no snapshot override is given but an as-of-default is provided, the resolver must pin via
   * that timestamp, resolving it to a concrete snapshot.
   */
  @Test
  void snapshot_asof_default() {
    NameRef n = name("c", "ns", "t4");
    ResourceId tableId = rid("T4");
    metadataGraph.bind(n, tableId);
    metadataGraph.setAsOfSnapshot(tableId, 654L);

    Timestamp ts = Timestamp.newBuilder().setSeconds(50).build();

    SnapshotPin p =
        resolver
            .resolveInputs(
                "cid",
                List.of(QueryInput.newBuilder().setName(n).build()),
                Optional.of(ts),
                Optional.empty())
            .snapshotSet()
            .getPins(0);

    // The as-of default resolves to a concrete snapshot; the pin names it.
    assertTrue(p.hasSnapshotId());
    assertEquals(654L, p.getSnapshotId());
    FakeGraph.PinCall call = metadataGraph.pinCalls().get(metadataGraph.pinCalls().size() - 1);
    assertEquals(Optional.of(ts), call.asOfDefault());
    assertEquals(SnapshotRef.WhichCase.WHICH_NOT_SET, call.override().getWhichCase());
  }

  /**
   * With no overrides and no as-of-default, the resolver must fall back to the CURRENT snapshot for
   * the table.
   */
  @Test
  void snapshot_fallback_current() {
    NameRef n = name("c", "ns", "t5");
    ResourceId tableId = rid("T5");
    metadataGraph.bind(n, tableId);
    metadataGraph.setCurrentSnapshot(tableId, 4444L);

    SnapshotPin p =
        resolver
            .resolveInputs(
                "cid",
                List.of(QueryInput.newBuilder().setName(n).build()),
                Optional.empty(),
                Optional.empty())
            .snapshotSet()
            .getPins(0);

    assertEquals(4444, p.getSnapshotId());
  }

  /**
   * When specifying a direct tableId instead of a name, snapshot resolution should behave
   * identically to using a NameRef that resolves to the same ResourceId.
   */
  @Test
  void direct_table_id() {
    ResourceId rid = rid("TABX");
    metadataGraph.setCurrentSnapshot(rid, 222L);

    SnapshotPin p =
        resolver
            .resolveInputs(
                "cid",
                List.of(QueryInput.newBuilder().setTableId(rid).build()),
                Optional.empty(),
                Optional.empty())
            .snapshotSet()
            .getPins(0);

    assertEquals("TABX", p.getTableId().getId());
    assertEquals(222, p.getSnapshotId());
  }

  /** Views are not pinned directly; only their base tables (if any) are pinned. */
  @Test
  void direct_view_id() {
    ResourceId rid = viewRid("VIEWX");

    var res =
        resolver.resolveInputs(
            "cid",
            List.of(QueryInput.newBuilder().setViewId(rid).build()),
            Optional.empty(),
            Optional.empty());

    assertEquals("VIEWX", res.resolved().get(0).getId());
    assertTrue(res.snapshotSet().getPinsList().isEmpty());
  }

  @Test
  void view_pins_cache_base_tables() {
    ResourceId base1 = rid("BASE1");
    ResourceId base2 = rid("BASE2");
    ResourceId viewId = viewRid("VIEW_A");
    metadataGraph.addNode(viewNode(viewId, List.of(nameRef(base1), nameRef(base2))));
    metadataGraph.setCurrentSnapshot(base1, 101);
    metadataGraph.setCurrentSnapshot(base2, 202);

    List<SnapshotPin> pins =
        resolver
            .resolveInputs(
                "cid",
                List.of(QueryInput.newBuilder().setViewId(viewId).build()),
                Optional.empty(),
                Optional.empty())
            .snapshotSet()
            .getPinsList();

    List<String> ids = pins.stream().map(pin -> pin.getTableId().getId()).toList();
    assertTrue(ids.containsAll(List.of("BASE1", "BASE2")));
    assertEquals(
        2,
        metadataGraph.pinCalls().stream()
            .filter(call -> List.of("BASE1", "BASE2").contains(call.tableId().getId()))
            .count());
  }

  @Test
  void nested_view_pins_table_once() {
    ResourceId table = rid("TABLE_B");
    ResourceId innerView = viewRid("VIEW_INNER");
    ResourceId outerView = viewRid("VIEW_OUTER");
    metadataGraph.addNode(viewNode(innerView, List.of(nameRef(table))));
    metadataGraph.addNode(viewNode(outerView, List.of(nameRef(innerView))));
    metadataGraph.setCurrentSnapshot(table, 303);

    List<SnapshotPin> pins =
        resolver
            .resolveInputs(
                "cid",
                List.of(QueryInput.newBuilder().setViewId(outerView).build()),
                Optional.empty(),
                Optional.empty())
            .snapshotSet()
            .getPinsList();

    assertTrue(
        pins.stream()
            .filter(pin -> pin.getTableId().getKind() == ResourceKind.RK_TABLE)
            .allMatch(pin -> pin.getTableId().getId().equals("TABLE_B")));
    long tablePinCount =
        metadataGraph.pinCalls().stream()
            .filter(call -> call.tableId().getId().equals("TABLE_B"))
            .count();
    assertEquals(1, tablePinCount);
  }

  /**
   * Protobuf oneof rule: the last field set wins. Setting snapshot_id and then as_of results in a
   * SnapshotRef that contains only as_of.
   */
  @Test
  void snapshot_override_last_field_wins() {
    NameRef n = name("c", "ns", "t6");
    ResourceId tableId = rid("T6");
    metadataGraph.bind(n, tableId);
    metadataGraph.setAsOfSnapshot(tableId, 987L);

    Timestamp ts = Timestamp.newBuilder().setSeconds(999).build();

    SnapshotRef ref =
        SnapshotRef.newBuilder().setSnapshotId(555).setAsOf(ts).build(); // last field wins

    assertEquals(SnapshotRef.WhichCase.AS_OF, ref.getWhichCase());
    assertFalse(ref.hasSnapshotId());
    assertEquals(ts, ref.getAsOf());

    QueryInput qi = QueryInput.newBuilder().setName(n).setSnapshot(ref).build();

    SnapshotPin p =
        resolver
            .resolveInputs("cid", List.of(qi), Optional.empty(), Optional.empty())
            .snapshotSet()
            .getPins(0);

    // The surviving as_of resolves to the concrete snapshot, not the discarded snapshot_id 555.
    assertEquals(987L, p.getSnapshotId());
  }

  /**
   * Multiple inputs should be resolved independently and results must preserve input order. This
   * ensures stable output ordering, which callers rely on.
   */
  @Test
  void multiple_inputs_resolve_in_order() {
    NameRef n1 = name("c", "x", "t1");
    NameRef n2 = name("c", "y", "t2");

    ResourceId r1 = rid("T1");
    ResourceId r2 = rid("T2");
    metadataGraph.bind(n1, r1);
    metadataGraph.bind(n2, r2);
    metadataGraph.setCurrentSnapshot(r1, 10L);
    metadataGraph.setCurrentSnapshot(r2, 20L);

    var res =
        resolver.resolveInputs(
            "cid",
            List.of(
                QueryInput.newBuilder().setName(n1).build(),
                QueryInput.newBuilder().setName(n2).build()),
            Optional.empty(),
            Optional.empty());

    assertEquals(List.of("T1", "T2"), res.resolved().stream().map(ResourceId::getId).toList());
    assertEquals(10, res.snapshotSet().getPins(0).getSnapshotId());
    assertEquals(20, res.snapshotSet().getPins(1).getSnapshotId());
  }

  /** NameRef with multiple nested path components must resolve correctly. */
  @Test
  void resolve_nested_paths() {
    NameRef n = name("catA", "lvl1", "lvl2", "tbl");
    ResourceId nested = rid("NESTED");
    metadataGraph.bind(n, nested);

    var res =
        resolver.resolveInputs(
            "cid",
            List.of(QueryInput.newBuilder().setName(n).build()),
            Optional.empty(),
            Optional.empty());

    assertEquals("NESTED", res.resolved().get(0).getId());
  }

  /**
   * NameRef containing a ResourceId must short-circuit p2 resolution logic. i.e.
   * directory.resolveTable/resolveView MUST NOT be called.
   */
  @Test
  void resolve_name_with_explicit_resource_id() {
    ResourceId rid = rid("DIRECT");
    NameRef n =
        NameRef.newBuilder()
            .setCatalog("c")
            .addPath("ns")
            .setName("tbl")
            .setResourceId(rid)
            .build();

    var res =
        resolver.resolveInputs(
            "cid",
            List.of(QueryInput.newBuilder().setName(n).build()),
            Optional.empty(),
            Optional.empty());

    assertEquals("DIRECT", res.resolved().get(0).getId());
  }

  /** Empty snapshot ref should behave like "no override" and fall back to CURRENT. */
  @Test
  void snapshot_empty_snapshotref_behaves_as_no_override() {
    NameRef n = name("c", "ns", "tbl");
    ResourceId tableId = rid("T_EMPTY");
    metadataGraph.bind(n, tableId);
    metadataGraph.setCurrentSnapshot(tableId, 999L);

    QueryInput qi =
        QueryInput.newBuilder().setName(n).setSnapshot(SnapshotRef.newBuilder().build()).build();

    SnapshotPin pin =
        resolver
            .resolveInputs("cid", List.of(qi), Optional.empty(), Optional.empty())
            .snapshotSet()
            .getPins(0);

    assertEquals(999, pin.getSnapshotId());
  }

  /**
   * Test that an input mixing tableId and name resolution behaves as expected when combined in the
   * same request list.
   */
  @Test
  void mixed_tableid_and_name_inputs() {
    NameRef n = name("c", "x", "tblA");
    ResourceId ridB = rid("tblB");

    ResourceId ridA = rid("tblA");
    metadataGraph.bind(n, ridA);
    metadataGraph.setCurrentSnapshot(ridA, 100L);
    metadataGraph.setCurrentSnapshot(ridB, 200L);

    var res =
        resolver.resolveInputs(
            "cid",
            List.of(
                QueryInput.newBuilder().setName(n).build(),
                QueryInput.newBuilder().setTableId(ridB).build()),
            Optional.empty(),
            Optional.empty());

    assertEquals(List.of("tblA", "tblB"), res.resolved().stream().map(ResourceId::getId).toList());
    assertEquals(100L, res.snapshotSet().getPins(0).getSnapshotId());
    assertEquals(200L, res.snapshotSet().getPins(1).getSnapshotId());
  }

  /**
   * If a builder sets no target (no table, no view, no name), resolver must throw INVALID_ARGUMENT.
   */
  @Test
  void missing_target_case_must_error() {
    QueryInput qi = QueryInput.newBuilder().build();

    assertThrows(
        StatusRuntimeException.class,
        () -> resolver.resolveInputs("cid", List.of(qi), Optional.empty(), Optional.empty()));
  }

  /**
   * A view with an explicit AS OF override is legal. The override must be applied to base table
   * dependency pins (views themselves are not pinned).
   */
  @Test
  void view_with_asof_override_applies_to_base_pins() {
    ResourceId base = rid("BASE_ASOF");
    ResourceId viewId = viewRid("V1");
    metadataGraph.addNode(viewNode(viewId, List.of(nameRef(base))));
    // AS-OF resolves to a concrete snapshot at pin time; that resolved id is what the pin carries.
    metadataGraph.setAsOfSnapshot(base, 555L);

    Timestamp ts = Timestamp.newBuilder().setSeconds(202).build();

    QueryInput qi =
        QueryInput.newBuilder()
            .setViewId(viewId)
            .setSnapshot(SnapshotRef.newBuilder().setAsOf(ts))
            .build();

    List<SnapshotPin> pins =
        resolver
            .resolveInputs("cid", List.of(qi), Optional.empty(), Optional.empty())
            .snapshotSet()
            .getPinsList();

    assertEquals(1, pins.size());
    assertEquals("BASE_ASOF", pins.get(0).getTableId().getId());
    // The projected pin names the resolved snapshot, not the raw timestamp.
    assertEquals(555L, pins.get(0).getSnapshotId());

    // Ensure the effective AS-OF was passed through to tablePinFor for the base table.
    FakeGraph.PinCall call = metadataGraph.pinCalls().get(metadataGraph.pinCalls().size() - 1);
    assertEquals("BASE_ASOF", call.tableId().getId());

    // Dependency pinning uses the effective AS-OF as the "default" timestamp parameter.
    assertTrue(call.asOfDefault().isPresent());
    assertEquals(ts, call.asOfDefault().get());

    // No explicit per-table override is passed when pinning view dependencies.
    assertTrue(
        call.override() == null
            || call.override().getWhichCase() == SnapshotRef.WhichCase.WHICH_NOT_SET);
  }

  @Test
  void view_with_explicit_current_override_ignores_asof_default_for_base_pins() {
    ResourceId base = rid("BASE_EXPLICIT_CURRENT");
    ResourceId viewId = viewRid("V_EXPLICIT_CURRENT");
    metadataGraph.addNode(viewNode(viewId, List.of(nameRef(base))));
    metadataGraph.setCurrentSnapshot(base, 101L);
    metadataGraph.setAsOfSnapshot(base, 555L);
    Timestamp defaultAsOf = Timestamp.newBuilder().setSeconds(202).build();

    QueryInput input =
        QueryInput.newBuilder()
            .setViewId(viewId)
            .setSnapshot(SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT))
            .build();

    List<SnapshotPin> pins =
        resolver
            .resolveInputs("cid", List.of(input), Optional.of(defaultAsOf), Optional.empty())
            .snapshotSet()
            .getPinsList();

    assertEquals(1, pins.size());
    assertEquals(101L, pins.get(0).getSnapshotId());
    FakeGraph.PinCall call = metadataGraph.pinCalls().get(metadataGraph.pinCalls().size() - 1);
    assertTrue(call.asOfDefault().isEmpty());
  }

  @Test
  void view_with_snapshot_override_is_invalid() {
    ResourceId base = rid("BASE_INVALID");
    ResourceId viewId = viewRid("V_INVALID");
    metadataGraph.addNode(viewNode(viewId, List.of(nameRef(base))));

    QueryInput qi =
        QueryInput.newBuilder()
            .setViewId(viewId)
            .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(123))
            .build();

    assertThrows(
        StatusRuntimeException.class,
        () -> resolver.resolveInputs("cid", List.of(qi), Optional.empty(), Optional.empty()));
  }

  @Test
  void first_touch_pin_reused_when_same_table_seen_twice() {
    // First-touch wins: an explicit snapshot pins the table, and a later current-snapshot
    // reference to the same table reuses that pin rather than upgrading or replacing it.
    ResourceId base = rid("BASE_REUSE");
    metadataGraph.setCurrentSnapshot(base, 42L);

    List<SnapshotPin> pins =
        resolver
            .resolveInputs(
                "cid",
                List.of(
                    QueryInput.newBuilder()
                        .setTableId(base)
                        .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(888))
                        .build(),
                    QueryInput.newBuilder().setTableId(base).build()),
                Optional.empty(),
                Optional.empty())
            .snapshotSet()
            .getPinsList();

    assertEquals(1, pins.size());
    assertEquals("BASE_REUSE", pins.get(0).getTableId().getId());
    assertEquals(888, pins.get(0).getSnapshotId());
  }

  @Test
  void conflicting_explicit_snapshots_for_same_table_fail() {
    // Two incompatible temporal intents for the same table must fail planning rather than
    // silently depend on resolution order.
    ResourceId base = rid("BASE_CONFLICT");

    assertThrows(
        StatusRuntimeException.class,
        () ->
            resolver.resolveInputs(
                "cid",
                List.of(
                    QueryInput.newBuilder()
                        .setTableId(base)
                        .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(888))
                        .build(),
                    QueryInput.newBuilder()
                        .setTableId(base)
                        .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(999))
                        .build()),
                Optional.empty(),
                Optional.empty()));
  }

  @Test
  void earlier_pin_conflict_precedes_a_later_malformed_input() {
    ResourceId table = rid("CONFLICT_PRECEDENCE");

    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                resolver.resolveInputs(
                    "cid",
                    List.of(
                        QueryInput.newBuilder()
                            .setTableId(table)
                            .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(888))
                            .build(),
                        QueryInput.newBuilder()
                            .setTableId(table)
                            .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(999))
                            .build(),
                        QueryInput.getDefaultInstance()),
                    Optional.empty(),
                    Optional.empty()));

    assertEquals(io.grpc.Status.Code.FAILED_PRECONDITION, failure.getStatus().getCode());
  }

  @Test
  void viewBaseConflictPrecedesFailureFromALaterBase() {
    ResourceId base = rid("VIEW_CONFLICT_BASE");
    ResourceId laterBase = rid("VIEW_FAILING_BASE");
    ResourceId view = viewRid("VIEW_CONFLICT_PRECEDENCE");
    metadataGraph.setAsOfSnapshot(base, 999L);
    metadataGraph.addNode(viewNode(view, List.of(nameRef(base), nameRef(laterBase))));
    metadataGraph.failPinFor(laterBase);
    Timestamp asOf = Timestamp.newBuilder().setSeconds(123).build();

    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                resolver.resolveInputs(
                    "cid",
                    List.of(
                        QueryInput.newBuilder()
                            .setTableId(base)
                            .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(888))
                            .build(),
                        QueryInput.newBuilder()
                            .setViewId(view)
                            .setSnapshot(SnapshotRef.newBuilder().setAsOf(asOf))
                            .build()),
                    Optional.empty(),
                    Optional.empty()));

    assertEquals(io.grpc.Status.Code.FAILED_PRECONDITION, failure.getStatus().getCode());
  }

  @Test
  void conflicting_asof_and_explicit_snapshot_for_same_table_fail() {
    // An AS_OF reference resolves to a concrete snapshot; if a later explicit-snapshot reference to
    // the same table names a different snapshot, the two temporal intents conflict and planning
    // fails — the AS_OF must not silently win or be upgraded by resolution order.
    ResourceId base = rid("BASE_ASOF_CONFLICT");
    metadataGraph.setAsOfSnapshot(base, 700L);
    Timestamp ts = Timestamp.newBuilder().setSeconds(303).build();

    assertThrows(
        StatusRuntimeException.class,
        () ->
            resolver.resolveInputs(
                "cid",
                List.of(
                    QueryInput.newBuilder()
                        .setTableId(base)
                        .setSnapshot(SnapshotRef.newBuilder().setAsOf(ts))
                        .build(),
                    QueryInput.newBuilder()
                        .setTableId(base)
                        .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(999))
                        .build()),
                Optional.empty(),
                Optional.empty()));
  }

  @Test
  void asof_and_explicit_resolving_to_same_snapshot_are_compatible() {
    // First-touch AS_OF resolves to snapshot 700; a later explicit reference to the same snapshot
    // is compatible and reuses the pin, which still projects the resolved snapshot id.
    ResourceId base = rid("BASE_ASOF_REUSE");
    metadataGraph.setAsOfSnapshot(base, 700L);
    Timestamp ts = Timestamp.newBuilder().setSeconds(404).build();

    List<SnapshotPin> pins =
        resolver
            .resolveInputs(
                "cid",
                List.of(
                    QueryInput.newBuilder()
                        .setTableId(base)
                        .setSnapshot(SnapshotRef.newBuilder().setAsOf(ts))
                        .build(),
                    QueryInput.newBuilder()
                        .setTableId(base)
                        .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(700))
                        .build()),
                Optional.empty(),
                Optional.empty())
            .snapshotSet()
            .getPinsList();

    assertEquals(1, pins.size());
    assertEquals("BASE_ASOF_REUSE", pins.get(0).getTableId().getId());
    assertEquals(700L, pins.get(0).getSnapshotId());
  }

  @Test
  void currentSnapshotPinCacheReusedAcrossResolveCalls() {
    ResourceId tableId = rid("CACHE_TABLE");
    metadataGraph.setCurrentSnapshot(tableId, 1234L);
    QueryInput input = QueryInput.newBuilder().setTableId(tableId).build();
    ConcurrentMap<ResourceId, CompletableFuture<TablePin>> cache = new ConcurrentHashMap<>();

    resolver.resolveInputs(
        "", "cid-a", List.of(input), Optional.empty(), Optional.empty(), cache, null);
    resolver.resolveInputs(
        "", "cid-b", List.of(input), Optional.empty(), Optional.empty(), cache, null);

    long tablePinCalls =
        metadataGraph.pinCalls().stream().filter(call -> call.tableId().equals(tableId)).count();
    assertEquals(1L, tablePinCalls);
    assertEquals(1, cache.size());
  }

  @Test
  void discardedCurrentPinRemainsCachedAfterItsProvisionalRootsAreReleased() {
    ResourceId tableId = rid("DISCARDED_CURRENT_CACHE");
    var graph =
        new FakeGraph() {
          @Override
          public TablePin tablePinFor(
              String correlationId,
              ResourceId id,
              SnapshotRef override,
              Optional<Timestamp> asOfDefault) {
            TablePin pin = super.tablePinFor(correlationId, id, override, asOfDefault);
            return pin.toBuilder()
                .setTableBlobUri("s3://" + id.getId() + "/table-" + pin.getSnapshotId() + ".pb")
                .setSnapshotBlobUri("s3://" + id.getId() + "/snap-" + pin.getSnapshotId() + ".pb")
                .build();
          }
        };
    graph.setCurrentSnapshot(tableId, 20L);
    QueryContextStore store = org.mockito.Mockito.mock(QueryContextStore.class);
    var withStore = new QueryInputResolver(graph, store);
    ConcurrentMap<ResourceId, CompletableFuture<TablePin>> cache = new ConcurrentHashMap<>();
    org.mockito.Mockito.doAnswer(
            invocation -> {
              assertTrue(cache.containsKey(tableId), "stream cache must outlive one resolution");
              assertEquals(10L, cache.get(tableId).join().getSnapshotId());
              return null;
            })
        .when(store)
        .releaseResolvingPinBlobs(
            org.mockito.ArgumentMatchers.eq("q-discarded-current"),
            org.mockito.ArgumentMatchers.argThat(
                roots -> roots.contains("s3://DISCARDED_CURRENT_CACHE/snap-20.pb")));

    QueryInput explicit =
        QueryInput.newBuilder()
            .setTableId(tableId)
            .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(10L))
            .build();
    QueryInput current = QueryInput.newBuilder().setTableId(tableId).build();
    withStore.resolveInputs(
        "q-discarded-current",
        "cid-first",
        List.of(explicit, current),
        Optional.empty(),
        Optional.empty(),
        cache,
        null);

    assertTrue(cache.containsKey(tableId));
    org.mockito.Mockito.verify(store)
        .releaseResolvingPinBlobs(
            org.mockito.ArgumentMatchers.eq("q-discarded-current"),
            org.mockito.ArgumentMatchers.argThat(
                roots -> roots.contains("s3://DISCARDED_CURRENT_CACHE/snap-20.pb")));
    graph.setCurrentSnapshot(tableId, 30L);
    TablePin refreshed =
        withStore
            .resolveInputs(
                "q-discarded-current",
                "cid-second",
                List.of(current),
                Optional.empty(),
                Optional.empty(),
                cache,
                null)
            .relationPinSet()
            .getPins(0)
            .getTablePin();

    assertEquals(10L, refreshed.getSnapshotId());
    assertEquals(
        1L,
        graph.pinCalls().stream()
            .filter(
                call ->
                    call.tableId().equals(tableId)
                        && (call.override() == null
                            || call.override().getWhichCase()
                                == SnapshotRef.WhichCase.WHICH_NOT_SET))
            .count());
  }

  @Test
  void concurrentPlanningKeepsCurrentHolderPublishedAcrossWaiters() throws Exception {
    ResourceId shared = rid("DEFERRED_CURRENT_RETIREMENT");
    ResourceId blocked = rid("DEFERRED_CURRENT_BLOCKER");
    metadataGraph.setCurrentSnapshot(shared, 20L);
    metadataGraph.setCurrentSnapshot(blocked, 30L);
    CountDownLatch blockerEntered = new CountDownLatch(1);
    CountDownLatch releaseBlocker = new CountDownLatch(1);
    metadataGraph.beforeTablePin(
        tableId -> {
          if (!tableId.equals(blocked)) {
            return;
          }
          blockerEntered.countDown();
          try {
            releaseBlocker.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new java.util.concurrent.CancellationException();
          }
        });

    AtomicInteger removals = new AtomicInteger();
    ConcurrentMap<ResourceId, CompletableFuture<TablePin>> cache =
        new ConcurrentHashMap<>() {
          @Override
          public boolean remove(Object key, Object value) {
            boolean removed = super.remove(key, value);
            if (removed && shared.equals(key)) {
              removals.incrementAndGet();
            }
            return removed;
          }
        };
    QueryInput explicit =
        QueryInput.newBuilder()
            .setTableId(shared)
            .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(10L))
            .build();
    QueryInput current = QueryInput.newBuilder().setTableId(shared).build();
    QueryInput blocker = QueryInput.newBuilder().setTableId(blocked).build();

    CompletableFuture<QueryInputResolver.ResolutionResult> resolution =
        CompletableFuture.supplyAsync(
            () ->
                resolver.resolveInputs(
                    "q-deferred-retirement",
                    "cid",
                    List.of(explicit, current, current, blocker),
                    Optional.empty(),
                    Optional.empty(),
                    cache,
                    null));

    try {
      assertTrue(blockerEntered.await(2, TimeUnit.SECONDS));
      assertEquals(0, removals.get());
    } finally {
      releaseBlocker.countDown();
    }
    QueryInputResolver.ResolutionResult result = resolution.get(2, TimeUnit.SECONDS);

    assertEquals(0, removals.get());
    assertTrue(cache.containsKey(shared));
    assertEquals(List.of(shared, shared, shared, blocked), result.resolved());
    assertEquals(2, result.relationPinSet().getPinsCount());
  }

  @Test
  void currentSnapshotWaiterRetriesWhenSharedHolderRetiresBeforeValidation() {
    ResourceId shared = rid("RETIRED_CURRENT_WAITER");
    metadataGraph.setCurrentSnapshot(shared, 30L);
    TablePin retiredPin =
        TablePin.newBuilder()
            .setTableId(shared)
            .setPinKind(PinKind.PIN_KIND_CURRENT)
            .setSnapshotId(20L)
            .setTableBlobUri("s3://RETIRED_CURRENT_WAITER/table-20.pb")
            .setSnapshotBlobUri("s3://RETIRED_CURRENT_WAITER/snapshot-20.pb")
            .build();
    AtomicBoolean retireBeforeValidation = new AtomicBoolean(true);
    ConcurrentMap<ResourceId, CompletableFuture<TablePin>> cache =
        new ConcurrentHashMap<>() {
          @Override
          public CompletableFuture<TablePin> get(Object key) {
            if (shared.equals(key) && retireBeforeValidation.compareAndSet(true, false)) {
              super.remove(key);
              return null;
            }
            return super.get(key);
          }
        };
    cache.put(shared, CompletableFuture.completedFuture(retiredPin));

    QueryInputResolver.ResolutionResult result =
        resolver.resolveInputs(
            "q-retired-current-waiter",
            "cid",
            List.of(QueryInput.newBuilder().setTableId(shared).build()),
            Optional.empty(),
            Optional.empty(),
            cache,
            null);

    assertEquals(30L, result.relationPinSet().getPins(0).getTablePin().getSnapshotId());
    assertEquals(
        1L,
        metadataGraph.pinCalls().stream().filter(call -> call.tableId().equals(shared)).count());
    assertTrue(cache.containsKey(shared));
  }

  @Test
  void concurrentCacheEvictsPinsOwnedByAFailedResolution() {
    ResourceId successful = rid("CONCURRENT_CACHE_SUCCESS");
    ResourceId failing = rid("CONCURRENT_CACHE_FAILURE");
    var graph = new FakeGraph();
    graph.failPinFor(failing);
    var withStore =
        new QueryInputResolver(graph, org.mockito.Mockito.mock(QueryContextStore.class));
    ConcurrentMap<ResourceId, CompletableFuture<TablePin>> cache = new ConcurrentHashMap<>();

    assertThrows(
        StatusRuntimeException.class,
        () ->
            withStore.resolveInputs(
                "q-concurrent-cache",
                "cid",
                List.of(
                    QueryInput.newBuilder().setTableId(successful).build(),
                    QueryInput.newBuilder().setTableId(failing).build()),
                Optional.empty(),
                Optional.empty(),
                cache,
                null));

    assertTrue(cache.isEmpty());
  }

  @Test
  void failedResolutionEvictsFirstTouchReplacementFromSharedCurrentCache() {
    ResourceId shared = rid("FAILED_REPLACEMENT_CURRENT");
    ResourceId failing = rid("FAILED_REPLACEMENT_ERROR");
    var graph = new FakeGraph();
    graph.setCurrentSnapshot(shared, 20L);
    graph.failPinFor(failing);
    var withStore =
        new QueryInputResolver(graph, org.mockito.Mockito.mock(QueryContextStore.class));
    ConcurrentMap<ResourceId, CompletableFuture<TablePin>> cache = new ConcurrentHashMap<>();
    QueryInput explicit =
        QueryInput.newBuilder()
            .setTableId(shared)
            .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(10L))
            .build();
    QueryInput current = QueryInput.newBuilder().setTableId(shared).build();
    QueryInput failure = QueryInput.newBuilder().setTableId(failing).build();

    assertThrows(
        StatusRuntimeException.class,
        () ->
            withStore.resolveInputs(
                "q-failed-replacement",
                "cid",
                List.of(explicit, current, failure),
                Optional.empty(),
                Optional.empty(),
                cache,
                null));

    assertFalse(cache.containsKey(shared));
  }

  @Test
  void failedResolutionPreservesConcurrentCacheEntriesOwnedByAnEarlierCall() {
    ResourceId cached = rid("CONCURRENT_CACHE_EXISTING");
    ResourceId failing = rid("CONCURRENT_CACHE_NEW_FAILURE");
    var graph = new FakeGraph();
    graph.failPinFor(failing);
    var withStore =
        new QueryInputResolver(graph, org.mockito.Mockito.mock(QueryContextStore.class));
    ConcurrentMap<ResourceId, CompletableFuture<TablePin>> cache = new ConcurrentHashMap<>();

    withStore.resolveInputs(
        "q-concurrent-cache-existing",
        "cid-first",
        List.of(QueryInput.newBuilder().setTableId(cached).build()),
        Optional.empty(),
        Optional.empty(),
        cache,
        null);
    CompletableFuture<TablePin> existing = cache.get(cached);

    assertThrows(
        StatusRuntimeException.class,
        () ->
            withStore.resolveInputs(
                "q-concurrent-cache-existing",
                "cid-second",
                List.of(
                    QueryInput.newBuilder().setTableId(cached).build(),
                    QueryInput.newBuilder().setTableId(failing).build()),
                Optional.empty(),
                Optional.empty(),
                cache,
                null));

    assertSame(existing, cache.get(cached));
    assertEquals(Set.of(cached), cache.keySet());
  }

  @Test
  void cacheWaiterRootsPinBeforeFailedOwnerCanReleaseIt() throws Exception {
    ResourceId shared = rid("CONCURRENT_CACHE_HANDOFF");
    ResourceId failing = rid("CONCURRENT_CACHE_HANDOFF_FAILURE");
    CountDownLatch failingLookupStarted = new CountDownLatch(1);
    CountDownLatch allowFailure = new CountDownLatch(1);
    CountDownLatch waiterRegistrationStarted = new CountDownLatch(1);
    CountDownLatch allowWaiterRegistration = new CountDownLatch(1);
    CountDownLatch ownerReleasedRoot = new CountDownLatch(1);
    AtomicInteger sharedRegistrations = new AtomicInteger();
    var graph = new FakeGraph();
    graph.failPinFor(failing);
    graph.beforeTablePin(
        tableId -> {
          if (tableId.equals(failing)) {
            failingLookupStarted.countDown();
            try {
              allowFailure.await();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new java.util.concurrent.CancellationException();
            }
          }
        });
    QueryContextStore store = org.mockito.Mockito.mock(QueryContextStore.class);
    org.mockito.Mockito.doAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              java.util.Collection<String> roots = invocation.getArgument(2);
              if (roots.contains("s3://CONCURRENT_CACHE_HANDOFF/table.pb")
                  && sharedRegistrations.incrementAndGet() == 2) {
                waiterRegistrationStarted.countDown();
                allowWaiterRegistration.await();
              }
              return null;
            })
        .when(store)
        .registerResolvingPinBlobs(
            org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.any(ResourceId.class),
            org.mockito.ArgumentMatchers.any());
    org.mockito.Mockito.doAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              java.util.Collection<String> roots = invocation.getArgument(1);
              if (roots.contains("s3://CONCURRENT_CACHE_HANDOFF/table.pb")) {
                ownerReleasedRoot.countDown();
              }
              return null;
            })
        .when(store)
        .releaseResolvingPinBlobs(
            org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.any());
    var withStore = new QueryInputResolver(graph, store);
    ConcurrentMap<ResourceId, CompletableFuture<TablePin>> cache = new ConcurrentHashMap<>();

    CompletableFuture<Throwable> owner =
        resolveCancellable(
            () ->
                withStore.resolveInputs(
                    "q-cache-owner",
                    "cid-owner",
                    List.of(
                        QueryInput.newBuilder().setTableId(shared).build(),
                        QueryInput.newBuilder().setTableId(failing).build()),
                    Optional.empty(),
                    Optional.empty(),
                    cache,
                    null));
    try {
      assertTrue(failingLookupStarted.await(1, TimeUnit.SECONDS));
      for (int attempt = 0;
          attempt < 100 && (cache.get(shared) == null || !cache.get(shared).isDone());
          attempt++) {
        Thread.sleep(10);
      }
      assertTrue(cache.get(shared).isDone());

      CompletableFuture<QueryInputResolver.ResolutionResult> waiter =
          CompletableFuture.supplyAsync(
              () ->
                  withStore.resolveInputs(
                      "q-cache-waiter",
                      "cid-waiter",
                      List.of(QueryInput.newBuilder().setTableId(shared).build()),
                      Optional.empty(),
                      Optional.empty(),
                      cache,
                      null));
      assertTrue(waiterRegistrationStarted.await(1, TimeUnit.SECONDS));

      allowFailure.countDown();
      assertFalse(ownerReleasedRoot.await(100, TimeUnit.MILLISECONDS));
      allowWaiterRegistration.countDown();

      assertEquals(shared, waiter.get(1, TimeUnit.SECONDS).resolved().getFirst());
      assertTrue(owner.get(1, TimeUnit.SECONDS) instanceof StatusRuntimeException);
      assertTrue(ownerReleasedRoot.await(1, TimeUnit.SECONDS));
    } finally {
      allowFailure.countDown();
      allowWaiterRegistration.countDown();
    }
  }

  // ----------------------------------------------------------------------
  // Base-relation enrichment (catalog + search-path fallback)
  // ----------------------------------------------------------------------

  @Test
  void base_relation_blank_catalog_filled_from_default_catalog() {
    ResourceId catalogId =
        ResourceId.newBuilder().setId("CAT_ID").setKind(ResourceKind.RK_CATALOG).build();
    metadataGraph.setCatalogName(catalogId, "mycat");

    // Base relation NameRef has no catalog — just path + name
    NameRef baseName = NameRef.newBuilder().addPath("sales").setName("customers").build();
    ResourceId tableId = rid("CUST_TABLE");
    // After enrichment catalog will be "mycat", so bind the enriched form
    metadataGraph.bind(
        NameRef.newBuilder().setCatalog("mycat").addPath("sales").setName("customers").build(),
        tableId);
    metadataGraph.setCurrentSnapshot(tableId, 42);

    ResourceId viewId = viewRid("VIEW_ENRICH_CAT");
    metadataGraph.addNode(viewNode(viewId, List.of(baseName)));

    List<SnapshotPin> pins =
        resolver
            .resolveInputs(
                "cid",
                List.of(QueryInput.newBuilder().setViewId(viewId).build()),
                Optional.empty(),
                Optional.of(catalogId))
            .snapshotSet()
            .getPinsList();

    assertEquals(1, pins.size());
    assertEquals("CUST_TABLE", pins.get(0).getTableId().getId());
  }

  @Test
  void base_relation_empty_path_filled_from_creation_search_path() {
    // Base relation NameRef has catalog but no path
    NameRef baseName = NameRef.newBuilder().setCatalog("mycat").setName("orders").build();
    ResourceId tableId = rid("ORDERS_TABLE");
    // After enrichment path will be ["reporting"], so bind the enriched form
    metadataGraph.bind(
        NameRef.newBuilder().setCatalog("mycat").addPath("reporting").setName("orders").build(),
        tableId);
    metadataGraph.setCurrentSnapshot(tableId, 55);

    ResourceId viewId = viewRid("VIEW_ENRICH_PATH");
    // Build a ViewNode with creationSearchPath = ["reporting"]
    metadataGraph.addNode(viewNodeWithSearchPath(viewId, List.of(baseName), List.of("reporting")));

    List<SnapshotPin> pins =
        resolver
            .resolveInputs(
                "cid",
                List.of(QueryInput.newBuilder().setViewId(viewId).build()),
                Optional.empty(),
                Optional.empty()) // no default catalog needed since catalog already set
            .snapshotSet()
            .getPinsList();

    assertEquals(1, pins.size());
    assertEquals("ORDERS_TABLE", pins.get(0).getTableId().getId());
  }

  @Test
  void base_relation_resource_id_ref_bypasses_enrichment() {
    ResourceId catalogId =
        ResourceId.newBuilder().setId("CAT2").setKind(ResourceKind.RK_CATALOG).build();
    metadataGraph.setCatalogName(catalogId, "wrongcat");

    ResourceId tableId = rid("DIRECT_TABLE");
    // NameRef with resource_id — should resolve directly, not enriched with catalog
    NameRef baseName = NameRef.newBuilder().setResourceId(tableId).build();
    metadataGraph.setCurrentSnapshot(tableId, 77);

    ResourceId viewId = viewRid("VIEW_DIRECT");
    metadataGraph.addNode(viewNode(viewId, List.of(baseName)));

    List<SnapshotPin> pins =
        resolver
            .resolveInputs(
                "cid",
                List.of(QueryInput.newBuilder().setViewId(viewId).build()),
                Optional.empty(),
                Optional.of(catalogId))
            .snapshotSet()
            .getPinsList();

    assertEquals(1, pins.size());
    assertEquals("DIRECT_TABLE", pins.get(0).getTableId().getId());
  }

  // ----------------------------------------------------------------------
  // Helpers / test doubles (Composition, not inheritance)
  // ----------------------------------------------------------------------

  static class FakeGraph extends TestCatalogOverlay {

    private final Map<NameRef, ResourceId> nameBindings = new HashMap<>();
    private final Map<NameRef, RuntimeException> failures = new HashMap<>();
    private final Map<String, Long> currentSnapshots = new HashMap<>();
    private final Map<String, Long> asOfSnapshots = new HashMap<>();
    // Written from resolution tasks, which may run in parallel; keep it thread-safe.
    private final List<PinCall> pinCalls =
        java.util.Collections.synchronizedList(new ArrayList<>());
    private final Set<ResourceId> failingPins = new HashSet<>();
    private final Map<ResourceId, String> catalogNames = new HashMap<>();
    private Consumer<ResourceId> beforeTablePin = ignored -> {};

    FakeGraph() {}

    @Override
    public boolean supportsConcurrentResolution() {
      return true;
    }

    void setCatalogName(ResourceId id, String name) {
      catalogNames.put(id, name);
    }

    @Override
    public Optional<ai.floedb.floecat.metagraph.model.CatalogNode> catalog(ResourceId id) {
      String name = catalogNames.get(id);
      if (name == null) return Optional.empty();
      return Optional.of(
          new ai.floedb.floecat.metagraph.model.CatalogNode(
              id,
              "blob://test/v1",
              name,
              Map.of(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Map.of()));
    }

    // MIMIC MetadataGraph API ---------------------------------------------

    @Override
    public Optional<ResourceId> resolveName(String correlationId, NameRef ref) {
      if (ref.hasResourceId()) {
        return Optional.of(ref.getResourceId());
      }
      RuntimeException failure = failures.get(ref);
      if (failure != null) {
        throw failure;
      }
      ResourceId id = nameBindings.get(ref);
      if (id == null) {
        return Optional.empty();
      }
      return Optional.of(id);
    }

    @Override
    public Optional<ResourceId> resolveName(
        String correlationId, NameRef ref, EngineContext engineContext) {
      return resolveName(correlationId, ref);
    }

    @Override
    public TablePin tablePinFor(
        String correlationId,
        ResourceId tableId,
        SnapshotRef override,
        Optional<Timestamp> asOfDefault) {

      beforeTablePin.accept(tableId);
      if (failingPins.contains(tableId)) {
        throw new StatusRuntimeException(io.grpc.Status.NOT_FOUND);
      }
      pinCalls.add(new PinCall(correlationId, tableId, override, asOfDefault));

      TablePin.Builder builder =
          TablePin.newBuilder()
              .setTableId(tableId)
              .setTableBlobUri("s3://" + tableId.getId() + "/table.pb")
              .setSnapshotBlobUri("s3://" + tableId.getId() + "/snap.pb");

      if (override != null && override.hasSnapshotId()) {
        builder.setPinKind(PinKind.PIN_KIND_SNAPSHOT_ID).setSnapshotId(override.getSnapshotId());
      } else if (override != null && override.hasAsOf()) {
        // Mirror the real resolver: AS_OF resolves once to a concrete snapshot at pin time. The
        // timestamp is kept only as provenance; the resolved snapshot id is the pin's identity.
        builder
            .setPinKind(PinKind.PIN_KIND_AS_OF)
            .setOriginalAsOf(override.getAsOf())
            .setSnapshotId(asOfSnapshots.getOrDefault(tableId.getId(), 0L));
      } else if (asOfDefault.isPresent()) {
        builder
            .setPinKind(PinKind.PIN_KIND_AS_OF)
            .setOriginalAsOf(asOfDefault.get())
            .setSnapshotId(asOfSnapshots.getOrDefault(tableId.getId(), 0L));
      } else {
        long snapshot = currentSnapshots.getOrDefault(tableId.getId(), 0L);
        builder.setPinKind(PinKind.PIN_KIND_CURRENT);
        if (snapshot > 0) builder.setSnapshotId(snapshot);
      }

      return builder.build();
    }

    // Helpers --------------------------------------------------------------

    void bind(NameRef ref, ResourceId id) {
      nameBindings.put(ref, id);
    }

    void fail(NameRef ref, RuntimeException ex) {
      failures.put(ref, ex);
    }

    void setCurrentSnapshot(ResourceId id, long snapshotId) {
      currentSnapshots.put(id.getId(), snapshotId);
    }

    void setAsOfSnapshot(ResourceId id, long snapshotId) {
      asOfSnapshots.put(id.getId(), snapshotId);
    }

    void failPinFor(ResourceId id) {
      failingPins.add(id);
    }

    void beforeTablePin(Consumer<ResourceId> callback) {
      beforeTablePin = callback;
    }

    List<PinCall> pinCalls() {
      return pinCalls;
    }

    record PinCall(
        String correlationId,
        ResourceId tableId,
        SnapshotRef override,
        Optional<Timestamp> asOfDefault) {}
  }

  /** Blocks one table-pin lookup until the test releases it. */
  static final class BlockingPinGraph extends FakeGraph {
    private final String blockedTableId;
    final UninterruptibleBlocker slowPinBlocker = new UninterruptibleBlocker();
    final CountDownLatch slowPinStarted = slowPinBlocker.started;
    final CountDownLatch allowSlowPin = slowPinBlocker.release;

    BlockingPinGraph(String blockedTableId) {
      this.blockedTableId = blockedTableId;
    }

    @Override
    public TablePin tablePinFor(
        String correlationId,
        ResourceId tableId,
        SnapshotRef override,
        Optional<Timestamp> asOfDefault) {
      if (blockedTableId.equals(tableId.getId())) {
        slowPinBlocker.await();
      }
      return super.tablePinFor(correlationId, tableId, override, asOfDefault);
    }
  }

  /** Blocks the final caller-thread pin callback so cancellation can race its return. */
  static final class BlockingThreadConfinedPinGraph extends FakeGraph {
    final UninterruptibleBlocker pinBlocker = new UninterruptibleBlocker();

    @Override
    public boolean supportsConcurrentResolution() {
      return false;
    }

    @Override
    public TablePin tablePinFor(
        String correlationId,
        ResourceId tableId,
        SnapshotRef override,
        Optional<Timestamp> asOfDefault) {
      pinBlocker.await();
      return super.tablePinFor(correlationId, tableId, override, asOfDefault);
    }
  }

  /** Fails if a planning callback runs off the thread that began the request. */
  static final class CallerThreadOnlyGraph extends FakeGraph {
    private final Thread callerThread;
    private final List<Thread> planningThreads = new ArrayList<>();

    CallerThreadOnlyGraph(Thread callerThread) {
      this.callerThread = callerThread;
    }

    @Override
    public boolean supportsConcurrentResolution() {
      return false;
    }

    @Override
    public TablePin tablePinFor(
        String correlationId,
        ResourceId tableId,
        SnapshotRef override,
        Optional<Timestamp> asOfDefault) {
      if (Thread.currentThread() != callerThread) {
        throw new AssertionError("planning callback escaped the request thread");
      }
      planningThreads.add(Thread.currentThread());
      return super.tablePinFor(correlationId, tableId, override, asOfDefault);
    }

    List<Thread> planningThreads() {
      return planningThreads;
    }
  }

  /** Fails one lookup only after the sibling has published its fully processed pin. */
  static final class FailingAfterFastPinGraph extends FakeGraph {
    private final CountDownLatch fastPinCompleted = new CountDownLatch(1);
    private final ConcurrentMap<ResourceId, CompletableFuture<TablePin>> currentSnapshotPinCache;
    private CompletableFuture<TablePin> fastPinHolder;

    FailingAfterFastPinGraph(
        ConcurrentMap<ResourceId, CompletableFuture<TablePin>> currentSnapshotPinCache) {
      this.currentSnapshotPinCache = currentSnapshotPinCache;
    }

    @Override
    public TablePin tablePinFor(
        String correlationId,
        ResourceId tableId,
        SnapshotRef override,
        Optional<Timestamp> asOfDefault) {
      if ("FAIL".equals(tableId.getId())) {
        try {
          if (!fastPinCompleted.await(1, TimeUnit.SECONDS)) {
            throw new AssertionError("fast pin lookup did not complete");
          }
          fastPinHolder.get(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new AssertionError("failing pin lookup interrupted", e);
        } catch (ExecutionException | TimeoutException e) {
          throw new AssertionError("fast pin lookup did not publish successfully", e);
        }
        throw new IllegalStateException("planned failure");
      }
      TablePin pin = super.tablePinFor(correlationId, tableId, override, asOfDefault);
      if ("FAST".equals(tableId.getId())) {
        fastPinHolder = currentSnapshotPinCache.get(tableId);
        if (fastPinHolder == null) {
          throw new AssertionError("fast pin lookup did not publish a cache holder");
        }
        fastPinCompleted.countDown();
      }
      return pin;
    }
  }

  private static NameRef nameRef(ResourceId id) {
    return NameRef.newBuilder().setResourceId(id).build();
  }

  private ViewNode viewNode(ResourceId id, List<NameRef> baseRelations) {
    return viewNodeWithSearchPath(id, baseRelations, List.of());
  }

  private ViewNode viewNodeWithSearchPath(
      ResourceId id, List<NameRef> baseRelations, List<String> searchPath) {
    ResourceId catalog =
        ResourceId.newBuilder().setId("catalog").setKind(ResourceKind.RK_CATALOG).build();
    ResourceId namespace =
        ResourceId.newBuilder().setId("namespace").setKind(ResourceKind.RK_NAMESPACE).build();
    return new ViewNode(
        id,
        "blob://test/v1",
        catalog,
        namespace,
        "view",
        "sql",
        "dialect",
        List.of(),
        baseRelations,
        searchPath,
        GraphNodeOrigin.USER,
        Map.of(),
        Optional.empty(),
        Map.of(),
        Map.of());
  }
}
