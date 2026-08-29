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

package ai.floedb.floecat.service.it.cost;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.MutinyTableStatisticsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorSpec;
import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.SourceSelector;
import ai.floedb.floecat.query.rpc.BeginQueryRequest;
import ai.floedb.floecat.query.rpc.GetUserObjectsRequest;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.query.rpc.TableReferenceCandidate;
import ai.floedb.floecat.query.rpc.UserObjectsBundleChunk;
import ai.floedb.floecat.query.rpc.UserObjectsServiceGrpc;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.it.profiles.StoreCostProfile;
import ai.floedb.floecat.service.testsupport.RecordingStoreReadObserver;
import ai.floedb.floecat.service.testsupport.StoreCostMeter;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import com.google.protobuf.FieldMask;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * What one warm request costs the metadata stores today, on main.
 *
 * <p>A gate, and a report. It issues the same resolution three times -- the first pays for lazy
 * initialisation, the second leaves only what a warm request costs, and the third is measured --
 * then reports what that third one spent: how many pointer reads and blob fetches, which exact keys
 * and uris, and the caller behind every fetch. The cost is then ASSERTED against a formula, because
 * a number nothing checks is a number that rots, and this one is the evidence every PR built on top
 * of this branch cites for its saving.
 *
 * <p>An integration test rather than a wired-by-hand one on purpose. The question is what the
 * DEPLOYED service does, and a test that assembles its own object graph cannot see a read that only
 * bypasses a cache in the container's wiring: it would report a flattering number while the running
 * service still went to storage. So this runs the real service through the same store decorators
 * used in production, selects a detailed local observer instead of the metrics observer, and
 * reports the whole picture before asserting anything, so a failure never leaves you with a number
 * and no idea which read produced it.
 */
@QuarkusTest
@TestProfile(StoreCostProfile.class)
class WarmRequestStoreCostIT {

  @GrpcClient("floecat")
  QueryServiceGrpc.QueryServiceBlockingStub lifecycle;

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalogService;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("floecat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot;

  @GrpcClient("floecat")
  MutinyTableStatisticsServiceGrpc.MutinyTableStatisticsServiceStub stats;

  @GrpcClient("floecat")
  ConnectorsGrpc.ConnectorsBlockingStub connectors;

  @GrpcClient("floecat")
  Channel channel;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;
  @Inject RecordingStoreReadObserver reads;

  /** The pair, plus the protocol for measuring through it and the proof it is the wired pair. */
  @Inject StoreCostMeter meter;

  @BeforeEach
  void setUp() {
    // Before the fixture, not after: the liveness check below is asserting that THIS test's
    // fixture read the stores, and leftovers from the previous test would answer it instead.
    meter.resetBetweenTests();
    resetter.wipeAll();
    seeder.seedData();
  }

  /**
   * Records the cost of a request naming {@code tableCount} tables.
   *
   * <p>Varied rather than fixed at one, because the count is the only way to tell a per-request
   * read from a per-table one, and that distinction decides what is worth caching at all. A read
   * that happens once however wide the query needs no cache; one that scales with the table list is
   * the whole cost of a join. A single-table measurement cannot separate them and reads as if every
   * number were fixed overhead.
   */
  @ParameterizedTest(name = "{0} table(s) in one request")
  @ValueSource(ints = {1, 2, 4, 8})
  void recordWhatASecondIdenticalRequestCostsTheStores(int tableCount) {
    String catName = "cost_cat";
    var cat = TestSupport.createCatalog(catalogService, catName, "");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "cost_ns", List.of("table"), "cost namespace");

    var requestBuilder = GetUserObjectsRequest.newBuilder();
    for (int i = 0; i < tableCount; i++) {
      String tableName = "cost_orders_" + i;
      var tbl =
          TestSupport.createTable(
              table,
              cat.getResourceId(),
              ns.getResourceId(),
              tableName,
              "s3://bucket/" + tableName,
              "{\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"int\",\"required\":true}]}",
              "table for the warm request cost");
      TestSupport.createFinalizedSnapshot(
          snapshot, stats, tbl.getResourceId(), 2001L, System.currentTimeMillis() - 1000L);
      // A resolution only reaches the warm path if the table has an upstream, so the connector is
      // fixture rather than decoration.
      attachConnector(
          "cost-conn-" + i,
          cat.getResourceId(),
          ns.getResourceId(),
          tbl.getResourceId(),
          List.of("examples", "iceberg"));
      requestBuilder.addTables(
          TableReferenceCandidate.newBuilder()
              .addCandidates(
                  QueryInput.newBuilder().setName(TestSupport.fq(catName, ns, tableName)).build())
              .build());
    }

    var begin =
        lifecycle.beginQuery(
            BeginQueryRequest.newBuilder().setDefaultCatalogId(cat.getResourceId()).build());
    var request = requestBuilder.setQueryId(begin.getQuery().getQueryId()).build();

    // Creating the fixture already reads and writes through these stores, and a write publishes
    // into the cache, so the FIRST request is warm too — there is no cold request to compare
    // against. That the counters moved during setup is what proves the instrumentation is live.
    meter.assertWiredAndLive();

    // Twice, not once. The first request this JVM serves pays for lazy initialisation -- CDI
    // proxies, first touch of every cache -- and that cost lands in whichever parameterised case
    // runs first, which is not the same case on every run. A second warm request leaves only what
    // a warm request actually costs.
    assertResolved(collect(request), tableCount);
    assertResolved(collect(request), tableCount);

    meter.measure(() -> assertResolved(collect(request), tableCount));

    // The blob section carries each fetch with the call site that made it: a count says the cost
    // was exceeded, the caller says whether that was a cache bypass or a read the request owes.
    System.out.println(meter.report("warm request store cost, " + tableCount + " table(s)"));

    // Each Cost carries the accessor it is measured against, so a coefficient cannot be printed
    // here against one counter and asserted below against another. Read ONCE, into this list: the
    // print and the assertions are two passes, and reading the counters in each would let the
    // diagnostic describe a different observation than the one that failed.
    List<Map.Entry<Cost, Integer>> observed =
        COSTS.stream().map(c -> Map.entry(c, c.observe().applyAsInt(this))).toList();

    System.out.println(
        "cost     "
            + observed.stream()
                .map(o -> o.getKey().against(o.getValue(), tableCount))
                .collect(Collectors.joining("  |  ")));

    // Reported first, asserted second: a failing assertion hides every number after it, and the
    // report above carries the whole picture into the failure. One assertAll, so a coefficient
    // failing first cannot hide the listing check -- and a LIST is the one call the design forbids
    // on a warm path.
    var checks = new ArrayList<Executable>();
    observed.forEach(
        o -> {
          Cost cost = o.getKey();
          int count = o.getValue();
          checks.add(
              () -> assertEquals(cost.at(tableCount), count, cost.against(count, tableCount)));
        });
    checks.add(
        () ->
            assertEquals(
                0, reads.blobListCalls(), "a warm request must not enumerate the blob store"));
    checks.add(
        () ->
            assertEquals(
                0, reads.pointerPrefixWalks(), "a warm request must not walk the pointer store"));
    assertAll(checks);
  }

  /** A cost coefficient and the counter it is measured against. */
  private record Cost(
      String label, int perTable, int perRequest, ToIntFunction<WarmRequestStoreCostIT> observe) {
    int at(int tableCount) {
      return perTable * tableCount + perRequest;
    }

    String formula() {
      return perTable + "n + " + perRequest;
    }

    String against(int observed, int tableCount) {
      int recorded = at(tableCount);
      if (observed == recorded) {
        return label + " " + observed + " as recorded (" + formula() + ")";
      }
      return label
          + " "
          + observed
          + ", recorded "
          + recorded
          + " ("
          + formula()
          + ") -- "
          + (observed > recorded ? "a read was added" : "this got CHEAPER; lower the coefficient");
    }
  }

  /**
   * KV round trips, not keys: a getBatch of eight is one. See {@code floecat.core.store.requests}.
   */
  private static final Cost KV = new Cost("KV round trips", 8, 1, t -> t.reads.pointerRoundTrips());

  /**
   * What the five per table and the one per request are, measured per fetch with its caller.
   *
   * <p>Per request: the account blob, read once by the inbound call context. Per table: the pinned
   * root blob at registration, the root's manifest page, the published stats generation manifest,
   * the SAME generation manifest again for the frozen token, and the target-stats record. The first
   * four are live by design -- their emptiness is the retention verdict, so they are the reads
   * {@code docs/caching.md} lists as deliberately uncached. The fifth is not cacheable by URI at
   * all: target-stats blobs are written to deterministic URIs a re-capture can overwrite.
   *
   * <p>The generation manifest appearing twice is a duplicate, not a rounding: the
   * published-generation guard and the frozen-token read each fetch it. Recorded as it stands --
   * the coefficient is what the read path costs, not what it ought to cost.
   */
  private static final Cost S3_GET =
      new Cost("S3 objects GET", 5, 1, t -> t.reads.blobObjectGets());

  /**
   * Both HEADs are pointer-meta reads of the table root: one at pin construction ({@code
   * TableRootRepository.metaForSafe}) and one for the currency check at pin registration ({@code
   * metaForSafeLive}). No per-request part -- a request that names no table pays none.
   *
   * <p>Two rather than one because {@code metaForSafeLive} invalidates the pointer cache without
   * repopulating it, so the next request's {@code metaForSafe} always misses. A change that made
   * the live read repopulate would drop this to 1n -- a real saving, and one that should arrive as
   * a coefficient this test makes fall, not as a number nobody noticed moving.
   */
  private static final Cost S3_HEAD = new Cost("S3 objects HEAD", 2, 0, t -> t.reads.blobHeads());

  private static final List<Cost> COSTS = List.of(KV, S3_GET, S3_HEAD);

  /** One deadline for collecting a response, used by both the call and the future that waits. */
  private static final Duration COLLECT_DEADLINE = Duration.ofSeconds(5);

  private void assertResolved(List<UserObjectsBundleChunk> chunks, int expectedTables) {
    assertFalse(chunks.isEmpty(), "the request must have returned something to resolve");
    UserObjectsBundleChunk end = chunks.get(chunks.size() - 1);
    assertEquals(
        expectedTables, end.getEnd().getFoundCount(), "the request must have resolved every table");
  }

  private List<UserObjectsBundleChunk> collect(GetUserObjectsRequest request) {
    return collectAsync(request).toCompletableFuture().join();
  }

  private CompletionStage<List<UserObjectsBundleChunk>> collectAsync(
      GetUserObjectsRequest request) {
    UserObjectsServiceGrpc.UserObjectsServiceStub async =
        UserObjectsServiceGrpc.newStub(channel)
            .withDeadlineAfter(COLLECT_DEADLINE.toSeconds(), TimeUnit.SECONDS);
    CompletableFuture<List<UserObjectsBundleChunk>> future = new CompletableFuture<>();
    List<UserObjectsBundleChunk> chunks = Collections.synchronizedList(new ArrayList<>());
    async.getUserObjects(
        request,
        new StreamObserver<UserObjectsBundleChunk>() {
          @Override
          public void onNext(UserObjectsBundleChunk chunk) {
            chunks.add(chunk);
          }

          @Override
          public void onError(Throwable t) {
            future.completeExceptionally(t);
          }

          @Override
          public void onCompleted() {
            future.complete(new ArrayList<>(chunks));
          }
        });
    return future.orTimeout(COLLECT_DEADLINE.toSeconds(), TimeUnit.SECONDS);
  }

  /**
   * Attaches a table to a new connector, local to this suite.
   *
   * <p>Creating the connector and then binding the table to it with an {@code upstream} field mask
   * is one operation from a test's point of view, and writing it out separately each time is how
   * the four copies drifted.
   */
  private void attachConnector(
      String connectorName,
      ResourceId catalogId,
      ResourceId namespaceId,
      ResourceId tableId,
      List<String> sourceNamespacePath) {
    var source = SourceSelector.newBuilder();
    sourceNamespacePath.forEach(
        segment -> source.setNamespace(source.getNamespaceBuilder().addSegments(segment)));
    Connector connector =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName(connectorName)
                .setKind(ConnectorKind.CK_UNITY)
                .setUri("dummy://ignored")
                .setSource(source)
                .setDestination(
                    DestinationTarget.newBuilder()
                        .setCatalogId(catalogId)
                        .setNamespaceId(namespaceId))
                .setAuth(AuthConfig.newBuilder().setScheme("none"))
                .build());
    var upstream =
        UpstreamRef.newBuilder()
            .setConnectorId(connector.getResourceId())
            .setUri("dummy://ignored")
            .setTableDisplayName(connector.getDisplayName() + "_src")
            .setFormat(TableFormat.TF_ICEBERG)
            .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_FIELD_ID)
            .addAllNamespacePath(sourceNamespacePath)
            .build();
    table.updateTable(
        UpdateTableRequest.newBuilder()
            .setTableId(tableId)
            .setSpec(TableSpec.newBuilder().setUpstream(upstream).build())
            .setUpdateMask(FieldMask.newBuilder().addPaths("upstream").build())
            .build());
  }
}
