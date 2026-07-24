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

package ai.floedb.floecat.service.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ColumnStatsTarget;
import ai.floedb.floecat.catalog.rpc.ConstraintColumnRef;
import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.catalog.rpc.MutinyTableStatisticsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.PutTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.PutTargetStatsRequest;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TableConstraintsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.query.rpc.BeginQueryRequest;
import ai.floedb.floecat.query.rpc.DescribeInputsRequest;
import ai.floedb.floecat.query.rpc.DescribeInputsResponse;
import ai.floedb.floecat.query.rpc.FetchTableConstraintsRequest;
import ai.floedb.floecat.query.rpc.FetchTargetStatsRequest;
import ai.floedb.floecat.query.rpc.GetUserObjectsRequest;
import ai.floedb.floecat.query.rpc.InitScanRequest;
import ai.floedb.floecat.query.rpc.PlannerStatsServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.floecat.query.rpc.QuerySchemaServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.query.rpc.TableConstraintsBundleChunk;
import ai.floedb.floecat.query.rpc.TableConstraintsResult;
import ai.floedb.floecat.query.rpc.TableReferenceCandidate;
import ai.floedb.floecat.query.rpc.TableStatsRequest;
import ai.floedb.floecat.query.rpc.TargetStatsBundleChunk;
import ai.floedb.floecat.query.rpc.TargetStatsNeed;
import ai.floedb.floecat.query.rpc.TargetStatsResult;
import ai.floedb.floecat.query.rpc.UserObjectsBundleChunk;
import ai.floedb.floecat.query.rpc.UserObjectsServiceGrpc;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import com.google.protobuf.FieldMask;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Service-boundary proof that a table pin freezes the query's view of a table: after a query pins a
 * table, publishing a new snapshot and altering the table must not change what that query's schema
 * and scan see. Exercises the real RPC surface (BeginQuery / DescribeInputs / GetUserObjects /
 * InitScan) rather than unit-testing pin builders.
 */
@QuarkusTest
class QueryPinningConsistencyIT {

  @GrpcClient("floecat")
  QueryServiceGrpc.QueryServiceBlockingStub queries;

  @GrpcClient("floecat")
  QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub schemaSvc;

  @GrpcClient("floecat")
  QueryScanServiceGrpc.QueryScanServiceBlockingStub scan;

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("floecat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot;

  @GrpcClient("floecat")
  Channel channel;

  @GrpcClient("floecat")
  PlannerStatsServiceGrpc.PlannerStatsServiceBlockingStub plannerStats;

  @GrpcClient("floecat")
  MutinyTableStatisticsServiceGrpc.MutinyTableStatisticsServiceStub statsWriter;

  @GrpcClient("floecat")
  TableConstraintsServiceGrpc.TableConstraintsServiceBlockingStub constraintsWriter;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;
  @Inject ai.floedb.floecat.storage.spi.PointerStore pointerStore;

  private final String prefix = getClass().getSimpleName() + "_";

  private static final Schema SCHEMA_V1 =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
  private static final Schema SCHEMA_V2 =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "qty", Types.IntegerType.get()));

  @BeforeEach
  void reset() {
    resetter.wipeAll();
    seeder.seedData();
  }

  /** Fixture: a catalog/namespace/table with schema v1 and one initial snapshot. */
  private record Fixture(
      NameRef name,
      ai.floedb.floecat.common.rpc.ResourceId catalogId,
      ai.floedb.floecat.common.rpc.ResourceId tableId,
      long snap1) {}

  private Fixture createTableWithSnapshot(String tag) {
    var cat = TestSupport.createCatalog(catalog, prefix + tag, "");
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "sch", List.of("db"), "");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "orders",
            "s3://bucket/orders",
            SchemaParser.toJson(SCHEMA_V1),
            "");
    var snap1 =
        TestSupport.createFinalizedSnapshot(
            snapshot, statsWriter, tbl.getResourceId(), 1L, System.currentTimeMillis() - 10_000L);
    NameRef name =
        NameRef.newBuilder()
            .setCatalog(cat.getDisplayName())
            .addPath("db")
            .addPath("sch")
            .setName("orders")
            .build();
    return new Fixture(name, cat.getResourceId(), tbl.getResourceId(), snap1.getSnapshotId());
  }

  /** Publish a new snapshot and alter the table's schema to v2 — the "drift" a pin must ignore. */
  private long publishNewSnapshotAndAlterSchema(Fixture f) {
    table.updateTable(
        UpdateTableRequest.newBuilder()
            .setTableId(f.tableId())
            .setSpec(TableSpec.newBuilder().setSchemaJson(SchemaParser.toJson(SCHEMA_V2)))
            .setUpdateMask(FieldMask.newBuilder().addPaths("schema_json").build())
            .build());
    long snap2 =
        TestSupport.createFinalizedSnapshot(
                snapshot, statsWriter, f.tableId(), 2L, System.currentTimeMillis())
            .getSnapshotId();
    return snap2;
  }

  private String beginQuery(Fixture f) {
    return queries
        .beginQuery(
            BeginQueryRequest.newBuilder()
                .setDefaultCatalogId(f.catalogId())
                .setTtlSeconds(30)
                .build())
        .getQuery()
        .getQueryId();
  }

  private DescribeInputsResponse describe(String queryId, NameRef name, SnapshotRef ref) {
    QueryInput.Builder input = QueryInput.newBuilder().setName(name);
    if (ref != null) {
      input.setSnapshot(ref);
    }
    return schemaSvc.describeInputs(
        DescribeInputsRequest.newBuilder().setQueryId(queryId).addInputs(input).build());
  }

  private static SnapshotRef current() {
    return SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT).build();
  }

  private static SnapshotRef atSnapshot(long id) {
    return SnapshotRef.newBuilder().setSnapshotId(id).build();
  }

  private List<UserObjectsBundleChunk> getUserObjects(String queryId, NameRef name) {
    var request =
        GetUserObjectsRequest.newBuilder()
            .setQueryId(queryId)
            .addTables(
                TableReferenceCandidate.newBuilder()
                    .addCandidates(QueryInput.newBuilder().setName(name)))
            .build();
    UserObjectsServiceGrpc.UserObjectsServiceStub async =
        UserObjectsServiceGrpc.newStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
    CompletableFuture<List<UserObjectsBundleChunk>> future = new CompletableFuture<>();
    List<UserObjectsBundleChunk> chunks = Collections.synchronizedList(new ArrayList<>());
    async.getUserObjects(
        request,
        new StreamObserver<>() {
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
    return future.orTimeout(10, TimeUnit.SECONDS).join();
  }

  private ai.floedb.floecat.query.rpc.TableInfo initScanTableInfo(String queryId, Fixture f) {
    var resp =
        scan.initScan(
            InitScanRequest.newBuilder().setQueryId(queryId).setTableId(f.tableId()).build());
    scan.closeScan(resp.getHandle());
    return resp.getTableInfo();
  }

  private long initScanSnapshotId(String queryId, Fixture f) {
    return initScanTableInfo(queryId, f).getSnapshotId();
  }

  private static int schemaFieldCount(ai.floedb.floecat.query.rpc.TableInfo info) {
    return SchemaParser.fromJson(info.getSchemaJson()).columns().size();
  }

  // ---------------------------------------------------------------------------
  // Mandatory scenario 1: explicit resolution then publish; schema + scan hold.
  // ---------------------------------------------------------------------------
  @Test
  void schemaAndScanKeepFirstPinnedStateAfterNewSnapshot() {
    Fixture f = createTableWithSnapshot("explicit");
    String queryId = beginQuery(f);

    // Pin the table at snapshot 1 (schema v1).
    var pinned = describe(queryId, f.name(), atSnapshot(f.snap1()));
    assertEquals(1, pinned.getSchemas(0).getColumnsCount());

    // Publish a new snapshot and alter the schema to v2.
    long snap2 = publishNewSnapshotAndAlterSchema(f);
    assertNotEquals(f.snap1(), snap2);

    // The already-pinned query still sees the first pinned schema (v1) via SS_CURRENT...
    var afterDrift = describe(queryId, f.name(), current());
    assertEquals(
        1,
        afterDrift.getSchemas(0).getColumnsCount(),
        "pinned query must keep schema v1 after the table moved to v2");

    // ...and init-scan builds from the pinned snapshot, not the freshly published one — including
    // the schema content, which would still be v1 even if only the id were pinned.
    var scanInfo = initScanTableInfo(queryId, f);
    assertEquals(f.snap1(), scanInfo.getSnapshotId());
    assertEquals(1, schemaFieldCount(scanInfo), "scan must serve the pinned v1 schema");
  }

  // ---------------------------------------------------------------------------
  // Mandatory scenario 2: begin without inputs, resolve via GetUserObjects, publish, hold.
  // ---------------------------------------------------------------------------
  @Test
  void lazyResolutionViaGetUserObjectsPinsFirstStateThroughPublish() {
    Fixture f = createTableWithSnapshot("lazy");
    String queryId = beginQuery(f);

    // First-touch discovery pins the current snapshot (snap1).
    List<UserObjectsBundleChunk> chunks = getUserObjects(queryId, f.name());
    assertTrue(chunks.stream().anyMatch(UserObjectsBundleChunk::hasResolutions));

    // Publish a new snapshot + schema after the pin.
    publishNewSnapshotAndAlterSchema(f);

    // DescribeInputs and init-scan for the same query use the first resolved pin (snap1/v1).
    var afterDrift = describe(queryId, f.name(), current());
    assertEquals(1, afterDrift.getSchemas(0).getColumnsCount());
    assertEquals(f.snap1(), initScanSnapshotId(queryId, f));
  }

  // ---------------------------------------------------------------------------
  // Feasible: incompatible temporal intents for the same table fail planning.
  // ---------------------------------------------------------------------------
  @Test
  void incompatibleTemporalIntentsForSameTableFail() {
    Fixture f = createTableWithSnapshot("conflict");
    long snap2 = publishNewSnapshotAndAlterSchema(f);
    String queryId = beginQuery(f);

    // Two explicit, different snapshots for the same table in one DescribeInputs call conflict.
    assertThrows(
        io.grpc.StatusRuntimeException.class,
        () ->
            schemaSvc.describeInputs(
                DescribeInputsRequest.newBuilder()
                    .setQueryId(queryId)
                    .addInputs(
                        QueryInput.newBuilder()
                            .setName(f.name())
                            .setSnapshot(atSnapshot(f.snap1())))
                    .addInputs(
                        QueryInput.newBuilder().setName(f.name()).setSnapshot(atSnapshot(snap2)))
                    .build()));
  }

  // ---------------------------------------------------------------------------
  // Feasible: the catalog-cache-facing pin fingerprint differs across queries that
  // pin the same table at different physical snapshots.
  // ---------------------------------------------------------------------------
  @Test
  void distinctPinFingerprintsAcrossQueriesForDifferentSnapshots() {
    Fixture f = createTableWithSnapshot("fingerprint");
    long snap2 = publishNewSnapshotAndAlterSchema(f);

    String queryA = beginQuery(f);
    describe(queryA, f.name(), atSnapshot(f.snap1()));
    String fpA = pinFingerprint(getUserObjects(queryA, f.name()));

    String queryB = beginQuery(f);
    describe(queryB, f.name(), atSnapshot(snap2));
    String fpB = pinFingerprint(getUserObjects(queryB, f.name()));

    assertTrue(!fpA.isEmpty() && !fpB.isEmpty(), "both queries must expose a pin fingerprint");
    assertNotEquals(fpA, fpB, "different pinned snapshots must yield different pin fingerprints");
  }

  // ---------------------------------------------------------------------------
  // Mandatory scenario 1 (stats + constraints half): a post-pin publish must not
  // change the stats or constraints the pinned query sees.
  // ---------------------------------------------------------------------------
  @Test
  void statsAndConstraintsKeepFirstPinnedStateAfterNewSnapshot() {
    Fixture f = createTableWithSnapshot("statscon");
    seedColumnStats(f.tableId(), f.snap1(), 1L, 111L);
    seedConstraint(f.tableId(), f.snap1(), "pk_snap1");

    String queryId = beginQuery(f);
    describe(queryId, f.name(), atSnapshot(f.snap1())); // pin snapshot 1

    // Publish a new snapshot with DIFFERENT stats and a different constraint.
    long snap2 = publishNewSnapshotAndAlterSchema(f);
    seedColumnStats(f.tableId(), snap2, 1L, 222L);
    seedConstraint(f.tableId(), snap2, "pk_snap2");

    // Stats for the pinned query come from snapshot 1, and the response echoes the pinned snapshot.
    TargetStatsResult stat =
        fetchTargetStats(queryId, f.tableId(), 1L).stream()
            .filter(TargetStatsResult::hasStats)
            .findFirst()
            .orElseThrow();
    assertEquals(f.snap1(), stat.getSnapshotId());
    assertEquals(f.snap1(), stat.getPinnedSnapshotId());
    assertEquals(111L, stat.getStats().getScalar().getRowCount());

    // Constraints for the pinned query are snapshot 1's, never the newly published snapshot 2's.
    List<TableConstraintsResult> constraints = fetchConstraints(queryId, f.tableId());
    assertTrue(
        constraints.stream()
            .flatMap(r -> r.getConstraintsList().stream())
            .anyMatch(c -> c.getName().equals("pk_snap1")),
        "pinned query must see snapshot 1's constraint");
    assertTrue(
        constraints.stream()
            .flatMap(r -> r.getConstraintsList().stream())
            .noneMatch(c -> c.getName().equals("pk_snap2")),
        "pinned query must not see the newly published snapshot's constraint");
  }

  // ---------------------------------------------------------------------------
  // An in-place UpdateSnapshot rewrites the blob behind the pinned snapshot id.
  // The pinned query keeps reading the exact blob it pinned; a new query sees
  // the rewritten one.
  // ---------------------------------------------------------------------------
  @Test
  void inPlaceSnapshotUpdateDoesNotDriftAPinnedScan() {
    Fixture f = createTableWithSnapshot("inplace");
    String queryId = beginQuery(f);
    describe(queryId, f.name(), atSnapshot(f.snap1())); // pin snapshot 1's blob identity

    // Rewrite the pinned snapshot in place (same id, new blob): its schema moves to v2.
    snapshot.updateSnapshot(
        ai.floedb.floecat.catalog.rpc.UpdateSnapshotRequest.newBuilder()
            .setSpec(
                ai.floedb.floecat.catalog.rpc.SnapshotSpec.newBuilder()
                    .setTableId(f.tableId())
                    .setSnapshotId(f.snap1())
                    .setSchemaJson(SchemaParser.toJson(SCHEMA_V2)))
            .setUpdateMask(FieldMask.newBuilder().addPaths("schema_json"))
            .build());

    // The pinned query still scans and describes the exact blob it pinned (schema v1) — the same
    // snapshot id now carries v2, but the pin's immutable blob identity shields the query from it.
    assertEquals(1, schemaFieldCount(initScanTableInfo(queryId, f)));
    assertEquals(1, describe(queryId, f.name(), current()).getSchemas(0).getColumnsCount());

    // A NEW query pins the rewritten current pair and sees the in-place v2 schema.
    String freshQuery = beginQuery(f);
    describe(freshQuery, f.name(), current());
    assertEquals(2, schemaFieldCount(initScanTableInfo(freshQuery, f)));
  }

  // ---------------------------------------------------------------------------
  // Constraints mutate in place under a stable snapshot. A bundle replaced after
  // the pin froze its version must not be served — the gate reports absent with
  // the exact bundle the query pinned, never the replacement.
  // ---------------------------------------------------------------------------
  @Test
  void constraintsReplacedUnderThePinnedSnapshotKeepServingThePinnedBundle() {
    Fixture f = createTableWithSnapshot("condrift");
    seedConstraint(f.tableId(), f.snap1(), "pk_v1");

    String queryId = beginQuery(f);
    describe(queryId, f.name(), atSnapshot(f.snap1())); // pin freezes the bundle ref

    // Replace the bundle under the SAME pinned snapshot (in-place constraint mutation).
    seedConstraint(f.tableId(), f.snap1(), "pk_v2");

    List<TableConstraintsResult> results = fetchConstraints(queryId, f.tableId());
    TableConstraintsResult result =
        results.stream()
            .filter(r -> r.getTableId().getId().equals(f.tableId().getId()))
            .findFirst()
            .orElseThrow();
    // Constraints are deterministic for the query's lifetime: the serving path loads the bundle
    // by the immutable ref the pin froze from its root entry, so the pinned query keeps seeing
    // pk_v1 — the exact bundle it pinned — and the in-place replacement never leaks in.
    assertEquals(
        ai.floedb.floecat.query.rpc.BundleResultStatus.BUNDLE_RESULT_STATUS_FOUND,
        result.getStatus(),
        "the pinned bundle keeps serving");
    assertTrue(
        result.getConstraintsList().stream().anyMatch(c -> c.getName().equals("pk_v1")),
        "the pinned bundle's constraints are served");
    assertTrue(
        results.stream()
            .flatMap(r -> r.getConstraintsList().stream())
            .noneMatch(c -> c.getName().equals("pk_v2")),
        "the post-pin bundle must never leak to the pinned query");
  }

  // ---------------------------------------------------------------------------
  // AS_OF resolves once at pin time; a later publish must not move the pin.
  // ---------------------------------------------------------------------------
  @Test
  void asOfPinHoldsThroughLaterPublish() {
    Fixture f = createTableWithSnapshot("asof");
    String queryId = beginQuery(f);

    // AS_OF "now" resolves to snapshot 1 (created 10s ago) and pins it.
    var asOf =
        SnapshotRef.newBuilder()
            .setAsOf(com.google.protobuf.util.Timestamps.fromMillis(System.currentTimeMillis()))
            .build();
    var pinned = describe(queryId, f.name(), asOf);
    assertEquals(1, pinned.getSchemas(0).getColumnsCount());

    publishNewSnapshotAndAlterSchema(f);

    // Both schema and scan keep the AS_OF-resolved snapshot, not the newer publish.
    var afterDrift = describe(queryId, f.name(), current());
    assertEquals(1, afterDrift.getSchemas(0).getColumnsCount());
    assertEquals(f.snap1(), initScanSnapshotId(queryId, f));
  }

  // ---------------------------------------------------------------------------
  // Mandatory scenario 4: an explicit-snapshot pin retains the pinned table blob
  // for table-scoped properties even after the table itself is altered.
  // ---------------------------------------------------------------------------
  @Test
  void explicitSnapshotPinKeepsPinnedTableBlobProperties() {
    Fixture f = createTableWithSnapshot("explicit-blob");
    String queryId = beginQuery(f);
    describe(queryId, f.name(), atSnapshot(f.snap1()));

    // ALTER the table's properties: the live table pointer moves to a new blob.
    table.updateTable(
        UpdateTableRequest.newBuilder()
            .setTableId(f.tableId())
            .setSpec(TableSpec.newBuilder().putProperties("scan_marker", "after-alter"))
            .setUpdateMask(FieldMask.newBuilder().addPaths("properties").build())
            .build());

    // The pinned scan serves table-scoped properties from the blob captured at pin time.
    assertTrue(!initScanTableInfo(queryId, f).getPropertiesMap().containsKey("scan_marker"));

    // A fresh query pinning the same snapshot captures the post-ALTER table blob and sees it.
    String freshQuery = beginQuery(f);
    describe(freshQuery, f.name(), atSnapshot(f.snap1()));
    assertEquals(
        "after-alter", initScanTableInfo(freshQuery, f).getPropertiesMap().get("scan_marker"));
  }

  // ---------------------------------------------------------------------------
  // Lazy migration: a table with no root (as any table predating roots) has one
  // synthesized from its legacy families on its first CURRENT read.
  // ---------------------------------------------------------------------------
  @Test
  void currentReadLazilyMaterializesAMissingTableRoot() {
    Fixture f = createTableWithSnapshot("lazy-repair");
    // Simulate a pre-root (legacy) table: drop the root out from under it. The legacy pointer
    // families still describe the table fully.
    pointerStore.delete(
        ai.floedb.floecat.service.repo.model.Keys.tableRootByTable(
            f.tableId().getAccountId(), f.tableId().getId()));

    // The first CURRENT read synthesizes and commits the root transparently, then pins it.
    String queryId = beginQuery(f);
    var described = describe(queryId, f.name(), current());
    assertEquals(1, described.getSchemas(0).getColumnsCount());
    assertEquals(f.snap1(), initScanSnapshotId(queryId, f));
  }

  private void seedColumnStats(ResourceId tableId, long snapshotId, long columnId, long rowCount) {
    var record =
        TargetStatsRecords.columnRecord(
            tableId,
            snapshotId,
            columnId,
            ScalarStats.newBuilder().setRowCount(rowCount).build(),
            null);
    statsWriter
        .putTargetStats(
            Multi.createFrom()
                .item(
                    PutTargetStatsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .addRecords(record)
                        .build()))
        .await()
        .atMost(Duration.ofSeconds(30));
  }

  private void seedConstraint(ResourceId tableId, long snapshotId, String name) {
    constraintsWriter.putTableConstraints(
        PutTableConstraintsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setConstraints(
                SnapshotConstraints.newBuilder()
                    .setTableId(tableId)
                    .addConstraints(
                        ConstraintDefinition.newBuilder()
                            .setName(name)
                            .setType(ConstraintType.CT_PRIMARY_KEY)
                            .addColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnId(1L)
                                    .setColumnName("id")
                                    .setOrdinal(1))))
            .build());
  }

  private List<TargetStatsResult> fetchTargetStats(
      String queryId, ResourceId tableId, long columnId) {
    FetchTargetStatsRequest request =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId(queryId)
            .addTables(
                TableStatsRequest.newBuilder()
                    .setTableId(tableId)
                    .addTargets(
                        TargetStatsNeed.newBuilder()
                            .setTarget(
                                StatsTarget.newBuilder()
                                    .setColumn(
                                        ColumnStatsTarget.newBuilder().setColumnId(columnId)))
                            .setPriority(1)))
            .build();
    List<TargetStatsResult> out = new ArrayList<>();
    Iterator<TargetStatsBundleChunk> chunks = plannerStats.getTargetStats(request);
    while (chunks.hasNext()) {
      TargetStatsBundleChunk chunk = chunks.next();
      if (chunk.hasBatch()) {
        out.addAll(chunk.getBatch().getTargetsList());
      }
    }
    return out;
  }

  private List<TableConstraintsResult> fetchConstraints(String queryId, ResourceId tableId) {
    FetchTableConstraintsRequest request =
        FetchTableConstraintsRequest.newBuilder().setQueryId(queryId).addTableIds(tableId).build();
    List<TableConstraintsResult> out = new ArrayList<>();
    Iterator<TableConstraintsBundleChunk> chunks = plannerStats.getTableConstraints(request);
    while (chunks.hasNext()) {
      TableConstraintsBundleChunk chunk = chunks.next();
      if (chunk.hasBatch()) {
        out.addAll(chunk.getBatch().getConstraintsList());
      }
    }
    return out;
  }

  private static String pinFingerprint(List<UserObjectsBundleChunk> chunks) {
    return chunks.stream()
        .filter(UserObjectsBundleChunk::hasResolutions)
        .flatMap(c -> c.getResolutions().getItemsList().stream())
        .map(r -> r.getRelation().getPinIdentity().getPinFingerprint())
        .filter(fp -> !fp.isEmpty())
        .findFirst()
        .orElse("");
  }
}
