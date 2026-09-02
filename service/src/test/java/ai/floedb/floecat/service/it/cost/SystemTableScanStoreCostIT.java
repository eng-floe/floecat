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

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.BeginQueryRequest;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.it.profiles.StoreCostProfile;
import ai.floedb.floecat.service.testsupport.RecordingStoreReadObserver;
import ai.floedb.floecat.service.testsupport.StoreCostMeter;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.system.rpc.OutputFormat;
import ai.floedb.floecat.system.rpc.QuerySystemScanServiceGrpc;
import ai.floedb.floecat.system.rpc.ScanSystemTableChunk;
import ai.floedb.floecat.system.rpc.ScanSystemTableRequest;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * What an {@code information_schema} scan costs the metadata stores, by catalog size.
 *
 * <p>A gate, and the companion to the resolution cost: this is the only query-path RPC whose cost
 * scales with how much is in the catalog rather than with what the query named. A resolution naming
 * two tables reads a bounded amount however large the catalog is; a listing does not, and the
 * question is what its curve looks like.
 *
 * <p>The catalog is populated to a given table count and then scanned twice, so the second scan is
 * warm in whatever sense the running service is warm. The cost is then ASSERTED, as a constant
 * rather than a formula: zero KV round trips and one blob object at both catalog sizes, and no
 * listing. A scan resolves the catalog and does not pay per table, so a number that starts scaling
 * with the catalog is the regression this exists to catch. The rows are asserted too, because a
 * scan that returned nothing would touch few stores and report a flattering number.
 */
@QuarkusTest
@TestProfile(StoreCostProfile.class)
class SystemTableScanStoreCostIT {

  private static final Metadata.Key<String> ENGINE_KIND_HEADER =
      Metadata.Key.of("x-engine-kind", Metadata.ASCII_STRING_MARSHALLER);

  private static final Metadata.Key<String> ENGINE_VERSION_HEADER =
      Metadata.Key.of("x-engine-version", Metadata.ASCII_STRING_MARSHALLER);

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("floecat")
  QueryServiceGrpc.QueryServiceBlockingStub queryService;

  @GrpcClient("floecat")
  QuerySystemScanServiceGrpc.QuerySystemScanServiceBlockingStub systemScan;

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

  /** Every catalog, namespace and table this suite creates carries this prefix. */
  private static final String FIXTURE_PREFIX = "syscost_";

  /**
   * Each case states the token its rows must name and EXACTLY how many rows must name it.
   *
   * <p>Exactly, not at least: a lower bound is satisfied by a scanner that returns too much, and
   * one with a zero bound is satisfied by anything at all. An exact count refuses both directions
   * -- {@code columns} yields one row per column and these fixtures have one column each, so a
   * scanner that started emitting rows for the namespace instead of the tables is caught here.
   *
   * <p>The token differs per table because they enumerate different things. {@code tables} and
   * {@code columns} name the tables. {@code schemata} names the CATALOG -- its schema column holds
   * the dotted path, so it never starts with the namespace's own name -- and it yields two rows,
   * because creating {@code analytics.syscost_ns} materialises {@code analytics} as well; a change
   * that stopped materialising ancestors would show up here.
   */
  @ParameterizedTest(name = "information_schema.{0}, {1} table(s) in the catalog")
  @CsvSource({
    "tables,1,orders_,1", "tables,8,orders_,8",
    "columns,1,orders_,1", "columns,8,orders_,8",
    "schemata,1,cat,2", "schemata,8,cat,2",
  })
  void recordWhatScanningInformationSchemaCostsTheStores(
      String systemTable, int tableCount, String rowToken, int expectedRows) {
    var cat = TestSupport.createCatalog(catalog, FIXTURE_PREFIX + "cat", "");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), FIXTURE_PREFIX + "ns", List.of("analytics"), "");
    for (int i = 0; i < tableCount; i++) {
      String tableName = FIXTURE_PREFIX + "orders_" + i;
      TestSupport.createTable(
          table,
          cat.getResourceId(),
          ns.getResourceId(),
          tableName,
          "s3://bucket/" + tableName,
          "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
          "table for the system scan cost");
    }

    var request =
        ScanSystemTableRequest.newBuilder()
            .setQueryId(beginQuery(cat.getResourceId()))
            .setTableId(
                SystemNodeRegistry.resourceId(
                    "pg",
                    ResourceKind.RK_TABLE,
                    NameRefUtil.name("information_schema", systemTable)))
            .setOutputFormat(OutputFormat.ROWS)
            .build();
    var stub = withEngine(systemScan, "pg");

    assertScanned(collect(stub, request), systemTable, rowToken, expectedRows);

    // Both checks belong here, before the reset, not after it. The measured window is meant to
    // reach zero store reads as the cache work lands, so a liveness check on the MEASURED request
    // would turn red exactly when the design succeeds. The fixture phase always reads.
    meter.assertWiredAndLive();

    meter.measure(() -> assertScanned(collect(stub, request), systemTable, rowToken, expectedRows));

    System.out.println(
        meter.report(
            "information_schema." + systemTable + " scan cost, " + tableCount + " table(s)"));

    // Asserted, like the resolution suite. This used to record without gating, because the
    // app-scoped caches are shared across the JVM and the same scan read one pointer alone and
    // twenty after a neighbour had evicted what it relied on. The profile now sizes those caches
    // past anything a fixture creates, and the cost is flat: zero KV round trips and one S3 object,
    // at both catalog sizes, alone and after the sibling suite. Flat and unasserted is the worst of
    // both -- it looks like evidence and defends nothing.
    //
    // A constant, not a formula: a system-table scan resolves the catalog and reads its root, and
    // does not pay per table in the catalog. That is the finding this suite exists to record, so a
    // number that starts scaling with tableCount is exactly what should fail here.
    assertEquals(
        0,
        reads.pointerRoundTrips(),
        "a system-table scan must not reach the pointer store at all: the catalog and namespace"
            + " resolutions it needs are served by the pointer cache");
    assertEquals(
        1,
        reads.blobObjectGets(),
        "a system-table scan must not scale its blob reads with the catalog");
    assertEquals(
        0, reads.blobHeads(), "a system-table scan must not probe the blob store per table");

    // Both halves of "must not walk the store", because the round-trip total above hides either
    // one: a scan that replaced its single get with a single listing over the whole catalog holds
    // that total at 1 while the cost grows with the catalog, which is the regression this suite is
    // here to catch.
    assertEquals(
        0, reads.blobListCalls(), "a scan must not walk the blob store to find out what exists");
    assertEquals(
        0,
        reads.pointerPrefixWalks(),
        "a scan must not walk the pointer store to find out what exists");
  }

  /**
   * Fails unless the scan returned exactly the rows this fixture should produce.
   *
   * <p>Not decoration. A scan that returned no rows, or that listed only the seeded catalog, would
   * make almost no store calls and report the cheapest cost in the file.
   */
  private void assertScanned(
      List<ScanSystemTableChunk> chunks, String systemTable, String rowToken, int expectedRows) {
    String token = FIXTURE_PREFIX + rowToken;
    long listed =
        chunks.stream()
            .filter(ScanSystemTableChunk::hasRow)
            .map(chunk -> chunk.getRow().getValuesList())
            .filter(values -> values.stream().anyMatch(v -> v.startsWith(token)))
            .count();
    assertEquals(
        expectedRows,
        listed,
        "information_schema."
            + systemTable
            + " must return exactly this many rows naming "
            + token);
  }

  private String beginQuery(ResourceId catalogId) {
    return queryService
        .beginQuery(BeginQueryRequest.newBuilder().setDefaultCatalogId(catalogId).build())
        .getQuery()
        .getQueryId();
  }

  private QuerySystemScanServiceGrpc.QuerySystemScanServiceBlockingStub withEngine(
      QuerySystemScanServiceGrpc.QuerySystemScanServiceBlockingStub stub, String engineKind) {
    Metadata metadata = new Metadata();
    metadata.put(ENGINE_KIND_HEADER, engineKind);
    metadata.put(ENGINE_VERSION_HEADER, "");
    return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
  }

  private List<ScanSystemTableChunk> collect(
      QuerySystemScanServiceGrpc.QuerySystemScanServiceBlockingStub stub,
      ScanSystemTableRequest request) {
    Iterator<ScanSystemTableChunk> chunks = stub.scanSystemTable(request);
    List<ScanSystemTableChunk> collected = new ArrayList<>();
    while (chunks.hasNext()) {
      collected.add(chunks.next());
    }
    return collected;
  }
}
