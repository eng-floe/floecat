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

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableStats;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.BeginQueryRequest;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.query.rpc.SnapshotSet;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.catalog.StatsProviderFactory;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.system.rpc.OutputFormat;
import ai.floedb.floecat.system.rpc.QuerySystemScanServiceGrpc;
import ai.floedb.floecat.system.rpc.ScanSystemTableChunk;
import ai.floedb.floecat.system.rpc.ScanSystemTableRequest;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.scanner.utils.EngineCatalogNames;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for QuerySystemScanService.
 *
 * <p>These tests validate: - correct resolution of system tables - catalog scoping - engine scoping
 * - engine-agnostic visibility of information_schema
 */
@QuarkusTest
public class QuerySystemScanServiceIT {

  @GrpcClient("floecat")
  QuerySystemScanServiceGrpc.QuerySystemScanServiceBlockingStub systemScan;

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("floecat")
  QueryServiceGrpc.QueryServiceBlockingStub queryService;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;
  @Inject QueryContextStore queryStore;
  @Inject StatsProviderFactory statsFactory;
  @Inject StatsRepository statsRepository;

  private String catalogPrefix = getClass().getSimpleName() + "_";

  private static ResourceId systemTable(String engineKind, String schema, String table) {
    String kind =
        (engineKind == null || engineKind.isBlank())
            ? EngineCatalogNames.FLOECAT_DEFAULT_CATALOG
            : engineKind;
    return SystemNodeRegistry.resourceId(
        kind, ResourceKind.RK_TABLE, NameRefUtil.name(schema, table));
  }

  private String beginQuery(ResourceId catalogId) {
    return queryService
        .beginQuery(BeginQueryRequest.newBuilder().setDefaultCatalogId(catalogId).build())
        .getQuery()
        .getQueryId();
  }

  @BeforeEach
  void reset() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void informationSchemaReturnsRowsForQueryCatalog() {
    var catName = catalogPrefix + "info";
    var cat = TestSupport.createCatalog(catalog, catName, "");
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "ns", List.of("analytics"), "");
    TestSupport.createTable(
        table,
        cat.getResourceId(),
        ns.getResourceId(),
        "orders",
        "s3://bucket/orders",
        "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
        "orders table");

    ResourceId systemTableId = systemTable("pg", "information_schema", "tables");
    var stub = withEngine(systemScan, "pg");
    List<ScanSystemTableChunk> chunks =
        collectChunks(
            stub,
            ScanSystemTableRequest.newBuilder()
                .setQueryId(beginQuery(cat.getResourceId()))
                .setTableId(systemTableId)
                .setOutputFormat(OutputFormat.ROWS)
                .build());

    List<List<String>> rows = rows(chunks);
    assertTrue(rows.size() > 0);
    List<List<String>> ordersRows =
        rows.stream().filter(r -> r.size() > 2 && "orders".equals(r.get(2))).toList();
    assertEquals(1, ordersRows.size(), "There should be exactly one 'orders' table row");
    List<String> ordersRow = ordersRows.get(0);
    assertEquals(cat.getDisplayName(), ordersRow.get(0), "table_catalog should match catalog name");
    assertEquals(
        "analytics.ns", ordersRow.get(1), "table_schema should be hierarchical 'analytics.ns'");
    assertEquals("orders", ordersRow.get(2), "table_name should be 'orders'");
    assertEquals("BASE TABLE", ordersRow.get(3), "table_type should be 'BASE TABLE'");
  }

  @Test
  void systemScanIsScopedToQueryCatalog() {
    var catA = TestSupport.createCatalog(catalog, catalogPrefix + "A", "");
    var catB = TestSupport.createCatalog(catalog, catalogPrefix + "B", "");

    var nsA = TestSupport.createNamespace(namespace, catA.getResourceId(), "nsA", List.of("a"), "");
    var nsB = TestSupport.createNamespace(namespace, catB.getResourceId(), "nsB", List.of("b"), "");

    TestSupport.createTable(
        table,
        catA.getResourceId(),
        nsA.getResourceId(),
        "foo",
        "s3://bucket/foo",
        "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
        "foo");

    TestSupport.createTable(
        table,
        catB.getResourceId(),
        nsB.getResourceId(),
        "bar",
        "s3://bucket/bar",
        "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
        "bar");

    ResourceId systemTableId = systemTable("trino", "information_schema", "tables");

    var stub = withEngine(systemScan, "trino");
    List<ScanSystemTableChunk> chunks =
        collectChunks(
            stub,
            ScanSystemTableRequest.newBuilder()
                .setQueryId(beginQuery(catB.getResourceId()))
                .setTableId(systemTableId)
                .setOutputFormat(OutputFormat.ROWS)
                .build());

    List<List<String>> rows = rows(chunks);
    assertFalse(
        rows.stream().anyMatch(r -> r.size() > 2 && "foo".equals(r.get(2))),
        "Rows should not contain 'foo'");
    List<List<String>> barRows =
        rows.stream().filter(r -> r.size() > 2 && "bar".equals(r.get(2))).toList();
    assertEquals(1, barRows.size(), "There should be exactly one 'bar' table row");
    assertEquals("b.nsB", barRows.get(0).get(1), "Schema value for 'bar' should be 'b.nsB'");
  }

  @Test
  void statsProviderIsBestEffortDuringSystemScan() throws IOException {
    var cat = TestSupport.createCatalog(catalog, catalogPrefix + "stats", "");
    String queryId = beginQuery(cat.getResourceId());
    QueryContext preScan = queryStore.get(queryId).orElseThrow();
    SnapshotSet preSnapshots = SnapshotSet.parseFrom(preScan.getSnapshotSet());
    assertEquals(0, preSnapshots.getPinsCount(), "BeginQuery should start with zero pins");

    ResourceId systemTableId = systemTable("pg", "information_schema", "tables");
    TableStats stats =
        TableStats.newBuilder()
            .setTableId(systemTableId)
            .setSnapshotId(987L)
            .setRowCount(1)
            .build();
    statsRepository.putTableStats(systemTableId, 987L, stats);

    var provider = statsFactory.forQuery(preScan, "corr-stats");
    assertTrue(provider.tableStats(systemTableId).isEmpty(), "Unpinned tables should skip stats");

    var stub = withEngine(systemScan, "pg");
    List<ScanSystemTableChunk> chunks =
        collectChunks(
            stub,
            ScanSystemTableRequest.newBuilder()
                .setQueryId(queryId)
                .setTableId(systemTableId)
                .setOutputFormat(OutputFormat.ROWS)
                .build());

    assertFalse(
        chunks.isEmpty(), "System scan should emit rows even when stats provider is active");
    QueryContext postScan = queryStore.get(queryId).orElseThrow();
    SnapshotSet postSnapshots = SnapshotSet.parseFrom(postScan.getSnapshotSet());
    assertEquals(0, postSnapshots.getPinsCount(), "System scan must not add snapshot pins");
    assertTrue(
        statsFactory.forQuery(postScan, "corr-check").tableStats(systemTableId).isEmpty(),
        "Stats provider still returns empty after scan");
  }

  @Test
  void pgCatalogRejectedForNonPostgresEngine() {
    var cat = TestSupport.createCatalog(catalog, "catName", "");
    ResourceId systemTableId = systemTable("", "pg_catalog", "pg_class");

    var stub = withEngine(systemScan, "");
    Iterator<ScanSystemTableChunk> stream =
        stub.scanSystemTable(
            ScanSystemTableRequest.newBuilder()
                .setQueryId(beginQuery(cat.getResourceId()))
                .setTableId(systemTableId)
                .build());
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            stream::next,
            "Streaming RPC should report invalid argument");

    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
  }

  @Test
  void informationSchemaVisibleForAnyEngine() {
    String engineKind = "no_engine";
    var catName = catalogPrefix + engineKind;

    var cat = TestSupport.createCatalog(catalog, catName, "");
    ResourceId systemTableId = systemTable(engineKind, "information_schema", "schemata");

    var stub = withEngine(systemScan, engineKind);
    List<ScanSystemTableChunk> chunks =
        collectChunks(
            stub,
            ScanSystemTableRequest.newBuilder()
                .setQueryId(beginQuery(cat.getResourceId()))
                .setTableId(systemTableId)
                .setOutputFormat(OutputFormat.ROWS)
                .build());

    List<List<String>> rows = rows(chunks);
    boolean found =
        rows.stream()
            .anyMatch(
                r ->
                    r.size() > 1
                        && engineKind.equals(r.get(0))
                        && "information_schema".equals(r.get(1)));
    assertTrue(
        found,
        "There should be a row with table_catalog = no-engine and schema = 'information_schema'");
  }

  @Test
  void informationSchemaReturnsArrowBatchesForQueryCatalog() throws IOException {
    var catName = catalogPrefix + "arrow";
    var cat = TestSupport.createCatalog(catalog, catName, "");
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "ns", List.of("analytics"), "");
    TestSupport.createTable(
        table,
        cat.getResourceId(),
        ns.getResourceId(),
        "orders",
        "s3://bucket/orders",
        "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
        "orders table");

    ResourceId systemTableId = systemTable("pg", "information_schema", "tables");
    var stub = withEngine(systemScan, "pg");
    List<ScanSystemTableChunk> chunks =
        collectChunks(
            stub,
            ScanSystemTableRequest.newBuilder()
                .setQueryId(beginQuery(cat.getResourceId()))
                .setTableId(systemTableId)
                .setOutputFormat(OutputFormat.ARROW_IPC)
                .build());

    assertFalse(chunks.isEmpty(), "Should receive at least one chunk");
    assertTrue(chunks.get(0).hasArrowSchemaIpc(), "First chunk should be the schema");
    List<List<String>> rows = arrowRows(chunks);
    assertTrue(
        rows.stream().anyMatch(row -> row.size() > 2 && "orders".equals(row.get(2))),
        "Arrow payload must include the 'orders' table");
  }

  @Test
  void arrowOutputIsDefaultWhenUnspecified() throws IOException {
    var catName = catalogPrefix + "defaultArrow";
    var cat = TestSupport.createCatalog(catalog, catName, "");
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "ns", List.of("analytics"), "");
    TestSupport.createTable(
        table,
        cat.getResourceId(),
        ns.getResourceId(),
        "shipments",
        "s3://bucket/shipments",
        "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
        "shipments table");

    ResourceId systemTableId = systemTable("pg", "information_schema", "tables");
    var stub = withEngine(systemScan, "pg");
    List<ScanSystemTableChunk> chunks =
        collectChunks(
            stub,
            ScanSystemTableRequest.newBuilder()
                .setQueryId(beginQuery(cat.getResourceId()))
                .setTableId(systemTableId)
                .build());

    assertFalse(chunks.isEmpty(), "Arrow default stream must contain at least one chunk");
    assertTrue(chunks.get(0).hasArrowSchemaIpc(), "Default stream begins with schema");
    List<List<String>> rows = arrowRows(chunks);
    assertTrue(
        rows.stream().anyMatch(row -> row.size() > 2 && "shipments".equals(row.get(2))),
        "Default Arrow payload must include the 'shipments' table");
  }

  // ------------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------------
  private List<List<String>> rows(List<ScanSystemTableChunk> chunks) {
    return chunks.stream()
        .filter(ScanSystemTableChunk::hasRow)
        .map(chunk -> List.copyOf(chunk.getRow().getValuesList()))
        .toList();
  }

  private List<List<String>> arrowRows(List<ScanSystemTableChunk> chunks) throws IOException {
    List<List<String>> rows = new ArrayList<>();
    ByteArrayOutputStream combined = new ByteArrayOutputStream();
    int schemaCount = 0;
    int schemaIndex = -1;
    for (int i = 0; i < chunks.size(); i++) {
      ScanSystemTableChunk chunk = chunks.get(i);
      if (chunk.hasArrowSchemaIpc()) {
        schemaCount++;
        schemaIndex = i;
        assertEquals(
            0, schemaIndex, "Arrow schema chunk must be emitted as the first chunk in the stream");
        assertTrue(schemaCount <= 1, "Exactly one schema chunk is allowed");
        combined.write(chunk.getArrowSchemaIpc().toByteArray());
        continue;
      }
      if (chunk.hasArrowBatchIpc()) {
        combined.write(chunk.getArrowBatchIpc().toByteArray());
      }
    }
    assertEquals(1, schemaCount, "Arrow stream must contain exactly one schema chunk");
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        ArrowStreamReader reader =
            new ArrowStreamReader(new ByteArrayInputStream(combined.toByteArray()), allocator)) {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      while (reader.loadNextBatch()) {
        for (int rowIndex = 0; rowIndex < root.getRowCount(); rowIndex++) {
          List<String> row = new ArrayList<>();
          for (FieldVector vector : root.getFieldVectors()) {
            Object value = vector.getObject(rowIndex);
            row.add(value == null ? null : value.toString());
          }
          rows.add(row);
        }
      }
    }
    return rows;
  }

  private List<ScanSystemTableChunk> collectChunks(
      QuerySystemScanServiceGrpc.QuerySystemScanServiceBlockingStub stub,
      ScanSystemTableRequest request) {
    Iterator<ScanSystemTableChunk> iterator = stub.scanSystemTable(request);
    List<ScanSystemTableChunk> chunks = new ArrayList<>();
    while (iterator.hasNext()) {
      chunks.add(iterator.next());
    }
    return chunks;
  }

  private static final Metadata.Key<String> ENGINE_KIND_HEADER =
      Metadata.Key.of("x-engine-kind", Metadata.ASCII_STRING_MARSHALLER);

  private static final Metadata.Key<String> ENGINE_VERSION_HEADER =
      Metadata.Key.of("x-engine-version", Metadata.ASCII_STRING_MARSHALLER);

  private QuerySystemScanServiceGrpc.QuerySystemScanServiceBlockingStub withEngine(
      QuerySystemScanServiceGrpc.QuerySystemScanServiceBlockingStub stub, String engineKind) {

    Metadata metadata = new Metadata();
    metadata.put(ENGINE_KIND_HEADER, engineKind);
    metadata.put(ENGINE_VERSION_HEADER, "");

    return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
  }
}
