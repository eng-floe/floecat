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

import ai.floedb.floecat.catalog.rpc.*;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.connector.rpc.*;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.query.rpc.*;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for QueryScanService (FetchScanBundle).
 *
 * <p>After API changes: - BeginQuery no longer takes inputs. - DescribeInputs establishes pinned
 * tables + snapshots.
 */
@QuarkusTest
class QueryScanServiceIT {

  @GrpcClient("floecat")
  QueryServiceGrpc.QueryServiceBlockingStub lifecycle;

  @GrpcClient("floecat")
  QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub schema;

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
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  @GrpcClient("floecat")
  ConnectorsGrpc.ConnectorsBlockingStub connectors;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  String catalogPrefix = this.getClass().getSimpleName() + "_";

  @BeforeEach
  void reset() {
    resetter.wipeAll();
    seeder.seedData();
  }

  /** Ensures FetchScanBundle returns empty bundle for a pinned table. */
  @Test
  void fetchScanBundleReturnsBundleForPinnedTable() throws Exception {

    var catName = catalogPrefix + "scan_cat";
    var cat = TestSupport.createCatalog(catalog, catName, "");
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "scan_ns", List.of("scan"), "");

    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "scan_orders",
            "s3://bucket/scan_orders",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "scan table");

    var snap =
        TestSupport.createSnapshot(
            snapshot, tbl.getResourceId(), 501L, System.currentTimeMillis() - 1000L);

    var connector = createDummyConnector(cat.getResourceId(), ns.getResourceId(), "bundle");
    attachConnectorToTable(tbl.getResourceId(), connector);

    var name =
        NameRef.newBuilder().setCatalog(catName).addPath("scan").setName("scan_orders").build();

    var begin =
        lifecycle.beginQuery(
            BeginQueryRequest.newBuilder().setDefaultCatalogId(cat.getResourceId()).build());

    var queryId = begin.getQuery().getQueryId();

    // Pin tables + snapshots
    schema.describeInputs(
        DescribeInputsRequest.newBuilder()
            .setQueryId(queryId)
            .addInputs(
                QueryInput.newBuilder()
                    .setName(name)
                    .setTableId(tbl.getResourceId())
                    .setSnapshot(
                        SnapshotRef.newBuilder().setSnapshotId(snap.getSnapshotId()).build())
                    .build())
            .build());

    InitScanRequest initReq =
        InitScanRequest.newBuilder().setQueryId(queryId).setTableId(tbl.getResourceId()).build();
    var resp = collectScanFileStream(initReq);

    assertEquals(0, resp.dataFiles().size());
    assertEquals(0, resp.deleteFiles().size());
  }

  @Test
  void fetchScanBundleReturnsTableInfo() throws Exception {

    var catName = catalogPrefix + "scan_info";
    var cat = TestSupport.createCatalog(catalog, catName, "");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "scan_info", List.of("scan"), "");

    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "scan_meta",
            "s3://bucket/scan_meta",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "table with metadata");

    var metadataLocation = "s3://bucket/meta/scan_meta.metadata.json";
    FieldMask mask = FieldMask.newBuilder().addPaths("properties").build();
    tbl =
        table
            .updateTable(
                UpdateTableRequest.newBuilder()
                    .setTableId(tbl.getResourceId())
                    .setSpec(
                        TableSpec.newBuilder().putProperties("metadata-location", metadataLocation))
                    .setUpdateMask(mask)
                    .build())
            .getTable();

    var snap =
        TestSupport.createSnapshot(
            snapshot, tbl.getResourceId(), 602L, System.currentTimeMillis() - 2000L);

    var connector = createDummyConnector(cat.getResourceId(), ns.getResourceId(), "meta");
    attachConnectorToTable(tbl.getResourceId(), connector);

    var name =
        NameRef.newBuilder().setCatalog(catName).addPath("scan").setName("scan_meta").build();

    var begin =
        lifecycle.beginQuery(
            BeginQueryRequest.newBuilder().setDefaultCatalogId(cat.getResourceId()).build());

    var queryId = begin.getQuery().getQueryId();

    schema.describeInputs(
        DescribeInputsRequest.newBuilder()
            .setQueryId(queryId)
            .addInputs(
                QueryInput.newBuilder()
                    .setName(name)
                    .setTableId(tbl.getResourceId())
                    .setSnapshot(
                        SnapshotRef.newBuilder().setSnapshotId(snap.getSnapshotId()).build())
                    .build())
            .build());

    InitScanRequest initReq =
        InitScanRequest.newBuilder().setQueryId(queryId).setTableId(tbl.getResourceId()).build();
    var resp = collectScanFileStream(initReq);

    assertNotNull(resp.tableInfo());
    var info = resp.tableInfo();
    assertEquals(tbl.getResourceId(), info.getTableId());
    assertEquals(tbl.getSchemaJson(), info.getSchemaJson());
    assertEquals(metadataLocation, info.getMetadataLocation());
    assertEquals(1, info.getPropertiesCount());
    assertEquals(metadataLocation, info.getPropertiesMap().get("metadata-location"));
  }

  /** Ensures FetchScanBundle rejects unpinned tables. */
  @Test
  void fetchScanBundleRejectsUnpinnedTables() throws Exception {

    var catName = catalogPrefix + "scan_nf";
    var cat = TestSupport.createCatalog(catalog, catName, "");
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "scan_nf", List.of("nf"), "");

    var tblA =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "nf_orders",
            "s3://bucket/nf_orders",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "nf table");

    var tblB =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "nf_other",
            "s3://bucket/nf_other",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "nf other table");

    var snap =
        TestSupport.createSnapshot(
            snapshot, tblA.getResourceId(), 777L, System.currentTimeMillis() - 5000L);

    var connector = createDummyConnector(cat.getResourceId(), ns.getResourceId(), "reject");
    attachConnectorToTable(tblA.getResourceId(), connector);

    // Start query
    var begin =
        lifecycle.beginQuery(
            BeginQueryRequest.newBuilder().setDefaultCatalogId(cat.getResourceId()).build());

    var queryId = begin.getQuery().getQueryId();

    // Pin ONLY table A
    schema.describeInputs(
        DescribeInputsRequest.newBuilder()
            .setQueryId(queryId)
            .addInputs(
                QueryInput.newBuilder()
                    .setTableId(tblA.getResourceId())
                    .setSnapshot(
                        SnapshotRef.newBuilder().setSnapshotId(snap.getSnapshotId()).build())
                    .build())
            .build());

    // Try scanning table B → should fail
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                collectScanFileStream(
                    InitScanRequest.newBuilder()
                        .setQueryId(queryId)
                        .setTableId(tblB.getResourceId())
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "not pinned for query");
  }

  /* -----------------------------------------------------------------------
   * Helpers
   * --------------------------------------------------------------------- */

  private Connector createDummyConnector(
      ResourceId catalogId, ResourceId namespaceId, String suffix) {

    var source =
        SourceSelector.newBuilder()
            .setNamespace(NamespacePath.newBuilder().addSegments("examples").addSegments("iceberg"))
            .build();

    var destination =
        DestinationTarget.newBuilder().setCatalogId(catalogId).setNamespaceId(namespaceId).build();

    var spec =
        ConnectorSpec.newBuilder()
            .setDisplayName("qs-" + suffix)
            .setKind(ConnectorKind.CK_UNITY)
            .setUri("dummy://ignored")
            .setSource(source)
            .setDestination(destination)
            .setAuth(AuthConfig.newBuilder().setScheme("none"))
            .build();

    return TestSupport.createConnector(connectors, spec);
  }

  private void attachConnectorToTable(ResourceId tableId, Connector connector) {

    var upstream =
        UpstreamRef.newBuilder()
            .setConnectorId(connector.getResourceId())
            .setUri("dummy://ignored")
            .setTableDisplayName(connector.getDisplayName() + "_src")
            .setFormat(TableFormat.TF_ICEBERG)
            .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_FIELD_ID)
            .addNamespacePath("examples")
            .addNamespacePath("iceberg")
            .build();

    var spec = TableSpec.newBuilder().setUpstream(upstream).build();

    var mask = FieldMask.newBuilder().addPaths("upstream").build();

    table.updateTable(
        UpdateTableRequest.newBuilder()
            .setTableId(tableId)
            .setSpec(spec)
            .setUpdateMask(mask)
            .build());
  }

  private record ScanFileStreamResponse(
      TableInfo tableInfo, List<ScanFile> dataFiles, List<ScanFile> deleteFiles) {}

  private ScanFileStreamResponse collectScanFileStream(InitScanRequest request) {
    InitScanResponse initResp = scan.initScan(request);
    ScanHandle handle = initResp.getHandle();
    List<ScanFile> deleteFiles = new ArrayList<>();
    var deleteIter = scan.streamDeleteFiles(handle);
    while (deleteIter.hasNext()) {
      DeleteFileBatch batch = deleteIter.next();
      for (var delete : batch.getItemsList()) {
        deleteFiles.add(delete.getFile());
      }
    }
    List<ScanFile> dataFiles = new ArrayList<>();
    var dataIter = scan.streamDataFiles(handle);
    while (dataIter.hasNext()) {
      DataFileBatch batch = dataIter.next();
      for (var data : batch.getItemsList()) {
        dataFiles.add(data.getFile());
      }
    }
    scan.closeScan(handle);
    return new ScanFileStreamResponse(initResp.getTableInfo(), dataFiles, deleteFiles);
  }

  private static final AtomicLong SNAPSHOT_SEQ = new AtomicLong(1000);

  private ScanHandle prepareScanHandle(String tag) throws Exception {
    var catName = catalogPrefix + "scan_precondition_" + tag;
    var cat = TestSupport.createCatalog(catalog, catName, "");
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "scan", List.of("scan"), "");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "scan_table_" + tag,
            "s3://bucket/scan_table_" + tag,
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "scan table");
    var snap =
        TestSupport.createSnapshot(
            snapshot,
            tbl.getResourceId(),
            SNAPSHOT_SEQ.getAndIncrement(),
            System.currentTimeMillis());
    var connector = createDummyConnector(cat.getResourceId(), ns.getResourceId(), tag);
    attachConnectorToTable(tbl.getResourceId(), connector);

    var begin =
        lifecycle.beginQuery(
            BeginQueryRequest.newBuilder().setDefaultCatalogId(cat.getResourceId()).build());
    var queryId = begin.getQuery().getQueryId();
    schema.describeInputs(
        DescribeInputsRequest.newBuilder()
            .setQueryId(queryId)
            .addInputs(
                QueryInput.newBuilder()
                    .setTableId(tbl.getResourceId())
                    .setSnapshot(
                        SnapshotRef.newBuilder().setSnapshotId(snap.getSnapshotId()).build())
                    .build())
            .build());
    return scan.initScan(
            InitScanRequest.newBuilder()
                .setQueryId(queryId)
                .setTableId(tbl.getResourceId())
                .build())
        .getHandle();
  }

  @Test
  void streamDataBeforeDeletesFails() throws Exception {
    ScanHandle handle = prepareScanHandle("precondition");
    StatusRuntimeException ex =
        assertThrows(StatusRuntimeException.class, () -> scan.streamDataFiles(handle).hasNext());
    assertEquals(Status.Code.FAILED_PRECONDITION, ex.getStatus().getCode());
    scan.closeScan(handle);
  }

  @Test
  void closeScanIsIdempotent() throws Exception {
    ScanHandle handle = prepareScanHandle("close twice");
    scan.streamDeleteFiles(handle).forEachRemaining(batch -> {});
    scan.closeScan(handle);
    assertDoesNotThrow(() -> scan.closeScan(handle));
  }

  @Test
  void incompleteDeleteStreamPreventsData() throws Exception {
    ScanHandle handle = prepareScanHandle("delete-short");
    scan.streamDeleteFiles(handle);
    // intentionally keep delete stream unsettled (never drained)
    assertThrows(StatusRuntimeException.class, () -> scan.streamDataFiles(handle).hasNext());
    scan.closeScan(handle);
  }
}
