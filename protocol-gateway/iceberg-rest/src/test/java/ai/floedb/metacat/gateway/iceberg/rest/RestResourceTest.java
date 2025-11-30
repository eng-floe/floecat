package ai.floedb.metacat.gateway.iceberg.rest;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.metacat.catalog.rpc.CreateNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.CreateTableResponse;
import ai.floedb.metacat.catalog.rpc.DeleteNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.DeleteTableRequest;
import ai.floedb.metacat.catalog.rpc.DeleteViewRequest;
import ai.floedb.metacat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.metacat.catalog.rpc.GetNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.metacat.catalog.rpc.ListNamespacesResponse;
import ai.floedb.metacat.catalog.rpc.ListTablesRequest;
import ai.floedb.metacat.catalog.rpc.ListTablesResponse;
import ai.floedb.metacat.catalog.rpc.ListViewsResponse;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.metacat.catalog.rpc.PartitionField;
import ai.floedb.metacat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.metacat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.metacat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.ResolveTableResponse;
import ai.floedb.metacat.catalog.rpc.ResolveViewResponse;
import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.metacat.catalog.rpc.UpdateTableResponse;
import ai.floedb.metacat.catalog.rpc.UpdateViewResponse;
import ai.floedb.metacat.catalog.rpc.View;
import ai.floedb.metacat.catalog.rpc.ViewServiceGrpc;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcClients;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcWithHeaders;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.log.RequestLoggingFilter;
import io.restassured.filter.log.ResponseLoggingFilter;
import io.restassured.specification.RequestSpecification;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@QuarkusTest
class RestResourceTest {
  @InjectMock GrpcWithHeaders grpc;
  @InjectMock GrpcClients clients;

  private TableServiceGrpc.TableServiceBlockingStub tableStub;
  private DirectoryServiceGrpc.DirectoryServiceBlockingStub directoryStub;
  private NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaceStub;
  private ViewServiceGrpc.ViewServiceBlockingStub viewStub;
  private SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotStub;
  private TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statsStub;
  private RequestSpecification defaultSpec;

  @BeforeEach
  void setUp() {
    tableStub = mock(TableServiceGrpc.TableServiceBlockingStub.class);
    directoryStub = mock(DirectoryServiceGrpc.DirectoryServiceBlockingStub.class);
    namespaceStub = mock(NamespaceServiceGrpc.NamespaceServiceBlockingStub.class);
    viewStub = mock(ViewServiceGrpc.ViewServiceBlockingStub.class);
    snapshotStub = mock(SnapshotServiceGrpc.SnapshotServiceBlockingStub.class);
    statsStub = mock(TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub.class);

    when(clients.table()).thenReturn(tableStub);
    when(clients.directory()).thenReturn(directoryStub);
    when(clients.namespace()).thenReturn(namespaceStub);
    when(clients.view()).thenReturn(viewStub);
    when(clients.snapshot()).thenReturn(snapshotStub);
    when(clients.stats()).thenReturn(statsStub);
    when(grpc.raw()).thenReturn(clients);
    when(grpc.withHeaders(tableStub)).thenReturn(tableStub);
    when(grpc.withHeaders(directoryStub)).thenReturn(directoryStub);
    when(grpc.withHeaders(namespaceStub)).thenReturn(namespaceStub);
    when(grpc.withHeaders(viewStub)).thenReturn(viewStub);
    when(grpc.withHeaders(snapshotStub)).thenReturn(snapshotStub);
    when(grpc.withHeaders(statsStub)).thenReturn(statsStub);
    ResourceId catalogId = ResourceId.newBuilder().setId("cat:default").build();
    when(directoryStub.resolveCatalog(any()))
        .thenReturn(ResolveCatalogResponse.newBuilder().setResourceId(catalogId).build());
    defaultSpec =
        new RequestSpecBuilder()
            .addHeader("x-tenant-id", "tenant1")
            .addHeader("authorization", "Bearer token")
            .build();
    RestAssured.requestSpecification = defaultSpec;
    RestAssured.filters(new RequestLoggingFilter(), new ResponseLoggingFilter());
  }

  @Test
  void listsTablesWithPagination() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());

    Table table1 =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .setDisplayName("orders")
            .build();
    Table table2 =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:customers"))
            .setDisplayName("customers")
            .build();

    // We still return a PageResponse from the mock, but we don't assert the token at REST level.
    PageResponse page = PageResponse.newBuilder().setNextPageToken("next").setTotalSize(3).build();

    when(tableStub.listTables(any()))
        .thenReturn(
            ListTablesResponse.newBuilder()
                .addTables(table1)
                .addTables(table2)
                .setPage(page)
                .build());

    given()
        .header("x-tenant-id", "tenant1")
        .when()
        .get("/v1/foo/namespaces/db/tables?pageSize=2&pageToken=tok")
        .then()
        .statusCode(200)
        // verify mapping to identifiers
        .body("identifiers.size()", equalTo(2))
        .body("identifiers[0].name", equalTo("orders"))
        .body("identifiers[0].namespace[0]", equalTo("db"))
        .body("identifiers[1].name", equalTo("customers"))
        .body("identifiers[1].namespace[0]", equalTo("db"));
    // no assertion on $['next-page-token']

    ArgumentCaptor<ListTablesRequest> req = ArgumentCaptor.forClass(ListTablesRequest.class);
    verify(tableStub).listTables(req.capture());

    // verify pagination inputs were passed through
    assertEquals(2, req.getValue().getPage().getPageSize());
    assertEquals("tok", req.getValue().getPage().getPageToken());
    assertEquals(nsId, req.getValue().getNamespaceId());
  }

  @Test
  void listsViewsWithPagination() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());

    View view1 =
        View.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:reports"))
            .setDisplayName("reports")
            .build();
    View view2 =
        View.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:dashboards"))
            .setDisplayName("dashboards")
            .build();

    PageResponse page = PageResponse.newBuilder().setNextPageToken("np").setTotalSize(3).build();

    when(viewStub.listViews(any()))
        .thenReturn(
            ListViewsResponse.newBuilder().addViews(view1).addViews(view2).setPage(page).build());

    given()
        .when()
        .get("/v1/foo/namespaces/db/views?pageSize=10&pageToken=tok")
        .then()
        .statusCode(200)
        // verify mapping to identifiers
        .body("identifiers.size()", equalTo(2))
        .body("identifiers[0].name", equalTo("reports"))
        .body("identifiers[0].namespace[0]", equalTo("db"))
        .body("identifiers[1].name", equalTo("dashboards"))
        .body("identifiers[1].namespace[0]", equalTo("db"));
    // no assertion on $['next-page-token']
  }

  @Test
  void createsNamespace() {
    ResourceId nsId = ResourceId.newBuilder().setId("foo:analytics").build();
    Namespace created =
        Namespace.newBuilder()
            .setResourceId(nsId)
            .setDisplayName("analytics")
            .setDescription("desc")
            .addParents("foo")
            .build();
    when(namespaceStub.createNamespace(any()))
        .thenReturn(CreateNamespaceResponse.newBuilder().setNamespace(created).build());

    given()
        .body("{\"namespace\":\"analytics\",\"description\":\"desc\"}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces")
        .then()
        .statusCode(201)
        .body("namespace[0]", equalTo("analytics"))
        .body("properties.description", equalTo("desc"));
  }

  @Test
  void listsAndGetsNamespace() {
    ResourceId nsId = ResourceId.newBuilder().setId("foo:analytics").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());

    Namespace ns = Namespace.newBuilder().setResourceId(nsId).setDisplayName("analytics").build();
    PageResponse page = PageResponse.newBuilder().setTotalSize(1).build();
    when(namespaceStub.listNamespaces(any()))
        .thenReturn(ListNamespacesResponse.newBuilder().addNamespaces(ns).setPage(page).build());
    when(namespaceStub.getNamespace(any()))
        .thenReturn(GetNamespaceResponse.newBuilder().setNamespace(ns).build());

    given()
        .when()
        .get("/v1/foo/namespaces?recursive=true&pageSize=5")
        .then()
        .statusCode(200)
        .body("namespaces[0][0]", equalTo("analytics"));

    given()
        .when()
        .get("/v1/foo/namespaces/analytics")
        .then()
        .statusCode(200)
        .body("namespace[0]", equalTo("analytics"));

    ArgumentCaptor<ListNamespacesRequest> req =
        ArgumentCaptor.forClass(ListNamespacesRequest.class);
    verify(namespaceStub).listNamespaces(req.capture());
    assertEquals(5, req.getValue().getPage().getPageSize());
    assertEquals(true, req.getValue().getRecursive());
  }

  @Test
  void deletesNamespaceHonorsRequireEmpty() {
    ResourceId nsId = ResourceId.newBuilder().setId("foo:analytics").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());

    given().when().delete("/v1/foo/namespaces/analytics?requireEmpty=false").then().statusCode(204);

    ArgumentCaptor<DeleteNamespaceRequest> req =
        ArgumentCaptor.forClass(DeleteNamespaceRequest.class);
    verify(namespaceStub).deleteNamespace(req.capture());
    assertEquals(false, req.getValue().getRequireEmpty());
  }

  @Test
  void updatesAndDeletesView() {
    ResourceId viewId = ResourceId.newBuilder().setId("cat:db:reports").build();
    when(directoryStub.resolveView(any()))
        .thenReturn(ResolveViewResponse.newBuilder().setResourceId(viewId).build());

    View updated =
        View.newBuilder()
            .setResourceId(viewId)
            .setDisplayName("reports_new")
            .setSql("select 1")
            .build();
    when(viewStub.updateView(any()))
        .thenReturn(UpdateViewResponse.newBuilder().setView(updated).build());

    given()
        .body("{\"name\":\"reports_new\",\"sql\":\"select 1\"}")
        .header("Content-Type", "application/json")
        .when()
        .put("/v1/foo/namespaces/db/views/reports")
        .then()
        .statusCode(200)
        .body("metadata.versions[0].representations[0].sql", equalTo("select 1"));

    given().when().delete("/v1/foo/namespaces/db/views/reports").then().statusCode(204);

    verify(viewStub).deleteView(any(DeleteViewRequest.class));
  }

  @Test
  void listsSnapshots() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    Timestamp upstream = Timestamp.newBuilder().setSeconds(1700000000L).build();
    Timestamp ingested = Timestamp.newBuilder().setSeconds(1700000001L).build();
    PartitionSpecInfo partitionSpec =
        PartitionSpecInfo.newBuilder()
            .setSpecId(1)
            .setSpecName("iceberg-spec")
            .addFields(
                PartitionField.newBuilder()
                    .setFieldId(99)
                    .setName("pcol")
                    .setTransform("identity")
                    .build())
            .build();
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(42L)
            .setParentSnapshotId(7L)
            .setSchemaJson("{\"type\":\"struct\"}")
            .setUpstreamCreatedAt(upstream)
            .setIngestedAt(ingested)
            .setPartitionSpec(partitionSpec)
            .build();
    PageResponse page = PageResponse.newBuilder().setTotalSize(1).build();
    when(snapshotStub.listSnapshots(any()))
        .thenReturn(
            ai.floedb.metacat.catalog.rpc.ListSnapshotsResponse.newBuilder()
                .addSnapshots(snapshot)
                .setPage(page)
                .build());

    given()
        .when()
        .get("/v1/foo/namespaces/db/tables/orders/snapshots?pageSize=5")
        .then()
        .statusCode(200)
        .body("snapshots[0].snapshotId", equalTo(42))
        .body("snapshots[0].parentSnapshotId", equalTo(7))
        .body("snapshots[0].schemaJson", equalTo("{\"type\":\"struct\"}"))
        .body("snapshots[0].partitionSpec.specId", equalTo(1))
        .body("snapshots[0].partitionSpec.fields[0].name", equalTo("pcol"))
        .body("page.totalSize", equalTo(1));

    when(snapshotStub.getSnapshot(any()))
        .thenReturn(
            ai.floedb.metacat.catalog.rpc.GetSnapshotResponse.newBuilder()
                .setSnapshot(snapshot)
                .build());

    given()
        .when()
        .get("/v1/foo/namespaces/db/tables/orders/snapshots/42")
        .then()
        .statusCode(200)
        .body("snapshotId", equalTo(42))
        .body("upstreamCreatedAt", equalTo("2023-11-14T22:13:20Z"))
        .body("ingestedAt", equalTo("2023-11-14T22:13:21Z"))
        .body("partitionSpec.specName", equalTo("iceberg-spec"));

    when(snapshotStub.createSnapshot(any()))
        .thenReturn(
            ai.floedb.metacat.catalog.rpc.CreateSnapshotResponse.newBuilder()
                .setSnapshot(snapshot)
                .build());

    given()
        .body("{\"snapshotId\":43,\"parentSnapshotId\":42}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders/snapshots")
        .then()
        .statusCode(201)
        .body("snapshotId", equalTo(42));

    given()
        .when()
        .post("/v1/foo/namespaces/db/tables/orders/snapshots/42/rollback")
        .then()
        .statusCode(501)
        .body("error.code", equalTo(501))
        .body("error.type", equalTo("UnsupportedOperationException"));

    given()
        .when()
        .delete("/v1/foo/namespaces/db/tables/orders/snapshots/42")
        .then()
        .statusCode(204);
  }

  @Test
  void listsSchemas() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    Timestamp upstream = Timestamp.newBuilder().setSeconds(1700000000L).build();
    Timestamp ingested = Timestamp.newBuilder().setSeconds(1700000001L).build();
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setSnapshotId(43L)
            .setSchemaJson("{\"type\":\"struct\"}")
            .setUpstreamCreatedAt(upstream)
            .setIngestedAt(ingested)
            .build();
    PageResponse page = PageResponse.newBuilder().setNextPageToken("tok").setTotalSize(1).build();
    when(snapshotStub.listSnapshots(any()))
        .thenReturn(
            ai.floedb.metacat.catalog.rpc.ListSnapshotsResponse.newBuilder()
                .addSnapshots(snapshot)
                .setPage(page)
                .build());

    given()
        .when()
        .get("/v1/foo/namespaces/db/tables/orders/schemas?pageSize=3")
        .then()
        .statusCode(200)
        .body("schemas[0].schemaJson", equalTo("{\"type\":\"struct\"}"))
        .body("schemas[0].upstreamCreatedAt", equalTo("2023-11-14T22:13:20Z"))
        .body("schemas[0].ingestedAt", equalTo("2023-11-14T22:13:21Z"))
        .body("page.totalSize", equalTo(1));
  }

  @Test
  void listsPartitionSpecs() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    PartitionSpecInfo spec =
        PartitionSpecInfo.newBuilder()
            .setSpecId(2)
            .setSpecName("delta-spec")
            .addFields(
                PartitionField.newBuilder()
                    .setFieldId(1)
                    .setName("bucket")
                    .setTransform("identity")
                    .build())
            .build();
    Timestamp upstream = Timestamp.newBuilder().setSeconds(1700000000L).build();
    Timestamp ingested = Timestamp.newBuilder().setSeconds(1700000001L).build();
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setSnapshotId(44L)
            .setPartitionSpec(spec)
            .setUpstreamCreatedAt(upstream)
            .setIngestedAt(ingested)
            .build();
    PageResponse page = PageResponse.newBuilder().setTotalSize(1).build();
    when(snapshotStub.listSnapshots(any()))
        .thenReturn(
            ai.floedb.metacat.catalog.rpc.ListSnapshotsResponse.newBuilder()
                .addSnapshots(snapshot)
                .setPage(page)
                .build());

    given()
        .when()
        .get("/v1/foo/namespaces/db/tables/orders/partition-specs?pageToken=tok")
        .then()
        .statusCode(200)
        .body("specs[0].partitionSpec.specId", equalTo(2))
        .body("specs[0].partitionSpec.fields[0].fieldId", equalTo(1))
        .body("specs[0].upstreamCreatedAt", equalTo("2023-11-14T22:13:20Z"))
        .body("page.totalSize", equalTo(1));
  }

  @Test
  void fetchesStats() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    var stats =
        ai.floedb.metacat.catalog.rpc.TableStats.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(42)
            .setRowCount(100)
            .setDataFileCount(1)
            .setTotalSizeBytes(64)
            .build();
    when(statsStub.getTableStats(any()))
        .thenReturn(
            ai.floedb.metacat.catalog.rpc.GetTableStatsResponse.newBuilder()
                .setStats(stats)
                .build());

    var column =
        ai.floedb.metacat.catalog.rpc.ColumnStats.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(42)
            .setColumnId("c1")
            .setColumnName("col")
            .setLogicalType("string")
            .setValueCount(100)
            .build();
    when(statsStub.listColumnStats(any()))
        .thenReturn(
            ai.floedb.metacat.catalog.rpc.ListColumnStatsResponse.newBuilder()
                .addColumns(column)
                .build());

    var file =
        ai.floedb.metacat.catalog.rpc.FileColumnStats.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(42)
            .setFilePath("file")
            .setRowCount(100)
            .setSizeBytes(64)
            .setPartitionSpecId(0)
            .addColumns(column)
            .build();
    when(statsStub.listFileColumnStats(any()))
        .thenReturn(
            ai.floedb.metacat.catalog.rpc.ListFileColumnStatsResponse.newBuilder()
                .addFileColumns(file)
                .build());

    given()
        .when()
        .get("/v1/foo/namespaces/db/tables/orders/snapshots/42/stats")
        .then()
        .statusCode(200)
        .body("table.rowCount", equalTo(100))
        .body("columns[0].columnName", equalTo("col"))
        .body("files[0].filePath", equalTo("file"));
  }

  @Test
  void createsUpdatesAndDeletesTable() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    Table created =
        Table.newBuilder()
            .setResourceId(tableId)
            .setDisplayName("orders")
            .setCatalogId(ResourceId.newBuilder().setId("cat"))
            .setNamespaceId(nsId)
            .putProperties("metadata-location", "s3://bucket/path/metadata.json")
            .build();
    when(tableStub.createTable(any()))
        .thenReturn(CreateTableResponse.newBuilder().setTable(created).build());

    given()
        .body("{\"name\":\"orders\"}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables")
        .then()
        .statusCode(200)
        .body("'metadata-location'", equalTo("s3://bucket/path/metadata.json"))
        .body("metadata.properties.'metadata-location'", equalTo("s3://bucket/path/metadata.json"));

    Table updated =
        Table.newBuilder()
            .setResourceId(tableId)
            .setDisplayName("orders_new")
            .setCatalogId(ResourceId.newBuilder().setId("cat"))
            .setNamespaceId(nsId)
            .putProperties("metadata-location", "s3://bucket/path/metadata2.json")
            .build();
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(updated).build());

    given()
        .body("{\"name\":\"orders_new\",\"schemaJson\":\"{}\"}")
        .header("Content-Type", "application/json")
        .when()
        .put("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(200)
        .body("'metadata-location'", equalTo("s3://bucket/path/metadata2.json"));

    given().when().delete("/v1/foo/namespaces/db/tables/orders").then().statusCode(204);

    verify(tableStub).deleteTable(any(DeleteTableRequest.class));
  }

  @Test
  void mapsGrpcErrorToIcebergError() {
    StatusRuntimeException ex =
        Status.PERMISSION_DENIED.withDescription("nope").asRuntimeException();
    when(tableStub.listTables(any())).thenThrow(ex);
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(
            ResolveNamespaceResponse.newBuilder()
                .setResourceId(ResourceId.newBuilder().setId("cat:db"))
                .build());

    given()
        .when()
        .get("/v1/foo/namespaces/db/tables")
        .then()
        .statusCode(403)
        .body("error.message", equalTo("nope"))
        .body("error.type", equalTo("ForbiddenException"))
        .body("error.code", equalTo(403));

    verify(grpc).withHeaders(tableStub);
  }

  @Test
  void missingTenantHeaderReturns401() {
    RestAssured.requestSpecification = null;
    given()
        .when()
        .get("/v1/foo/namespaces/db/tables")
        .then()
        .statusCode(401)
        .body("error.message", equalTo("missing tenant header"))
        .body("error.code", equalTo(401));
    RestAssured.requestSpecification = defaultSpec;
  }

  @Test
  void missingAuthHeaderReturns401() {
    RestAssured.requestSpecification = null;
    given()
        .header("x-tenant-id", "tenant1")
        .when()
        .get("/v1/foo/namespaces/db/tables")
        .then()
        .statusCode(401)
        .body("error.message", equalTo("missing authorization header"))
        .body("error.code", equalTo(401));
    RestAssured.requestSpecification = defaultSpec;
  }

  @Test
  void returnsConfigDto() {
    given()
        .when()
        .get("/v1/config")
        .then()
        .statusCode(200)
        .body("defaults.'catalog-name'", equalTo("metacat"))
        .body("endpoints", hasItems("POST /v1/{prefix}/tables/rename", "POST /v1/{prefix}/views/rename"));
  }
}
