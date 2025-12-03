package ai.floedb.metacat.gateway.iceberg.rest;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.metacat.catalog.rpc.CreateNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.CreateSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.CreateSnapshotResponse;
import ai.floedb.metacat.catalog.rpc.CreateTableResponse;
import ai.floedb.metacat.catalog.rpc.DeleteNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.DeleteSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.DeleteSnapshotResponse;
import ai.floedb.metacat.catalog.rpc.DeleteTableRequest;
import ai.floedb.metacat.catalog.rpc.DeleteViewRequest;
import ai.floedb.metacat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.metacat.catalog.rpc.GetNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.GetSnapshotResponse;
import ai.floedb.metacat.catalog.rpc.GetTableResponse;
import ai.floedb.metacat.catalog.rpc.IcebergBlobMetadata;
import ai.floedb.metacat.catalog.rpc.IcebergEncryptedKey;
import ai.floedb.metacat.catalog.rpc.IcebergMetadata;
import ai.floedb.metacat.catalog.rpc.IcebergPartitionStatisticsFile;
import ai.floedb.metacat.catalog.rpc.IcebergRef;
import ai.floedb.metacat.catalog.rpc.IcebergSortField;
import ai.floedb.metacat.catalog.rpc.IcebergSortOrder;
import ai.floedb.metacat.catalog.rpc.IcebergStatisticsFile;
import ai.floedb.metacat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.metacat.catalog.rpc.ListNamespacesResponse;
import ai.floedb.metacat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.metacat.catalog.rpc.ListTablesRequest;
import ai.floedb.metacat.catalog.rpc.ListTablesResponse;
import ai.floedb.metacat.catalog.rpc.ListViewsResponse;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.metacat.catalog.rpc.PartitionField;
import ai.floedb.metacat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.metacat.catalog.rpc.PutTableStatsRequest;
import ai.floedb.metacat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.metacat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.ResolveTableResponse;
import ai.floedb.metacat.catalog.rpc.ResolveViewResponse;
import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.metacat.catalog.rpc.UpdateSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.UpdateTableRequest;
import ai.floedb.metacat.catalog.rpc.UpdateTableResponse;
import ai.floedb.metacat.catalog.rpc.UpdateViewResponse;
import ai.floedb.metacat.catalog.rpc.UpstreamRef;
import ai.floedb.metacat.catalog.rpc.View;
import ai.floedb.metacat.catalog.rpc.ViewServiceGrpc;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.connector.rpc.Connector;
import ai.floedb.metacat.connector.rpc.ConnectorsGrpc;
import ai.floedb.metacat.connector.rpc.CreateConnectorRequest;
import ai.floedb.metacat.connector.rpc.CreateConnectorResponse;
import ai.floedb.metacat.connector.rpc.DestinationTarget;
import ai.floedb.metacat.connector.rpc.SyncCaptureRequest;
import ai.floedb.metacat.connector.rpc.SyncCaptureResponse;
import ai.floedb.metacat.connector.rpc.TriggerReconcileRequest;
import ai.floedb.metacat.connector.rpc.TriggerReconcileResponse;
import ai.floedb.metacat.execution.rpc.ScanBundle;
import ai.floedb.metacat.execution.rpc.ScanFile;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcClients;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.metacat.query.rpc.BeginQueryRequest;
import ai.floedb.metacat.query.rpc.BeginQueryResponse;
import ai.floedb.metacat.query.rpc.DescribeInputsRequest;
import ai.floedb.metacat.query.rpc.FetchScanBundleRequest;
import ai.floedb.metacat.query.rpc.FetchScanBundleResponse;
import ai.floedb.metacat.query.rpc.GetQueryResponse;
import ai.floedb.metacat.query.rpc.Operator;
import ai.floedb.metacat.query.rpc.QueryDescriptor;
import ai.floedb.metacat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.metacat.query.rpc.QuerySchemaServiceGrpc;
import ai.floedb.metacat.query.rpc.QueryServiceGrpc;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.specification.RequestSpecification;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@QuarkusTest
class RestResourceTest {
  @InjectMock GrpcWithHeaders grpc;
  @InjectMock GrpcClients clients;
  @Inject StagedTableRepository stageRepository;

  private TableServiceGrpc.TableServiceBlockingStub tableStub;
  private DirectoryServiceGrpc.DirectoryServiceBlockingStub directoryStub;
  private NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaceStub;
  private ViewServiceGrpc.ViewServiceBlockingStub viewStub;
  private SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotStub;
  private TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statsStub;
  private QueryServiceGrpc.QueryServiceBlockingStub queryStub;
  private QueryScanServiceGrpc.QueryScanServiceBlockingStub queryScanStub;
  private QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub querySchemaStub;
  private ConnectorsGrpc.ConnectorsBlockingStub connectorsStub;
  private RequestSpecification defaultSpec;

  @BeforeEach
  void setUp() {
    tableStub = mock(TableServiceGrpc.TableServiceBlockingStub.class);
    directoryStub = mock(DirectoryServiceGrpc.DirectoryServiceBlockingStub.class);
    namespaceStub = mock(NamespaceServiceGrpc.NamespaceServiceBlockingStub.class);
    viewStub = mock(ViewServiceGrpc.ViewServiceBlockingStub.class);
    snapshotStub = mock(SnapshotServiceGrpc.SnapshotServiceBlockingStub.class);
    statsStub = mock(TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub.class);
    queryStub = mock(QueryServiceGrpc.QueryServiceBlockingStub.class);
    queryScanStub = mock(QueryScanServiceGrpc.QueryScanServiceBlockingStub.class);
    querySchemaStub = mock(QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub.class);
    connectorsStub = mock(ConnectorsGrpc.ConnectorsBlockingStub.class);

    when(clients.table()).thenReturn(tableStub);
    when(clients.directory()).thenReturn(directoryStub);
    when(clients.namespace()).thenReturn(namespaceStub);
    when(clients.view()).thenReturn(viewStub);
    when(clients.snapshot()).thenReturn(snapshotStub);
    when(clients.stats()).thenReturn(statsStub);
    when(clients.query()).thenReturn(queryStub);
    when(clients.queryScan()).thenReturn(queryScanStub);
    when(clients.querySchema()).thenReturn(querySchemaStub);
    when(clients.connectors()).thenReturn(connectorsStub);
    when(grpc.raw()).thenReturn(clients);
    when(grpc.withHeaders(tableStub)).thenReturn(tableStub);
    when(grpc.withHeaders(directoryStub)).thenReturn(directoryStub);
    when(grpc.withHeaders(namespaceStub)).thenReturn(namespaceStub);
    when(grpc.withHeaders(viewStub)).thenReturn(viewStub);
    when(grpc.withHeaders(snapshotStub)).thenReturn(snapshotStub);
    when(grpc.withHeaders(statsStub)).thenReturn(statsStub);
    when(grpc.withHeaders(queryStub)).thenReturn(queryStub);
    when(grpc.withHeaders(queryScanStub)).thenReturn(queryScanStub);
    when(grpc.withHeaders(querySchemaStub)).thenReturn(querySchemaStub);
    when(grpc.withHeaders(connectorsStub)).thenReturn(connectorsStub);
    when(querySchemaStub.describeInputs(any()))
        .thenReturn(ai.floedb.metacat.query.rpc.DescribeInputsResponse.getDefaultInstance());
    when(snapshotStub.createSnapshot(any()))
        .thenReturn(CreateSnapshotResponse.newBuilder().build());
    when(snapshotStub.deleteSnapshot(any()))
        .thenReturn(DeleteSnapshotResponse.newBuilder().build());
    when(snapshotStub.listSnapshots(any())).thenReturn(ListSnapshotsResponse.getDefaultInstance());
    when(snapshotStub.getSnapshot(any())).thenReturn(GetSnapshotResponse.getDefaultInstance());
    when(connectorsStub.triggerReconcile(any()))
        .thenReturn(TriggerReconcileResponse.newBuilder().setJobId("job").build());
    when(connectorsStub.syncCapture(any())).thenReturn(SyncCaptureResponse.newBuilder().build());
    ResourceId catalogId = ResourceId.newBuilder().setId("cat:default").build();
    when(directoryStub.resolveCatalog(any()))
        .thenReturn(ResolveCatalogResponse.newBuilder().setResourceId(catalogId).build());
    defaultSpec =
        new RequestSpecBuilder()
            .addHeader("x-tenant-id", "tenant1")
            .addHeader("authorization", "Bearer token")
            .build();
    RestAssured.requestSpecification = defaultSpec;
    stageRepository.clear();
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
  void getTableHonorsEtagAndSnapshotsParameter() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());
    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(ResourceId.newBuilder().setId("cat").build())
            .setNamespaceId(ResourceId.newBuilder().setId("cat:db").build())
            .setDisplayName("orders")
            .putProperties("metadata-location", "s3://bucket/path/metadata.json")
            .putProperties("current-snapshot-id", "5")
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(table).build());

    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .setMetadataLocation("s3://bucket/path/metadata.json")
            .putRefs("main", IcebergRef.newBuilder().setSnapshotId(5).setType("branch").build())
            .build();
    Snapshot metaSnapshot =
        Snapshot.newBuilder().setTableId(tableId).setSnapshotId(5).setIceberg(metadata).build();
    when(snapshotStub.getSnapshot(any()))
        .thenReturn(GetSnapshotResponse.newBuilder().setSnapshot(metaSnapshot).build());

    Snapshot snapshot1 =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(5)
            .setManifestList("manifest1")
            .putSummary("operation", "append")
            .build();
    Snapshot snapshot2 =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(6)
            .setManifestList("manifest2")
            .putSummary("operation", "overwrite")
            .build();
    when(snapshotStub.listSnapshots(any()))
        .thenReturn(
            ListSnapshotsResponse.newBuilder()
                .addSnapshots(snapshot1)
                .addSnapshots(snapshot2)
                .build());

    given()
        .when()
        .get("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(200)
        .header("ETag", equalTo("\"s3://bucket/path/metadata.json\""))
        .body("metadata.snapshots.size()", equalTo(2));

    given()
        .header("If-None-Match", "\"s3://bucket/path/metadata.json\"")
        .when()
        .get("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(304);

    given()
        .queryParam("snapshots", "refs")
        .when()
        .get("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(200)
        .body("metadata.snapshots.size()", equalTo(1))
        .body("metadata.snapshots[0].'snapshot-id'", equalTo(5));
  }

  @Test
  void registerTableCreatesAndEnqueuesReconcile() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());

    Table created =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:new_table"))
            .setDisplayName("new_table")
            .putProperties("metadata-location", "s3://b/db/new_table/metadata.json")
            .build();
    when(tableStub.createTable(any()))
        .thenReturn(CreateTableResponse.newBuilder().setTable(created).build());
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(created).build());

    ResourceId connectorId =
        ResourceId.newBuilder()
            .setId("conn-1")
            .setKind(ResourceKind.RK_CONNECTOR)
            .setTenantId("tenant1")
            .build();
    Connector connector =
        Connector.newBuilder()
            .setResourceId(connectorId)
            .setDestination(
                DestinationTarget.newBuilder()
                    .setCatalogId(ResourceId.newBuilder().setId("cat:default").build())
                    .setNamespaceId(nsId)
                    .setTableId(created.getResourceId())
                    .build())
            .build();
    when(connectorsStub.createConnector(any()))
        .thenReturn(CreateConnectorResponse.newBuilder().setConnector(connector).build());
    when(connectorsStub.triggerReconcile(any()))
        .thenReturn(TriggerReconcileResponse.newBuilder().setJobId("job-1").build());

    given()
        .header("x-tenant-id", "tenant1")
        .contentType(MediaType.APPLICATION_JSON)
        .body(
            "{\"name\":\"new_table\",\"metadata-location\":\"s3://b/db/new_table/metadata.json\"}")
        .when()
        .post("/v1/foo/namespaces/db/register")
        .then()
        .statusCode(200)
        .body("metadata-location", equalTo("s3://b/db/new_table/metadata.json"));

    ArgumentCaptor<CreateConnectorRequest> createReq =
        ArgumentCaptor.forClass(CreateConnectorRequest.class);
    verify(connectorsStub).createConnector(createReq.capture());
    assertEquals("s3://b/db/new_table/metadata.json", createReq.getValue().getSpec().getUri());
    assertEquals("new_table", createReq.getValue().getSpec().getSource().getTable());
    assertEquals(nsId, createReq.getValue().getSpec().getDestination().getNamespaceId());
    assertEquals("none", createReq.getValue().getSpec().getAuth().getScheme());
    assertEquals(
        "s3://b/db/new_table/metadata.json",
        createReq.getValue().getSpec().getPropertiesMap().get("external.metadata-location"));

    ArgumentCaptor<TriggerReconcileRequest> trigger =
        ArgumentCaptor.forClass(TriggerReconcileRequest.class);
    verify(connectorsStub).triggerReconcile(trigger.capture());
    assertEquals(connectorId, trigger.getValue().getConnectorId());
    assertEquals(1, trigger.getValue().getDestinationNamespacePathsCount());
    assertEquals("new_table", trigger.getValue().getDestinationTableDisplayName());

    ArgumentCaptor<SyncCaptureRequest> captureReq =
        ArgumentCaptor.forClass(SyncCaptureRequest.class);
    verify(connectorsStub).syncCapture(captureReq.capture());
    assertEquals(connectorId, captureReq.getValue().getConnectorId());
    assertEquals("new_table", captureReq.getValue().getDestinationTableDisplayName());
    assertFalse(captureReq.getValue().getIncludeStatistics());
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
        .body(
            """
            {
              "name":"orders",
              "location":"s3://warehouse/db/orders",
              "schema":{
                "schema-id":1,
                "last-column-id":1,
                "type":"struct",
                "fields":[{"id":1,"name":"id","required":true,"type":"long"}]
              }
            }
            """)
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
  void stageCreatePersistsMetadataWithoutRpc() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());

    given()
        .body(stageCreateRequest("orders"))
        .header("Iceberg-Transaction-Id", "stage-1")
        .contentType(MediaType.APPLICATION_JSON)
        .when()
        .post("/v1/foo/namespaces/db/tables")
        .then()
        .statusCode(200)
        .body("stage-id", equalTo("stage-1"))
        .body("requirements[0].type", equalTo("assert-create"));

    verify(tableStub, never()).createTable(any());
    StagedTableKey key =
        new StagedTableKey("tenant1", "foo", List.of("db"), "orders", "stage-1");
    assertTrue(stageRepository.get(key).isPresent());
  }

  @Test
  void transactionCommitCreatesStagedTable() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());
    when(directoryStub.resolveTable(any()))
        .thenThrow(new StatusRuntimeException(Status.NOT_FOUND));

    Table created =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .setDisplayName("orders")
            .setCatalogId(ResourceId.newBuilder().setId("cat"))
            .setNamespaceId(nsId)
            .putProperties("metadata-location", "s3://bucket/orders/metadata.json")
            .build();
    when(tableStub.createTable(any()))
        .thenReturn(CreateTableResponse.newBuilder().setTable(created).build());
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(created).build());

    ResourceId connectorId =
        ResourceId.newBuilder().setId("conn-1").setKind(ResourceKind.RK_CONNECTOR).build();
    Connector connector =
        Connector.newBuilder()
            .setResourceId(connectorId)
            .setDestination(
                DestinationTarget.newBuilder()
                    .setCatalogId(ResourceId.newBuilder().setId("cat").build())
                    .setNamespaceId(nsId)
                    .setTableId(created.getResourceId())
                    .build())
            .build();
    when(connectorsStub.createConnector(any()))
        .thenReturn(CreateConnectorResponse.newBuilder().setConnector(connector).build());

    // Stage the table metadata.
    given()
        .body(stageCreateRequest("orders"))
        .header("Iceberg-Transaction-Id", "stage-commit")
        .contentType(MediaType.APPLICATION_JSON)
        .when()
        .post("/v1/foo/namespaces/db/tables")
        .then()
        .statusCode(200);

    given()
        .body(
            """
            {
              "staged-ref-updates": [
                {
                  "table": {"namespace":["db"], "name":"orders"},
                  "stage-id": "stage-commit"
                }
              ]
            }
            """)
        .contentType(MediaType.APPLICATION_JSON)
        .when()
        .post("/v1/foo/tables/transactions/commit")
        .then()
        .statusCode(200)
        .body("results[0].table.name", equalTo("orders"))
        .body("results[0].stage-id", equalTo("stage-commit"))
        .body("results[0].metadata-location", equalTo("s3://bucket/orders/metadata.json"));

    StagedTableKey key =
        new StagedTableKey("tenant1", "foo", List.of("db"), "orders", "stage-commit");
    assertTrue(stageRepository.get(key).isEmpty());
    verify(tableStub, times(1)).createTable(any());
    verify(connectorsStub, times(1)).createConnector(any());
  }

  @Test
  void stageCommitViaTableEndpointCreatesTable() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());
    when(directoryStub.resolveTable(any()))
        .thenThrow(new StatusRuntimeException(Status.NOT_FOUND));

    Table created =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .setDisplayName("orders")
            .setNamespaceId(nsId)
            .putProperties("metadata-location", "s3://bucket/orders/metadata.json")
            .build();
    when(tableStub.createTable(any()))
        .thenReturn(CreateTableResponse.newBuilder().setTable(created).build());
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(created).build());
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(created).build());

    ResourceId connectorId =
        ResourceId.newBuilder()
            .setId("conn-2")
            .setKind(ResourceKind.RK_CONNECTOR)
            .setTenantId("tenant1")
            .build();
    Connector connector =
        Connector.newBuilder()
            .setResourceId(connectorId)
            .setDestination(
                DestinationTarget.newBuilder()
                    .setCatalogId(ResourceId.newBuilder().setId("cat:default").build())
                    .setNamespaceId(nsId)
                    .setTableId(created.getResourceId())
                    .build())
            .build();
    when(connectorsStub.createConnector(any()))
        .thenReturn(CreateConnectorResponse.newBuilder().setConnector(connector).build());

    given()
        .body(stageCreateRequest("orders"))
        .header("Iceberg-Transaction-Id", "stage-table")
        .contentType(MediaType.APPLICATION_JSON)
        .when()
        .post("/v1/foo/namespaces/db/tables")
        .then()
        .statusCode(200);

    given()
        .body(
            """
            {
              "stage-id": "stage-table",
              "requirements": [{"type":"assert-create"}],
              "updates": []
            }
            """)
        .contentType(MediaType.APPLICATION_JSON)
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(200);

    StagedTableKey key =
        new StagedTableKey("tenant1", "foo", List.of("db"), "orders", "stage-table");
    assertTrue(stageRepository.get(key).isEmpty());
    verify(tableStub, times(1)).createTable(any());
    verify(connectorsStub, times(1)).createConnector(any());
  }

  @Test
  void createTableWithMetadataLocationCreatesConnector() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());

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

    ResourceId connectorId =
        ResourceId.newBuilder()
            .setId("conn-1")
            .setKind(ResourceKind.RK_CONNECTOR)
            .setTenantId("tenant1")
            .build();
    Connector connector =
        Connector.newBuilder()
            .setResourceId(connectorId)
            .setDestination(
                DestinationTarget.newBuilder()
                    .setCatalogId(ResourceId.newBuilder().setId("cat:default").build())
                    .setNamespaceId(nsId)
                    .setTableId(tableId)
                    .build())
            .build();
    when(connectorsStub.createConnector(any()))
        .thenReturn(CreateConnectorResponse.newBuilder().setConnector(connector).build());

    given()
        .body(
            """
            {
              "name":"orders",
              "location":"s3://warehouse/db/orders",
              "schema":{
                "schema-id":1,
                "last-column-id":1,
                "type":"struct",
                "fields":[{"id":1,"name":"id","required":true,"type":"long"}]
              },
              "properties":{"metadata-location":"s3://bucket/path/metadata.json"}
            }
            """)
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables")
        .then()
        .statusCode(200);

    verify(connectorsStub).createConnector(any(CreateConnectorRequest.class));
    verify(connectorsStub).syncCapture(any(SyncCaptureRequest.class));
    verify(connectorsStub).triggerReconcile(any(TriggerReconcileRequest.class));
  }

  @Test
  void commitSupportsSetLocationUpdate() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    UpstreamRef upstream =
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_ICEBERG)
            .setUri("s3://bucket/path/")
            .build();
    Table current =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(ResourceId.newBuilder().setId("cat"))
            .setNamespaceId(nsId)
            .setUpstream(upstream)
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(current).build());

    UpstreamRef updatedUpstream = upstream.toBuilder().setUri("s3://bucket/new_path/").build();
    Table updated = Table.newBuilder(current).setUpstream(updatedUpstream).build();
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(updated).build());

    given()
        .body(
            "{\"updates\":[{\"action\":\"set-location\",\"location\":\"s3://bucket/new_path/\"}]}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(200)
        .body("metadata.location", equalTo("s3://bucket/new_path/"));

    ArgumentCaptor<UpdateTableRequest> request = ArgumentCaptor.forClass(UpdateTableRequest.class);
    verify(tableStub).updateTable(request.capture());
    assertEquals("s3://bucket/new_path/", request.getValue().getSpec().getUpstream().getUri());
  }

  @Test
  void commitAddSnapshotCreatesPlaceholderAndTriggersReconcile() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    UpstreamRef upstream =
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_ICEBERG)
            .setConnectorId(ResourceId.newBuilder().setId("conn").build())
            .build();
    Table existing =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(ResourceId.newBuilder().setId("cat"))
            .setNamespaceId(nsId)
            .setUpstream(upstream)
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(existing).build());
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(existing).build());

    given()
        .body(
            "{\"updates\":[{\"action\":\"add-snapshot\",\"snapshot\":{"
                + "\"snapshot-id\":5,"
                + "\"timestamp-ms\":1000,"
                + "\"parent-snapshot-id\":4,"
                + "\"manifest-list\":\"s3://bucket/manifest.avro\","
                + "\"summary\":{\"operation\":\"append\"}"
                + "}}]}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(200);

    ArgumentCaptor<CreateSnapshotRequest> snapReq =
        ArgumentCaptor.forClass(CreateSnapshotRequest.class);
    verify(snapshotStub).createSnapshot(snapReq.capture());
    assertEquals(5, snapReq.getValue().getSpec().getSnapshotId());
    verify(connectorsStub).triggerReconcile(any());
    verify(connectorsStub).syncCapture(any());
  }

  @Test
  void commitRemoveSnapshotsCallsDelete() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    Table existing =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(ResourceId.newBuilder().setId("cat"))
            .setNamespaceId(nsId)
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(existing).build());
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(existing).build());

    given()
        .body("{\"updates\":[{\"action\":\"remove-snapshots\",\"snapshot-ids\":[7,8]}]}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(200);

    ArgumentCaptor<DeleteSnapshotRequest> delReq =
        ArgumentCaptor.forClass(DeleteSnapshotRequest.class);
    verify(snapshotStub, times(2)).deleteSnapshot(delReq.capture());
    assertEquals(7L, delReq.getAllValues().get(0).getSnapshotId());
    assertEquals(8L, delReq.getAllValues().get(1).getSnapshotId());
    verify(connectorsStub, never()).triggerReconcile(any());
    verify(connectorsStub, never()).syncCapture(any());
  }

  @Test
  void commitSetSnapshotRefUpdatesMetadata() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    UpstreamRef upstream =
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_ICEBERG)
            .setConnectorId(ResourceId.newBuilder().setId("conn").build())
            .build();
    Table existing =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(ResourceId.newBuilder().setId("cat"))
            .setNamespaceId(nsId)
            .setUpstream(upstream)
            .putProperties("current-snapshot-id", "4")
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(existing).build());
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(existing).build());

    Snapshot snapshot =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(5)
            .setIceberg(
                IcebergMetadata.newBuilder()
                    .setTableUuid("uuid")
                    .putRefs(
                        "main", IcebergRef.newBuilder().setType("branch").setSnapshotId(4).build())
                    .build())
            .build();
    when(snapshotStub.getSnapshot(any()))
        .thenReturn(GetSnapshotResponse.newBuilder().setSnapshot(snapshot).build());

    given()
        .body(
            """
            {"updates":[
              {"action":"add-snapshot","snapshot":{
                "snapshot-id":5,
                "timestamp-ms":1000,
                "manifest-list":"s3://bucket/manifest.avro",
                "summary":{"operation":"append"}
              }},
              {"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":5}
            ]}
            """)
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(200);

    ArgumentCaptor<UpdateSnapshotRequest> updateReq =
        ArgumentCaptor.forClass(UpdateSnapshotRequest.class);
    verify(snapshotStub).updateSnapshot(updateReq.capture());
    assertEquals(5L, updateReq.getValue().getSpec().getSnapshotId());
    assertEquals(
        5L, updateReq.getValue().getSpec().getIceberg().getRefsOrThrow("main").getSnapshotId());
  }

  @Test
  void commitRemoveSnapshotRefUpdatesMetadata() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    Table existing =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(ResourceId.newBuilder().setId("cat"))
            .setNamespaceId(nsId)
            .putProperties("current-snapshot-id", "9")
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(existing).build());
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(existing).build());

    Snapshot snapshot =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(9)
            .setIceberg(
                IcebergMetadata.newBuilder()
                    .setTableUuid("uuid")
                    .putRefs(
                        "dev", IcebergRef.newBuilder().setType("branch").setSnapshotId(8).build())
                    .build())
            .build();
    when(snapshotStub.getSnapshot(any()))
        .thenReturn(GetSnapshotResponse.newBuilder().setSnapshot(snapshot).build());

    given()
        .body("{\"updates\":[{\"action\":\"remove-snapshot-ref\",\"ref-name\":\"dev\"}]}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(200);

    ArgumentCaptor<UpdateSnapshotRequest> updateReq =
        ArgumentCaptor.forClass(UpdateSnapshotRequest.class);
    verify(snapshotStub).updateSnapshot(updateReq.capture());
    assertFalse(updateReq.getValue().getSpec().getIceberg().getRefsMap().containsKey("dev"));
  }

  @Test
  void commitMetadataUpdatesModifyIcebergMetadata() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    Table existing =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(ResourceId.newBuilder().setId("cat"))
            .setNamespaceId(nsId)
            .putProperties("current-snapshot-id", "4")
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(existing).build());
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(existing).build());

    Snapshot snapshot =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(4)
            .setIceberg(
                IcebergMetadata.newBuilder()
                    .setTableUuid("uuid")
                    .setFormatVersion(2)
                    .setCurrentSchemaId(1)
                    .setDefaultSpecId(1)
                    .setDefaultSortOrderId(1)
                    .addSchemas(
                        ai.floedb.metacat.catalog.rpc.IcebergSchema.newBuilder()
                            .setSchemaId(1)
                            .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
                            .build())
                    .addSchemas(
                        ai.floedb.metacat.catalog.rpc.IcebergSchema.newBuilder()
                            .setSchemaId(3)
                            .setSchemaJson(
                                "{\"type\":\"struct\",\"fields\":[{\"name\":\"c\",\"type\":\"long\"}]}")
                            .build())
                    .addPartitionSpecs(
                        PartitionSpecInfo.newBuilder()
                            .setSpecId(1)
                            .setSpecName("spec-1")
                            .addFields(
                                PartitionField.newBuilder()
                                    .setFieldId(1)
                                    .setName("category")
                                    .setTransform("identity")
                                    .build())
                            .build())
                    .addPartitionSpecs(
                        PartitionSpecInfo.newBuilder()
                            .setSpecId(2)
                            .setSpecName("spec-2")
                            .addFields(
                                PartitionField.newBuilder()
                                    .setFieldId(2)
                                    .setName("region")
                                    .setTransform("identity")
                                    .build())
                            .build())
                    .addSortOrders(
                        IcebergSortOrder.newBuilder()
                            .setSortOrderId(1)
                            .addFields(
                                IcebergSortField.newBuilder()
                                    .setSourceFieldId(1)
                                    .setTransform("identity")
                                    .setDirection("ASC")
                                    .setNullOrder("NULLS_FIRST")
                                    .build())
                            .build())
                    .addStatistics(
                        IcebergStatisticsFile.newBuilder()
                            .setSnapshotId(3)
                            .setStatisticsPath("s3://stats/old.avro")
                            .setFileSizeInBytes(64)
                            .setFileFooterSizeInBytes(16)
                            .addBlobMetadata(
                                IcebergBlobMetadata.newBuilder()
                                    .setType("DATA")
                                    .setSnapshotId(3)
                                    .setSequenceNumber(2)
                                    .addFields(1)
                                    .putProperties("foo", "bar")
                                    .build())
                            .build())
                    .addPartitionStatistics(
                        IcebergPartitionStatisticsFile.newBuilder()
                            .setSnapshotId(2)
                            .setStatisticsPath("s3://stats/part_old.avro")
                            .setFileSizeInBytes(128)
                            .build())
                    .addEncryptionKeys(
                        IcebergEncryptedKey.newBuilder()
                            .setKeyId("old")
                            .setEncryptedKeyMetadata(ByteString.copyFromUtf8("old"))
                            .build())
                    .build())
            .build();
    when(snapshotStub.getSnapshot(any()))
        .thenReturn(GetSnapshotResponse.newBuilder().setSnapshot(snapshot).build());

    given()
        .body(
            """
            {"updates":[
              {"action":"assign-uuid","uuid":"new-uuid"},
              {"action":"upgrade-format-version","format-version":3},
              {"action":"add-schema","schema":{
                "schema-id":7,
                "type":"struct",
                "fields":[]
              }},
              {"action":"set-current-schema","schema-id":-1},
              {"action":"remove-schemas","schema-ids":[3]},
              {"action":"add-spec","spec":{
                "spec-id":5,
                "fields":[{"name":"category","source-id":1,"transform":"identity"}]
              }},
              {"action":"set-default-spec","spec-id":-1},
              {"action":"remove-partition-specs","spec-ids":[1]},
              {"action":"add-sort-order","sort-order":{
                "order-id":3,
                "fields":[{"source-id":1,"transform":"identity","direction":"ASC","null-order":"NULLS_FIRST"}]
              }},
              {"action":"set-default-sort-order","sort-order-id":-1},
              {"action":"set-statistics","statistics":{
                "snapshot-id":4,
                "statistics-path":"s3://stats/new.avro",
                "file-size-in-bytes":128,
                "file-footer-size-in-bytes":32,
                "blob-metadata":[{"type":"DATA","snapshot-id":4,"sequence-number":10,"fields":[1,2],"properties":{"bar":"baz"}}]
              }},
              {"action":"remove-statistics","snapshot-id":3},
              {"action":"set-partition-statistics","partition-statistics":{
                "snapshot-id":4,
                "statistics-path":"s3://stats/partition_new.avro",
                "file-size-in-bytes":256
              }},
              {"action":"remove-partition-statistics","snapshot-id":2},
              {"action":"add-encryption-key","encryption-key":{
                "key-id":"new",
                "encrypted-key-metadata":"c2VjcmV0",
                "encrypted-by-id":"kms"
              }},
              {"action":"remove-encryption-key","key-id":"old"}
            ]}
            """)
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(200);

    ArgumentCaptor<UpdateSnapshotRequest> updateReq =
        ArgumentCaptor.forClass(UpdateSnapshotRequest.class);
    verify(snapshotStub).updateSnapshot(updateReq.capture());
    IcebergMetadata updated = updateReq.getValue().getSpec().getIceberg();
    assertEquals("new-uuid", updated.getTableUuid());
    assertEquals(3, updated.getFormatVersion());
    assertEquals(7, updated.getCurrentSchemaId());
    assertEquals(5, updated.getDefaultSpecId());
    assertEquals(3, updated.getDefaultSortOrderId());
    assertEquals(2, updated.getSchemasCount());
    assertEquals(1, updated.getSchemas(0).getSchemaId());
    assertEquals(7, updated.getSchemas(1).getSchemaId());
    assertEquals(2, updated.getPartitionSpecsCount());
    assertEquals(2, updated.getPartitionSpecs(0).getSpecId());
    assertEquals(5, updated.getPartitionSpecs(1).getSpecId());
    assertEquals(2, updated.getSortOrdersCount());
    assertEquals(3, updated.getSortOrders(1).getSortOrderId());
    assertEquals(1, updated.getStatisticsCount());
    assertEquals(4, updated.getStatistics(0).getSnapshotId());
    assertEquals("s3://stats/new.avro", updated.getStatistics(0).getStatisticsPath());
    assertEquals(1, updated.getPartitionStatisticsCount());
    assertEquals(4, updated.getPartitionStatistics(0).getSnapshotId());
    assertEquals(
        "s3://stats/partition_new.avro", updated.getPartitionStatistics(0).getStatisticsPath());
    assertEquals(1, updated.getEncryptionKeysCount());
    assertEquals("new", updated.getEncryptionKeys(0).getKeyId());
  }

  @Test
  void commitRequirementTableUuidMismatchReturns409() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    Table current =
        Table.newBuilder()
            .setResourceId(tableId)
            .putProperties("table-uuid", "actual-uuid")
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(current).build());

    given()
        .body("{\"requirements\":[{\"type\":\"assert-table-uuid\",\"uuid\":\"expected\"}]}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(409)
        .body("error.type", equalTo("CommitFailedException"));

    verify(tableStub, never()).updateTable(any());
  }

  @Test
  void commitRequirementAssertRefSnapshotIdMismatchReturns409() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    Table current =
        Table.newBuilder().setResourceId(tableId).putProperties("current-snapshot-id", "5").build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(current).build());

    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .putRefs("main", IcebergRef.newBuilder().setSnapshotId(5).setType("branch").build())
            .build();
    Snapshot metaSnapshot =
        Snapshot.newBuilder().setTableId(tableId).setSnapshotId(5).setIceberg(metadata).build();
    when(snapshotStub.getSnapshot(any()))
        .thenReturn(GetSnapshotResponse.newBuilder().setSnapshot(metaSnapshot).build());

    given()
        .body(
            "{\"requirements\":[{\"type\":\"assert-ref-snapshot-id\",\"ref\":\"main\",\"snapshot-id\":7}]}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(409)
        .body("error.type", equalTo("CommitFailedException"));

    verify(tableStub, never()).updateTable(any());
  }

  @Test
  void commitRequirementAssertRefSnapshotIdAllowsUpdate() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    Table current =
        Table.newBuilder().setResourceId(tableId).putProperties("current-snapshot-id", "5").build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(current).build());
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(current).build());

    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .putRefs("main", IcebergRef.newBuilder().setSnapshotId(5).setType("branch").build())
            .build();
    Snapshot metaSnapshot =
        Snapshot.newBuilder().setTableId(tableId).setSnapshotId(5).setIceberg(metadata).build();
    when(snapshotStub.getSnapshot(any()))
        .thenReturn(GetSnapshotResponse.newBuilder().setSnapshot(metaSnapshot).build());

    given()
        .body(
            "{\"requirements\":[{\"type\":\"assert-ref-snapshot-id\",\"ref\":\"main\",\"snapshot-id\":5}]}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(200);

    verify(tableStub).updateTable(any());
  }

  @Test
  void commitUnknownRequirementReturns400() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());
    Table current = Table.newBuilder().setResourceId(tableId).build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(current).build());

    given()
        .body("{\"requirements\":[{\"type\":\"unknown-requirement\"}]}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(400)
        .body("error.type", equalTo("ValidationException"));

    verify(tableStub, never()).updateTable(any());
  }

  @Test
  void commitUnknownUpdateReturns400() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());
    Table current = Table.newBuilder().setResourceId(tableId).build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(current).build());

    given()
        .body("{\"updates\":[{\"action\":\"unknown-update\"}]}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(400)
        .body("error.type", equalTo("ValidationException"));

    verify(tableStub, never()).updateTable(any());
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
        .body(
            "endpoints",
            hasItems("POST /v1/{prefix}/tables/rename", "POST /v1/{prefix}/views/rename"));
  }

  @Test
  void reportsMetrics() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());
    when(statsStub.putTableStats(any()))
        .thenReturn(ai.floedb.metacat.catalog.rpc.PutTableStatsResponse.getDefaultInstance());

    given()
        .body(
            """
            {
              "report-type":"scan",
              "table-name":"orders",
              "snapshot-id":5,
              "metrics":{
                "total-data-manifests":{"unit":"count","value":1}
              }
            }
            """)
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders/metrics")
        .then()
        .statusCode(204);

    ArgumentCaptor<PutTableStatsRequest> req = ArgumentCaptor.forClass(PutTableStatsRequest.class);
    verify(statsStub).putTableStats(req.capture());
    assertEquals(5L, req.getValue().getSnapshotId());
    assertEquals("scan", req.getValue().getStats().getPropertiesMap().get("report-type"));
  }

  @Test
  void plansTableScan() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());
    ScanFile file =
        ScanFile.newBuilder()
            .setFilePath("s3://bucket/file.parquet")
            .setFileFormat("PARQUET")
            .setFileSizeInBytes(10)
            .setRecordCount(5)
            .build();
    QueryDescriptor descriptor = QueryDescriptor.newBuilder().setQueryId("plan-1").build();
    ScanBundle bundle = ScanBundle.newBuilder().addDataFiles(file).build();
    when(queryStub.beginQuery(any()))
        .thenReturn(BeginQueryResponse.newBuilder().setQuery(descriptor).build());

    given()
        .body("{\"snapshot-id\":7}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders/plan")
        .then()
        .statusCode(200)
        .body("status", equalTo("in-progress"))
        .body("'plan-id'", equalTo("plan-1"))
        .body("'plan-tasks'.size()", equalTo(1))
        .body("'plan-tasks'[0]", equalTo("plan-1"))
        .body("$", not(hasKey("file-scan-tasks")));

    ArgumentCaptor<BeginQueryRequest> req = ArgumentCaptor.forClass(BeginQueryRequest.class);
    verify(queryStub).beginQuery(req.capture());

    ArgumentCaptor<DescribeInputsRequest> describe =
        ArgumentCaptor.forClass(DescribeInputsRequest.class);
    verify(querySchemaStub).describeInputs(describe.capture());
    assertEquals(tableId, describe.getValue().getInputs(0).getTableId());
    assertEquals(7L, describe.getValue().getInputs(0).getSnapshot().getSnapshotId());

    verify(queryScanStub, never()).fetchScanBundle(any());
  }

  @Test
  void fetchPlanReturnsCompletedPlan() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());
    QueryDescriptor descriptor = QueryDescriptor.newBuilder().setQueryId("plan-1").build();
    when(queryStub.beginQuery(any()))
        .thenReturn(BeginQueryResponse.newBuilder().setQuery(descriptor).build());
    when(queryStub.getQuery(any()))
        .thenReturn(GetQueryResponse.newBuilder().setQuery(descriptor).build());

    ScanFile file =
        ScanFile.newBuilder()
            .setFilePath("s3://bucket/file.parquet")
            .setFileFormat("PARQUET")
            .setFileSizeInBytes(20)
            .setRecordCount(10)
            .build();
    ScanBundle bundle = ScanBundle.newBuilder().addDataFiles(file).build();
    when(queryScanStub.fetchScanBundle(any()))
        .thenReturn(FetchScanBundleResponse.newBuilder().setBundle(bundle).build());

    given()
        .body("{\"snapshot-id\":7}")
        .header("Content-Type", "application/json")
        .post("/v1/foo/namespaces/db/tables/orders/plan")
        .then()
        .statusCode(200);

    given()
        .when()
        .get("/v1/foo/namespaces/db/tables/orders/plan/plan-1")
        .then()
        .statusCode(200)
        .body("status", equalTo("completed"))
        .body("'plan-tasks'.size()", equalTo(1))
        .body("'plan-tasks'[0]", equalTo("plan-1"))
        .body("'file-scan-tasks'[0].'data-file'.'file-path'", equalTo("s3://bucket/file.parquet"));

    ArgumentCaptor<FetchScanBundleRequest> fetch =
        ArgumentCaptor.forClass(FetchScanBundleRequest.class);
    verify(queryScanStub).fetchScanBundle(fetch.capture());
    assertEquals("plan-1", fetch.getValue().getQueryId());
    assertEquals(tableId, fetch.getValue().getTableId());
  }

  @Test
  void fetchPlanAppliesFilterPredicates() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());
    QueryDescriptor descriptor = QueryDescriptor.newBuilder().setQueryId("plan-1").build();
    when(queryStub.beginQuery(any()))
        .thenReturn(BeginQueryResponse.newBuilder().setQuery(descriptor).build());
    when(queryStub.getQuery(any()))
        .thenReturn(GetQueryResponse.newBuilder().setQuery(descriptor).build());

    ScanBundle bundle = ScanBundle.newBuilder().build();
    when(queryScanStub.fetchScanBundle(any()))
        .thenReturn(FetchScanBundleResponse.newBuilder().setBundle(bundle).build());

    String body =
        """
        {
          "snapshot-id":7,
          "case-sensitive":false,
          "filter":{
            "type":"and",
            "expressions":[
              {
                "type":"equal",
                "term":{"type":"reference","name":"CustomerID"},
                "literal":{"type":"long","value":5}
              },
              {
                "type":"is-null",
                "term":"DeletedFlag"
              }
            ]
          }
        }
        """;

    given()
        .body(body)
        .header("Content-Type", "application/json")
        .post("/v1/foo/namespaces/db/tables/orders/plan")
        .then()
        .statusCode(200);

    given()
        .when()
        .get("/v1/foo/namespaces/db/tables/orders/plan/plan-1")
        .then()
        .statusCode(200);

    ArgumentCaptor<FetchScanBundleRequest> fetch =
        ArgumentCaptor.forClass(FetchScanBundleRequest.class);
    verify(queryScanStub).fetchScanBundle(fetch.capture());
    assertEquals(2, fetch.getValue().getPredicatesCount());
    var first = fetch.getValue().getPredicates(0);
    assertEquals("customerid", first.getColumn());
    assertEquals(Operator.OP_EQ, first.getOp());
    assertEquals("5", first.getValues(0));
    var second = fetch.getValue().getPredicates(1);
    assertEquals(Operator.OP_IS_NULL, second.getOp());
    assertEquals("deletedflag", second.getColumn());
  }

  @Test
  void fetchScanTasksReturnsBundle() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());
    QueryDescriptor descriptor = QueryDescriptor.newBuilder().setQueryId("plan-1").build();
    when(queryStub.beginQuery(any()))
        .thenReturn(BeginQueryResponse.newBuilder().setQuery(descriptor).build());

    ScanFile file =
        ScanFile.newBuilder()
            .setFilePath("s3://bucket/task.parquet")
            .setFileFormat("PARQUET")
            .setFileSizeInBytes(20)
            .setRecordCount(10)
            .build();
    ScanBundle bundle = ScanBundle.newBuilder().addDataFiles(file).build();
    when(queryScanStub.fetchScanBundle(any()))
        .thenReturn(FetchScanBundleResponse.newBuilder().setBundle(bundle).build());

    given()
        .body("{}")
        .header("Content-Type", "application/json")
        .post("/v1/foo/namespaces/db/tables/orders/plan")
        .then()
        .statusCode(200);

    given()
        .body("{\"plan-task\":\"plan-1\"}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders/tasks")
        .then()
        .statusCode(200)
        .body("'file-scan-tasks'[0].'data-file'.'file-path'", equalTo("s3://bucket/task.parquet"));

    ArgumentCaptor<FetchScanBundleRequest> fetch =
        ArgumentCaptor.forClass(FetchScanBundleRequest.class);
    verify(queryScanStub).fetchScanBundle(fetch.capture());
    assertEquals("plan-1", fetch.getValue().getQueryId());
    assertEquals(tableId, fetch.getValue().getTableId());
  }

  @Test
  void fetchScanTasksMissingPlanTaskReturns400() {
    given()
        .body("{}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders/tasks")
        .then()
        .statusCode(400)
        .body("error.type", equalTo("ValidationException"));
  }

  @Test
  void loadCredentialsReturnsStaticCredentials() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    given()
        .when()
        .get("/v1/foo/namespaces/db/tables/orders/credentials")
        .then()
        .statusCode(200)
        .body("'storage-credentials'.size()", equalTo(1))
        .body("'storage-credentials'[0].prefix", equalTo("*"))
        .body("'storage-credentials'[0].config.type", equalTo("static"));
  }

  private String stageCreateRequest(String tableName) {
    return """
        {
          "name": "%s",
          "schema": {
            "schema-id": 1,
            "last-column-id": 1,
            "type": "struct",
            "fields": [
              {
                "id": 1,
                "name": "id",
                "required": true,
                "type": "int"
              }
            ]
          },
          "partition-spec": {
            "spec-id": 0,
            "fields": [
              {
                "name": "id",
                "field-id": 1,
                "source-id": 1,
                "transform": "identity"
              }
            ]
          },
          "write-order": {
            "sort-order-id": 0,
            "fields": [
              {
                "source-id": 1
              }
            ]
          },
          "properties": {
            "metadata-location": "s3://bucket/%s/metadata.json"
          },
          "location": "s3://bucket/%s",
          "stage-create": true
        }
        """
        .formatted(tableName, tableName, tableName);
  }
}
