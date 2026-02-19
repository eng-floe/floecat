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

package ai.floedb.floecat.gateway.iceberg.rest.resources.table;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.CreateTableResponse;
import ai.floedb.floecat.catalog.rpc.DeleteTableRequest;
import ai.floedb.floecat.catalog.rpc.GetNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.GetSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.GetTableResponse;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.floecat.catalog.rpc.ListTablesRequest;
import ai.floedb.floecat.catalog.rpc.ListTablesResponse;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.ResolveTableResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.catalog.rpc.UpdateTableResponse;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.Operator;
import ai.floedb.floecat.common.rpc.PageResponse;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.CreateConnectorRequest;
import ai.floedb.floecat.connector.rpc.CreateConnectorResponse;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.GetConnectorResponse;
import ai.floedb.floecat.connector.rpc.TriggerReconcileRequest;
import ai.floedb.floecat.connector.rpc.TriggerReconcileResponse;
import ai.floedb.floecat.connector.rpc.UpdateConnectorResponse;
import ai.floedb.floecat.execution.rpc.ScanBundle;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.gateway.iceberg.rest.common.IcebergHttpUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.TestS3Fixtures;
import ai.floedb.floecat.gateway.iceberg.rest.common.TrinoFixtureTestSupport;
import ai.floedb.floecat.gateway.iceberg.rest.resources.AbstractRestResourceTest;
import ai.floedb.floecat.gateway.iceberg.rest.resources.RestResourceTestProfile;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef;
import ai.floedb.floecat.query.rpc.BeginQueryRequest;
import ai.floedb.floecat.query.rpc.BeginQueryResponse;
import ai.floedb.floecat.query.rpc.DescribeInputsRequest;
import ai.floedb.floecat.query.rpc.FetchScanBundleRequest;
import ai.floedb.floecat.query.rpc.FetchScanBundleResponse;
import ai.floedb.floecat.query.rpc.GetQueryResponse;
import ai.floedb.floecat.query.rpc.QueryDescriptor;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import jakarta.ws.rs.core.MediaType;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@QuarkusTest
@TestProfile(RestResourceTestProfile.class)
class TableResourceTest extends AbstractRestResourceTest {
  private static final TrinoFixtureTestSupport.Fixture FIXTURE =
      TrinoFixtureTestSupport.simpleFixture();

  private Table.Builder baseTable(ResourceId tableId, ResourceId nsId) {
    return Table.newBuilder()
        .setResourceId(tableId)
        .setCatalogId(ResourceId.newBuilder().setId("cat"))
        .setNamespaceId(nsId)
        .setCreatedAt(Timestamps.fromMillis(FIXTURE.metadata().getLastUpdatedMs()))
        .putAllProperties(FIXTURE.table().getPropertiesMap());
  }

  private void stubSnapshotMetadata(String metadataLocation) {
    IcebergMetadata metadata =
        FIXTURE.metadata().toBuilder().setMetadataLocation(metadataLocation).build();
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setSnapshotId(metadata.getCurrentSnapshotId())
            .putFormatMetadata("iceberg", metadata.toByteString())
            .build();
    when(snapshotStub.getSnapshot(any()))
        .thenReturn(GetSnapshotResponse.newBuilder().setSnapshot(snapshot).build());
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

    PageResponse page = PageResponse.newBuilder().setNextPageToken("next").setTotalSize(3).build();

    when(tableStub.listTables(any()))
        .thenReturn(
            ListTablesResponse.newBuilder()
                .addTables(table1)
                .addTables(table2)
                .setPage(page)
                .build());

    given()
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
    List<Snapshot> fixtureSnapshots = FIXTURE.snapshots();
    Snapshot currentSnapshot = fixtureSnapshots.get(fixtureSnapshots.size() - 1);
    Table table =
        baseTable(tableId, ResourceId.newBuilder().setId("cat:db").build())
            .setDisplayName("orders")
            .putProperties("metadata-location", FIXTURE.metadataLocation())
            .putProperties("io-impl", "org.apache.iceberg.inmemory.InMemoryFileIO")
            .putProperties("current-snapshot-id", Long.toString(currentSnapshot.getSnapshotId()))
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(table).build());

    IcebergMetadata metadata =
        FIXTURE.metadata().toBuilder()
            .putRefs(
                "main",
                IcebergRef.newBuilder()
                    .setSnapshotId(currentSnapshot.getSnapshotId())
                    .setType("branch")
                    .build())
            .build();
    Snapshot metaSnapshot =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(currentSnapshot.getSnapshotId())
            .putFormatMetadata("iceberg", metadata.toByteString())
            .build();
    when(snapshotStub.getSnapshot(any()))
        .thenReturn(GetSnapshotResponse.newBuilder().setSnapshot(metaSnapshot).build());

    Snapshot snapshot1 = currentSnapshot.toBuilder().setTableId(tableId).build();
    Snapshot snapshot2 = fixtureSnapshots.get(0).toBuilder().setTableId(tableId).build();
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
        .header(
            "ETag", equalTo(IcebergHttpUtil.etagForMetadataLocation(FIXTURE.metadataLocation())))
        .body("metadata.snapshots.size()", equalTo(2))
        .body("'storage-credentials'", nullValue());

    given()
        .header(
            "If-None-Match", IcebergHttpUtil.etagForMetadataLocation(FIXTURE.metadataLocation()))
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
        .body("metadata.snapshots[0].'snapshot-id'", equalTo(currentSnapshot.getSnapshotId()));
  }

  @Test
  void loadTableWithAccessDelegationReturnsCredentials() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());
    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(ResourceId.newBuilder().setId("cat"))
            .setNamespaceId(nsId)
            .setDisplayName("orders")
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(table).build());

    IcebergMetadata metadata =
        FIXTURE.metadata().toBuilder().setMetadataLocation("s3://bucket/metadata.json").build();
    Snapshot metaSnapshot =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(metadata.getCurrentSnapshotId())
            .putFormatMetadata("iceberg", metadata.toByteString())
            .build();
    when(snapshotStub.getSnapshot(any()))
        .thenReturn(GetSnapshotResponse.newBuilder().setSnapshot(metaSnapshot).build());
    when(snapshotStub.listSnapshots(any()))
        .thenReturn(
            ListSnapshotsResponse.newBuilder().addAllSnapshots(FIXTURE.snapshots()).build());

    given()
        .header("X-Iceberg-Access-Delegation", "vended-credentials")
        .when()
        .get("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(200)
        .body("'storage-credentials'.size()", equalTo(1));
  }

  @Test
  void registerTableCreatesAndEnqueuesReconcile() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());

    Table created =
        baseTable(ResourceId.newBuilder().setId("cat:db:new_table").build(), nsId)
            .setDisplayName("new_table")
            .putProperties("io-impl", "org.apache.iceberg.inmemory.InMemoryFileIO")
            .build();
    when(tableStub.createTable(any()))
        .thenReturn(CreateTableResponse.newBuilder().setTable(created).build());
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(created).build());
    stubSnapshotMetadata(FIXTURE.metadataLocation());

    ResourceId connectorId =
        ResourceId.newBuilder()
            .setId("conn-1")
            .setKind(ResourceKind.RK_CONNECTOR)
            .setAccountId("account1")
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
        .contentType(MediaType.APPLICATION_JSON)
        .body(
            """
            {
              "name":"new_table",
              "metadata-location":"%s",
              "properties":{"io-impl":"org.apache.iceberg.inmemory.InMemoryFileIO"}
            }
            """
                .formatted(FIXTURE.metadataLocation()))
        .when()
        .post("/v1/foo/namespaces/db/register")
        .then()
        .statusCode(200)
        .body("metadata-location", equalTo(FIXTURE.metadataLocation()));

    ArgumentCaptor<CreateConnectorRequest> createReq =
        ArgumentCaptor.forClass(CreateConnectorRequest.class);
    verify(connectorsStub).createConnector(createReq.capture());
    assertEquals(
        FIXTURE.table().getPropertiesMap().get("location"),
        createReq.getValue().getSpec().getUri());
    assertEquals("new_table", createReq.getValue().getSpec().getSource().getTable());
    assertEquals(nsId, createReq.getValue().getSpec().getDestination().getNamespaceId());
    assertEquals("none", createReq.getValue().getSpec().getAuth().getScheme());
    assertEquals(
        FIXTURE.metadataLocation(),
        createReq.getValue().getSpec().getPropertiesMap().get("external.metadata-location"));

    ArgumentCaptor<TriggerReconcileRequest> trigger =
        ArgumentCaptor.forClass(TriggerReconcileRequest.class);
    verify(connectorsStub).triggerReconcile(trigger.capture());
    assertEquals(connectorId, trigger.getValue().getConnectorId());
    assertEquals(1, trigger.getValue().getDestinationNamespacePathsCount());
    assertEquals("new_table", trigger.getValue().getDestinationTableDisplayName());
  }

  @Test
  void registerTableConflictsWhenExistsAndNoOverwrite() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());

    when(tableStub.createTable(any())).thenThrow(new StatusRuntimeException(Status.ALREADY_EXISTS));

    given()
        .contentType(MediaType.APPLICATION_JSON)
        .body(
            """
            {
              "name":"existing_table",
              "metadata-location":"%s"
            }
            """
                .formatted(FIXTURE.metadataLocation()))
        .when()
        .post("/v1/foo/namespaces/db/register")
        .then()
        .statusCode(409)
        .body("error.type", equalTo("CommitFailedException"));
  }

  @Test
  void registerTableOverwriteUpdatesMetadata() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());

    when(tableStub.createTable(any())).thenThrow(new StatusRuntimeException(Status.ALREADY_EXISTS));

    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:existing_table").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    String oldMetadata =
        TestS3Fixtures.bucketUri(
            "metadata/00000-16393a9a-3433-440c-98f4-fe023ed03973.metadata.json");
    String newMetadata =
        TestS3Fixtures.bucketUri(
            "metadata/00001-084f601d-8c4e-4315-8747-5152a12ad2ea.metadata.json");
    ResourceId connectorId =
        ResourceId.newBuilder().setId("conn-1").setKind(ResourceKind.RK_CONNECTOR).build();
    Table existing =
        baseTable(tableId, nsId)
            .setDisplayName("existing_table")
            .setUpstream(UpstreamRef.newBuilder().setConnectorId(connectorId).build())
            .putProperties("metadata-location", oldMetadata)
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(existing).build());

    Table updated = existing.toBuilder().putProperties("metadata-location", newMetadata).build();
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(updated).build());
    stubSnapshotMetadata(newMetadata);

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
    when(connectorsStub.getConnector(any()))
        .thenReturn(GetConnectorResponse.newBuilder().setConnector(connector).build());

    when(connectorsStub.updateConnector(any()))
        .thenReturn(UpdateConnectorResponse.newBuilder().setConnector(connector).build());

    given()
        .contentType(MediaType.APPLICATION_JSON)
        .body(
            """
            {
              "name":"existing_table",
              "metadata-location":"%s",
              "overwrite":true
            }
            """
                .formatted(newMetadata))
        .when()
        .post("/v1/foo/namespaces/db/register")
        .then()
        .statusCode(200)
        .body("metadata-location", equalTo(newMetadata));

    ArgumentCaptor<UpdateTableRequest> updateCaptor =
        ArgumentCaptor.forClass(UpdateTableRequest.class);
    verify(tableStub, atLeast(1)).updateTable(updateCaptor.capture());
    boolean updatedProps =
        updateCaptor.getAllValues().stream()
            .anyMatch(
                req ->
                    req.getSpec()
                        .getPropertiesMap()
                        .getOrDefault("metadata-location", "")
                        .equals(newMetadata));
    assertTrue(updatedProps);
  }

  @Test
  void registerTableValidatesMetadataReadFailures() {
    when(metadataImportService.importMetadata(any(), any()))
        .thenThrow(new IllegalArgumentException("bad metadata"));
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());

    given()
        .contentType(MediaType.APPLICATION_JSON)
        .body(
            """
            {
              "name":"bad_table",
              "metadata-location":"s3://bucket/db/bad/metadata/00000-abc.metadata.json"
            }
            """)
        .when()
        .post("/v1/foo/namespaces/db/register")
        .then()
        .statusCode(400)
        .body("error.message", equalTo("bad metadata"));

    verify(tableStub, never()).createTable(any());
  }

  @Test
  void createsUpdatesAndDeletesTable() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    ResourceId connectorId = ResourceId.newBuilder().setId("conn-1").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    Table created =
        baseTable(tableId, nsId)
            .setDisplayName("orders")
            .putProperties("io-impl", "org.apache.iceberg.inmemory.InMemoryFileIO")
            .build();
    when(tableStub.createTable(any()))
        .thenReturn(CreateTableResponse.newBuilder().setTable(created).build());
    String requestBody = createTableRequest("orders");

    given()
        .body(requestBody)
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables")
        .then()
        .statusCode(200)
        .body("'metadata-location'", equalTo(FIXTURE.metadataLocation()))
        .body("metadata.properties.'metadata-location'", nullValue());

    Table existing =
        created.toBuilder()
            .setUpstream(UpstreamRef.newBuilder().setConnectorId(connectorId).build())
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(existing).build());

    given().when().delete("/v1/foo/namespaces/db/tables/orders").then().statusCode(204);

    verify(tableStub).deleteTable(any(DeleteTableRequest.class));
    verify(connectorsStub).deleteConnector(any());
  }

  @Test
  void createTableRequiresName() {
    given()
        .body(
            """
            {
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
        .statusCode(400)
        .body("error.type", equalTo("ValidationException"));

    verify(tableStub, never()).createTable(any());
  }

  @Test
  void createTableRequiresSchema() {
    given()
        .body(
            """
            {
              "name":"orders",
              "properties":{"io-impl":"org.apache.iceberg.inmemory.InMemoryFileIO"}
            }
            """)
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables")
        .then()
        .statusCode(400)
        .body("error.type", equalTo("ValidationException"));

    verify(tableStub, never()).createTable(any());
  }

  @Test
  void createTableAcceptsSpecMinimalRequest() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());

    Table created =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(ResourceId.newBuilder().setId("cat"))
            .setNamespaceId(nsId)
            .setDisplayName("orders")
            .build();
    when(tableStub.createTable(any()))
        .thenReturn(CreateTableResponse.newBuilder().setTable(created).build());

    given()
        .body(
            """
            {
              "name":"orders",
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
        .body("metadata.'format-version'", equalTo(1))
        .body("metadata.'partition-specs'[0].'spec-id'", equalTo(0))
        .body("metadata.'sort-orders'[0].'order-id'", equalTo(0));
  }

  @Test
  void createTableUsesRequestMetadataEvenWhenSnapshotMetadataDiffers() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());

    Table created = baseTable(tableId, nsId).setDisplayName("orders").build();
    when(tableStub.createTable(any()))
        .thenReturn(CreateTableResponse.newBuilder().setTable(created).build());

    IcebergMetadata differentMetadata =
        FIXTURE.metadata().toBuilder()
            .setFormatVersion(1)
            .setMetadataLocation("s3://bucket/other/metadata/00000-other.metadata.json")
            .build();
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setSnapshotId(differentMetadata.getCurrentSnapshotId())
            .putFormatMetadata("iceberg", differentMetadata.toByteString())
            .build();
    when(snapshotStub.getSnapshot(any()))
        .thenReturn(GetSnapshotResponse.newBuilder().setSnapshot(snapshot).build());

    given()
        .body(createTableRequest("orders"))
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables")
        .then()
        .statusCode(200)
        .body("metadata.'format-version'", equalTo(FIXTURE.metadata().getFormatVersion()))
        .body("'metadata-location'", equalTo(FIXTURE.metadataLocation()));
  }

  @Test
  void deleteTablePassesThroughRequest() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());
    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(ResourceId.newBuilder().setId("cat"))
            .setNamespaceId(nsId)
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(table).build());

    given()
        .when()
        .delete("/v1/foo/namespaces/db/tables/orders?purgeRequested=true")
        .then()
        .statusCode(204);

    ArgumentCaptor<DeleteTableRequest> deleteCaptor =
        ArgumentCaptor.forClass(DeleteTableRequest.class);
    verify(tableStub).deleteTable(deleteCaptor.capture());
    DeleteTableRequest sent = deleteCaptor.getValue();
    verify(tableDropCleanupService).purgeTableData(eq("foo"), eq("db"), eq("orders"), eq(table));
  }

  @Test
  void stageCreatePersistsMetadataWithoutRpc() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());

    given()
        .body(stageCreateRequest("orders"))
        .contentType(MediaType.APPLICATION_JSON)
        .when()
        .post("/v1/foo/namespaces/db/tables")
        .then()
        .statusCode(200)
        .body("metadata", notNullValue());

    verify(tableStub, never()).createTable(any());
    java.util.Optional<StagedTableEntry> entry =
        stageRepository.findSingle("account1", "foo", List.of("db"), "orders");
    assertTrue(entry.isPresent());
    StagedTableEntry stored = entry.get();
    assertTrue(stored.key().stageId() != null && !stored.key().stageId().isBlank());
  }

  @Test
  void stageCreateUsesNamespaceLocationWhenMissingRequestLocation() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());
    Namespace ns =
        Namespace.newBuilder()
            .setResourceId(nsId)
            .setDisplayName("db")
            .putProperties("location", "s3://warehouse/db")
            .build();
    when(namespaceStub.getNamespace(any()))
        .thenReturn(GetNamespaceResponse.newBuilder().setNamespace(ns).build());

    Map<String, Object> field = new LinkedHashMap<>();
    field.put("id", 1);
    field.put("name", "id");
    field.put("required", true);
    field.put("type", "long");
    Map<String, Object> schema = new LinkedHashMap<>();
    schema.put("schema-id", 1);
    schema.put("last-column-id", 1);
    schema.put("type", "struct");
    schema.put("fields", List.of(field));
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("name", "orders");
    payload.put("schema", schema);
    payload.put("stage-create", true);

    given()
        .body(payload)
        .contentType(MediaType.APPLICATION_JSON)
        .when()
        .post("/v1/foo/namespaces/db/tables")
        .then()
        .statusCode(200)
        .body("metadata.location", equalTo("s3://warehouse/db/orders"));
  }

  @Test
  void createTableUsesNamespaceLocationWhenMissingRequestLocation() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());
    Namespace ns =
        Namespace.newBuilder()
            .setResourceId(nsId)
            .setDisplayName("db")
            .putProperties("location", "s3://warehouse/db")
            .build();
    when(namespaceStub.getNamespace(any()))
        .thenReturn(GetNamespaceResponse.newBuilder().setNamespace(ns).build());

    Table created =
        baseTable(ResourceId.newBuilder().setId("cat:db:orders").build(), nsId)
            .setDisplayName("orders")
            .build();
    when(tableStub.createTable(any()))
        .thenReturn(CreateTableResponse.newBuilder().setTable(created).build());

    Map<String, Object> field = new LinkedHashMap<>();
    field.put("id", 1);
    field.put("name", "id");
    field.put("required", true);
    field.put("type", "long");
    Map<String, Object> schema = new LinkedHashMap<>();
    schema.put("schema-id", 1);
    schema.put("last-column-id", 1);
    schema.put("type", "struct");
    schema.put("fields", List.of(field));
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("name", "orders");
    payload.put("schema", schema);

    given()
        .body(payload)
        .contentType(MediaType.APPLICATION_JSON)
        .when()
        .post("/v1/foo/namespaces/db/tables")
        .then()
        .statusCode(200)
        .body("metadata.location", equalTo("s3://warehouse/db/orders"));
  }

  @Test
  void createTableWithMetadataLocationCreatesConnector() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());

    Table created =
        baseTable(tableId, nsId)
            .setDisplayName("orders")
            .putProperties("io-impl", "org.apache.iceberg.inmemory.InMemoryFileIO")
            .build();
    when(tableStub.createTable(any()))
        .thenReturn(CreateTableResponse.newBuilder().setTable(created).build());

    ResourceId connectorId =
        ResourceId.newBuilder()
            .setId("conn-1")
            .setKind(ResourceKind.RK_CONNECTOR)
            .setAccountId("account1")
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
        .body(createTableRequest("orders"))
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables")
        .then()
        .statusCode(200)
        .body("'metadata-location'", equalTo(FIXTURE.metadataLocation()));
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
  void missingAccountHeaderUsesDefaultAccount() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .setDisplayName("orders")
            .build();
    when(tableStub.listTables(any()))
        .thenReturn(
            ListTablesResponse.newBuilder()
                .addTables(table)
                .setPage(PageResponse.newBuilder().build())
                .build());

    RestAssured.requestSpecification = null;
    given()
        .header("authorization", "Bearer token")
        .when()
        .get("/v1/foo/namespaces/db/tables")
        .then()
        .statusCode(200)
        .body("identifiers[0].name", equalTo("orders"));
    RestAssured.requestSpecification = defaultSpec;
  }

  @Test
  void missingAuthHeaderReturns401() {
    RestAssured.requestSpecification = null;
    given()
        .when()
        .get("/v1/foo/namespaces/db/tables")
        .then()
        .statusCode(401)
        .body("error.message", equalTo("missing authorization header"))
        .body("error.code", equalTo(401));
    RestAssured.requestSpecification = defaultSpec;
  }

  @Test
  void reportsMetrics() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    given()
        .body(
            """
            {
              "report-type":"scan",
              "table-name":"orders",
              "snapshot-id":5,
              "filter":{"type":"always-true"},
              "schema-id":1,
              "projected-field-ids":[1],
              "projected-field-names":["id"],
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

    verify(statsStub, never()).putTableStats(any());
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
    when(queryScanStub.fetchScanBundle(any()))
        .thenReturn(FetchScanBundleResponse.newBuilder().setBundle(bundle).build());

    given()
        .body("{\"snapshot-id\":7}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders/plan")
        .then()
        .statusCode(200)
        .body("status", equalTo("completed"))
        .body("'plan-id'", equalTo("plan-1"))
        .body("'plan-tasks'.size()", equalTo(1))
        .body("'plan-tasks'[0]", equalTo("plan-1-task-0"))
        .body("'file-scan-tasks'.size()", equalTo(1))
        .body("'file-scan-tasks'[0].'data-file'.'file-path'", equalTo("s3://bucket/file.parquet"))
        .body("'delete-files'.size()", equalTo(0));

    ArgumentCaptor<BeginQueryRequest> req = ArgumentCaptor.forClass(BeginQueryRequest.class);
    verify(queryStub).beginQuery(req.capture());

    ArgumentCaptor<DescribeInputsRequest> describe =
        ArgumentCaptor.forClass(DescribeInputsRequest.class);
    verify(querySchemaStub).describeInputs(describe.capture());
    assertEquals(tableId, describe.getValue().getInputs(0).getTableId());
    assertEquals(7L, describe.getValue().getInputs(0).getSnapshot().getSnapshotId());

    verify(queryScanStub, times(1)).fetchScanBundle(any());
  }

  @Test
  void planTableScanFailureReturnsError() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());
    when(queryStub.beginQuery(any())).thenThrow(new RuntimeException("boom"));

    given()
        .body("{\"snapshot-id\":7}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders/plan")
        .then()
        .statusCode(500)
        .body("error.type", equalTo("InternalServerError"))
        .body("error.message", equalTo("boom"))
        .body("error.code", equalTo(500));
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
        .body("'plan-tasks'[0]", equalTo("plan-1-task-0"))
        .body("'file-scan-tasks'.size()", equalTo(1))
        .body("'file-scan-tasks'[0].'data-file'.'file-path'", equalTo("s3://bucket/file.parquet"))
        .body("'delete-files'.size()", equalTo(0));

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

    given().when().get("/v1/foo/namespaces/db/tables/orders/plan/plan-1").then().statusCode(200);

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
        .body("{\"plan-task\":\"plan-1-task-0\"}")
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
    String expectedAccessKey =
        System.getProperty(
            "floecat.gateway.storage-credential.properties.s3.access-key-id",
            System.getProperty("floecat.fixture.aws.s3.access-key-id", "test-key"));
    String expectedSecretKey =
        System.getProperty(
            "floecat.gateway.storage-credential.properties.s3.secret-access-key",
            System.getProperty("floecat.fixture.aws.s3.secret-access-key", "test-secret"));

    given()
        .when()
        .get("/v1/foo/namespaces/db/tables/orders/credentials")
        .then()
        .statusCode(200)
        .body("'storage-credentials'.size()", equalTo(1))
        .body("'storage-credentials'[0].prefix", equalTo("*"))
        .body("'storage-credentials'[0].config.type", equalTo("s3"))
        .body("'storage-credentials'[0].config.'s3.access-key-id'", equalTo(expectedAccessKey))
        .body("'storage-credentials'[0].config.'s3.secret-access-key'", equalTo(expectedSecretKey));
  }

  @Test
  void metricsMissingRequiredFieldsReturns400() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    given()
        .body("{\"report-type\":\"scan\"}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders/metrics")
        .then()
        .statusCode(400)
        .body("error.type", equalTo("ValidationException"));
  }

  @Test
  void metricsScanReportReturns204() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    given()
        .body(
            """
            {
              "report-type":"scan",
              "table-name":"orders",
              "snapshot-id":1,
              "filter":{"type":"always-true"},
              "schema-id":1,
              "projected-field-ids":[1],
              "projected-field-names":["id"],
              "metrics":{}
            }
            """)
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders/metrics")
        .then()
        .statusCode(204);
  }
}
