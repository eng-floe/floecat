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

package ai.floedb.floecat.gateway.iceberg.rest.resources.view;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.CreateViewRequest;
import ai.floedb.floecat.catalog.rpc.CreateViewResponse;
import ai.floedb.floecat.catalog.rpc.DeleteViewRequest;
import ai.floedb.floecat.catalog.rpc.GetViewResponse;
import ai.floedb.floecat.catalog.rpc.ListViewsResponse;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.ResolveViewResponse;
import ai.floedb.floecat.catalog.rpc.UpdateViewRequest;
import ai.floedb.floecat.catalog.rpc.UpdateViewResponse;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.common.rpc.PageResponse;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.ViewMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.ViewRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.InMemoryS3FileIO;
import ai.floedb.floecat.gateway.iceberg.rest.resources.AbstractRestResourceTest;
import ai.floedb.floecat.gateway.iceberg.rest.resources.RestResourceTestProfile;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rest.services.view.ViewMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.services.view.ViewMetadataService.MetadataContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@QuarkusTest
@TestProfile(RestResourceTestProfile.class)
class ViewResourceTest extends AbstractRestResourceTest {

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
        .body("identifiers.size()", equalTo(2))
        .body("identifiers[0].name", equalTo("reports"))
        .body("identifiers[0].namespace[0]", equalTo("db"))
        .body("identifiers[1].name", equalTo("dashboards"))
        .body("identifiers[1].namespace[0]", equalTo("db"))
        .body("next-page-token", equalTo("np"));
  }

  @Test
  void createViewPersistsMetadata() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());

    when(viewStub.createView(any()))
        .thenAnswer(
            inv -> {
              CreateViewRequest request = inv.getArgument(0);
              ViewSpec spec = request.getSpec();
              View created =
                  View.newBuilder()
                      .setResourceId(ResourceId.newBuilder().setId("cat:db:new_view"))
                      .setDisplayName(spec.getDisplayName())
                      .setSql(spec.getSql())
                      .putAllProperties(spec.getPropertiesMap())
                      .build();
              return CreateViewResponse.newBuilder().setView(created).build();
            });

    given()
        .body(
            """
            {
              "name":"new_view",
              "location":"s3://warehouse/views/db/new_view/metadata.json",
              "schema":{
                "schema-id":1,
                "type":"struct",
                "fields":[]
              },
              "view-version":{
                "version-id":1,
                "timestamp-ms":1700000000,
                "schema-id":1,
                "summary":{"operation":"create"},
                "representations":[{"type":"sql","sql":"select 1","dialect":"ansi"}],
                "default-namespace":["db"]
              },
              "properties":{"comment":"demo"}
            }
            """)
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/views")
        .then()
        .statusCode(200)
        .body("metadata-location", equalTo("s3://warehouse/views/db/new_view/metadata.json"))
        .body("metadata.properties.comment", equalTo("demo"))
        .body("metadata.versions[0].representations[0].sql", equalTo("select 1"));

    ArgumentCaptor<CreateViewRequest> createCaptor =
        ArgumentCaptor.forClass(CreateViewRequest.class);
    verify(viewStub).createView(createCaptor.capture());
    assertTrue(
        createCaptor
            .getValue()
            .getSpec()
            .getPropertiesMap()
            .containsKey(ViewMetadataService.METADATA_PROPERTY_KEY));
  }

  @Test
  void deletesView() {
    ResourceId viewId = ResourceId.newBuilder().setId("cat:db:reports").build();
    when(directoryStub.resolveView(any()))
        .thenReturn(ResolveViewResponse.newBuilder().setResourceId(viewId).build());

    given().when().delete("/v1/foo/namespaces/db/views/reports").then().statusCode(204);

    verify(viewStub).deleteView(any(DeleteViewRequest.class));
  }

  @Test
  void getsViewContract() throws Exception {
    ObjectMapper json = new ObjectMapper();
    ResourceId viewId = ResourceId.newBuilder().setId("cat:db:reports").build();
    when(directoryStub.resolveView(any()))
        .thenReturn(ResolveViewResponse.newBuilder().setResourceId(viewId).build());

    ViewRequests.ViewRepresentation rep =
        new ViewRequests.ViewRepresentation("sql", "select 1", "ansi");
    ViewRequests.ViewVersion version =
        new ViewRequests.ViewVersion(
            1, 1700000000L, 1, Map.of("operation", "create"), List.of(rep), List.of("db"), null);
    ViewRequests.Create createReq =
        new ViewRequests.Create(
            "reports",
            "s3://warehouse/views/db/reports/metadata.json",
            json.readTree("{\"schema-id\":1,\"type\":\"struct\",\"fields\":[]}"),
            version,
            Map.of("comment", "demo"));
    MetadataContext context = viewMetadataService.fromCreate(List.of("db"), "reports", createReq);
    Map<String, String> props = viewMetadataService.buildPropertyMap(context);
    View view =
        View.newBuilder()
            .setResourceId(viewId)
            .setDisplayName("reports")
            .setSql(context.sql())
            .putAllProperties(props)
            .build();
    when(viewStub.getView(any())).thenReturn(GetViewResponse.newBuilder().setView(view).build());

    given()
        .when()
        .get("/v1/foo/namespaces/db/views/reports")
        .then()
        .statusCode(200)
        .body("metadata-location", equalTo("s3://warehouse/views/db/reports/metadata.json"))
        .body("metadata.current-version-id", equalTo(1));
  }

  @Test
  void getsViewMissingContract() {
    when(directoryStub.resolveView(any())).thenThrow(new StatusRuntimeException(Status.NOT_FOUND));

    given().when().get("/v1/foo/namespaces/db/views/missing").then().statusCode(404);
  }

  @Test
  void commitViewAddsVersion() throws Exception {
    ObjectMapper json = new ObjectMapper();
    ResourceId viewId = ResourceId.newBuilder().setId("cat:db:reports").build();
    when(directoryStub.resolveView(any()))
        .thenReturn(ResolveViewResponse.newBuilder().setResourceId(viewId).build());

    ViewRequests.ViewRepresentation rep =
        new ViewRequests.ViewRepresentation("sql", "select 1", "ansi");
    ViewRequests.ViewVersion version =
        new ViewRequests.ViewVersion(
            1, 1700000000L, 1, Map.of("operation", "create"), List.of(rep), List.of("db"), null);
    ViewRequests.Create baseCreate =
        new ViewRequests.Create(
            "reports",
            null,
            json.readTree("{\"schema-id\":1,\"type\":\"struct\",\"fields\":[]}"),
            version,
            Map.of("comment", "base"));
    MetadataContext baseContext =
        viewMetadataService.fromCreate(List.of("db"), "reports", baseCreate);
    Map<String, String> baseProps = viewMetadataService.buildPropertyMap(baseContext);
    View existing =
        View.newBuilder()
            .setResourceId(viewId)
            .setDisplayName("reports")
            .setSql(baseContext.sql())
            .putAllProperties(baseProps)
            .build();
    when(viewStub.getView(any()))
        .thenReturn(GetViewResponse.newBuilder().setView(existing).build());

    when(viewStub.updateView(any()))
        .thenAnswer(
            inv -> {
              UpdateViewRequest request = inv.getArgument(0);
              View updated =
                  View.newBuilder()
                      .setResourceId(viewId)
                      .setDisplayName("reports")
                      .setSql(request.getSpec().getSql())
                      .putAllProperties(request.getSpec().getPropertiesMap())
                      .build();
              return UpdateViewResponse.newBuilder().setView(updated).build();
            });

    given()
        .body(
            ("""
            {
              "requirements":[{"type":"assert-view-uuid","uuid":"%s"}],
              "updates":[
                {
                  "action":"add-view-version",
                  "view-version":{
                    "version-id":2,
                    "timestamp-ms":1700000100,
                    "schema-id":1,
                    "summary":{"operation":"replace"},
                    "representations":[{"type":"sql","sql":"select 2","dialect":"ansi"}],
                    "default-namespace":["db"]
                  }
                }
              ]
            }
            """)
                .formatted(baseContext.metadata().viewUuid()))
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/views/reports")
        .then()
        .statusCode(200)
        .body("metadata.current-version-id", equalTo(2))
        .body("metadata.versions.size()", equalTo(2))
        .body("metadata.versions[1].representations[0].sql", equalTo("select 2"));
  }

  @Test
  void registerViewLoadsMetadataFromLocation() throws Exception {
    Path root = Path.of("target/test-fake-s3");
    Files.createDirectories(root);
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());

    String metadataLocation = "s3://bucket/views/db/registered/metadata.json";
    ViewMetadataView metadata =
        new ViewMetadataView(
            "uuid-1",
            1,
            "s3://bucket/views/db/registered",
            0,
            List.of(
                new ViewMetadataView.ViewVersion(
                    0,
                    1700000000L,
                    0,
                    Map.of("operation", "create"),
                    List.of(new ViewMetadataView.ViewRepresentation("sql", "select 1", "ansi")),
                    List.of("db"),
                    null)),
            List.of(new ViewMetadataView.ViewHistoryEntry(0, 1700000000L)),
            List.of(new ViewMetadataView.SchemaSummary(0, "struct", List.of(), List.of())),
            Map.of("comment", "registered"));
    writeMetadataFile(root, metadataLocation, metadata);

    when(viewStub.createView(any()))
        .thenAnswer(
            inv -> {
              CreateViewRequest request = inv.getArgument(0);
              ViewSpec spec = request.getSpec();
              View created =
                  View.newBuilder()
                      .setResourceId(ResourceId.newBuilder().setId("cat:db:registered"))
                      .setDisplayName(spec.getDisplayName())
                      .setSql(spec.getSql())
                      .putAllProperties(spec.getPropertiesMap())
                      .build();
              return CreateViewResponse.newBuilder().setView(created).build();
            });

    given()
        .body(
            """
            {
              "name":"registered",
              "metadata-location":"s3://bucket/views/db/registered/metadata.json"
            }
            """)
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/register-view")
        .then()
        .statusCode(200)
        .body("metadata-location", equalTo(metadataLocation))
        .body("metadata.properties.comment", equalTo("registered"));

    ArgumentCaptor<CreateViewRequest> createCaptor =
        ArgumentCaptor.forClass(CreateViewRequest.class);
    verify(viewStub).createView(createCaptor.capture());
    ViewSpec createdSpec = createCaptor.getValue().getSpec();
    assertTrue(
        createdSpec.getPropertiesMap().containsKey(ViewMetadataService.METADATA_PROPERTY_KEY));
    assertTrue(
        metadataLocation.equals(
            createdSpec
                .getPropertiesMap()
                .get(ViewMetadataService.METADATA_LOCATION_PROPERTY_KEY)));
  }

  @Test
  void renameViewUpdatesNamespaceAndName() {
    ResourceId sourceViewId = ResourceId.newBuilder().setId("cat:db:old_view").build();
    ResourceId destinationNamespaceId = ResourceId.newBuilder().setId("cat:analytics").build();
    when(directoryStub.resolveView(any()))
        .thenReturn(ResolveViewResponse.newBuilder().setResourceId(sourceViewId).build());
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(
            ResolveNamespaceResponse.newBuilder().setResourceId(destinationNamespaceId).build());
    when(viewStub.updateView(any())).thenReturn(UpdateViewResponse.newBuilder().build());

    given()
        .body(
            """
            {
              "source":{"namespace":["db"],"name":"old_view"},
              "destination":{"namespace":["analytics"],"name":"new_view"}
            }
            """)
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/views/rename")
        .then()
        .statusCode(204);

    ArgumentCaptor<UpdateViewRequest> updateCaptor =
        ArgumentCaptor.forClass(UpdateViewRequest.class);
    verify(viewStub).updateView(updateCaptor.capture());
    UpdateViewRequest sent = updateCaptor.getValue();
    assertEquals(sourceViewId, sent.getViewId());
    assertEquals(destinationNamespaceId, sent.getSpec().getNamespaceId());
    assertEquals("new_view", sent.getSpec().getDisplayName());
    assertTrue(sent.getUpdateMask().getPathsList().contains("namespace_id"));
    assertTrue(sent.getUpdateMask().getPathsList().contains("display_name"));
  }

  @Test
  void renameViewReturnsNoSuchViewWhenSourceMissing() {
    when(directoryStub.resolveView(any())).thenThrow(Status.NOT_FOUND.asRuntimeException());

    given()
        .body(
            """
            {
              "source":{"namespace":["db"],"name":"missing_view"},
              "destination":{"namespace":["analytics"],"name":"new_view"}
            }
            """)
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/views/rename")
        .then()
        .statusCode(404)
        .body("error.type", equalTo("NoSuchViewException"))
        .body("error.message", equalTo("View db.missing_view not found"));
  }

  @Test
  void renameViewReturnsNoSuchNamespaceWhenDestinationMissing() {
    when(directoryStub.resolveView(any()))
        .thenReturn(
            ResolveViewResponse.newBuilder()
                .setResourceId(ResourceId.newBuilder().setId("cat:db:old_view").build())
                .build());
    when(directoryStub.resolveNamespace(any())).thenThrow(Status.NOT_FOUND.asRuntimeException());

    given()
        .body(
            """
            {
              "source":{"namespace":["db"],"name":"old_view"},
              "destination":{"namespace":["missing"],"name":"new_view"}
            }
            """)
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/views/rename")
        .then()
        .statusCode(404)
        .body("error.type", equalTo("NoSuchNamespaceException"))
        .body("error.message", equalTo("Namespace missing not found"));
  }

  @Test
  void renameViewPropagatesUnexpectedGrpcError() {
    when(directoryStub.resolveView(any())).thenThrow(Status.INTERNAL.asRuntimeException());

    given()
        .body(
            """
            {
              "source":{"namespace":["db"],"name":"old_view"},
              "destination":{"namespace":["analytics"],"name":"new_view"}
            }
            """)
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/views/rename")
        .then()
        .statusCode(500);
  }

  @Test
  void viewExistsHeadContract() {
    ResourceId viewId = ResourceId.newBuilder().setId("cat:db:reports").build();
    when(directoryStub.resolveView(any()))
        .thenReturn(ResolveViewResponse.newBuilder().setResourceId(viewId).build());

    given().when().head("/v1/foo/namespaces/db/views/reports").then().statusCode(204);
  }

  @Test
  void viewExistsHeadNotFoundContract() {
    when(directoryStub.resolveView(any())).thenThrow(new StatusRuntimeException(Status.NOT_FOUND));

    given().when().head("/v1/foo/namespaces/db/views/missing").then().statusCode(404);
  }

  private void writeMetadataFile(Path root, String location, ViewMetadataView metadata)
      throws Exception {
    FileIO fileIo = createTestFileIo(root);
    OutputFile output = fileIo.newOutputFile(location);
    byte[] payload = new ObjectMapper().writeValueAsBytes(metadata);
    try (OutputStream stream = output.createOrOverwrite()) {
      stream.write(payload);
    }
    fileIo.close();
  }

  private FileIO createTestFileIo(Path root) {
    if (Boolean.parseBoolean(System.getProperty("floecat.fixtures.use-aws-s3", "false"))) {
      Map<String, String> ioProps = new LinkedHashMap<>();
      addIfPresent(ioProps, "io-impl", "floecat.fixture.aws.io-impl");
      addIfPresent(ioProps, "s3.endpoint", "floecat.fixture.aws.s3.endpoint");
      addIfPresent(ioProps, "s3.region", "floecat.fixture.aws.s3.region");
      addIfPresent(ioProps, "s3.access-key-id", "floecat.fixture.aws.s3.access-key-id");
      addIfPresent(ioProps, "s3.secret-access-key", "floecat.fixture.aws.s3.secret-access-key");
      addIfPresent(ioProps, "s3.session-token", "floecat.fixture.aws.s3.session-token");
      addIfPresent(ioProps, "s3.path-style-access", "floecat.fixture.aws.s3.path-style-access");
      return FileIoFactory.createFileIo(ioProps, null, false);
    }
    FileIO fileIo = new InMemoryS3FileIO();
    fileIo.initialize(Map.of("fs.floecat.test-root", root.toString()));
    return fileIo;
  }

  private static void addIfPresent(Map<String, String> props, String key, String sysProp) {
    String value = System.getProperty(sysProp);
    if (value != null && !value.isBlank()) {
      props.put(key, value);
    }
  }
}
