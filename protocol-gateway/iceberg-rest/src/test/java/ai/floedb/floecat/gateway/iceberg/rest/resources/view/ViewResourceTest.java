package ai.floedb.floecat.gateway.iceberg.rest.resources.view;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
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
import ai.floedb.floecat.gateway.iceberg.rest.api.request.ViewRequests;
import ai.floedb.floecat.gateway.iceberg.rest.resources.AbstractRestResourceTest;
import ai.floedb.floecat.gateway.iceberg.rest.resources.RestResourceTestProfile;
import ai.floedb.floecat.gateway.iceberg.rest.services.view.ViewMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.services.view.ViewMetadataService.MetadataContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.util.List;
import java.util.Map;
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
        .body("identifiers[1].namespace[0]", equalTo("db"));
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
}
