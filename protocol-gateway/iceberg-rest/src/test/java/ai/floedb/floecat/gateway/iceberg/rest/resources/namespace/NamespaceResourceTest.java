package ai.floedb.floecat.gateway.iceberg.rest.resources.namespace;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.CreateNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.DeleteNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.GetNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.floecat.catalog.rpc.ListNamespacesResponse;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.floecat.common.rpc.PageResponse;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.resources.AbstractRestResourceTest;
import ai.floedb.floecat.gateway.iceberg.rest.resources.RestResourceTestProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@QuarkusTest
@TestProfile(RestResourceTestProfile.class)
class NamespaceResourceTest extends AbstractRestResourceTest {

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
        .body("namespaces[0][0]", equalTo("analytics"))
        .body("next-page-token", nullValue());

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
  void headNamespaceChecksExistence() {
    ResourceId nsId = ResourceId.newBuilder().setId("foo:analytics").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());
    Namespace ns = Namespace.newBuilder().setResourceId(nsId).setDisplayName("analytics").build();
    when(namespaceStub.getNamespace(any()))
        .thenReturn(GetNamespaceResponse.newBuilder().setNamespace(ns).build());

    given().when().head("/v1/foo/namespaces/analytics").then().statusCode(204);

    ArgumentCaptor<GetNamespaceRequest> req = ArgumentCaptor.forClass(GetNamespaceRequest.class);
    verify(namespaceStub).getNamespace(req.capture());
    assertEquals(nsId, req.getValue().getNamespaceId());
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
}
