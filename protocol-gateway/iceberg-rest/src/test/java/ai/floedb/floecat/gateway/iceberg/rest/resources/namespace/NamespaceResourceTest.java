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

package ai.floedb.floecat.gateway.iceberg.rest.resources.namespace;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.CreateNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.CreateNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.DeleteNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.GetNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.floecat.catalog.rpc.ListNamespacesResponse;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.UpdateNamespaceResponse;
import ai.floedb.floecat.common.rpc.PageResponse;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.resources.AbstractRestResourceTest;
import ai.floedb.floecat.gateway.iceberg.rest.resources.RestResourceTestProfile;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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
        Namespace.newBuilder().setResourceId(nsId).setDisplayName("analytics").build();

    when(namespaceStub.createNamespace(any()))
        .thenReturn(CreateNamespaceResponse.newBuilder().setNamespace(created).build());

    given()
        .body("{\"namespace\":[\"analytics\"]}")
        .header("Content-Type", "application/json")
        .header("Idempotency-Key", "017F22E2-79B0-7CC3-98C4-DC0C0C07398F")
        .when()
        .post("/v1/foo/namespaces")
        .then()
        .statusCode(200)
        .body("namespace[0]", equalTo("analytics"));

    ArgumentCaptor<CreateNamespaceRequest> req =
        ArgumentCaptor.forClass(CreateNamespaceRequest.class);
    verify(namespaceStub).createNamespace(req.capture());
    assertEquals("017F22E2-79B0-7CC3-98C4-DC0C0C07398F", req.getValue().getIdempotency().getKey());
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
        .get("/v1/foo/namespaces?pageSize=5")
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
  }

  @Test
  void deletesNamespaceRequiresEmpty() {
    ResourceId nsId = ResourceId.newBuilder().setId("foo:analytics").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());

    given().when().delete("/v1/foo/namespaces/analytics").then().statusCode(204);

    ArgumentCaptor<DeleteNamespaceRequest> req =
        ArgumentCaptor.forClass(DeleteNamespaceRequest.class);
    verify(namespaceStub).deleteNamespace(req.capture());
    assertEquals(true, req.getValue().getRequireEmpty());
  }

  @Test
  void namespaceExistsHeadContract() {
    ResourceId nsId = ResourceId.newBuilder().setId("foo:analytics").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());

    given().when().head("/v1/foo/namespaces/analytics").then().statusCode(204);
  }

  @Test
  void namespaceExistsHeadNotFoundContract() {
    when(directoryStub.resolveNamespace(any()))
        .thenThrow(new StatusRuntimeException(Status.NOT_FOUND));

    given().when().head("/v1/foo/namespaces/missing").then().statusCode(404);
  }

  @Test
  void updateNamespacePropertiesContract() {
    ResourceId nsId = ResourceId.newBuilder().setId("foo:analytics").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());
    Namespace existing =
        Namespace.newBuilder()
            .setResourceId(nsId)
            .setCatalogId(ResourceId.newBuilder().setId("foo").build())
            .setDisplayName("analytics")
            .putProperties("owner", "team-old")
            .putProperties("region", "us-east-1")
            .build();
    when(namespaceStub.getNamespace(any()))
        .thenReturn(GetNamespaceResponse.newBuilder().setNamespace(existing).build());
    when(namespaceStub.updateNamespace(any()))
        .thenReturn(UpdateNamespaceResponse.newBuilder().setNamespace(existing).build());

    given()
        .body("{\"removals\":[\"region\",\"missing\"],\"updates\":{\"owner\":\"team-new\"}}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/analytics/properties")
        .then()
        .statusCode(200)
        .body("updated[0]", equalTo("owner"))
        .body("removed[0]", equalTo("region"))
        .body("missing[0]", equalTo("missing"));
  }

  @Test
  void updateNamespacePropertiesDuplicateKeyReturns422() {
    ResourceId nsId = ResourceId.newBuilder().setId("foo:analytics").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());

    given()
        .body("{\"removals\":[\"owner\"],\"updates\":{\"owner\":\"team-new\"}}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/analytics/properties")
        .then()
        .statusCode(422)
        .body("error.type", equalTo("ValidationException"));
  }
}
