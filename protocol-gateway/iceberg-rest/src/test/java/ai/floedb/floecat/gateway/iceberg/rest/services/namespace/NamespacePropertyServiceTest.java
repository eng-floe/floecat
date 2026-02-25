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

package ai.floedb.floecat.gateway.iceberg.rest.services.namespace;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.GetNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.UpdateNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.UpdateNamespaceResponse;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.NamespacePropertiesResponse;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.NamespacePropertiesRequest;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.NamespaceRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.NamespaceClient;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class NamespacePropertyServiceTest {
  private final NamespacePropertyService service = new NamespacePropertyService();
  private final NamespaceClient namespaceClient = mock(NamespaceClient.class);
  private final ResourceId namespaceId = ResourceId.newBuilder().setId("cat:db").build();

  @BeforeEach
  void setUp() {
    service.namespaceClient = namespaceClient;
  }

  @Test
  void updateRejectsReservedPropertyPrefixInUpdates() {
    Response response =
        service.update(
            namespaceContext(),
            new NamespacePropertiesRequest(List.of(), Map.of("polaris.internal", "x")));

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = (IcebergErrorResponse) response.getEntity();
    assertTrue(error.error().message().contains("reserved prefix"));
    verify(namespaceClient, never()).getNamespace(any());
    verify(namespaceClient, never()).updateNamespace(any());
  }

  @Test
  void updateRejectsReservedPropertyPrefixInRemovals() {
    Response response =
        service.update(
            namespaceContext(),
            new NamespacePropertiesRequest(List.of("polaris.internal"), Map.of()));

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    verify(namespaceClient, never()).getNamespace(any());
    verify(namespaceClient, never()).updateNamespace(any());
  }

  @Test
  void updateRejectsConflictingRemovalAndUpdateKeys() {
    Response response =
        service.update(
            namespaceContext(),
            new NamespacePropertiesRequest(List.of("owner"), Map.of("owner", "team-a")));

    assertEquals(422, response.getStatus());
    IcebergErrorResponse error = (IcebergErrorResponse) response.getEntity();
    assertEquals("ValidationException", error.error().type());
    verify(namespaceClient, never()).getNamespace(any());
    verify(namespaceClient, never()).updateNamespace(any());
  }

  @Test
  void updateMergesPropertiesAndReportsUpdatedRemovedAndMissing() {
    Namespace existing =
        Namespace.newBuilder()
            .setResourceId(namespaceId)
            .setCatalogId(ResourceId.newBuilder().setId("cat").build())
            .setDisplayName("db")
            .addParents("db")
            .putProperties("owner", "team-old")
            .putProperties("region", "us-east-1")
            .setDescription("desc")
            .build();
    when(namespaceClient.getNamespace(any()))
        .thenReturn(GetNamespaceResponse.newBuilder().setNamespace(existing).build());
    when(namespaceClient.updateNamespace(any()))
        .thenReturn(UpdateNamespaceResponse.newBuilder().setNamespace(existing).build());

    Map<String, String> updates = new LinkedHashMap<>();
    updates.put("owner", "team-new");
    updates.put("retention-days", "30");
    NamespacePropertiesRequest request =
        new NamespacePropertiesRequest(List.of("region", "missing", "missing"), updates);

    Response response = service.update(namespaceContext(), request);

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    NamespacePropertiesResponse body = (NamespacePropertiesResponse) response.getEntity();
    assertEquals(List.of("owner", "retention-days"), body.updated());
    assertEquals(List.of("region"), body.removed());
    assertEquals(List.of("missing"), body.missing());

    ArgumentCaptor<UpdateNamespaceRequest> updateCaptor =
        ArgumentCaptor.forClass(UpdateNamespaceRequest.class);
    verify(namespaceClient).updateNamespace(updateCaptor.capture());
    UpdateNamespaceRequest sent = updateCaptor.getValue();
    assertEquals(namespaceId, sent.getNamespaceId());
    assertEquals(List.of("properties"), sent.getUpdateMask().getPathsList());
    assertEquals("team-new", sent.getSpec().getPropertiesOrThrow("owner"));
    assertEquals("30", sent.getSpec().getPropertiesOrThrow("retention-days"));
    assertTrue(!sent.getSpec().getPropertiesMap().containsKey("region"));
    assertEquals("desc", sent.getSpec().getDescription());
  }

  @Test
  void updateWithNullRequestPreservesPropertiesAndReturnsEmptyDelta() {
    Namespace existing =
        Namespace.newBuilder()
            .setResourceId(namespaceId)
            .setCatalogId(ResourceId.newBuilder().setId("cat").build())
            .setDisplayName("db")
            .putProperties("owner", "team")
            .build();
    when(namespaceClient.getNamespace(any()))
        .thenReturn(GetNamespaceResponse.newBuilder().setNamespace(existing).build());
    when(namespaceClient.updateNamespace(any()))
        .thenReturn(UpdateNamespaceResponse.newBuilder().setNamespace(existing).build());

    Response response = service.update(namespaceContext(), null);

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    NamespacePropertiesResponse body = (NamespacePropertiesResponse) response.getEntity();
    assertEquals(List.of(), body.updated());
    assertEquals(List.of(), body.removed());
    assertEquals(List.of(), body.missing());
    verify(namespaceClient).updateNamespace(any());
  }

  private NamespaceRequestContext namespaceContext() {
    return new NamespaceRequestContext(null, "db", List.of("db"), namespaceId);
  }
}
