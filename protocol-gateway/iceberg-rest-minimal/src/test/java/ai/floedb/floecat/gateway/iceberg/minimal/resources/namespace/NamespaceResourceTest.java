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

package ai.floedb.floecat.gateway.iceberg.minimal.resources.namespace;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.ListNamespacesResponse;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.common.rpc.PageResponse;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.NamespaceDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.NamespaceListResponseDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.NamespaceCreateRequest;
import ai.floedb.floecat.gateway.iceberg.minimal.services.namespace.NamespaceBackend;
import io.grpc.Status;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class NamespaceResourceTest {
  private final NamespaceBackend backend = Mockito.mock(NamespaceBackend.class);
  private final NamespaceResource resource = new NamespaceResource(backend);

  @Test
  void createsNamespace() {
    Namespace created =
        Namespace.newBuilder().setDisplayName("analytics").putProperties("owner", "team-a").build();
    when(backend.create(
            eq("foo"), eq(List.of("analytics")), eq(Map.of("owner", "team-a")), eq("idem-1")))
        .thenReturn(created);

    NamespaceDto dto =
        (NamespaceDto)
            resource
                .create(
                    "foo",
                    "idem-1",
                    new NamespaceCreateRequest(List.of("analytics"), Map.of("owner", "team-a")))
                .getEntity();

    assertEquals(List.of("analytics"), dto.namespace());
    assertEquals("team-a", dto.properties().get("owner"));
  }

  @Test
  void listsNamespaces() {
    Namespace namespace =
        Namespace.newBuilder().addParents("sales").setDisplayName("analytics").build();
    when(backend.list("foo", List.of(), 5, null))
        .thenReturn(
            ListNamespacesResponse.newBuilder()
                .addNamespaces(namespace)
                .setPage(PageResponse.newBuilder().setTotalSize(1).build())
                .build());

    NamespaceListResponseDto dto =
        (NamespaceListResponseDto) resource.list("foo", null, 5, null).getEntity();

    assertEquals(List.of(List.of("sales", "analytics")), dto.namespaces());
    assertNull(dto.nextPageToken());
  }

  @Test
  void getsNamespace() {
    when(backend.get("foo", List.of("analytics")))
        .thenReturn(Namespace.newBuilder().setDisplayName("analytics").build());

    NamespaceDto dto = (NamespaceDto) resource.get("foo", "analytics").getEntity();

    assertEquals(List.of("analytics"), dto.namespace());
  }

  @Test
  void headExistsReturns204() {
    assertEquals(204, resource.exists("foo", "analytics").getStatus());
    verify(backend).exists("foo", List.of("analytics"));
  }

  @Test
  void missingNamespaceHeadReturns404WithoutEntity() {
    doThrow(Status.NOT_FOUND.withDescription("missing").asRuntimeException())
        .when(backend)
        .exists("foo", List.of("missing"));

    var response = resource.exists("foo", "missing");

    assertEquals(404, response.getStatus());
    assertNull(response.getEntity());
  }

  @Test
  void deleteReturns204() {
    assertEquals(204, resource.delete("foo", "analytics", "idem-1").getStatus());
    verify(backend).delete("foo", List.of("analytics"));
  }

  @Test
  void missingNamespaceReturns404() {
    when(backend.get(any(), any()))
        .thenThrow(Status.NOT_FOUND.withDescription("missing").asRuntimeException());

    assertEquals(404, resource.get("foo", "missing").getStatus());
  }
}
