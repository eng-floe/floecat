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

package ai.floedb.floecat.gateway.iceberg.minimal.resources.view;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.ListViewsResponse;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.PageResponse;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.LoadViewResultDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.ViewListResponseDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.ViewRequests;
import ai.floedb.floecat.gateway.iceberg.minimal.services.view.ViewBackend;
import ai.floedb.floecat.gateway.iceberg.minimal.services.view.ViewMetadataService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.grpc.Status;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ViewResourceTest {
  private final ViewBackend backend = Mockito.mock(ViewBackend.class);
  private final ViewMetadataService metadataService = new ViewMetadataService();
  private final ViewResource resource = new ViewResource(backend, metadataServiceWithMapper());

  @Test
  void listsViews() {
    when(backend.list("foo", List.of("db"), 10, null))
        .thenReturn(
            ListViewsResponse.newBuilder()
                .addViews(View.newBuilder().setDisplayName("orders_view"))
                .setPage(PageResponse.newBuilder().setTotalSize(1).build())
                .build());

    ViewListResponseDto dto =
        (ViewListResponseDto) resource.list("foo", "db", 10, null).getEntity();

    assertEquals("orders_view", dto.identifiers().getFirst().name());
    assertEquals(List.of("db"), dto.identifiers().getFirst().namespace());
  }

  @Test
  void getsViewLoadResult() {
    View view =
        View.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("view-1"))
            .setDisplayName("orders_view")
            .setSql("select 1")
            .putProperties("metadata-location", "s3://bucket/views/orders_view.metadata.json")
            .build();
    when(backend.get("foo", List.of("db"), "orders_view")).thenReturn(view);

    LoadViewResultDto dto =
        (LoadViewResultDto) resource.get("foo", "db", "orders_view").getEntity();

    assertEquals("s3://bucket/views/orders_view.metadata.json", dto.metadataLocation());
    assertEquals(0, dto.metadata().currentVersionId());
  }

  @Test
  void createsView() {
    var schema = JsonNodeFactory.instance.objectNode().put("type", "struct").put("schema-id", 0);
    schema
        .putArray("fields")
        .addObject()
        .put("id", 1)
        .put("name", "c")
        .put("required", false)
        .put("type", "int");
    View created =
        View.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("view-1"))
            .setDisplayName("orders_view")
            .setSql("select 1")
            .setDialect("ansi")
            .putProperties("metadata-location", "floecat://views/db/orders_view/metadata.json")
            .build();
    when(backend.create(
            eq("foo"),
            eq(List.of("db")),
            eq("orders_view"),
            any(),
            any(),
            any(),
            argThat(
                columns ->
                    columns != null
                        && columns.size() == 1
                        && "c".equals(columns.getFirst().getName())),
            any(),
            eq("idem-1")))
        .thenReturn(created);

    Response response =
        resource.create(
            "foo",
            "db",
            "idem-1",
            new ViewRequests.Create(
                "orders_view",
                null,
                schema,
                new ViewRequests.ViewVersion(
                    0,
                    1L,
                    0,
                    Map.of(),
                    List.of(new ViewRequests.ViewRepresentation("sql", "select 1", "ansi")),
                    List.of("db"),
                    null),
                Map.of("owner", "qa")));

    assertEquals(200, response.getStatus());
  }

  @Test
  void commitsViewMetadata() {
    View current =
        View.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("view-1"))
            .setDisplayName("orders_view")
            .setSql("select 1")
            .setDialect("ansi")
            .putProperties(
                "view.metadata.json",
                "{\"view-uuid\":\"v1\",\"format-version\":1,\"location\":\"floecat://views/db/orders_view/metadata.json\",\"current-version-id\":0,"
                    + "\"versions\":[{\"version-id\":0,\"timestamp-ms\":1,\"schema-id\":0,"
                    + "\"representations\":[{\"type\":\"sql\",\"sql\":\"select 1\",\"dialect\":\"ansi\"}],"
                    + "\"default-namespace\":[\"db\"]}],\"version-log\":[{\"version-id\":0,\"timestamp-ms\":1}],"
                    + "\"schemas\":[{\"schema-id\":0,\"type\":\"struct\",\"fields\":[],\"identifier-field-ids\":[]}],\"properties\":{}}")
            .putProperties("metadata-location", "floecat://views/db/orders_view/metadata.json")
            .build();
    when(backend.get("foo", List.of("db"), "orders_view")).thenReturn(current);
    when(backend.update(eq("foo"), eq(List.of("db")), eq("orders_view"), any(), any()))
        .thenAnswer(invocation -> current.toBuilder().setSql(invocation.getArgument(3)).build());

    Response response =
        resource.commit(
            "foo",
            "db",
            "orders_view",
            "idem-1",
            new ViewRequests.Commit(
                List.of(),
                List.of(
                    new ObjectMapper()
                        .createObjectNode()
                        .put("action", "set-location")
                        .put("location", "floecat://views/db/orders_view/v2.metadata.json"))));

    assertEquals(200, response.getStatus());
  }

  @Test
  void commitsViewFormatUpgrade() {
    View current =
        View.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("view-1"))
            .setDisplayName("orders_view")
            .setSql("select 1")
            .setDialect("ansi")
            .putProperties(
                "view.metadata.json",
                "{\"view-uuid\":\"v1\",\"format-version\":1,\"location\":\"floecat://views/db/orders_view/metadata.json\",\"current-version-id\":0,"
                    + "\"versions\":[{\"version-id\":0,\"timestamp-ms\":1,\"schema-id\":0,"
                    + "\"representations\":[{\"type\":\"sql\",\"sql\":\"select 1\",\"dialect\":\"ansi\"}],"
                    + "\"default-namespace\":[\"db\"]}],\"version-log\":[{\"version-id\":0,\"timestamp-ms\":1}],"
                    + "\"schemas\":[{\"schema-id\":0,\"type\":\"struct\",\"fields\":[],\"identifier-field-ids\":[]}],\"properties\":{}}")
            .putProperties("metadata-location", "floecat://views/db/orders_view/metadata.json")
            .build();
    when(backend.get("foo", List.of("db"), "orders_view")).thenReturn(current);
    when(backend.update(eq("foo"), eq(List.of("db")), eq("orders_view"), any(), any()))
        .thenAnswer(
            invocation -> current.toBuilder().putAllProperties(invocation.getArgument(4)).build());

    Response response =
        resource.commit(
            "foo",
            "db",
            "orders_view",
            "idem-1",
            new ViewRequests.Commit(
                List.of(),
                List.of(
                    new ObjectMapper()
                        .createObjectNode()
                        .put("action", "upgrade-format-version")
                        .put("format-version", 2))));

    assertEquals(200, response.getStatus());
    LoadViewResultDto dto = (LoadViewResultDto) response.getEntity();
    assertEquals(2, dto.metadata().formatVersion());
  }

  @Test
  void missingViewHeadReturns404WithoutEntity() {
    doThrow(Status.NOT_FOUND.withDescription("missing").asRuntimeException())
        .when(backend)
        .exists("foo", List.of("db"), "missing");

    Response response = resource.exists("foo", "db", "missing");

    assertEquals(404, response.getStatus());
    assertNull(response.getEntity());
  }

  private ViewMetadataService metadataServiceWithMapper() {
    try {
      var field = ViewMetadataService.class.getDeclaredField("mapper");
      field.setAccessible(true);
      field.set(metadataService, new ObjectMapper());
    } catch (ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
    return metadataService;
  }
}
