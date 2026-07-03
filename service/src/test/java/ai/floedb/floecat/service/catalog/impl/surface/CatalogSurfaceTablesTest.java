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
package ai.floedb.floecat.service.catalog.impl.surface;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.GetTableRequest;
import ai.floedb.floecat.catalog.rpc.ListTablesRequest;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.query.rpc.TableBackendKind;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CatalogSurfaceTablesTest {

  private static final String ACCOUNT_ID = "acct";
  private static final String CORRELATION_ID = "corr";

  private final ResourceId catalogId = id(ResourceKind.RK_CATALOG, "cat");
  private final ResourceId namespaceId = id(ResourceKind.RK_NAMESPACE, "ns");

  private TableRepository tableRepo;
  private TestCatalogOverlay overlay;
  private CatalogSurfaceTables surface;

  @BeforeEach
  void setup() {
    tableRepo = mock(TableRepository.class);
    overlay = new TestCatalogOverlay();
    surface = new CatalogSurfaceTables(tableRepo, overlay);

    overlay.addNode(
        new NamespaceNode(
            namespaceId,
            1L,
            Instant.now(),
            catalogId,
            List.of(),
            "public",
            GraphNodeOrigin.USER,
            Map.of(),
            Map.of()));
  }

  @Test
  void listTablesKeepsRepoPhaseBeforeSystemPhase() {
    Table userTable = table("orders");
    when(tableRepo.list(eq(ACCOUNT_ID), eq("cat"), eq("ns"), eq(1), eq(""), any()))
        .thenAnswer(
            invocation -> {
              StringBuilder nextOut = invocation.getArgument(5);
              nextOut.append("repo-next");
              return List.of(userTable);
            });
    when(tableRepo.count(ACCOUNT_ID, "cat", "ns")).thenReturn(1);
    overlay.addRelation(namespaceId, systemTable("z_system"));
    overlay.addRelation(namespaceId, systemTable("a_system"));

    var firstPage =
        surface.listTables(
            ListTablesRequest.newBuilder()
                .setNamespaceId(namespaceId)
                .setPage(PageRequest.newBuilder().setPageSize(1))
                .build(),
            ACCOUNT_ID,
            CORRELATION_ID);

    assertEquals(List.of("orders"), names(firstPage.getTablesList()));
    assertEquals("repo-next", firstPage.getPage().getNextPageToken());
    assertEquals(3, firstPage.getPage().getTotalSize());

    when(tableRepo.list(eq(ACCOUNT_ID), eq("cat"), eq("ns"), eq(1), eq("repo-next"), any()))
        .thenAnswer(
            invocation -> {
              StringBuilder nextOut = invocation.getArgument(5);
              nextOut.append("");
              return List.of();
            });

    var bridgePage =
        surface.listTables(
            ListTablesRequest.newBuilder()
                .setNamespaceId(namespaceId)
                .setPage(PageRequest.newBuilder().setPageSize(1).setPageToken("repo-next"))
                .build(),
            ACCOUNT_ID,
            CORRELATION_ID);

    assertEquals(List.of("a_system"), names(bridgePage.getTablesList()));
    assertTrue(bridgePage.getPage().getNextPageToken().startsWith("tbl:"));
    assertFalse(bridgePage.getPage().getNextPageToken().equals("tbl:"));
    assertEquals(3, bridgePage.getPage().getTotalSize());

    var systemPage =
        surface.listTables(
            ListTablesRequest.newBuilder()
                .setNamespaceId(namespaceId)
                .setPage(
                    PageRequest.newBuilder()
                        .setPageSize(1)
                        .setPageToken(bridgePage.getPage().getNextPageToken()))
                .build(),
            ACCOUNT_ID,
            CORRELATION_ID);

    assertEquals(List.of("z_system"), names(systemPage.getTablesList()));
    assertTrue(systemPage.getPage().getNextPageToken().startsWith("tbl:"));
    verify(tableRepo).list(eq(ACCOUNT_ID), eq("cat"), eq("ns"), eq(1), eq("repo-next"), any());
  }

  @Test
  void listTablesRejectsMalformedServicePageToken() {
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                surface.listTables(
                    ListTablesRequest.newBuilder()
                        .setNamespaceId(namespaceId)
                        .setPage(PageRequest.newBuilder().setPageToken("tbl:%%%"))
                        .build(),
                    ACCOUNT_ID,
                    CORRELATION_ID));

    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
  }

  @Test
  void getTableReadsSystemTableFromCatalogSurfaceWithoutRepoLookup() {
    var systemTable = systemTable("engine_tables");
    overlay.addRelation(namespaceId, systemTable);

    var response =
        surface.getTable(
            GetTableRequest.newBuilder().setTableId(systemTable.id()).build(), CORRELATION_ID);

    assertEquals(systemTable.id(), response.getTable().getResourceId());
    assertEquals("engine_tables", response.getTable().getDisplayName());
    assertEquals(0L, response.getMeta().getPointerVersion());
    verifyNoInteractions(tableRepo);
  }

  private static List<String> names(List<Table> tables) {
    return tables.stream().map(Table::getDisplayName).toList();
  }

  private Table table(String displayName) {
    return Table.newBuilder()
        .setResourceId(id(ResourceKind.RK_TABLE, "tbl_" + displayName))
        .setCatalogId(catalogId)
        .setNamespaceId(namespaceId)
        .setDisplayName(displayName)
        .setSchemaJson("{}")
        .build();
  }

  private SystemTableNode systemTable(String displayName) {
    return new SystemTableNode.GenericSystemTableNode(
        id(ResourceKind.RK_TABLE, "sys_" + displayName),
        1L,
        Instant.now(),
        "engine",
        displayName,
        namespaceId,
        List.of(),
        null,
        null,
        TableBackendKind.TABLE_BACKEND_KIND_ENGINE);
  }

  private static ResourceId id(ResourceKind kind, String id) {
    return ResourceId.newBuilder().setAccountId(ACCOUNT_ID).setKind(kind).setId(id).build();
  }
}
