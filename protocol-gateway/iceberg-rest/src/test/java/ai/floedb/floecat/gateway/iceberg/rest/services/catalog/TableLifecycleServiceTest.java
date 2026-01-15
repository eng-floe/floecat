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

package ai.floedb.floecat.gateway.iceberg.rest.services.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.DeleteTableRequest;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ListTablesRequest;
import ai.floedb.floecat.catalog.rpc.ListTablesResponse;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.ResolveTableResponse;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.common.rpc.PageResponse;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcClients;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.TableClient;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TableLifecycleServiceTest {
  private final TableLifecycleService service = new TableLifecycleService();
  private final GrpcWithHeaders grpc = mock(GrpcWithHeaders.class);
  private final GrpcClients clients = mock(GrpcClients.class);
  private final TableServiceGrpc.TableServiceBlockingStub tableStub =
      mock(TableServiceGrpc.TableServiceBlockingStub.class);
  private final DirectoryServiceGrpc.DirectoryServiceBlockingStub directoryStub =
      mock(DirectoryServiceGrpc.DirectoryServiceBlockingStub.class);

  @BeforeEach
  void setUp() {
    service.grpc = grpc;
    service.tableClient = new TableClient(grpc);
    when(grpc.raw()).thenReturn(clients);
    when(clients.table()).thenReturn(tableStub);
    when(clients.directory()).thenReturn(directoryStub);
    when(grpc.withHeaders(tableStub)).thenReturn(tableStub);
    when(grpc.withHeaders(directoryStub)).thenReturn(directoryStub);
  }

  @Test
  void listTablesTransformsResponse() {
    ResourceId namespaceId = ResourceId.newBuilder().setId("cat:db").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(namespaceId).build());
    Table table =
        Table.newBuilder()
            .setDisplayName("orders")
            .setResourceId(ResourceId.newBuilder().build())
            .build();
    var page = PageResponse.newBuilder().setNextPageToken("next-token").build();
    when(tableStub.listTables(any()))
        .thenReturn(ListTablesResponse.newBuilder().addTables(table).setPage(page).build());

    TableLifecycleService.ListTablesResult result = service.listTables("cat", "db", 50, "cursor");

    assertEquals(1, result.identifiers().size());
    TableIdentifierDto identifier = result.identifiers().get(0);
    assertEquals(List.of("db"), identifier.namespace());
    assertEquals("orders", identifier.name());
    assertEquals("next-token", result.nextPageToken());

    ArgumentCaptor<ListTablesRequest> captor = ArgumentCaptor.forClass(ListTablesRequest.class);
    verify(tableStub).listTables(captor.capture());
    ListTablesRequest sent = captor.getValue();
    assertEquals(namespaceId, sent.getNamespaceId());
    assertEquals("cursor", sent.getPage().getPageToken());
    assertEquals(50, sent.getPage().getPageSize());
  }

  @Test
  void deleteTableResolvesIdentifiers() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    service.deleteTable("cat", "db", "orders");

    ArgumentCaptor<DeleteTableRequest> deleteCaptor =
        ArgumentCaptor.forClass(DeleteTableRequest.class);
    verify(tableStub).deleteTable(deleteCaptor.capture());
    assertEquals(tableId, deleteCaptor.getValue().getTableId());
  }
}
