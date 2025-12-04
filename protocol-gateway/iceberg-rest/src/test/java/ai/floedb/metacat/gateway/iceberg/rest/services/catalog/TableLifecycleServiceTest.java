package ai.floedb.metacat.gateway.iceberg.rest.services.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.metacat.catalog.rpc.CreateTableRequest;
import ai.floedb.metacat.catalog.rpc.CreateTableResponse;
import ai.floedb.metacat.catalog.rpc.DeleteTableRequest;
import ai.floedb.metacat.catalog.rpc.GetTableRequest;
import ai.floedb.metacat.catalog.rpc.GetTableResponse;
import ai.floedb.metacat.catalog.rpc.ListTablesRequest;
import ai.floedb.metacat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.metacat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.ResolveTableResponse;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableSpec;
import ai.floedb.metacat.catalog.rpc.UpdateTableRequest;
import ai.floedb.metacat.catalog.rpc.UpdateTableResponse;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcClients;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
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
  private final ai.floedb.metacat.catalog.rpc.DirectoryServiceGrpc.DirectoryServiceBlockingStub
      directoryStub =
          mock(
              ai.floedb.metacat.catalog.rpc.DirectoryServiceGrpc.DirectoryServiceBlockingStub
                  .class);

  @BeforeEach
  void setUp() {
    service.grpc = grpc;
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
        .thenReturn(
            ai.floedb.metacat.catalog.rpc.ListTablesResponse.newBuilder()
                .addTables(table)
                .setPage(page)
                .build());

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
  void createTableAppliesIdempotencyKey() {
    TableSpec.Builder spec = TableSpec.newBuilder().setDisplayName("orders");
    Table created = Table.newBuilder().setDisplayName("orders").build();
    when(tableStub.createTable(any()))
        .thenReturn(CreateTableResponse.newBuilder().setTable(created).build());

    Table result = service.createTable(spec, "idem-key");
    assertSame(created, result);

    ArgumentCaptor<CreateTableRequest> captor = ArgumentCaptor.forClass(CreateTableRequest.class);
    verify(tableStub).createTable(captor.capture());
    CreateTableRequest sent = captor.getValue();
    assertEquals(spec.build(), sent.getSpec());
    assertEquals("idem-key", sent.getIdempotency().getKey());
  }

  @Test
  void getAndUpdateTableDelegateToStubs() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    Table table = Table.newBuilder().setDisplayName("orders").build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(table).build());
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(table).build());

    assertSame(table, service.getTable(tableId));

    UpdateTableRequest request =
        UpdateTableRequest.newBuilder()
            .setTableId(tableId)
            .setSpec(TableSpec.newBuilder().setDisplayName("orders").build())
            .build();
    assertSame(table, service.updateTable(request));

    ArgumentCaptor<GetTableRequest> getCaptor = ArgumentCaptor.forClass(GetTableRequest.class);
    verify(tableStub).getTable(getCaptor.capture());
    assertEquals(tableId, getCaptor.getValue().getTableId());
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

  @Test
  void resolveHelpersReturnResourceIds() {
    ResourceId catalogId = ResourceId.newBuilder().setId("cat").build();
    ResourceId namespaceId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveCatalog(any()))
        .thenReturn(ResolveCatalogResponse.newBuilder().setResourceId(catalogId).build());
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(namespaceId).build());
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    assertEquals(catalogId, service.resolveCatalogId("cat"));
    assertEquals(namespaceId, service.resolveNamespaceId("cat", "db"));
    assertEquals(tableId, service.resolveTableId("cat", "db", "orders"));
  }
}
