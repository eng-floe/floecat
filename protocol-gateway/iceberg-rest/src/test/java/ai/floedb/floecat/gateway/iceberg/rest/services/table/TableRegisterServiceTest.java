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

package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.CatalogRef;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.NamespaceRef;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService.ImportedMetadata;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.load.TableLoadService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.transaction.TransactionCommitService;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TableRegisterServiceTest {
  private final TableRegisterService service = new TableRegisterService();
  private final TableLifecycleService tableLifecycleService =
      Mockito.mock(TableLifecycleService.class);
  private final TableMetadataImportService tableMetadataImportService =
      Mockito.mock(TableMetadataImportService.class);
  private final GrpcServiceFacade snapshotClient = Mockito.mock(GrpcServiceFacade.class);
  private final TransactionCommitService transactionCommitService =
      Mockito.mock(TransactionCommitService.class);
  private final TableLoadService tableLoadService = Mockito.mock(TableLoadService.class);
  private final TableRegisterRequestBuilder tableRegisterRequestBuilder =
      new TableRegisterRequestBuilder();
  private final TableGatewaySupport tableSupport = Mockito.mock(TableGatewaySupport.class);

  @BeforeEach
  void setUp() {
    service.tableLifecycleService = tableLifecycleService;
    service.tableMetadataImportService = tableMetadataImportService;
    service.snapshotClient = snapshotClient;
    service.transactionCommitService = transactionCommitService;
    service.tableLoadService = tableLoadService;
    service.tableRegisterRequestBuilder = tableRegisterRequestBuilder;
    when(tableSupport.resolveRegisterFileIoProperties(any())).thenReturn(Map.of());
    when(tableSupport.defaultCredentials()).thenReturn(List.of());
  }

  @Test
  void registerUsesRegisterCommitPath() {
    NamespaceRef namespaceRef =
        new NamespaceRef(
            new CatalogRef(
                "pref",
                "examples",
                ResourceId.newBuilder().setAccountId("acct-1").setId("cat-id").build()),
            "iceberg",
            List.of("iceberg"),
            ResourceId.newBuilder().setAccountId("acct-1").setId("ns-id").build());
    TableRequests.Register request =
        new TableRequests.Register(
            "trino_test",
            "s3://yb-iceberg-tpcds/trino_test/metadata/00002-503f4508-3824-4cb6-bdf1-4bd6bf5a0ade.metadata.json",
            false,
            Map.of());
    ImportedMetadata imported =
        new ImportedMetadata(
            "{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"i\",\"required\":false,\"type\":\"int\"}]}",
            Map.of(
                "location", "s3://yb-iceberg-tpcds/trino_test/",
                "metadata-location",
                    "s3://yb-iceberg-tpcds/trino_test/metadata/00002-503f4508-3824-4cb6-bdf1-4bd6bf5a0ade.metadata.json"),
            "s3://yb-iceberg-tpcds/trino_test/",
            IcebergMetadata.getDefaultInstance(),
            null,
            List.of());
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build();
    Table table = Table.newBuilder().setResourceId(tableId).setDisplayName("trino_test").build();

    when(tableMetadataImportService.importMetadata(eq(request.metadataLocation()), any()))
        .thenReturn(imported);
    when(transactionCommitService.commitRegister(any(), any(), any(), eq(tableSupport)))
        .thenReturn(Response.noContent().build());
    when(tableLifecycleService.resolveTableId(
            eq("examples"), eq(List.of("iceberg")), eq("trino_test")))
        .thenReturn(tableId);
    when(tableLifecycleService.getTable(eq(tableId))).thenReturn(table);
    when(tableLoadService.loadResolvedTable(
            eq("trino_test"), any(Table.class), eq(List.of()), eq(tableSupport)))
        .thenReturn(Response.ok().build());

    Response response = service.register(namespaceRef, "idem", request, tableSupport);

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    verify(transactionCommitService)
        .commitRegister(eq("pref"), eq("idem"), any(), eq(tableSupport));
    verify(transactionCommitService, never()).commit(any(), any(), any(), any());
  }
}
