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

package ai.floedb.floecat.gateway.iceberg.rest.table.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.support.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.table.IcebergMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.table.StageState;
import ai.floedb.floecat.gateway.iceberg.rest.table.StagedTableEntry;
import ai.floedb.floecat.gateway.iceberg.rest.table.StagedTableKey;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import jakarta.ws.rs.core.Response;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TableCommitResponseServiceTest {
  private final TableCommitResponseService service = new TableCommitResponseService();
  private final IcebergMetadataService icebergMetadataService =
      Mockito.mock(IcebergMetadataService.class);
  private final GrpcServiceFacade grpcClient = Mockito.mock(GrpcServiceFacade.class);

  TableCommitResponseServiceTest() {
    service.icebergMetadataService = icebergMetadataService;
    service.grpcClient = grpcClient;
  }

  @Test
  void buildCommitResponseUsesCommandSpecificTableSupport() {
    TableGatewaySupport commandSupport = Mockito.mock(TableGatewaySupport.class);
    ResourceId tableId = ResourceId.newBuilder().setId("tbl-1").build();
    Table table = Table.newBuilder().setResourceId(tableId).setDisplayName("orders").build();
    when(commandSupport.resolveTableId("catalog", List.of("db"), "orders")).thenReturn(tableId);
    when(commandSupport.getTable(tableId)).thenReturn(table);
    when(commandSupport.defaultFileIoProperties()).thenReturn(Map.of("fs", "test"));
    when(icebergMetadataService.resolveCurrentIcebergMetadata(table, commandSupport))
        .thenReturn(IcebergMetadata.newBuilder().build());
    when(icebergMetadataService.resolveMetadata(
            eq("orders"), eq(table), any(IcebergMetadata.class), eq(Map.of("fs", "test")), any()))
        .thenReturn(
            new IcebergMetadataService.ResolvedMetadata(
                null, metadataView("s3://warehouse/db/orders/metadata/00001.metadata.json"), null));

    Response response =
        service.buildCommitResponse(
            command(commandSupport), new TableRequests.Commit(List.of(), List.of()), null);

    CommitTableResponseDto body =
        assertInstanceOf(CommitTableResponseDto.class, response.getEntity());
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals("s3://warehouse/db/orders/metadata/00001.metadata.json", body.metadataLocation());
    verify(commandSupport).resolveTableId("catalog", List.of("db"), "orders");
    verify(commandSupport).getTable(tableId);
    verify(commandSupport, never()).resolveTableId("other", List.of("db"), "orders");
  }

  @Test
  void buildCommitResponseFallsBackToMinimalSuccessWhenHydrationFails() {
    TableGatewaySupport commandSupport = Mockito.mock(TableGatewaySupport.class);
    when(commandSupport.resolveTableId("catalog", List.of("db"), "orders"))
        .thenThrow(new IllegalStateException("boom"));

    StagedTableEntry stagedEntry =
        new StagedTableEntry(
            new StagedTableKey("acct-1", "catalog", List.of("db"), "orders", "stage-1"),
            ResourceId.newBuilder().setId("cat").build(),
            ResourceId.newBuilder().setId("ns").build(),
            new TableRequests.Create(
                "orders",
                null,
                "s3://warehouse/db/orders",
                Map.of(
                    "metadata-location", "s3://warehouse/db/orders/metadata/staged.metadata.json"),
                null,
                null,
                true),
            TableSpec.getDefaultInstance(),
            List.of(),
            StageState.STAGED,
            Instant.now(),
            Instant.now(),
            "idem");

    Response response =
        service.buildCommitResponse(
            command(commandSupport),
            new TableRequests.Commit(
                List.of(),
                List.of(
                    Map.of(
                        "action",
                        "set-properties",
                        "updates",
                        Map.of(
                            "metadata-location",
                            "s3://warehouse/db/orders/metadata/00001.metadata.json")))),
            stagedEntry);

    CommitTableResponseDto body =
        assertInstanceOf(CommitTableResponseDto.class, response.getEntity());
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals("s3://warehouse/db/orders/metadata/00001.metadata.json", body.metadataLocation());
    assertEquals(null, body.metadata());
  }

  private TransactionCommitService.CommitCommand command(TableGatewaySupport tableSupport) {
    return new TransactionCommitService.CommitCommand(
        "foo",
        "db",
        List.of("db"),
        "orders",
        "catalog",
        ResourceId.newBuilder().setId("cat").build(),
        ResourceId.newBuilder().setId("ns").build(),
        "idem",
        null,
        null,
        new TableRequests.Commit(List.of(), List.of()),
        tableSupport);
  }

  private TableMetadataView metadataView(String location) {
    return new TableMetadataView(
        2, null, null, location, null, Map.of(), null, null, null, null, null, null, null,
        List.of(), List.of(), List.of(), Map.of(), List.of(), List.of(), List.of(), List.of(),
        List.of());
  }
}
