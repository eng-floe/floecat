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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.compat.TableFormatSupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataResult;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.SnapshotMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TableCommitServiceTest {
  private final TableCommitService service = new TableCommitService();
  private final IcebergGatewayConfig config = org.mockito.Mockito.mock(IcebergGatewayConfig.class);
  private final IcebergGatewayConfig.DeltaCompatConfig deltaCompatConfig =
      org.mockito.Mockito.mock(IcebergGatewayConfig.DeltaCompatConfig.class);
  private final TableLifecycleService tableLifecycleService =
      org.mockito.Mockito.mock(TableLifecycleService.class);
  private final TableCommitSideEffectService sideEffectService =
      org.mockito.Mockito.mock(TableCommitSideEffectService.class);
  private final TableCommitMaterializationService materializationService =
      org.mockito.Mockito.mock(TableCommitMaterializationService.class);
  private final CommitResponseBuilder responseBuilder =
      org.mockito.Mockito.mock(CommitResponseBuilder.class);
  private final SnapshotMetadataService snapshotMetadataService =
      org.mockito.Mockito.mock(SnapshotMetadataService.class);
  private final TableMetadataImportService tableMetadataImportService =
      org.mockito.Mockito.mock(TableMetadataImportService.class);
  private final TableGatewaySupport tableSupport =
      org.mockito.Mockito.mock(TableGatewaySupport.class);
  private final TransactionCommitService transactionCommitService =
      org.mockito.Mockito.mock(TransactionCommitService.class);

  @BeforeEach
  void setUp() {
    service.config = config;
    service.tableLifecycleService = tableLifecycleService;
    service.sideEffectService = sideEffectService;
    service.materializationService = materializationService;
    service.responseBuilder = responseBuilder;
    service.snapshotMetadataService = snapshotMetadataService;
    service.tableMetadataImportService = tableMetadataImportService;
    service.tableFormatSupport = new TableFormatSupport();
    service.transactionCommitService = transactionCommitService;

    when(config.deltaCompat()).thenReturn(Optional.of(deltaCompatConfig));
    when(deltaCompatConfig.enabled()).thenReturn(false);
    when(deltaCompatConfig.readOnly()).thenReturn(true);
    when(tableMetadataImportService.importMetadata(any(), any()))
        .thenReturn(
            new TableMetadataImportService.ImportedMetadata(
                null, Map.of(), null, null, null, List.of()));
  }

  @Test
  void commitValidatesRequiredBody() {
    Response nullCommand = service.commit(null);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), nullCommand.getStatus());

    Response nullRequest = service.commit(command(null));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), nullRequest.getStatus());
  }

  @Test
  void commitReturnsAtomicCommitErrorDirectly() {
    when(transactionCommitService.commit(any(), any(), any(), any()))
        .thenReturn(Response.status(Response.Status.CONFLICT).build());
    Table table = tableRecord("cat:db:orders");
    when(tableLifecycleService.resolveTableId(eq("catalog"), eq(List.of("db")), eq("orders")))
        .thenReturn(table.getResourceId());
    when(tableLifecycleService.getTable(table.getResourceId())).thenReturn(table);

    Response response = service.commit(command(emptyCommitRequest()));

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
  }

  @Test
  void commitDelegatesToAtomicTransactionAndBuildsResponse() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    Table table = tableRecord("cat:db:orders");
    when(tableLifecycleService.resolveTableId(eq("catalog"), eq(List.of("db")), eq("orders")))
        .thenReturn(tableId);
    when(tableLifecycleService.getTable(tableId)).thenReturn(table);
    when(transactionCommitService.commit(any(), any(), any(), any()))
        .thenReturn(Response.noContent().build());
    when(tableSupport.loadCurrentMetadata(table)).thenReturn(IcebergMetadata.getDefaultInstance());

    TableMetadataView metadataView =
        new TableMetadataView(
            2,
            null,
            null,
            "s3://warehouse/db/orders/metadata/00001.metadata.json",
            null,
            Map.of(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            List.of(),
            List.of(),
            List.of(),
            Map.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());
    CommitTableResponseDto dto =
        new CommitTableResponseDto(metadataView.metadataLocation(), metadataView);
    when(responseBuilder.removedSnapshotIds(any())).thenReturn(Set.of());
    when(responseBuilder.buildInitialResponse(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(dto);
    when(responseBuilder.buildFinalResponse(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(dto);

    Response response = service.commit(command(emptyCommitRequest()));

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    ArgumentCaptor<TransactionCommitRequest> txRequestCaptor =
        ArgumentCaptor.forClass(TransactionCommitRequest.class);
    verify(transactionCommitService)
        .commit(eq("foo"), eq("idem"), txRequestCaptor.capture(), eq(tableSupport));
    TransactionCommitRequest txRequest = txRequestCaptor.getValue();
    assertEquals(1, txRequest.tableChanges().size());
    assertEquals("orders", txRequest.tableChanges().get(0).identifier().name());
    assertTrue(txRequest.tableChanges().get(0).identifier().namespace().contains("db"));
  }

  @Test
  void commitMaterializesBeforeAtomicCommitAndInjectsMetadataLocationUpdate() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    Table table = tableRecord("cat:db:orders");
    when(tableLifecycleService.resolveTableId(eq("catalog"), eq(List.of("db")), eq("orders")))
        .thenReturn(tableId);
    when(tableLifecycleService.getTable(tableId)).thenReturn(table);
    when(transactionCommitService.commit(any(), any(), any(), any()))
        .thenReturn(Response.noContent().build());
    when(tableSupport.loadCurrentMetadata(table)).thenReturn(IcebergMetadata.getDefaultInstance());

    TableMetadataView metadataView =
        new TableMetadataView(
            2,
            null,
            null,
            "s3://warehouse/db/orders/metadata/00001.metadata.json",
            null,
            Map.of(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            List.of(),
            List.of(),
            List.of(),
            Map.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());
    CommitTableResponseDto dto =
        new CommitTableResponseDto(metadataView.metadataLocation(), metadataView);
    when(responseBuilder.removedSnapshotIds(any())).thenReturn(Set.of());
    when(responseBuilder.buildInitialResponse(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(dto);
    when(materializationService.materializeMetadata(any(), any(), any(), any(), any(), any()))
        .thenReturn(
            MaterializeMetadataResult.success(
                metadataView.withMetadataLocation(
                    "s3://warehouse/db/orders/metadata/00002.metadata.json"),
                "s3://warehouse/db/orders/metadata/00002.metadata.json"));
    when(responseBuilder.buildFinalResponse(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(dto);

    Response response = service.commit(command(emptyCommitRequest()));

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    ArgumentCaptor<TransactionCommitRequest> txRequestCaptor =
        ArgumentCaptor.forClass(TransactionCommitRequest.class);
    verify(transactionCommitService)
        .commit(eq("foo"), eq("idem"), txRequestCaptor.capture(), eq(tableSupport));
    var updates = txRequestCaptor.getValue().tableChanges().get(0).updates();
    assertTrue(
        updates.stream()
            .filter(u -> "set-properties".equals(u.get("action")))
            .map(u -> u.get("updates"))
            .filter(Map.class::isInstance)
            .map(Map.class::cast)
            .anyMatch(
                m ->
                    "s3://warehouse/db/orders/metadata/00002.metadata.json"
                        .equals(m.get("metadata-location"))));
  }

  @Test
  void commitRejectsDeltaWhenCompatReadOnlyEnabled() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    Table deltaTable =
        Table.newBuilder()
            .setResourceId(tableId)
            .setUpstream(UpstreamRef.newBuilder().setFormat(TableFormat.TF_DELTA).build())
            .build();

    when(deltaCompatConfig.enabled()).thenReturn(true);
    when(deltaCompatConfig.readOnly()).thenReturn(true);
    when(tableLifecycleService.resolveTableId(eq("catalog"), eq(List.of("db")), eq("orders")))
        .thenReturn(tableId);
    when(tableLifecycleService.getTable(tableId)).thenReturn(deltaTable);

    Response response = service.commit(command(emptyCommitRequest()));

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    verify(transactionCommitService, never()).commit(any(), any(), any(), any());
  }

  private TableRequests.Commit emptyCommitRequest() {
    return new TableRequests.Commit(List.of(), List.of());
  }

  private Table tableRecord(String id) {
    return Table.newBuilder()
        .setResourceId(ResourceId.newBuilder().setId(id))
        .putProperties("location", "s3://warehouse/db/orders")
        .putProperties("metadata-location", "s3://warehouse/db/orders/metadata/00001.metadata.json")
        .build();
  }

  private TableCommitService.CommitCommand command(TableRequests.Commit request) {
    return new TableCommitService.CommitCommand(
        "foo",
        "db",
        List.of("db"),
        "orders",
        "catalog",
        ResourceId.newBuilder().setId("cat").build(),
        ResourceId.newBuilder().setId("cat:db").build(),
        "idem",
        null,
        null,
        request,
        tableSupport);
  }
}
