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

package ai.floedb.floecat.gateway.iceberg.rest.services.table.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataResult;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.CommitResponseBuilder;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TableCommitPlanner;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TableCommitService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TablePropertyService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.materialization.TableCommitMaterializationService;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TransactionCommitTablePlanningSupportTest {

  @Test
  void planExistingTableChangeUsesResolvedMaterializedMetadataForCommittedTable() {
    TransactionCommitTablePlanningSupport support = new TransactionCommitTablePlanningSupport();
    support.transactionCommitExecutionSupport =
        Mockito.mock(TransactionCommitExecutionSupport.class);
    support.tableCommitPlanner = Mockito.mock(TableCommitPlanner.class);
    support.responseBuilder = Mockito.mock(CommitResponseBuilder.class);
    support.tablePropertyService = Mockito.mock(TablePropertyService.class);
    support.materializationService = Mockito.mock(TableCommitMaterializationService.class);

    TableGatewaySupport tableSupport = Mockito.mock(TableGatewaySupport.class);
    ResourceId catalogId = ResourceId.newBuilder().setAccountId("acct").setId("cat-1").build();
    ResourceId namespaceId = ResourceId.newBuilder().setAccountId("acct").setId("ns-1").build();
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("tbl-1").build();

    Table plannedTable = Table.newBuilder().setResourceId(tableId).setDisplayName("orders").build();
    Table canonicalizedTable =
        plannedTable.toBuilder().putProperties("owner", "integration").build();
    Table resolvedTable = plannedTable.toBuilder().putProperties("owner", "txn-owner-a").build();

    TableMetadataView initialMetadata =
        new TableMetadataView(
            2,
            "uuid-1",
            "s3://bucket/orders",
            "s3://bucket/orders/metadata/00001.metadata.json",
            null,
            Map.of("owner", "integration"),
            1,
            1,
            0,
            0,
            0,
            null,
            0L,
            List.of(),
            List.of(),
            List.of(),
            Map.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());
    TableMetadataView resolvedMetadata =
        new TableMetadataView(
            2,
            "uuid-1",
            "s3://bucket/orders",
            "s3://bucket/orders/metadata/00002.metadata.json",
            null,
            Map.of("owner", "txn-owner-a"),
            1,
            1,
            0,
            0,
            0,
            null,
            0L,
            List.of(),
            List.of(),
            List.of(),
            Map.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    TableRequests.Commit request =
        new TableRequests.Commit(
            List.of(),
            List.of(Map.of("action", "set-properties", "updates", Map.of("owner", "txn-owner-a"))));
    TableCommitService.CommitCommand command =
        new TableCommitService.CommitCommand(
            "examples",
            "db",
            List.of("db"),
            "orders",
            "examples",
            catalogId,
            namespaceId,
            null,
            null,
            null,
            null,
            request,
            tableSupport);

    when(support.tableCommitPlanner.plan(any(), any(), any(), eq(tableId)))
        .thenReturn(new TableCommitPlanner.PlanResult(plannedTable, null));
    when(support.responseBuilder.buildInitialResponse(
            eq("orders"), eq(plannedTable), eq(tableId), any(), eq(tableSupport)))
        .thenReturn(
            new CommitTableResponseDto(initialMetadata.metadataLocation(), initialMetadata));
    when(support.tablePropertyService.applyCanonicalMetadataProperties(
            plannedTable, initialMetadata))
        .thenReturn(canonicalizedTable);
    when(support.materializationService.materializeMetadata(
            eq("db"),
            eq("orders"),
            eq(canonicalizedTable),
            eq(initialMetadata),
            eq(initialMetadata.metadataLocation()),
            eq(false)))
        .thenReturn(
            MaterializeMetadataResult.success(
                resolvedMetadata, resolvedMetadata.metadataLocation()));
    when(support.tablePropertyService.applyCanonicalMetadataProperties(
            canonicalizedTable, resolvedMetadata))
        .thenReturn(resolvedTable);

    var result =
        support.planExistingTableChange(
            TransactionState.TS_OPEN,
            "tx-1",
            command,
            tableId,
            catalogId,
            namespaceId,
            "db",
            "orders",
            null,
            List.of(),
            request.updates(),
            tableSupport,
            false);

    assertNull(result.error());
    assertEquals(resolvedTable, result.table());
    assertEquals(resolvedMetadata.metadataLocation(), result.metadataLocation());
  }
}
