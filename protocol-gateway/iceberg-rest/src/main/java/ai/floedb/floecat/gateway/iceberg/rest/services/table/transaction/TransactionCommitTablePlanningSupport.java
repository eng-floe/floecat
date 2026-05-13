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

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataResult;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.CommitResponseBuilder;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TableCommitPlanner;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TableCommitService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TablePropertyService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.materialization.TableCommitMaterializationService;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import com.google.protobuf.util.Timestamps;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TransactionCommitTablePlanningSupport {
  private static final Logger LOG = Logger.getLogger(TransactionCommitTablePlanningSupport.class);

  @Inject TransactionCommitExecutionSupport transactionCommitExecutionSupport;
  @Inject TableLifecycleService tableLifecycleService;
  @Inject GrpcServiceFacade grpcClient;
  @Inject TableCommitPlanner tableCommitPlanner;
  @Inject CommitResponseBuilder responseBuilder;
  @Inject TablePropertyService tablePropertyService;
  @Inject TableCommitMaterializationService materializationService;

  record PlannedExistingTableChange(
      Table table, long pointerVersion, String metadataLocation, Response error) {}

  private record PreMaterializedTable(Table table, String metadataLocation, Response error) {}

  PlannedExistingTableChange planExistingTableChange(
      TransactionState currentState,
      String txId,
      TableCommitService.CommitCommand command,
      ResourceId tableId,
      ResourceId catalogId,
      ResourceId namespaceId,
      String namespace,
      String tableName,
      ai.floedb.floecat.catalog.rpc.GetTableResponse tableResponse,
      List<Map<String, Object>> requirements,
      List<Map<String, Object>> updates,
      TableGatewaySupport tableSupport,
      boolean preMaterializeAssertCreate) {
    ai.floedb.floecat.catalog.rpc.GetTableResponse currentResponse = tableResponse;
    for (int attempt = 0; attempt < 2; attempt++) {
      Table persistedTable =
          currentResponse == null || !currentResponse.hasTable()
              ? newCreateTableStub(tableId, catalogId, namespaceId, tableName)
              : currentResponse.getTable();
      long pointerVersion =
          currentResponse != null && currentResponse.hasMeta()
              ? currentResponse.getMeta().getPointerVersion()
              : 0L;
      Response nullRefRequirementError =
          TransactionCommitRequestSupport.validateNullSnapshotRefRequirements(
              tableSupport, persistedTable, requirements);
      if (nullRefRequirementError != null) {
        transactionCommitExecutionSupport.abortIfOpen(
            currentState, txId, "null snapshot-id ref requirement failed");
        return new PlannedExistingTableChange(null, 0L, null, nullRefRequirementError);
      }
      Supplier<Table> workingTableSupplier = () -> persistedTable;
      Supplier<Table> requirementTableSupplier = () -> persistedTable;
      var plan =
          tableCommitPlanner.plan(command, workingTableSupplier, requirementTableSupplier, tableId);
      if (plan.hasError()) {
        transactionCommitExecutionSupport.abortIfOpen(
            currentState, txId, "transaction planning failed");
        return new PlannedExistingTableChange(null, 0L, null, plan.error());
      }

      PreMaterializedTable preMaterialized =
          preMaterializeTableBeforeCommit(
              namespace,
              tableName,
              tableId,
              plan.table(),
              requirements == null ? List.of() : List.copyOf(requirements),
              updates == null ? List.of() : List.copyOf(updates),
              tableSupport,
              preMaterializeAssertCreate);
      if (preMaterialized.error() != null) {
        transactionCommitExecutionSupport.abortIfOpen(
            currentState, txId, "metadata materialization failed before atomic commit");
        return new PlannedExistingTableChange(null, 0L, null, preMaterialized.error());
      }
      if (currentResponse == null) {
        return new PlannedExistingTableChange(
            preMaterialized.table(), pointerVersion, preMaterialized.metadataLocation(), null);
      }

      ai.floedb.floecat.catalog.rpc.GetTableResponse latestResponse =
          tableLifecycleService.getTableResponse(tableId);
      long latestPointerVersion =
          latestResponse != null && latestResponse.hasMeta()
              ? latestResponse.getMeta().getPointerVersion()
              : 0L;
      if (latestPointerVersion == pointerVersion) {
        return new PlannedExistingTableChange(
            preMaterialized.table(), pointerVersion, preMaterialized.metadataLocation(), null);
      }
      currentResponse = latestResponse;
    }

    return new PlannedExistingTableChange(
        null,
        0L,
        null,
        IcebergErrorResponses.failure(
            "table changed during commit planning",
            "CommitFailedException",
            Response.Status.CONFLICT));
  }

  Table normalizeTableIdentity(Table table, ResourceId tableId) {
    if (table == null || tableId == null) {
      return table;
    }
    if (table.hasResourceId()
        && table.getResourceId().getId().equals(tableId.getId())
        && table.getResourceId().getAccountId().equals(tableId.getAccountId())) {
      return table;
    }
    return table.toBuilder().setResourceId(tableId).build();
  }

  ResourceId reserveCreateTableId(
      String txId, List<String> namespacePath, String catalogName, String tableName) {
    String namespace =
        namespacePath == null || namespacePath.isEmpty() ? "" : String.join(".", namespacePath);
    String tableFq =
        namespace.isBlank()
            ? catalogName + "." + tableName
            : catalogName + "." + namespace + "." + tableName;
    return grpcClient
        .reserveTransactionTableId(
            ai.floedb.floecat.transaction.rpc.ReserveTransactionTableIdRequest.newBuilder()
                .setTxId(txId)
                .setTableFq(tableFq)
                .build())
        .getTableId();
  }

  private PreMaterializedTable preMaterializeTableBeforeCommit(
      String namespace,
      String tableName,
      ResourceId tableId,
      Table plannedTable,
      List<Map<String, Object>> requirements,
      List<Map<String, Object>> updates,
      TableGatewaySupport tableSupport,
      boolean preMaterializeAssertCreate) {
    if (plannedTable == null || tableSupport == null) {
      return new PreMaterializedTable(plannedTable, null, null);
    }
    if (!preMaterializeAssertCreate
        && TransactionCommitRequestSupport.hasRequirementType(
            requirements, CommitUpdateInspector.REQUIREMENT_ASSERT_CREATE)) {
      return new PreMaterializedTable(plannedTable, null, null);
    }
    String requestedLocation =
        CommitUpdateInspector.inspectUpdates(updates).requestedMetadataLocation();
    if (requestedLocation != null && !isValidExplicitMetadataLocation(requestedLocation)) {
      return new PreMaterializedTable(
          plannedTable,
          null,
          IcebergErrorResponses.validation("metadata-location must be a valid URI"));
    }
    boolean skipMaterialization = requestedLocation != null;
    var commitView =
        responseBuilder.buildInitialResponse(
            tableName,
            plannedTable,
            tableId,
            new TableRequests.Commit(List.of(), updates == null ? List.of() : List.copyOf(updates)),
            tableSupport);
    TableMetadataView commitMetadata = commitView == null ? null : commitView.metadata();
    if (commitMetadata == null) {
      return new PreMaterializedTable(plannedTable, null, null);
    }
    Table canonicalizedTable =
        tablePropertyService.applyCanonicalMetadataProperties(plannedTable, commitMetadata);
    if (skipMaterialization) {
      return new PreMaterializedTable(canonicalizedTable, requestedLocation, null);
    }
    MaterializeMetadataResult result =
        materializationService.materializeMetadata(
            namespace,
            tableName,
            canonicalizedTable,
            commitMetadata,
            commitView.metadataLocation());
    if (result == null) {
      return new PreMaterializedTable(canonicalizedTable, null, null);
    }
    if (result.error() != null) {
      return new PreMaterializedTable(plannedTable, null, result.error());
    }
    String location = result.metadataLocation();
    if (location == null || location.isBlank()) {
      return new PreMaterializedTable(canonicalizedTable, null, null);
    }
    return new PreMaterializedTable(canonicalizedTable, location, null);
  }

  private boolean isValidExplicitMetadataLocation(String metadataLocation) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return false;
    }
    try {
      URI uri = URI.create(metadataLocation);
      if (uri.getScheme() == null || uri.getScheme().isBlank()) {
        return false;
      }
      String directory = MetadataLocationUtil.metadataDirectory(metadataLocation);
      return directory != null && !directory.isBlank();
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  private Table newCreateTableStub(
      ResourceId tableId, ResourceId catalogId, ResourceId namespaceId, String tableName) {
    Table.Builder builder = Table.newBuilder();
    if (tableId != null) {
      builder.setResourceId(tableId);
    }
    if (catalogId != null) {
      builder.setCatalogId(catalogId);
    }
    if (namespaceId != null) {
      builder.setNamespaceId(namespaceId);
    }
    if (tableName != null && !tableName.isBlank()) {
      builder.setDisplayName(tableName);
    }
    builder.setCreatedAt(Timestamps.fromMillis(System.currentTimeMillis()));
    builder.setUpstream(
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_ICEBERG)
            .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_FIELD_ID)
            .build());
    return builder.build();
  }
}
