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

package ai.floedb.floecat.gateway.iceberg.rest.table;

import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.asStringMap;
import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.firstNonBlank;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.GetTableResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.support.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.support.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.support.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.support.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.support.RefPropertyUtil;
import ai.floedb.floecat.gateway.iceberg.rest.support.SnapshotMetadataUtil;
import ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergCommitJournalEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergCommitOutboxEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.storage.kv.Keys;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.iceberg.TableMetadata;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TransactionPlanningService {
  private static final Logger LOG = Logger.getLogger(TransactionPlanningService.class);
  private static final int COMMIT_JOURNAL_VERSION = 1;
  private static final int COMMIT_OUTBOX_VERSION = 1;
  private static final String ICEBERG_METADATA_KEY = "iceberg";

  @Inject TableUpdatePlanner tableUpdatePlanner;
  @Inject TablePropertyService tablePropertyService;
  @Inject ConnectorProvisioningService connectorProvisioningService;
  @Inject IcebergMetadataService icebergMetadataService;
  @Inject TableCommitJournalService commitJournalService;
  @Inject TableCommitOutboxService commitOutboxService;
  @Inject TransactionApplyService transactionApplyService;
  @Inject GrpcServiceFacade grpcClient;

  public record PlanningCommand(
      String accountId,
      String txId,
      String prefix,
      String catalogName,
      ResourceId catalogId,
      String idempotencyBase,
      String requestHash,
      long txCreatedAtMs,
      TransactionState currentState,
      List<TransactionCommitRequest.TableChange> changes,
      TableGatewaySupport tableSupport,
      boolean preMaterializeAssertCreate) {}

  public record PlanningResult(
      Response error,
      List<ai.floedb.floecat.transaction.rpc.TxChange> txChanges,
      List<TableCommitOutboxService.WorkItem> outboxWorkItems) {}

  record SnapshotChangePlan(
      List<ai.floedb.floecat.transaction.rpc.TxChange> txChanges, Response error) {}

  record PreMaterializedTable(
      ai.floedb.floecat.catalog.rpc.Table table, TableMetadata tableMetadata, Response error) {}

  record PlannedChange(
      List<String> namespacePath,
      ResourceId namespaceId,
      String tableName,
      ResourceId tableId,
      ai.floedb.floecat.catalog.rpc.Table table,
      TableMetadata tableMetadata,
      List<Map<String, Object>> updates,
      long expectedVersion) {}

  PlanningResult prepareCommit(PlanningCommand command) {
    if (command.currentState() == TransactionState.TS_APPLY_FAILED_CONFLICT) {
      return new PlanningResult(
          IcebergErrorResponses.failure(
              "transaction commit failed", "CommitFailedException", Response.Status.CONFLICT),
          List.of(),
          List.of());
    }

    boolean alreadyApplied = command.currentState() == TransactionState.TS_APPLIED;
    List<PlannedChange> planned = new ArrayList<>();
    for (TransactionCommitRequest.TableChange change : command.changes()) {
      try {
        PlannedChange plannedChange =
            planTransactionChange(
                change,
                command.accountId(),
                command.txId(),
                command.prefix(),
                command.catalogName(),
                command.catalogId(),
                command.idempotencyBase(),
                command.tableSupport(),
                alreadyApplied,
                command.preMaterializeAssertCreate());
        planned.add(plannedChange);
      } catch (WebApplicationException e) {
        maybeAbortOpenTransaction(
            command.currentState(), command.txId(), "transaction change planning failed");
        return new PlanningResult(e.getResponse(), List.of(), List.of());
      } catch (IllegalArgumentException e) {
        maybeAbortOpenTransaction(
            command.currentState(), command.txId(), "transaction change validation failed");
        return new PlanningResult(
            IcebergErrorResponses.validation(e.getMessage()), List.of(), List.of());
      } catch (StatusRuntimeException e) {
        if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
          maybeAbortOpenTransaction(
              command.currentState(), command.txId(), "table not found during planning");
          return new PlanningResult(
              IcebergErrorResponses.noSuchTable(
                  "Table not found while planning transaction change"),
              List.of(),
              List.of());
        }
        maybeAbortOpenTransaction(
            command.currentState(), command.txId(), "transaction planning failed");
        throw e;
      } catch (RuntimeException e) {
        maybeAbortOpenTransaction(
            command.currentState(), command.txId(), "transaction planning failed");
        throw e;
      }
    }

    List<ai.floedb.floecat.transaction.rpc.TxChange> txChanges = new ArrayList<>();
    List<TableCommitOutboxService.WorkItem> outboxItems = new ArrayList<>();
    for (var plan : planned) {
      Response assemblyError =
          appendTransactionChangesForPlan(
              plan,
              command.accountId(),
              command.txId(),
              command.prefix(),
              command.catalogId(),
              command.tableSupport(),
              alreadyApplied,
              command.requestHash(),
              command.txCreatedAtMs(),
              txChanges,
              outboxItems);
      if (assemblyError != null) {
        maybeAbortOpenTransaction(
            command.currentState(), command.txId(), "transaction change assembly failed");
        return new PlanningResult(assemblyError, List.of(), List.of());
      }
    }

    List<TableCommitOutboxService.WorkItem> outboxWorkItems =
        alreadyApplied
            ? loadReplayWorkItems(
                command.accountId(),
                command.txId(),
                command.txCreatedAtMs(),
                command.requestHash(),
                planned)
            : outboxItems;

    return new PlanningResult(null, List.copyOf(txChanges), List.copyOf(outboxWorkItems));
  }

  private void maybeAbortOpenTransaction(
      TransactionState currentState, String txId, String reason) {
    if (currentState == TransactionState.TS_OPEN) {
      transactionApplyService.abortQuietly(txId, reason);
    }
  }

  private PlannedChange planTransactionChange(
      TransactionCommitRequest.TableChange change,
      String accountId,
      String txId,
      String prefix,
      String catalogName,
      ResourceId catalogId,
      String idempotencyBase,
      TableGatewaySupport tableSupport,
      boolean alreadyApplied,
      boolean preMaterializeAssertCreate) {
    var identifier = change.identifier();
    if (identifier == null || identifier.name() == null || identifier.name().isBlank()) {
      throw new IllegalArgumentException("table identifier is required");
    }
    if (change.requirements() == null) {
      throw new IllegalArgumentException("requirements are required");
    }
    if (change.updates() == null) {
      throw new IllegalArgumentException("updates are required");
    }
    Response inputValidationError =
        validateCommitChangeInputs(change.requirements(), change.updates());
    if (inputValidationError != null) {
      throw new WebApplicationException(inputValidationError);
    }

    boolean assertCreateRequested = requiresAssertCreate(change.requirements());
    List<String> namespacePath =
        identifier.namespace() == null ? List.of() : List.copyOf(identifier.namespace());
    String namespace = namespacePath.isEmpty() ? "" : String.join(".", namespacePath);
    ResourceId namespaceId = tableSupport.resolveNamespaceId(catalogName, namespacePath);
    var command =
        new TransactionCommitService.CommitCommand(
            prefix,
            namespace,
            namespacePath,
            identifier.name(),
            catalogName,
            catalogId,
            namespaceId,
            idempotencyBase,
            null,
            null,
            new ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests.Commit(
                change.requirements(), change.updates()),
            tableSupport);

    ResourceId tableId;
    GetTableResponse tableResponse;
    try {
      tableId = tableSupport.resolveTableId(catalogName, namespacePath, identifier.name());
      tableResponse = tableSupport.getTableResponse(tableId);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND || !assertCreateRequested) {
        throw e;
      }
      tableId =
          atomicCreateTableId(
              accountId, txId, catalogId, namespaceId, namespacePath, identifier.name());
      tableResponse = null;
    }
    if (assertCreateRequested && tableResponse != null && tableResponse.hasTable()) {
      throw new WebApplicationException(
          IcebergErrorResponses.failure(
              "assert-create failed", "CommitFailedException", Response.Status.CONFLICT));
    }

    ai.floedb.floecat.catalog.rpc.Table persistedTable =
        tableResponse == null || !tableResponse.hasTable()
            ? newCreateTableStub(tableId, catalogId, namespaceId, identifier.name())
            : tableResponse.getTable();
    Response nullRefRequirementError =
        validateNullSnapshotRefRequirements(tableSupport, persistedTable, change.requirements());
    if (nullRefRequirementError != null) {
      throw new WebApplicationException(nullRefRequirementError);
    }

    long pointerVersion =
        tableResponse != null && tableResponse.hasMeta()
            ? tableResponse.getMeta().getPointerVersion()
            : 0L;
    ai.floedb.floecat.catalog.rpc.Table updated = persistedTable;
    TableMetadata committedMetadata = null;
    if (!alreadyApplied) {
      Supplier<ai.floedb.floecat.catalog.rpc.Table> workingTableSupplier = () -> persistedTable;
      Supplier<ai.floedb.floecat.catalog.rpc.Table> requirementTableSupplier = () -> persistedTable;
      var plan =
          tableUpdatePlanner.planTransaction(
              command, workingTableSupplier, requirementTableSupplier, tableId);
      if (plan.hasError()) {
        throw new WebApplicationException(plan.error());
      }
      updated = plan.table();
      PreMaterializedTable preMaterialized =
          preMaterializeTableBeforeCommit(
              namespace,
              identifier.name(),
              tableId,
              updated,
              List.copyOf(change.requirements()),
              List.copyOf(change.updates()),
              tableSupport,
              preMaterializeAssertCreate);
      updated = preMaterialized.table();
      committedMetadata = preMaterialized.tableMetadata();
    }

    return new PlannedChange(
        namespacePath,
        namespaceId,
        identifier.name(),
        tableId,
        updated,
        committedMetadata,
        List.copyOf(change.updates()),
        pointerVersion);
  }

  private Response appendTransactionChangesForPlan(
      PlannedChange plan,
      String accountId,
      String txId,
      String prefix,
      ResourceId catalogId,
      TableGatewaySupport tableSupport,
      boolean alreadyApplied,
      String requestHash,
      long txCreatedAtMs,
      List<ai.floedb.floecat.transaction.rpc.TxChange> txChanges,
      List<TableCommitOutboxService.WorkItem> outboxItems) {
    var tableForTx = plan.table();
    ResourceId scopedTableId = scopeTableIdWithAccount(plan.tableId(), accountId);
    ConnectorProvisioningService.ProvisionResult connectorResolution =
        connectorProvisioningService.resolveOrCreateForCommit(
            accountId,
            txId,
            prefix,
            tableSupport,
            plan.namespacePath(),
            plan.namespaceId(),
            catalogId,
            plan.tableName(),
            scopedTableId,
            tableForTx);
    if (connectorResolution.error() != null) {
      return connectorResolution.error();
    }

    tableForTx = connectorResolution.table();
    if (!connectorResolution.connectorTxChanges().isEmpty()) {
      txChanges.addAll(connectorResolution.connectorTxChanges());
    }
    CommitUpdateInspector.Parsed parsedUpdates =
        CommitUpdateInspector.inspectUpdates(plan.updates());
    txChanges.add(
        ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
            .setTableId(plan.tableId())
            .setTable(tableForTx)
            .setPrecondition(Precondition.newBuilder().setExpectedVersion(plan.expectedVersion()))
            .build());

    SnapshotChangePlan snapshotChangePlan =
        planAtomicSnapshotChanges(
            accountId,
            plan.tableId(),
            tableForTx,
            plan.tableMetadata(),
            tableSupport,
            plan.updates(),
            alreadyApplied);
    if (snapshotChangePlan.error() != null) {
      return snapshotChangePlan.error();
    }
    if (!snapshotChangePlan.txChanges().isEmpty()) {
      txChanges.addAll(snapshotChangePlan.txChanges());
    }

    IcebergCommitJournalEntry journal =
        buildCommitJournalEntry(
            txId,
            requestHash,
            scopedTableId,
            plan.namespacePath(),
            plan.tableName(),
            connectorResolution.connectorId(),
            parsedUpdates.addedSnapshotIds(),
            parsedUpdates.removedSnapshotIds(),
            tableForTx,
            txCreatedAtMs);
    String pendingKey =
        Keys.tableCommitOutboxPendingPointer(txCreatedAtMs, accountId, scopedTableId.getId(), txId);
    txChanges.add(
        ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
            .setTargetPointerKey(
                Keys.tableCommitJournalPointer(accountId, scopedTableId.getId(), txId))
            .setPayload(ByteString.copyFrom(journal.toByteArray()))
            .build());
    txChanges.add(
        ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
            .setTargetPointerKey(pendingKey)
            .setPayload(
                ByteString.copyFrom(
                    buildCommitOutboxEntry(
                            txId, requestHash, accountId, scopedTableId.getId(), txCreatedAtMs)
                        .toByteArray()))
            .build());
    outboxItems.add(commitOutboxService.toWorkItem(pendingKey, journal));
    return null;
  }

  private List<TableCommitOutboxService.WorkItem> loadReplayWorkItems(
      String accountId,
      String txId,
      long txCreatedAtMs,
      String requestHash,
      List<PlannedChange> planned) {
    if (txId == null || txId.isBlank() || planned == null || planned.isEmpty()) {
      return List.of();
    }
    List<TableCommitOutboxService.WorkItem> out = new ArrayList<>();
    for (var plan : planned) {
      ResourceId scopedTableId = scopeTableIdWithAccount(plan.tableId(), accountId);
      if (scopedTableId == null || scopedTableId.getId().isBlank()) {
        continue;
      }
      String pendingKey =
          Keys.tableCommitOutboxPendingPointer(
              txCreatedAtMs, accountId, scopedTableId.getId(), txId);
      if (!commitOutboxService.isPending(pendingKey)) {
        continue;
      }
      try {
        var journal = commitJournalService.get(accountId, scopedTableId.getId(), txId).orElse(null);
        if (journal == null) {
          LOG.warnf(
              "Skipping replay side effects for tx=%s tableId=%s; commit journal missing",
              txId, scopedTableId.getId());
          continue;
        }
        if (!requestHash.equals(journal.getRequestHash())) {
          LOG.warnf(
              "Skipping replay side effects for tx=%s tableId=%s; commit journal hash mismatch",
              txId, scopedTableId.getId());
          continue;
        }
        if (!journal.hasTableId() || journal.getTableName().isBlank()) {
          LOG.warnf(
              "Skipping replay side effects for tx=%s tableId=%s; commit journal incomplete",
              txId, scopedTableId.getId());
          continue;
        }
        out.add(commitOutboxService.toWorkItem(pendingKey, journal));
      } catch (RuntimeException e) {
        LOG.warnf(
            e,
            "Skipping replay side effects for tx=%s tableId=%s; commit journal unreadable",
            txId,
            scopedTableId.getId());
      }
    }
    return out;
  }

  private Response validateCommitChangeInputs(
      List<Map<String, Object>> requirements, List<Map<String, Object>> updates) {
    for (Map<String, Object> requirement : requirements) {
      if (requirement == null) {
        return IcebergErrorResponses.validation("commit requirement entry cannot be null");
      }
      Object typeObj = requirement.get("type");
      String type = typeObj instanceof String value ? value : null;
      if (type == null || type.isBlank()) {
        return IcebergErrorResponses.validation("commit requirement missing type");
      }
      if (!CommitUpdateInspector.isSupportedRequirementType(type)) {
        return IcebergErrorResponses.validation("unsupported commit requirement: " + type);
      }
    }
    for (Map<String, Object> update : updates) {
      if (update == null) {
        return IcebergErrorResponses.validation("unsupported commit update action: <missing>");
      }
      Object actionObj = update.get("action");
      String action = actionObj instanceof String value ? value : null;
      if (action == null || action.isBlank()) {
        return IcebergErrorResponses.validation("unsupported commit update action: <missing>");
      }
      if (!CommitUpdateInspector.isSupportedUpdateAction(action)) {
        return IcebergErrorResponses.validation("unsupported commit update action: " + action);
      }
    }
    return null;
  }

  private boolean requiresAssertCreate(List<Map<String, Object>> requirements) {
    if (requirements == null || requirements.isEmpty()) {
      return false;
    }
    for (Map<String, Object> requirement : requirements) {
      if (CommitUpdateInspector.REQUIREMENT_ASSERT_CREATE.equals(
          requirement == null ? null : requirement.get("type"))) {
        return true;
      }
    }
    return false;
  }

  private ResourceId atomicCreateTableId(
      String accountId,
      String txId,
      ResourceId catalogId,
      ResourceId namespaceId,
      List<String> namespacePath,
      String tableName) {
    String catalogPart = catalogId == null ? "<catalog>" : catalogId.getId();
    String namespacePart =
        namespacePath == null || namespacePath.isEmpty()
            ? (namespaceId == null ? "<namespace>" : namespaceId.getId())
            : String.join(".", namespacePath);
    String seed =
        (txId == null ? "" : txId)
            + "|"
            + catalogPart
            + "|"
            + namespacePart
            + "|"
            + (tableName == null ? "" : tableName);
    UUID deterministicId =
        UUID.nameUUIDFromBytes(seed.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    return ResourceId.newBuilder()
        .setAccountId(accountId == null ? "" : accountId)
        .setId("tbl-" + deterministicId)
        .setKind(ResourceKind.RK_TABLE)
        .build();
  }

  private ai.floedb.floecat.catalog.rpc.Table newCreateTableStub(
      ResourceId tableId, ResourceId catalogId, ResourceId namespaceId, String tableName) {
    ai.floedb.floecat.catalog.rpc.Table.Builder builder =
        ai.floedb.floecat.catalog.rpc.Table.newBuilder();
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

  private Response validateNullSnapshotRefRequirements(
      TableGatewaySupport tableSupport,
      ai.floedb.floecat.catalog.rpc.Table table,
      List<Map<String, Object>> requirements) {
    if (requirements == null || requirements.isEmpty()) {
      return null;
    }
    for (Map<String, Object> requirement : requirements) {
      if (requirement == null) {
        continue;
      }
      Object typeObj = requirement.get("type");
      String type = typeObj instanceof String value ? value : null;
      if (!"assert-ref-snapshot-id".equals(type) || !requirement.containsKey("snapshot-id")) {
        continue;
      }
      if (requirement.get("snapshot-id") != null) {
        continue;
      }
      Object refObj = requirement.get("ref");
      String refName = refObj instanceof String value ? value : null;
      if (refName == null || refName.isBlank()) {
        return IcebergErrorResponses.validation("assert-ref-snapshot-id requires ref");
      }
      if (snapshotRefExists(tableSupport, table, refName)) {
        return IcebergErrorResponses.failure(
            "assert-ref-snapshot-id failed for ref " + refName,
            "CommitFailedException",
            Response.Status.CONFLICT);
      }
    }
    return null;
  }

  private boolean snapshotRefExists(
      TableGatewaySupport tableSupport, ai.floedb.floecat.catalog.rpc.Table table, String refName) {
    if (refName == null || refName.isBlank()) {
      return false;
    }
    try {
      var metadata =
          icebergMetadataService == null
              ? null
              : icebergMetadataService.resolveCurrentIcebergMetadata(table, tableSupport);
      if (snapshotRefExistsInMetadata(metadata, refName)) {
        return true;
      }
    } catch (RuntimeException ignored) {
      // Fall back to table properties.
    }
    if (table == null) {
      return false;
    }
    if (hasCurrentMainSnapshot(table.getPropertiesMap(), refName)) {
      return true;
    }
    String encodedRefs = table.getPropertiesMap().get(RefPropertyUtil.PROPERTY_KEY);
    if (encodedRefs == null || encodedRefs.isBlank()) {
      return false;
    }
    Map<String, Map<String, Object>> refs = RefPropertyUtil.decode(encodedRefs);
    if (!refs.containsKey(refName)) {
      return false;
    }
    Long snapshotId = TableMappingUtil.asLong(refs.get(refName).get("snapshot-id"));
    return snapshotId != null && snapshotId >= 0L;
  }

  private PreMaterializedTable preMaterializeTableBeforeCommit(
      String namespace,
      String tableName,
      ResourceId tableId,
      ai.floedb.floecat.catalog.rpc.Table plannedTable,
      List<Map<String, Object>> requirements,
      List<Map<String, Object>> updates,
      TableGatewaySupport tableSupport,
      boolean preMaterializeAssertCreate) {
    if (plannedTable == null) {
      return new PreMaterializedTable(plannedTable, null, null);
    }
    var commitRequest =
        new ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests.Commit(
            requirements == null ? List.of() : List.copyOf(requirements),
            updates == null ? List.of() : List.copyOf(updates));
    CommitUpdateInspector.Parsed parsed = CommitUpdateInspector.inspect(commitRequest);
    boolean assertCreateRequested = requiresAssertCreate(requirements);
    boolean firstWriteAssertCreate = assertCreateRequested && parsed.containsSnapshotUpdates();
    if (!preMaterializeAssertCreate && assertCreateRequested && !firstWriteAssertCreate) {
      return new PreMaterializedTable(plannedTable, null, null);
    }
    if (icebergMetadataService == null) {
      return new PreMaterializedTable(plannedTable, null, null);
    }
    TableMetadata baseMetadata =
        resolveBaseMetadata(
            tableName, tableId, plannedTable, commitRequest, tableSupport, firstWriteAssertCreate);
    if (baseMetadata == null) {
      return new PreMaterializedTable(plannedTable, null, null);
    }
    String baseMetadataLocation = metadataLocation(baseMetadata);

    TableMetadata canonicalMetadata =
        icebergMetadataService.applyCommitUpdates(baseMetadata, plannedTable, commitRequest);
    String canonicalMetadataLocation =
        firstNonBlank(
            metadataLocation(canonicalMetadata),
            plannedTable.getPropertiesMap().get("metadata-location"),
            baseMetadataLocation);
    if (canonicalMetadata != null
        && canonicalMetadataLocation != null
        && !canonicalMetadataLocation.isBlank()) {
      canonicalMetadata =
          TableMetadata.buildFrom(canonicalMetadata)
              .discardChanges()
              .withMetadataLocation(canonicalMetadataLocation)
              .build();
    }
    if (canonicalMetadata == null) {
      return new PreMaterializedTable(plannedTable, null, null);
    }
    ai.floedb.floecat.catalog.rpc.Table canonicalizedTable =
        tablePropertyService.applyCanonicalMetadataProperties(plannedTable, canonicalMetadata);
    String requestedMetadataLocation = parsed.requestedMetadataLocation();
    boolean importedRegisterCommit =
        requestedMetadataLocation != null
            && !requestedMetadataLocation.isBlank()
            && parsed.containsSnapshotUpdates()
            && !parsed.containsCreateInitializationActions();
    if (requestedMetadataLocation != null
        && !requestedMetadataLocation.isBlank()
        && (!firstWriteAssertCreate || importedRegisterCommit)) {
      return new PreMaterializedTable(canonicalizedTable, canonicalMetadata, null);
    }

    IcebergMetadataService.MaterializeResult materialized;
    try {
      materialized =
          icebergMetadataService.materialize(
              namespace, tableName, canonicalMetadata, canonicalMetadata.metadataFileLocation());
    } catch (IllegalArgumentException e) {
      return new PreMaterializedTable(
          plannedTable,
          null,
          IcebergErrorResponses.failure(
              "metadata materialization failed",
              "MaterializeMetadataException",
              Response.Status.INTERNAL_SERVER_ERROR));
    }
    TableMetadata committedMetadata =
        materialized == null || materialized.tableMetadata() == null
            ? canonicalMetadata
            : materialized.tableMetadata();
    ai.floedb.floecat.catalog.rpc.Table materializedTable =
        tablePropertyService.applyCanonicalMetadataProperties(
            canonicalizedTable, committedMetadata);
    String metadataLocation = materialized == null ? null : materialized.metadataLocation();
    if (metadataLocation != null && !metadataLocation.isBlank()) {
      Map<String, String> props = new LinkedHashMap<>(materializedTable.getPropertiesMap());
      props.put("metadata-location", metadataLocation);
      materializedTable =
          materializedTable.toBuilder().clearProperties().putAllProperties(props).build();
    }
    return new PreMaterializedTable(materializedTable, committedMetadata, null);
  }

  private SnapshotChangePlan planAtomicSnapshotChanges(
      String accountId,
      ResourceId tableId,
      ai.floedb.floecat.catalog.rpc.Table table,
      TableMetadata committedMetadata,
      TableGatewaySupport tableSupport,
      List<Map<String, Object>> updates,
      boolean alreadyApplied) {
    if (alreadyApplied || tableId == null || updates == null || updates.isEmpty()) {
      return new SnapshotChangePlan(List.of(), null);
    }
    String resolvedAccountId = firstNonBlank(tableId.getAccountId(), accountId);
    if (resolvedAccountId == null || resolvedAccountId.isBlank()) {
      return new SnapshotChangePlan(
          List.of(),
          IcebergErrorResponses.failure(
              "missing account id for snapshot atomic changes",
              "CommitStateUnknownException",
              Response.Status.SERVICE_UNAVAILABLE));
    }
    ResourceId scopedTableId =
        resolvedAccountId.equals(tableId.getAccountId())
            ? tableId
            : tableId.toBuilder().setAccountId(resolvedAccountId).build();
    List<ai.floedb.floecat.transaction.rpc.TxChange> out = new ArrayList<>();
    for (Map<String, Object> update : updates) {
      String action = CommitUpdateInspector.actionOf(update);
      if (!CommitUpdateInspector.ACTION_ADD_SNAPSHOT.equals(action)) {
        continue;
      }
      Map<String, Object> snapshotMap = TableMappingUtil.asObjectMap(update.get("snapshot"));
      if (snapshotMap == null || snapshotMap.isEmpty()) {
        return new SnapshotChangePlan(
            List.of(), IcebergErrorResponses.validation("add-snapshot requires snapshot"));
      }
      Long snapshotId = TableMappingUtil.asLong(snapshotMap.get("snapshot-id"));
      if (snapshotId == null || snapshotId < 0) {
        return new SnapshotChangePlan(
            List.of(), IcebergErrorResponses.validation("add-snapshot requires snapshot-id"));
      }

      Snapshot snapshot;
      try {
        snapshot =
            fetchOrBuildSnapshotPayload(
                scopedTableId, table, committedMetadata, tableSupport, snapshotId, snapshotMap);
      } catch (StatusRuntimeException e) {
        return new SnapshotChangePlan(List.of(), transactionApplyService.mapPreCommitFailure(e));
      } catch (RuntimeException e) {
        return new SnapshotChangePlan(
            List.of(),
            IcebergErrorResponses.failure(
                "failed to fetch or build snapshot payload",
                "CommitStateUnknownException",
                Response.Status.SERVICE_UNAVAILABLE));
      }

      long upstreamCreatedMs =
          snapshot.hasUpstreamCreatedAt()
              ? Timestamps.toMillis(snapshot.getUpstreamCreatedAt())
              : clockMillis();
      String byIdKey =
          Keys.snapshotPointerById(
              resolvedAccountId, scopedTableId.getId(), snapshot.getSnapshotId());
      String byTimeKey =
          Keys.snapshotPointerByTime(
              resolvedAccountId,
              scopedTableId.getId(),
              snapshot.getSnapshotId(),
              upstreamCreatedMs);
      ByteString payload = ByteString.copyFrom(snapshot.toByteArray());
      out.add(
          ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
              .setTargetPointerKey(byIdKey)
              .setPayload(payload)
              .build());
      out.add(
          ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
              .setTargetPointerKey(byTimeKey)
              .setPayload(payload)
              .build());
    }
    return new SnapshotChangePlan(out, null);
  }

  private Snapshot fetchOrBuildSnapshotPayload(
      ResourceId tableId,
      ai.floedb.floecat.catalog.rpc.Table table,
      TableMetadata committedMetadata,
      TableGatewaySupport tableSupport,
      long snapshotId,
      Map<String, Object> snapshotMap) {
    try {
      var resp =
          grpcClient.getSnapshot(
              ai.floedb.floecat.catalog.rpc.GetSnapshotRequest.newBuilder()
                  .setTableId(tableId)
                  .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId).build())
                  .build());
      if (resp != null && resp.hasSnapshot()) {
        Snapshot existing = resp.getSnapshot();
        if (!existing.getFormatMetadataMap().containsKey(ICEBERG_METADATA_KEY)) {
          Map<String, String> existingSummary = SnapshotMetadataUtil.snapshotSummary(existing);
          String existingOperation = SnapshotMetadataUtil.snapshotOperation(existing);
          IcebergMetadata snapshotIcebergMetadata =
              buildSnapshotIcebergMetadata(
                  table,
                  committedMetadata,
                  tableSupport,
                  snapshotId,
                  existing.getSequenceNumber(),
                  existingOperation,
                  existingSummary);
          if (snapshotIcebergMetadata != null) {
            return existing.toBuilder()
                .putFormatMetadata(ICEBERG_METADATA_KEY, snapshotIcebergMetadata.toByteString())
                .build();
          }
        }
        return existing;
      }
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
        throw e;
      }
    }

    Snapshot.Builder builder = Snapshot.newBuilder().setTableId(tableId).setSnapshotId(snapshotId);
    Long upstreamCreated = TableMappingUtil.asLong(snapshotMap.get("timestamp-ms"));
    if (upstreamCreated != null && upstreamCreated > 0) {
      builder.setUpstreamCreatedAt(Timestamps.fromMillis(upstreamCreated));
    }
    Long parentId = TableMappingUtil.asLong(snapshotMap.get("parent-snapshot-id"));
    if (parentId != null) {
      builder.setParentSnapshotId(parentId);
    }
    Long sequenceNumber = TableMappingUtil.asLong(snapshotMap.get("sequence-number"));
    if (sequenceNumber != null && sequenceNumber > 0) {
      builder.setSequenceNumber(sequenceNumber);
    }
    String manifestList = TableMappingUtil.asString(snapshotMap.get("manifest-list"));
    if (manifestList != null && !manifestList.isBlank()) {
      builder.addManifestList(manifestList);
    }
    Integer schemaId = TableMappingUtil.asInteger(snapshotMap.get("schema-id"));
    if (schemaId != null && schemaId >= 0) {
      builder.setSchemaId(schemaId);
    }
    String schemaJson = TableMappingUtil.asString(snapshotMap.get("schema-json"));
    if (schemaJson != null && !schemaJson.isBlank()) {
      builder.setSchemaJson(schemaJson);
    }
    Map<String, String> summary = asStringMap(snapshotMap.get("summary"));
    String operation = TableMappingUtil.asString(snapshotMap.get("operation"));
    if (operation != null && !operation.isBlank() && !summary.containsKey("operation")) {
      summary = new LinkedHashMap<>(summary);
      summary.put("operation", operation);
    }
    String resolvedOperation =
        operation != null && !operation.isBlank() ? operation : summary.get("operation");
    IcebergMetadata snapshotIcebergMetadata =
        buildSnapshotIcebergMetadata(
            table,
            committedMetadata,
            tableSupport,
            snapshotId,
            sequenceNumber,
            resolvedOperation,
            summary);
    if (snapshotIcebergMetadata != null) {
      builder.putFormatMetadata(ICEBERG_METADATA_KEY, snapshotIcebergMetadata.toByteString());
    }
    if (!builder.hasUpstreamCreatedAt()) {
      builder.setUpstreamCreatedAt(Timestamps.fromMillis(clockMillis()));
    }
    return builder.build();
  }

  private IcebergMetadata buildSnapshotIcebergMetadata(
      ai.floedb.floecat.catalog.rpc.Table table,
      TableMetadata committedMetadata,
      TableGatewaySupport tableSupport,
      long snapshotId,
      Long sequenceNumber,
      String operation,
      Map<String, String> summary) {
    if (committedMetadata == null) {
      LOG.debugf(
          "Skipping snapshot format metadata synthesis without canonical TableMetadata tableId=%s snapshotId=%d",
          table == null || !table.hasResourceId() ? "<missing>" : table.getResourceId().getId(),
          snapshotId);
      return null;
    }
    IcebergMetadata base =
        icebergMetadataService.toIcebergMetadata(
            committedMetadata, committedMetadata.metadataFileLocation());
    if (base == null) {
      LOG.debugf(
          "Skipping snapshot format metadata synthesis without readable Iceberg metadata tableId=%s snapshotId=%d",
          table == null || !table.hasResourceId() ? "<missing>" : table.getResourceId().getId(),
          snapshotId);
      return null;
    }
    IcebergMetadata.Builder builder = base.toBuilder().setCurrentSnapshotId(snapshotId);
    if (operation != null && !operation.isBlank()) {
      builder.setOperation(operation);
    }
    if (summary != null && !summary.isEmpty()) {
      builder.putAllSummary(summary);
    }
    return builder.build();
  }

  private ResourceId scopeTableIdWithAccount(ResourceId tableId, String accountId) {
    if (tableId == null) {
      return null;
    }
    String resolvedAccount = firstNonBlank(tableId.getAccountId(), accountId);
    if (resolvedAccount == null || resolvedAccount.isBlank()) {
      return tableId;
    }
    if (resolvedAccount.equals(tableId.getAccountId())) {
      return tableId;
    }
    return tableId.toBuilder().setAccountId(resolvedAccount).build();
  }

  private long clockMillis() {
    return System.currentTimeMillis();
  }

  private IcebergCommitJournalEntry buildCommitJournalEntry(
      String txId,
      String requestHash,
      ResourceId tableId,
      List<String> namespacePath,
      String tableName,
      ResourceId connectorId,
      List<Long> addedSnapshotIds,
      List<Long> removedSnapshotIds,
      ai.floedb.floecat.catalog.rpc.Table table,
      long createdAtMs) {
    IcebergCommitJournalEntry.Builder builder =
        IcebergCommitJournalEntry.newBuilder()
            .setVersion(COMMIT_JOURNAL_VERSION)
            .setTxId(txId == null ? "" : txId)
            .setRequestHash(requestHash == null ? "" : requestHash)
            .setCreatedAtMs(Math.max(0L, createdAtMs));
    if (tableId != null) {
      builder.setTableId(tableId);
    }
    if (namespacePath != null && !namespacePath.isEmpty()) {
      builder.addAllNamespacePath(namespacePath);
    }
    if (tableName != null && !tableName.isBlank()) {
      builder.setTableName(tableName);
    }
    if (connectorId != null && !connectorId.getId().isBlank()) {
      builder.setConnectorId(connectorId);
    }
    if (addedSnapshotIds != null && !addedSnapshotIds.isEmpty()) {
      builder.addAllAddedSnapshotIds(addedSnapshotIds);
    }
    if (removedSnapshotIds != null && !removedSnapshotIds.isEmpty()) {
      builder.addAllRemovedSnapshotIds(removedSnapshotIds);
    }
    String metadataLocation =
        table == null ? null : MetadataLocationUtil.metadataLocation(table.getPropertiesMap());
    if (metadataLocation != null && !metadataLocation.isBlank()) {
      builder.setMetadataLocation(metadataLocation);
    }
    String tableUuid = tableUuid(table);
    if (tableUuid != null && !tableUuid.isBlank()) {
      builder.setTableUuid(tableUuid);
    }
    return builder.build();
  }

  private IcebergCommitOutboxEntry buildCommitOutboxEntry(
      String txId, String requestHash, String accountId, String tableId, long createdAtMs) {
    return IcebergCommitOutboxEntry.newBuilder()
        .setVersion(COMMIT_OUTBOX_VERSION)
        .setTxId(txId == null ? "" : txId)
        .setRequestHash(requestHash == null ? "" : requestHash)
        .setAccountId(accountId == null ? "" : accountId)
        .setTableId(tableId == null ? "" : tableId)
        .setCreatedAtMs(Math.max(0L, createdAtMs))
        .setAttemptCount(0)
        .setNextAttemptAtMs(0L)
        .setLastAttemptAtMs(0L)
        .setDeadLetteredAtMs(0L)
        .build();
  }

  private TableMetadata resolveBaseMetadata(
      String tableName,
      ResourceId tableId,
      ai.floedb.floecat.catalog.rpc.Table plannedTable,
      ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests.Commit commitRequest,
      TableGatewaySupport tableSupport,
      boolean firstWriteAssertCreate) {
    try {
      var resolved =
          icebergMetadataService.resolveMetadata(
              tableName,
              plannedTable,
              null,
              tableSupport == null ? Map.of() : tableSupport.defaultFileIoProperties(),
              List::of);
      if (resolved != null && resolved.tableMetadata() != null) {
        return resolved.tableMetadata();
      }
    } catch (RuntimeException e) {
      LOG.debugf(
          e,
          "Skipping base metadata resolution during pre-materialization tableId=%s table=%s",
          tableId == null ? "<missing>" : tableId.getId(),
          tableName);
    }
    return firstWriteAssertCreate
        ? icebergMetadataService.bootstrapTableMetadataFromCommit(plannedTable, commitRequest)
        : icebergMetadataService.bootstrapTableMetadata(
            tableName,
            plannedTable,
            new LinkedHashMap<>(plannedTable.getPropertiesMap()),
            null,
            List.of());
  }

  private boolean snapshotRefExistsInMetadata(IcebergMetadata metadata, String refName) {
    if (metadata == null) {
      return false;
    }
    if (metadata.getRefsMap().containsKey(refName)
        && metadata.getRefsOrThrow(refName).getSnapshotId() >= 0L) {
      return true;
    }
    return "main".equals(refName) && metadata.getCurrentSnapshotId() > 0L;
  }

  private boolean hasCurrentMainSnapshot(Map<String, String> properties, String refName) {
    if (!"main".equals(refName) || properties == null) {
      return false;
    }
    Long snapshotId = TableMappingUtil.asLong(properties.get("current-snapshot-id"));
    return snapshotId != null && snapshotId >= 0L;
  }

  private String tableUuid(ai.floedb.floecat.catalog.rpc.Table table) {
    return table == null ? null : table.getPropertiesMap().get("table-uuid");
  }

  private String metadataLocation(TableMetadata metadata) {
    return metadata == null
        ? null
        : firstNonBlank(
            metadata.metadataFileLocation(), metadata.properties().get("metadata-location"));
  }
}
