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

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.CatalogRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.RequestContextFactory;
import ai.floedb.floecat.gateway.iceberg.rest.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.CommitStageResolver;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.TransactionClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.SnapshotMetadataService;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef;
import ai.floedb.floecat.transaction.rpc.GetTransactionRequest;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@ApplicationScoped
public class TransactionCommitService {
  @Inject AccountContext accountContext;
  @Inject RequestContextFactory requestContextFactory;
  @Inject TableLifecycleService tableLifecycleService;
  @Inject TableCommitPlanner tableCommitPlanner;
  @Inject TableCommitSideEffectService sideEffectService;
  @Inject SnapshotMetadataService snapshotMetadataService;
  @Inject CommitStageResolver stageResolver;
  @Inject TransactionClient transactionClient;

  public Response commit(
      String prefix,
      String idempotencyKey,
      String transactionId,
      TransactionCommitRequest request,
      TableGatewaySupport tableSupport) {
    String accountId = accountContext.getAccountId();
    if (accountId == null || accountId.isBlank()) {
      return IcebergErrorResponses.validation("account context is required");
    }
    List<TransactionCommitRequest.TableChange> changes =
        request == null || request.tableChanges() == null ? List.of() : request.tableChanges();
    if (changes.isEmpty()) {
      return IcebergErrorResponses.validation("table-changes are required");
    }
    CatalogRequestContext catalogContext = requestContextFactory.catalog(prefix);
    String catalogName = catalogContext.catalogName();
    ResourceId catalogId = catalogContext.catalogId();

    List<PlannedChange> planned = new ArrayList<>();

    for (TransactionCommitRequest.TableChange change : changes) {
      var identifier = change.identifier();
      List<String> namespacePath =
          identifier.namespace() == null ? List.of() : List.copyOf(identifier.namespace());
      String namespace = namespacePath.isEmpty() ? "" : String.join(".", namespacePath);
      ResourceId namespaceId = tableLifecycleService.resolveNamespaceId(catalogName, namespacePath);
      TableRequests.Commit commitReq =
          new TableRequests.Commit(change.requirements(), change.updates());
      var command =
          new TableCommitService.CommitCommand(
              prefix,
              namespace,
              namespacePath,
              identifier.name(),
              catalogName,
              catalogId,
              namespaceId,
              idempotencyKey,
              change.stageId(),
              transactionId,
              commitReq,
              tableSupport);
      var stageResolution = stageResolver.resolve(command);
      if (stageResolution.hasError()) {
        return stageResolution.error();
      }
      ResourceId tableId = stageResolution.tableId();
      Supplier<ai.floedb.floecat.catalog.rpc.Table> tableSupplier =
          () -> {
            var staged = stageResolution.stagedTable();
            return staged != null ? staged : tableLifecycleService.getTable(tableId);
          };
      var plan = tableCommitPlanner.plan(command, tableSupplier, tableId);
      if (plan.hasError()) {
        return plan.error();
      }
      var updated = plan.table();
      List<Map<String, Object>> updates = change.updates() == null ? List.of() : change.updates();
      SnapshotPlan snapshotPlan = buildSnapshotPlan(tableSupport, updated, updates);
      planned.add(
          new PlannedChange(
              namespacePath, namespaceId, identifier.name(), tableId, updated, snapshotPlan));
    }

    List<ai.floedb.floecat.transaction.rpc.TxChange> txChanges = new ArrayList<>();
    List<SyncTarget> syncTargets = new ArrayList<>();
    for (var plan : planned) {
      txChanges.add(
          ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
              .setTableId(plan.tableId())
              .setTable(plan.table())
              .build());
      syncTargets.add(
          new SyncTarget(
              plan.namespacePath(),
              plan.namespaceId(),
              plan.tableName(),
              plan.tableId(),
              plan.table(),
              plan.snapshotPlan()));
    }

    String beginIdempotency = firstNonBlank(idempotencyKey, transactionId);
    var begin =
        transactionClient.beginTransaction(
            ai.floedb.floecat.transaction.rpc.BeginTransactionRequest.newBuilder()
                .setIdempotency(
                    ai.floedb.floecat.common.rpc.IdempotencyKey.newBuilder()
                        .setKey(beginIdempotency == null ? "" : beginIdempotency))
                .build());
    String txId = begin.getTransaction().getTxId();
    if (txId == null || txId.isBlank()) {
      return IcebergErrorResponses.failure(
          "Failed to begin transaction",
          "InternalServerError",
          Response.Status.INTERNAL_SERVER_ERROR);
    }

    var currentTxn =
        transactionClient.getTransaction(GetTransactionRequest.newBuilder().setTxId(txId).build());
    TransactionState currentState =
        currentTxn != null && currentTxn.hasTransaction()
            ? currentTxn.getTransaction().getState()
            : TransactionState.TS_UNSPECIFIED;
    if (currentState == TransactionState.TS_COMMITTED) {
      return Response.noContent().build();
    }

    String idempotencyBase = firstNonBlank(idempotencyKey, transactionId, txId);
    if (currentState != TransactionState.TS_PREPARED) {
      transactionClient.prepareTransaction(
          ai.floedb.floecat.transaction.rpc.PrepareTransactionRequest.newBuilder()
              .setTxId(txId)
              .addAllChanges(txChanges)
              .setIdempotency(
                  ai.floedb.floecat.common.rpc.IdempotencyKey.newBuilder()
                      .setKey(idempotencyBase == null ? "" : idempotencyBase + ":prepare"))
              .build());
    }
    Response preCommitError =
        applyPreCommitSnapshots(tableSupport, syncTargets, idempotencyKey, txId);
    if (preCommitError != null) {
      return preCommitError;
    }

    try {
      transactionClient.commitTransaction(
          ai.floedb.floecat.transaction.rpc.CommitTransactionRequest.newBuilder()
              .setTxId(txId)
              .setIdempotency(
                  ai.floedb.floecat.common.rpc.IdempotencyKey.newBuilder()
                      .setKey(idempotencyBase == null ? "" : idempotencyBase + ":commit"))
              .build());
    } catch (RuntimeException commitFailure) {
      rollbackSnapshotChanges(tableSupport, syncTargets);
      return IcebergErrorResponses.failure(
          "transaction commit failed",
          "CommitStateUnknownException",
          Response.Status.INTERNAL_SERVER_ERROR);
    }

    for (var target : syncTargets) {
      Response snapshotError =
          snapshotMetadataService.applySnapshotUpdates(
              tableSupport,
              target.tableId(),
              target.namespacePath(),
              target.tableName(),
              () -> target.table(),
              target.snapshotPlan().postCommitUpdates(),
              idempotencyKey);
      if (snapshotError != null) {
        return snapshotError;
      }
      String metadataLocation =
          MetadataLocationUtil.metadataLocation(target.table().getPropertiesMap());
      ResourceId connectorId =
          sideEffectService.synchronizeConnector(
              tableSupport,
              prefix,
              target.namespacePath(),
              target.namespaceId(),
              catalogId,
              target.tableName(),
              target.table(),
              null,
              metadataLocation,
              idempotencyKey);
      sideEffectService.runConnectorSync(
          tableSupport, connectorId, target.namespacePath(), target.tableName());
    }

    return Response.noContent().build();
  }

  private record SyncTarget(
      List<String> namespacePath,
      ResourceId namespaceId,
      String tableName,
      ResourceId tableId,
      ai.floedb.floecat.catalog.rpc.Table table,
      SnapshotPlan snapshotPlan) {}

  private record PlannedChange(
      List<String> namespacePath,
      ResourceId namespaceId,
      String tableName,
      ResourceId tableId,
      ai.floedb.floecat.catalog.rpc.Table table,
      SnapshotPlan snapshotPlan) {}

  private record SnapshotPlan(
      List<Map<String, Object>> additions,
      List<Map<String, Object>> refUpdates,
      List<Map<String, Object>> removals,
      List<Map<String, Object>> postCommitUpdates,
      List<Map<String, Object>> rollbackRefUpdates,
      List<ai.floedb.floecat.catalog.rpc.Snapshot> removedSnapshots) {}

  private SnapshotPlan buildSnapshotPlan(
      TableGatewaySupport tableSupport,
      ai.floedb.floecat.catalog.rpc.Table table,
      List<Map<String, Object>> updates) {
    List<Map<String, Object>> additions = new ArrayList<>();
    List<Map<String, Object>> refUpdates = new ArrayList<>();
    List<Map<String, Object>> removals = new ArrayList<>();
    List<Map<String, Object>> postCommit = new ArrayList<>();
    List<Map<String, Object>> rollbackRefs = new ArrayList<>();
    List<ai.floedb.floecat.catalog.rpc.Snapshot> removedSnapshots = new ArrayList<>();

    IcebergMetadata metadata = tableSupport.loadCurrentMetadata(table);
    Map<String, IcebergRef> currentRefs = metadata == null ? Map.of() : metadata.getRefsMap();

    for (Map<String, Object> update : updates) {
      String action = update == null ? null : String.valueOf(update.get("action"));
      if ("add-snapshot".equals(action)) {
        additions.add(update);
      } else if ("remove-snapshots".equals(action)) {
        removals.add(update);
      } else if ("set-snapshot-ref".equals(action) || "remove-snapshot-ref".equals(action)) {
        refUpdates.add(update);
        String refName = update == null ? null : String.valueOf(update.get("ref-name"));
        if (refName != null && !refName.isBlank()) {
          IcebergRef prior = currentRefs.get(refName);
          if ("set-snapshot-ref".equals(action)) {
            if (prior == null) {
              rollbackRefs.add(Map.of("action", "remove-snapshot-ref", "ref-name", refName));
            } else {
              rollbackRefs.add(refToUpdate(refName, prior));
            }
          } else {
            if (prior != null) {
              rollbackRefs.add(refToUpdate(refName, prior));
            }
          }
        }
      } else {
        postCommit.add(update);
      }
    }

    if (!removals.isEmpty()) {
      List<Long> ids = snapshotIdsFrom(removals);
      if (!ids.isEmpty()) {
        removedSnapshots.addAll(snapshotMetadataService.fetchSnapshots(table.getResourceId(), ids));
      }
    }

    return new SnapshotPlan(
        additions, refUpdates, removals, postCommit, rollbackRefs, removedSnapshots);
  }

  private Map<String, Object> refToUpdate(String refName, IcebergRef ref) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("action", "set-snapshot-ref");
    out.put("ref-name", refName);
    out.put("snapshot-id", ref.getSnapshotId());
    if (ref.getType() != null && !ref.getType().isBlank()) {
      out.put("type", ref.getType());
    }
    if (ref.getMaxReferenceAgeMs() > 0) {
      out.put("max-ref-age-ms", ref.getMaxReferenceAgeMs());
    }
    if (ref.getMaxSnapshotAgeMs() > 0) {
      out.put("max-snapshot-age-ms", ref.getMaxSnapshotAgeMs());
    }
    if (ref.getMinSnapshotsToKeep() > 0) {
      out.put("min-snapshots-to-keep", ref.getMinSnapshotsToKeep());
    }
    return out;
  }

  private Response applyPreCommitSnapshots(
      TableGatewaySupport tableSupport,
      List<SyncTarget> targets,
      String idempotencyKey,
      String txId) {
    for (var target : targets) {
      SnapshotPlan plan = target.snapshotPlan();
      Response addError =
          snapshotMetadataService.applySnapshotUpdates(
              tableSupport,
              target.tableId(),
              target.namespacePath(),
              target.tableName(),
              () -> target.table(),
              plan.additions(),
              idempotencyKey);
      if (addError != null) {
        rollbackSnapshotChanges(tableSupport, targets);
        transactionClient.abortTransaction(
            ai.floedb.floecat.transaction.rpc.AbortTransactionRequest.newBuilder()
                .setTxId(txId)
                .setReason("snapshot additions failed")
                .build());
        return addError;
      }
      Response refError =
          snapshotMetadataService.applySnapshotUpdates(
              tableSupport,
              target.tableId(),
              target.namespacePath(),
              target.tableName(),
              () -> target.table(),
              plan.refUpdates(),
              idempotencyKey);
      if (refError != null) {
        rollbackSnapshotChanges(tableSupport, targets);
        transactionClient.abortTransaction(
            ai.floedb.floecat.transaction.rpc.AbortTransactionRequest.newBuilder()
                .setTxId(txId)
                .setReason("snapshot ref updates failed")
                .build());
        return refError;
      }
      Response removeError =
          snapshotMetadataService.applySnapshotUpdates(
              tableSupport,
              target.tableId(),
              target.namespacePath(),
              target.tableName(),
              () -> target.table(),
              plan.removals(),
              idempotencyKey);
      if (removeError != null) {
        rollbackSnapshotChanges(tableSupport, targets);
        transactionClient.abortTransaction(
            ai.floedb.floecat.transaction.rpc.AbortTransactionRequest.newBuilder()
                .setTxId(txId)
                .setReason("snapshot removals failed")
                .build());
        return removeError;
      }
    }
    return null;
  }

  private void rollbackSnapshotChanges(TableGatewaySupport tableSupport, List<SyncTarget> targets) {
    for (var target : targets) {
      SnapshotPlan plan = target.snapshotPlan();
      List<Long> snapshotIds = snapshotIdsFrom(plan.additions());
      if (!snapshotIds.isEmpty()) {
        snapshotMetadataService.deleteSnapshots(target.tableId(), snapshotIds);
      }
      if (!plan.removedSnapshots().isEmpty()) {
        snapshotMetadataService.restoreSnapshots(target.tableId(), plan.removedSnapshots());
      }
      if (!plan.rollbackRefUpdates().isEmpty()) {
        snapshotMetadataService.applySnapshotUpdates(
            tableSupport,
            target.tableId(),
            target.namespacePath(),
            target.tableName(),
            () -> target.table(),
            plan.rollbackRefUpdates(),
            null);
      }
    }
  }

  private List<Long> snapshotIdsFrom(List<Map<String, Object>> updates) {
    if (updates == null || updates.isEmpty()) {
      return List.of();
    }
    List<Long> out = new ArrayList<>();
    for (Map<String, Object> update : updates) {
      if (update == null) {
        continue;
      }
      String action = String.valueOf(update.get("action"));
      if ("remove-snapshots".equals(action)) {
        Object idsObj = update.get("snapshot-ids");
        if (idsObj instanceof List<?> ids) {
          for (Object idObj : ids) {
            Long value = parseSnapshotId(idObj);
            if (value != null) {
              out.add(value);
            }
          }
        }
        continue;
      }
      Object snapshotObj = update.get("snapshot");
      if (snapshotObj instanceof Map<?, ?> snapshot) {
        Long value = parseSnapshotId(snapshot.get("snapshot-id"));
        if (value != null) {
          out.add(value);
        }
      }
    }
    return out;
  }

  private Long parseSnapshotId(Object idObj) {
    if (idObj instanceof Number num) {
      return num.longValue();
    }
    if (idObj instanceof String str) {
      try {
        return Long.parseLong(str);
      } catch (NumberFormatException ignored) {
        return null;
      }
    }
    return null;
  }

  private static String firstNonBlank(String... values) {
    if (values == null) {
      return null;
    }
    for (String value : values) {
      if (value != null && !value.isBlank()) {
        return value;
      }
    }
    return null;
  }
}
