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

import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.support.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.table.ConnectorProvisioningService;
import ai.floedb.floecat.storage.kv.Keys;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import com.google.protobuf.ByteString;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.iceberg.TableMetadata;

@ApplicationScoped
public class CommitPlanBuilder {
  @Inject ConnectorProvisioningService connectorProvisioningService;
  @Inject TableCommitOutboxService commitOutboxService;
  @Inject CreateCommitNormalizer createCommitNormalizer;
  @Inject CommitChangeRequestValidator commitChangeRequestValidator;
  @Inject CommitRequirementValidator commitRequirementValidator;
  @Inject CommitUpdateCompiler commitUpdateCompiler;
  @Inject CommitTargetResolver commitTargetResolver;
  @Inject TablePatchApplier tablePatchApplier;
  @Inject MetadataMaterializer metadataMaterializer;
  @Inject SnapshotTxChangeBuilder snapshotTxChangeBuilder;

  record NormalizedCommit(
      CommitTargetResolver.ResolvedTarget target, String namespace, ParsedCommit parsedCommit) {}

  record ProjectedCommit(
      CommitTargetResolver.ResolvedTarget target,
      ParsedCommit parsedCommit,
      ai.floedb.floecat.catalog.rpc.Table updatedTable) {}

  record MaterializedCommit(
      CommitTargetResolver.ResolvedTarget target,
      ParsedCommit parsedCommit,
      ai.floedb.floecat.catalog.rpc.Table updatedTable,
      TableMetadata committedMetadata) {}

  record PlannedTableCommit(
      List<String> namespacePath,
      ResourceId namespaceId,
      String tableName,
      ResourceId tableId,
      ai.floedb.floecat.catalog.rpc.Table table,
      ParsedCommit commit,
      TableMetadata tableMetadata,
      long expectedVersion) {}

  CommitPlan build(CommitRequestContext context) {
    if (context.currentState() == TransactionState.TS_APPLY_FAILED_CONFLICT) {
      throw new WebApplicationException(
          IcebergErrorResponses.failure(
              "transaction commit failed", "CommitFailedException", Response.Status.CONFLICT));
    }

    boolean alreadyApplied = context.currentState() == TransactionState.TS_APPLIED;
    if (alreadyApplied) {
      return new CommitPlan(
          List.of(),
          commitOutboxService.loadPendingWorkItemsForTx(
              context.accountId(), context.txId(), context.txCreatedAtMs(), context.requestHash()));
    }

    List<ai.floedb.floecat.transaction.rpc.TxChange> txChanges = new ArrayList<>();
    List<TableCommitOutboxService.WorkItem> outboxItems = new ArrayList<>();

    for (TransactionCommitRequest.TableChange change : context.changes()) {
      PlannedTableCommit plannedChange = planTransactionChange(change, context);
      Response assemblyError = appendTxArtifacts(plannedChange, context, txChanges, outboxItems);
      if (assemblyError != null) {
        throw new WebApplicationException(assemblyError);
      }
    }
    return new CommitPlan(List.copyOf(txChanges), List.copyOf(outboxItems));
  }

  private PlannedTableCommit planTransactionChange(
      TransactionCommitRequest.TableChange change, CommitRequestContext context) {
    Response inputValidationError = commitChangeRequestValidator.validate(change);
    if (inputValidationError != null) {
      throw new WebApplicationException(inputValidationError);
    }
    CommitTargetResolver.ResolvedTarget target = resolveTarget(change, context);
    NormalizedCommit normalized = normalizeCommit(change, context, target);
    ProjectedCommit projected = projectCommit(context, normalized);
    MaterializedCommit materialized = materializeCommit(context, normalized, projected);
    return new PlannedTableCommit(
        target.namespacePath(),
        target.namespaceId(),
        target.tableName(),
        target.tableId(),
        materialized.updatedTable(),
        normalized.parsedCommit(),
        materialized.committedMetadata(),
        target.pointerVersion());
  }

  private CommitTargetResolver.ResolvedTarget resolveTarget(
      TransactionCommitRequest.TableChange change, CommitRequestContext context) {
    return commitTargetResolver.resolve(context, change);
  }

  private NormalizedCommit normalizeCommit(
      TransactionCommitRequest.TableChange change,
      CommitRequestContext context,
      CommitTargetResolver.ResolvedTarget target) {
    ParsedCommit parsedCommit =
        ParsedCommit.from(
            createCommitNormalizer.enrichPendingCreateBootstrap(
                context.accountId(),
                context.catalogName(),
                target.namespacePath(),
                target.tableName(),
                context.catalogId(),
                target.namespaceId(),
                target.persistedTable(),
                new TableRequests.Commit(change.requirements(), change.updates()),
                context.tableSupport()));
    Response nullRefRequirementError =
        commitRequirementValidator.validateNullRefRequirements(
            context.tableSupport(), target.persistedTable(), parsedCommit.requirements());
    if (nullRefRequirementError != null) {
      throw new WebApplicationException(nullRefRequirementError);
    }
    String namespace =
        target.namespacePath().isEmpty() ? "" : String.join(".", target.namespacePath());
    return new NormalizedCommit(target, namespace, parsedCommit);
  }

  private ProjectedCommit projectCommit(CommitRequestContext context, NormalizedCommit normalized) {
    Supplier<ai.floedb.floecat.catalog.rpc.Table> workingTableSupplier =
        () -> normalized.target().persistedTable();
    Response requirementError =
        commitRequirementValidator.validate(
            context.tableSupport(), normalized.parsedCommit().requirements(), workingTableSupplier);
    if (requirementError != null) {
      throw new WebApplicationException(requirementError);
    }

    CommitUpdateCompiler.CompileResult compiled =
        commitUpdateCompiler.compile(
            normalized.parsedCommit().toCommitRequest(), workingTableSupplier, true);
    if (compiled.hasError()) {
      throw new WebApplicationException(compiled.error());
    }

    ai.floedb.floecat.catalog.rpc.Table updated =
        tablePatchApplier.apply(
            normalized.target().persistedTable(), compiled.patch().spec(), compiled.patch().mask());
    return new ProjectedCommit(normalized.target(), normalized.parsedCommit(), updated);
  }

  private MaterializedCommit materializeCommit(
      CommitRequestContext context, NormalizedCommit normalized, ProjectedCommit projected) {
    MetadataMaterializer.Result preMaterialized =
        metadataMaterializer.preMaterialize(
            normalized.namespace(),
            normalized.target().tableName(),
            normalized.target().tableId(),
            projected.updatedTable(),
            normalized.parsedCommit(),
            context.tableSupport(),
            context.preMaterializeAssertCreate(),
            normalized.target().hadCommittedSnapshot());
    if (preMaterialized.error() != null) {
      throw new WebApplicationException(preMaterialized.error());
    }
    return new MaterializedCommit(
        normalized.target(),
        normalized.parsedCommit(),
        preMaterialized.table(),
        preMaterialized.tableMetadata());
  }

  private Response appendTxArtifacts(
      PlannedTableCommit plan,
      CommitRequestContext context,
      List<ai.floedb.floecat.transaction.rpc.TxChange> txChanges,
      List<TableCommitOutboxService.WorkItem> outboxItems) {
    var tableForTx = plan.table();
    ResourceId scopedTableId =
        commitTargetResolver.scopeTableIdWithAccount(plan.tableId(), context.accountId());
    ConnectorProvisioningService.ProvisionResult connectorResolution =
        connectorProvisioningService.resolveOrCreateForCommit(
            context.accountId(),
            context.txId(),
            context.prefix(),
            context.tableSupport(),
            plan.namespacePath(),
            plan.namespaceId(),
            context.catalogId(),
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
    txChanges.add(
        ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
            .setTableId(plan.tableId())
            .setTable(tableForTx)
            .setPrecondition(Precondition.newBuilder().setExpectedVersion(plan.expectedVersion()))
            .build());

    SnapshotTxChangeBuilder.Result snapshotChangePlan =
        snapshotTxChangeBuilder.build(
            context.accountId(),
            plan.tableId(),
            tableForTx,
            plan.tableMetadata(),
            context.tableSupport(),
            plan.commit(),
            false);
    if (snapshotChangePlan.error() != null) {
      return snapshotChangePlan.error();
    }
    if (!snapshotChangePlan.txChanges().isEmpty()) {
      txChanges.addAll(snapshotChangePlan.txChanges());
    }

    return appendJournalAndOutboxChanges(
        plan,
        context,
        connectorResolution.connectorId(),
        tableForTx,
        scopedTableId,
        txChanges,
        outboxItems);
  }

  private Response appendJournalAndOutboxChanges(
      PlannedTableCommit plan,
      CommitRequestContext context,
      ResourceId connectorId,
      ai.floedb.floecat.catalog.rpc.Table tableForTx,
      ResourceId scopedTableId,
      List<ai.floedb.floecat.transaction.rpc.TxChange> txChanges,
      List<TableCommitOutboxService.WorkItem> outboxItems) {
    var journal =
        CommitJournalFactory.buildJournalEntry(
            context.txId(),
            context.requestHash(),
            scopedTableId,
            plan.namespacePath(),
            plan.tableName(),
            connectorId,
            plan.commit().parsed().addedSnapshotIds(),
            plan.commit().parsed().removedSnapshotIds(),
            tableForTx,
            context.txCreatedAtMs());
    String pendingKey =
        Keys.tableCommitOutboxPendingPointer(
            context.txCreatedAtMs(), context.accountId(), scopedTableId.getId(), context.txId());
    txChanges.add(
        ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
            .setTargetPointerKey(
                Keys.tableCommitJournalPointer(
                    context.accountId(), scopedTableId.getId(), context.txId()))
            .setPayload(ByteString.copyFrom(journal.toByteArray()))
            .build());
    txChanges.add(
        ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
            .setTargetPointerKey(pendingKey)
            .setPayload(
                ByteString.copyFrom(
                    CommitJournalFactory.buildOutboxEntry(
                            context.txId(),
                            context.requestHash(),
                            context.accountId(),
                            scopedTableId.getId(),
                            context.txCreatedAtMs())
                        .toByteArray()))
            .build());
    outboxItems.add(commitOutboxService.toWorkItem(pendingKey, journal));
    return null;
  }
}
