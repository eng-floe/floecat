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

@ApplicationScoped
public class CommitPlanBuilder {
  @Inject ConnectorProvisioningService connectorProvisioningService;
  @Inject TableCommitOutboxService commitOutboxService;
  @Inject CreateCommitNormalizer createCommitNormalizer;
  @Inject CommitRequirementValidator commitRequirementValidator;
  @Inject CommitUpdateCompiler commitUpdateCompiler;
  @Inject CommitTargetResolver commitTargetResolver;
  @Inject TablePatchApplier tablePatchApplier;
  @Inject MetadataMaterializer metadataMaterializer;
  @Inject SnapshotTxChangeBuilder snapshotTxChangeBuilder;

  record PlanBuildResult(CommitRequestContext context, CommitPlan plan) {}

  PlanBuildResult build(CommitRequestContext context) {
    if (context.currentState() == TransactionState.TS_APPLY_FAILED_CONFLICT) {
      throw new WebApplicationException(
          IcebergErrorResponses.failure(
              "transaction commit failed", "CommitFailedException", Response.Status.CONFLICT));
    }

    boolean alreadyApplied = context.currentState() == TransactionState.TS_APPLIED;
    if (alreadyApplied) {
      return new PlanBuildResult(
          context.withPlannedChanges(List.of()),
          new CommitPlan(
              List.of(),
              commitOutboxService.loadPendingWorkItemsForTx(
                  context.accountId(),
                  context.txId(),
                  context.txCreatedAtMs(),
                  context.requestHash())));
    }

    List<ai.floedb.floecat.transaction.rpc.TxChange> txChanges = new ArrayList<>();
    List<TableCommitOutboxService.WorkItem> outboxItems = new ArrayList<>();
    List<PlannedTableChange> plannedChanges = new ArrayList<>();

    for (ValidatedTableChange change : context.changes()) {
      PlannedTableChange plannedChange = planTransactionChange(change, context);
      plannedChanges.add(plannedChange);
      Response assemblyError = appendTxArtifacts(plannedChange, context, txChanges, outboxItems);
      if (assemblyError != null) {
        throw new WebApplicationException(assemblyError);
      }
    }

    return new PlanBuildResult(
        context.withPlannedChanges(plannedChanges),
        new CommitPlan(List.copyOf(txChanges), List.copyOf(outboxItems)));
  }

  private PlannedTableChange planTransactionChange(
      ValidatedTableChange change, CommitRequestContext context) {
    CommitTargetResolver.ResolvedTarget target = commitTargetResolver.resolve(context, change);
    ParsedCommit normalizedCommit = normalizeCommit(change, context, target);
    Supplier<ai.floedb.floecat.catalog.rpc.Table> workingTableSupplier = target::persistedTable;

    Response requirementError =
        commitRequirementValidator.validate(target.currentState(), normalizedCommit.requirements());
    if (requirementError != null) {
      throw new WebApplicationException(requirementError);
    }

    CommitUpdateCompiler.CompileResult compiled =
        commitUpdateCompiler.compile(normalizedCommit, workingTableSupplier, true);
    if (compiled.hasError()) {
      throw new WebApplicationException(compiled.error());
    }

    ai.floedb.floecat.catalog.rpc.Table updatedTable =
        tablePatchApplier.apply(
            target.persistedTable(), compiled.patch().spec(), compiled.patch().mask());

    String namespace =
        target.namespacePath().isEmpty() ? "" : String.join(".", target.namespacePath());
    MetadataMaterializer.Result materialized =
        metadataMaterializer.preMaterialize(
            namespace,
            target.tableName(),
            target.tableId(),
            updatedTable,
            normalizedCommit,
            target.currentState(),
            context.tableSupport(),
            context.preMaterializeAssertCreate(),
            target.hadCommittedSnapshot());
    if (materialized.error() != null) {
      throw new WebApplicationException(materialized.error());
    }

    return new PlannedTableChange(
        change,
        target,
        normalizedCommit,
        compiled.patch(),
        materialized.table(),
        materialized.tableMetadata());
  }

  private ParsedCommit normalizeCommit(
      ValidatedTableChange change,
      CommitRequestContext context,
      CommitTargetResolver.ResolvedTarget target) {
    ParsedCommit parsedCommit =
        ParsedCommit.from(
            createCommitNormalizer.enrichPendingCreateBootstrap(
                context.accountId(),
                context.catalogName(),
                target.namespacePath(),
                target.tableName(),
                target.persistedTable(),
                change.parsedCommit().toCommitRequest()));
    Response nullRefRequirementError =
        commitRequirementValidator.validateNullRefRequirements(
            target.currentState(), parsedCommit.requirements());
    if (nullRefRequirementError != null) {
      throw new WebApplicationException(nullRefRequirementError);
    }
    return parsedCommit;
  }

  private Response appendTxArtifacts(
      PlannedTableChange plan,
      CommitRequestContext context,
      List<ai.floedb.floecat.transaction.rpc.TxChange> txChanges,
      List<TableCommitOutboxService.WorkItem> outboxItems) {
    ai.floedb.floecat.catalog.rpc.Table tableForTx = plan.updatedTable();
    ResourceId scopedTableId =
        commitTargetResolver.scopeTableIdWithAccount(plan.target().tableId(), context.accountId());
    ConnectorProvisioningService.ProvisionResult connectorResolution =
        connectorProvisioningService.resolveOrCreateForCommit(
            context.accountId(),
            context.txId(),
            context.prefix(),
            context.tableSupport(),
            plan.target().namespacePath(),
            plan.target().namespaceId(),
            context.catalogId(),
            plan.target().tableName(),
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
            .setTableId(plan.target().tableId())
            .setTable(tableForTx)
            .setPrecondition(
                Precondition.newBuilder().setExpectedVersion(plan.target().pointerVersion()))
            .build());

    SnapshotTxChangeBuilder.Result snapshotChangePlan =
        snapshotTxChangeBuilder.build(
            context.accountId(),
            plan.target().tableId(),
            tableForTx,
            plan.committedMetadata(),
            context.tableSupport(),
            plan.normalizedCommit(),
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
      PlannedTableChange plan,
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
            plan.target().namespacePath(),
            plan.target().tableName(),
            connectorId,
            plan.normalizedCommit().parsed().addedSnapshotIds(),
            plan.normalizedCommit().parsed().removedSnapshotIds(),
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
