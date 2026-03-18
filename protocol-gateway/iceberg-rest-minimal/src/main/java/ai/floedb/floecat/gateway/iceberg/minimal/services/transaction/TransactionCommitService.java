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

package ai.floedb.floecat.gateway.iceberg.minimal.services.transaction;

import ai.floedb.floecat.catalog.rpc.GetTableResponse;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.minimal.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.minimal.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.minimal.services.compat.TableFormatSupport;
import ai.floedb.floecat.gateway.iceberg.minimal.services.metadata.TableMetadataImportService;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergCommitJournalEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergCommitReplayIndex;
import ai.floedb.floecat.storage.kv.Keys;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import ai.floedb.floecat.transaction.rpc.TxChange;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TransactionCommitService {
  private static final Logger LOG = Logger.getLogger(TransactionCommitService.class);
  private static final int COMMIT_JOURNAL_VERSION = 1;
  private static final int COMMIT_REPLAY_INDEX_VERSION = 1;
  private static final String REQUEST_HASH_PROPERTY = "iceberg.commit.request-hash";
  private static final Set<String> SUPPORTED_REQUIREMENTS =
      Set.of(
          "assert-create",
          "assert-table-uuid",
          "assert-current-schema-id",
          "assert-last-assigned-field-id",
          "assert-last-assigned-partition-id",
          "assert-default-spec-id",
          "assert-default-sort-order-id",
          "assert-ref-snapshot-id");
  private static final Set<String> SUPPORTED_UPDATES =
      Set.of(
          "set-properties",
          "remove-properties",
          "set-location",
          "assign-uuid",
          "upgrade-format-version",
          "add-schema",
          "set-current-schema",
          "add-spec",
          "set-default-spec",
          "remove-partition-specs",
          "add-sort-order",
          "set-default-sort-order",
          "add-snapshot",
          "set-statistics",
          "remove-statistics",
          "set-partition-statistics",
          "remove-partition-statistics",
          "add-encryption-key",
          "remove-encryption-key",
          "remove-schemas",
          "remove-snapshots",
          "set-snapshot-ref",
          "remove-snapshot-ref");

  private final TransactionBackend backend;
  private final MinimalGatewayConfig config;
  private final AccountContext accountContext;
  private final TableFormatSupport tableFormatSupport;
  private final IcebergMetadataCommitService metadataCommitService;
  private final ConnectorProvisioningService connectorProvisioningService;
  private final TableCommitJournalService commitJournalService;
  private final TableCommitSideEffectService sideEffectService;

  @Inject
  public TransactionCommitService(
      TransactionBackend backend,
      MinimalGatewayConfig config,
      AccountContext accountContext,
      TableFormatSupport tableFormatSupport,
      IcebergMetadataCommitService metadataCommitService,
      ConnectorProvisioningService connectorProvisioningService,
      TableCommitJournalService commitJournalService,
      TableCommitSideEffectService sideEffectService) {
    this.backend = backend;
    this.config = config;
    this.accountContext = accountContext;
    this.tableFormatSupport = tableFormatSupport;
    this.metadataCommitService = metadataCommitService;
    this.connectorProvisioningService = connectorProvisioningService;
    this.commitJournalService = commitJournalService;
    this.sideEffectService = sideEffectService;
  }

  public Response commit(String prefix, String idempotencyKey, TransactionCommitRequest request) {
    String accountId = requestAccountId();
    if (accountId == null) {
      return IcebergErrorResponses.validation("account context is required");
    }
    if (request == null || request.tableChanges() == null || request.tableChanges().isEmpty()) {
      return IcebergErrorResponses.validation("table-changes are required");
    }
    String duplicate = firstDuplicateIdentifier(request.tableChanges());
    if (duplicate != null) {
      return IcebergErrorResponses.validation(
          "duplicate table identifier in table-changes: " + duplicate);
    }

    String requestHash = requestHash(request.tableChanges());
    String beginKey = explicitIdempotencyKey(idempotencyKey);
    String txId = null;
    try {
      ResourceId catalogId = backend.resolveCatalog(prefix).getResourceId();
      var begun = backend.beginTransaction(beginKey, requestHash, transactionLifetime());
      txId = begun.getTransaction().getTxId();
      var current = backend.getTransaction(txId);
      TransactionState state =
          current != null && current.hasTransaction()
              ? current.getTransaction().getState()
              : TransactionState.TS_UNSPECIFIED;
      long txCreatedAtMs =
          current != null && current.hasTransaction() && current.getTransaction().hasCreatedAt()
              ? Timestamps.toMillis(current.getTransaction().getCreatedAt())
              : begun.getTransaction().hasCreatedAt()
                  ? Timestamps.toMillis(begun.getTransaction().getCreatedAt())
                  : System.currentTimeMillis();
      String existingHash =
          current != null && current.hasTransaction()
              ? current.getTransaction().getPropertiesMap().get(REQUEST_HASH_PROPERTY)
              : null;
      if (existingHash != null && !existingHash.isBlank() && !existingHash.equals(requestHash)) {
        return IcebergErrorResponses.conflict(
            "transaction request does not match existing transaction payload");
      }
      if (state == TransactionState.TS_APPLY_FAILED_CONFLICT) {
        return IcebergErrorResponses.conflict("transaction commit failed");
      }
      if (state == TransactionState.TS_UNSPECIFIED) {
        return IcebergErrorResponses.commitStateUnknown("transaction state is unknown");
      }
      if (state == TransactionState.TS_APPLIED) {
        try {
          validateReplayState(
              prefix,
              loadReplayValidationEntries(
                  prefix, catalogId, accountId, txId, requestHash, request.tableChanges()),
              request.tableChanges());
          processPostCommitTasks(
              txId,
              loadReplayPostCommitTasks(
                  prefix, catalogId, accountId, txId, requestHash, request.tableChanges()));
        } catch (StatusRuntimeException exception) {
          if (!isMissingReplayState(exception)) {
            throw exception;
          }
          LOG.infof(
              "Applied transaction replay state missing; accepting txId=%s prefix=%s",
              txId, prefix);
        }
        return Response.noContent().build();
      }

      PlannedTransaction planned =
          planChanges(
              prefix,
              catalogId,
              accountId,
              txId,
              txCreatedAtMs,
              requestHash,
              request.tableChanges());
      List<TxChange> changes = planned.changes();
      LOG.infof(
          "Prepared transaction plan prefix=%s txId=%s tableChanges=%d txChanges=%d state=%s",
          prefix, txId, request.tableChanges().size(), changes.size(), state);
      if (state == TransactionState.TS_OPEN) {
        backend.prepareTransaction(txId, suffixKey(idempotencyKey, "prepare"), changes);
        LOG.infof("Prepared transaction txId=%s changeCount=%d", txId, changes.size());
      }
      var committed = backend.commitTransaction(txId, suffixKey(idempotencyKey, "commit"));
      TransactionState commitState =
          committed != null && committed.hasTransaction()
              ? committed.getTransaction().getState()
              : TransactionState.TS_UNSPECIFIED;
      LOG.infof("Commit transaction txId=%s resultState=%s", txId, commitState);
      if (commitState == TransactionState.TS_APPLIED) {
        processPostCommitTasks(txId, planned.postCommitTasks());
        return Response.noContent().build();
      }
      if (commitState == TransactionState.TS_APPLY_FAILED_CONFLICT) {
        return IcebergErrorResponses.conflict("transaction commit failed");
      }
      if (commitState == TransactionState.TS_UNSPECIFIED) {
        return IcebergErrorResponses.commitStateUnknown("transaction commit state is unknown");
      }
      return IcebergErrorResponses.commitStateUnknown(
          "transaction commit did not reach applied state");
    } catch (StatusRuntimeException exception) {
      LOG.warnf(
          exception,
          "Transaction gRPC failure prefix=%s txId=%s status=%s description=%s",
          prefix,
          txId,
          exception.getStatus().getCode(),
          exception.getStatus().getDescription());
      if (txId != null
          && exception.getStatus().getCode() != Status.Code.ALREADY_EXISTS
          && exception.getStatus().getCode() != Status.Code.ABORTED) {
        abortQuietly(txId, "transaction planning or apply failed");
      }
      return mapGrpcFailure(exception);
    } catch (RuntimeException exception) {
      LOG.errorf(
          exception,
          "Transaction runtime failure prefix=%s txId=%s tableChanges=%d",
          prefix,
          txId,
          request.tableChanges().size());
      if (txId != null) {
        abortQuietly(txId, "transaction planning failed");
      }
      return IcebergErrorResponses.commitStateUnknown("transaction commit failed");
    }
  }

  public Response registerImported(
      String prefix,
      String idempotencyKey,
      List<String> namespacePath,
      String tableName,
      TableMetadataImportService.ImportedMetadata imported,
      Map<String, String> mergedProperties,
      boolean overwrite) {
    String accountId = requestAccountId();
    if (accountId == null) {
      return IcebergErrorResponses.validation("account context is required");
    }
    if (namespacePath == null || namespacePath.isEmpty()) {
      return IcebergErrorResponses.validation("namespace is required");
    }
    if (tableName == null || tableName.isBlank()) {
      return IcebergErrorResponses.validation("name is required");
    }
    if (imported == null || imported.icebergMetadata() == null) {
      return IcebergErrorResponses.validation("imported metadata is required");
    }
    String requestHash =
        requestHash(
            canonicalRegisterRequest(
                namespacePath, tableName, imported, mergedProperties, overwrite));
    String beginKey = explicitIdempotencyKey(idempotencyKey);
    String txId = null;
    try {
      ResourceId catalogId = backend.resolveCatalog(prefix).getResourceId();
      var begun = backend.beginTransaction(beginKey, requestHash, transactionLifetime());
      txId = begun.getTransaction().getTxId();
      var current = backend.getTransaction(txId);
      TransactionState state =
          current != null && current.hasTransaction()
              ? current.getTransaction().getState()
              : TransactionState.TS_UNSPECIFIED;
      long txCreatedAtMs =
          current != null && current.hasTransaction() && current.getTransaction().hasCreatedAt()
              ? Timestamps.toMillis(current.getTransaction().getCreatedAt())
              : begun.getTransaction().hasCreatedAt()
                  ? Timestamps.toMillis(begun.getTransaction().getCreatedAt())
                  : System.currentTimeMillis();
      String existingHash =
          current != null && current.hasTransaction()
              ? current.getTransaction().getPropertiesMap().get(REQUEST_HASH_PROPERTY)
              : null;
      if (existingHash != null && !existingHash.isBlank() && !existingHash.equals(requestHash)) {
        return IcebergErrorResponses.conflict(
            "transaction request does not match existing transaction payload");
      }
      if (state == TransactionState.TS_APPLIED) {
        try {
          validateImportedReplayState(
              prefix,
              loadImportedReplayValidationEntries(
                  prefix, catalogId, accountId, txId, requestHash, namespacePath, tableName),
              namespacePath,
              tableName.trim());
          processPostCommitTasks(
              txId,
              loadImportedReplayPostCommitTasks(
                  prefix, catalogId, accountId, txId, requestHash, namespacePath, tableName));
        } catch (StatusRuntimeException exception) {
          if (!isMissingReplayState(exception)) {
            throw exception;
          }
          LOG.infof(
              "Applied imported registration replay state missing; accepting txId=%s prefix=%s",
              txId, prefix);
        }
        return Response.noContent().build();
      }
      if (state == TransactionState.TS_APPLY_FAILED_CONFLICT) {
        return IcebergErrorResponses.conflict("transaction commit failed");
      }
      if (state == TransactionState.TS_UNSPECIFIED) {
        return IcebergErrorResponses.commitStateUnknown("transaction state is unknown");
      }

      PlannedTransaction planned =
          planImportedRegistration(
              prefix,
              catalogId,
              accountId,
              txId,
              txCreatedAtMs,
              requestHash,
              namespacePath,
              tableName.trim(),
              imported,
              mergedProperties == null ? Map.of() : mergedProperties,
              overwrite);
      if (state == TransactionState.TS_OPEN) {
        backend.prepareTransaction(txId, suffixKey(idempotencyKey, "prepare"), planned.changes());
      }
      var committed = backend.commitTransaction(txId, suffixKey(idempotencyKey, "commit"));
      TransactionState commitState =
          committed != null && committed.hasTransaction()
              ? committed.getTransaction().getState()
              : TransactionState.TS_UNSPECIFIED;
      if (commitState == TransactionState.TS_APPLIED) {
        processPostCommitTasks(txId, planned.postCommitTasks());
        return Response.noContent().build();
      }
      if (commitState == TransactionState.TS_APPLY_FAILED_CONFLICT) {
        return IcebergErrorResponses.conflict("transaction commit failed");
      }
      if (commitState == TransactionState.TS_UNSPECIFIED) {
        return IcebergErrorResponses.commitStateUnknown("transaction commit state is unknown");
      }
      return IcebergErrorResponses.commitStateUnknown(
          "transaction commit did not reach applied state");
    } catch (StatusRuntimeException exception) {
      if (txId != null
          && exception.getStatus().getCode() != Status.Code.ALREADY_EXISTS
          && exception.getStatus().getCode() != Status.Code.ABORTED) {
        abortQuietly(txId, "transaction planning or apply failed");
      }
      return mapGrpcFailure(exception);
    } catch (RuntimeException exception) {
      if (txId != null) {
        abortQuietly(txId, "transaction planning failed");
      }
      return IcebergErrorResponses.commitStateUnknown("transaction commit failed");
    }
  }

  private PlannedTransaction planChanges(
      String prefix,
      ResourceId catalogId,
      String accountId,
      String txId,
      long txCreatedAtMs,
      String requestHash,
      List<TransactionCommitRequest.TableChange> tableChanges) {
    List<TxChange> out = new ArrayList<>();
    List<PostCommitTask> postCommitTasks = new ArrayList<>();
    for (TransactionCommitRequest.TableChange change : tableChanges) {
      NormalizedIdentifier identifier = validateChange(change);
      List<String> namespacePath = identifier.namespacePath();
      String tableName = identifier.tableName();
      ResourceId namespaceId = backend.resolveNamespace(prefix, namespacePath).getResourceId();

      GetTableResponse existingResponse = null;
      ResourceId tableId = null;
      try {
        tableId = backend.resolveTable(prefix, namespacePath, tableName).getResourceId();
        existingResponse = backend.getTable(tableId);
      } catch (StatusRuntimeException exception) {
        if (exception.getStatus().getCode() != Status.Code.NOT_FOUND
            || !hasAssertCreate(change.requirements())) {
          throw exception;
        }
      }

      if (existingResponse == null && !hasAssertCreate(change.requirements())) {
        throw Status.NOT_FOUND
            .withDescription(
                "Table " + String.join(".", namespacePath) + "." + tableName + " not found")
            .asRuntimeException();
      }
      if (existingResponse != null && hasAssertCreate(change.requirements())) {
        throw Status.ALREADY_EXISTS.withDescription("assert-create failed").asRuntimeException();
      }

      Table currentTable;
      long expectedVersion;
      if (existingResponse != null && existingResponse.hasTable()) {
        currentTable = existingResponse.getTable();
        expectedVersion =
            existingResponse.hasMeta() ? existingResponse.getMeta().getPointerVersion() : 0L;
        tableId = currentTable.getResourceId();
      } else {
        tableId =
            atomicCreateTableId(accountId, txId, catalogId, namespaceId, namespacePath, tableName);
        currentTable =
            newCreateTableStub(tableId, catalogId, namespaceId, tableName, txCreatedAtMs);
        expectedVersion = 0L;
      }

      if (existingResponse != null && isDeltaReadOnlyCommitBlocked(currentTable)) {
        throw Status.ABORTED
            .withDescription(
                "Delta compatibility mode is read-only; table commits are disabled for Delta tables")
            .asRuntimeException();
      }
      Response nullRefRequirementError =
          validateNullSnapshotRefRequirements(currentTable, change.requirements());
      if (nullRefRequirementError != null) {
        if (nullRefRequirementError.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()) {
          throw Status.INVALID_ARGUMENT
              .withDescription("assert-ref-snapshot-id requires ref")
              .asRuntimeException();
        }
        throw Status.FAILED_PRECONDITION
            .withDescription(
                nullRefRequirementError.getEntity() == null
                    ? "assert-ref-snapshot-id failed"
                    : "assert-ref-snapshot-id failed")
            .asRuntimeException();
      }

      List<Map<String, Object>> updates = change.updates();
      boolean advancedCommit =
          containsAdvancedUpdates(updates) || containsAdvancedRequirements(change.requirements());
      Table updated;
      List<TxChange> extraChanges = List.of();
      if (advancedCommit) {
        if (existingResponse == null) {
          IcebergMetadataCommitService.PlannedCommit planned =
              metadataCommitService.planCreate(currentTable, tableId, updates);
          updated = planned.table();
          extraChanges = planned.extraChanges();
          LOG.infof(
              "Planned create commit table=%s namespace=%s tableId=%s updates=%d extraChanges=%d",
              tableName, namespacePath, tableId.getId(), updates.size(), extraChanges.size());
        } else {
          IcebergMetadataCommitService.PlannedCommit planned =
              metadataCommitService.plan(currentTable, tableId, change.requirements(), updates);
          updated = planned.table();
          extraChanges = planned.extraChanges();
          LOG.infof(
              "Planned update commit table=%s namespace=%s updates=%d extraChanges=%d",
              tableName, namespacePath, updates.size(), extraChanges.size());
        }
      } else {
        updated = applyUpdates(currentTable, updates);
      }
      ResourceId scopedTableId = scopeTableIdWithAccount(tableId, accountId);
      ConnectorProvisioningService.ProvisionResult provisioned =
          connectorProvisioningService.resolveOrCreateForCommit(
              accountId,
              txId,
              namespacePath,
              namespaceId,
              catalogId,
              tableName,
              scopedTableId,
              updated);
      updated = provisioned.table();
      if (!provisioned.connectorTxChanges().isEmpty()) {
        out.addAll(provisioned.connectorTxChanges());
      }
      out.add(
          TxChange.newBuilder()
              .setTableId(tableId)
              .setTable(updated)
              .setPrecondition(
                  Precondition.newBuilder().setExpectedVersion(expectedVersion).build())
              .build());
      out.addAll(extraChanges);
      IcebergCommitJournalEntry journal =
          buildCommitJournalEntry(
              txId,
              requestHash,
              scopedTableId,
              namespacePath,
              tableName,
              provisioned.connectorId(),
              addedSnapshotIds(updates),
              removedSnapshotIds(updates),
              updated,
              txCreatedAtMs);
      out.add(
          TxChange.newBuilder()
              .setTargetPointerKey(
                  Keys.tableCommitJournalPointer(accountId, scopedTableId.getId(), txId))
              .setPayload(ByteString.copyFrom(journal.toByteArray()))
              .build());
      postCommitTasks.add(
          new PostCommitTask(
              List.copyOf(journal.getNamespacePathList()),
              journal.getTableName(),
              journal.getTableId(),
              journal.hasConnectorId() ? journal.getConnectorId() : null,
              journal.getMetadataLocation().isBlank() ? null : journal.getMetadataLocation(),
              journal.getTableUuid().isBlank() ? null : journal.getTableUuid(),
              List.copyOf(journal.getAddedSnapshotIdsList()),
              List.copyOf(journal.getRemovedSnapshotIdsList())));
    }
    if (txId != null && !txId.isBlank()) {
      IcebergCommitReplayIndex replayIndex =
          buildCommitReplayIndex(txId, requestHash, postCommitTasks, txCreatedAtMs);
      out.add(
          TxChange.newBuilder()
              .setTargetPointerKey(Keys.tableCommitReplayPointer(accountId, txId))
              .setPayload(ByteString.copyFrom(replayIndex.toByteArray()))
              .build());
    }
    return new PlannedTransaction(List.copyOf(out), List.copyOf(postCommitTasks));
  }

  private void validateReplayState(
      String prefix,
      Map<String, IcebergCommitJournalEntry> replayEntries,
      List<TransactionCommitRequest.TableChange> tableChanges) {
    if (tableChanges == null || tableChanges.isEmpty()) {
      return;
    }
    for (TransactionCommitRequest.TableChange change : tableChanges) {
      NormalizedIdentifier identifier = validateChange(change);
      validateReplayTarget(
          prefix,
          replayEntries == null ? null : replayEntries.get(identifier.key()),
          identifier.namespacePath(),
          identifier.tableName(),
          hasAssertCreate(change.requirements()));
    }
  }

  private void validateImportedReplayState(
      String prefix,
      Map<String, IcebergCommitJournalEntry> replayEntries,
      List<String> namespacePath,
      String tableName) {
    String normalizedTableName = tableName == null ? "" : tableName.trim();
    String key =
        (namespacePath == null || namespacePath.isEmpty()
                ? ""
                : String.join(".", List.copyOf(namespacePath)) + ".")
            + normalizedTableName;
    validateReplayTarget(
        prefix,
        replayEntries == null ? null : replayEntries.get(key),
        namespacePath == null ? List.of() : List.copyOf(namespacePath),
        normalizedTableName,
        false);
  }

  private void validateReplayTarget(
      String prefix,
      IcebergCommitJournalEntry replayEntry,
      List<String> namespacePath,
      String tableName,
      boolean assertCreateRequested) {
    backend.resolveNamespace(prefix, namespacePath);

    GetTableResponse existingResponse = null;
    try {
      ResourceId tableId = backend.resolveTable(prefix, namespacePath, tableName).getResourceId();
      existingResponse = backend.getTable(tableId);
    } catch (StatusRuntimeException exception) {
      if (exception.getStatus().getCode() != Status.Code.NOT_FOUND) {
        throw exception;
      }
      if (replayEntry == null) {
        throw Status.UNAVAILABLE
            .withDescription(
                "Replay validation state is missing for "
                    + String.join(".", namespacePath)
                    + "."
                    + tableName)
            .asRuntimeException();
      }
      if (replayEntry != null && replayEntry.hasTableId()) {
        throw Status.NOT_FOUND
            .withDescription(
                "Table " + String.join(".", namespacePath) + "." + tableName + " not found")
            .asRuntimeException();
      }
      if (!assertCreateRequested) {
        throw Status.NOT_FOUND
            .withDescription(
                "Table " + String.join(".", namespacePath) + "." + tableName + " not found")
            .asRuntimeException();
      }
      return;
    }

    if (existingResponse == null || !existingResponse.hasTable()) {
      if (replayEntry == null) {
        throw Status.UNAVAILABLE
            .withDescription(
                "Replay validation state is missing for "
                    + String.join(".", namespacePath)
                    + "."
                    + tableName)
            .asRuntimeException();
      }
      if (replayEntry != null && replayEntry.hasTableId()) {
        throw Status.NOT_FOUND
            .withDescription(
                "Table " + String.join(".", namespacePath) + "." + tableName + " not found")
            .asRuntimeException();
      }
      if (!assertCreateRequested) {
        throw Status.NOT_FOUND
            .withDescription(
                "Table " + String.join(".", namespacePath) + "." + tableName + " not found")
            .asRuntimeException();
      }
      return;
    }

    if (replayEntry == null) {
      throw Status.UNAVAILABLE
          .withDescription(
              "Replay validation state is missing for "
                  + String.join(".", namespacePath)
                  + "."
                  + tableName)
          .asRuntimeException();
    }

    Table table = existingResponse.getTable();
    if (replayEntry.hasTableId() && !replayEntry.getTableId().equals(table.getResourceId())) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "Replayed transaction state does not match table identity for "
                  + String.join(".", namespacePath)
                  + "."
                  + tableName)
          .asRuntimeException();
    }
    if (!replayEntry.getMetadataLocation().isBlank()) {
      String currentMetadataLocation = tableMetadataLocation(table);
      if (!replayEntry.getMetadataLocation().equals(currentMetadataLocation)) {
        throw Status.FAILED_PRECONDITION
            .withDescription(
                "Replayed transaction state does not match metadata location for "
                    + String.join(".", namespacePath)
                    + "."
                    + tableName)
            .asRuntimeException();
      }
    }
    if (!replayEntry.getTableUuid().isBlank()) {
      String currentTableUuid = tableUuid(table);
      if (!replayEntry.getTableUuid().equals(currentTableUuid)) {
        throw Status.FAILED_PRECONDITION
            .withDescription(
                "Replayed transaction state does not match table UUID for "
                    + String.join(".", namespacePath)
                    + "."
                    + tableName)
            .asRuntimeException();
      }
    }
  }

  private PlannedTransaction planImportedRegistration(
      String prefix,
      ResourceId catalogId,
      String accountId,
      String txId,
      long txCreatedAtMs,
      String requestHash,
      List<String> namespacePath,
      String tableName,
      TableMetadataImportService.ImportedMetadata imported,
      Map<String, String> mergedProperties,
      boolean overwrite) {
    List<TxChange> out = new ArrayList<>();
    List<PostCommitTask> postCommitTasks = new ArrayList<>();
    ResourceId namespaceId = backend.resolveNamespace(prefix, namespacePath).getResourceId();

    GetTableResponse existingResponse = null;
    ResourceId tableId;
    try {
      tableId = backend.resolveTable(prefix, namespacePath, tableName).getResourceId();
      existingResponse = backend.getTable(tableId);
    } catch (StatusRuntimeException exception) {
      if (exception.getStatus().getCode() != Status.Code.NOT_FOUND) {
        throw exception;
      }
      existingResponse = null;
    }

    Table currentTable;
    long expectedVersion;
    List<Long> existingSnapshotIds;
    if (existingResponse != null && existingResponse.hasTable()) {
      currentTable = existingResponse.getTable();
      expectedVersion =
          existingResponse.hasMeta() ? existingResponse.getMeta().getPointerVersion() : 0L;
      tableId = currentTable.getResourceId();
      existingSnapshotIds =
          backend.listSnapshots(tableId).getSnapshotsList().stream()
              .map(ai.floedb.floecat.catalog.rpc.Snapshot::getSnapshotId)
              .filter(id -> id >= 0L)
              .toList();
    } else {
      if (overwrite) {
        // Register overwrite on a missing table behaves like create.
      }
      tableId =
          atomicCreateTableId(accountId, txId, catalogId, namespaceId, namespacePath, tableName);
      currentTable = newCreateTableStub(tableId, catalogId, namespaceId, tableName, txCreatedAtMs);
      expectedVersion = 0L;
      existingSnapshotIds = List.of();
    }

    IcebergMetadataCommitService.PlannedImportedCommit planned =
        metadataCommitService.planImported(
            currentTable, tableId, imported, mergedProperties, existingSnapshotIds);
    Table updated = planned.table();
    ResourceId scopedTableId = scopeTableIdWithAccount(tableId, accountId);
    ConnectorProvisioningService.ProvisionResult provisioned =
        connectorProvisioningService.resolveOrCreateForCommit(
            accountId,
            txId,
            namespacePath,
            namespaceId,
            catalogId,
            tableName,
            scopedTableId,
            updated);
    updated = provisioned.table();
    if (!provisioned.connectorTxChanges().isEmpty()) {
      out.addAll(provisioned.connectorTxChanges());
    }
    out.add(
        TxChange.newBuilder()
            .setTableId(tableId)
            .setTable(updated)
            .setPrecondition(Precondition.newBuilder().setExpectedVersion(expectedVersion).build())
            .build());
    out.addAll(planned.extraChanges());
    IcebergCommitJournalEntry journal =
        buildCommitJournalEntry(
            txId,
            requestHash,
            scopedTableId,
            namespacePath,
            tableName,
            provisioned.connectorId(),
            planned.effectiveSnapshotIds(),
            planned.removedSnapshotIds(),
            updated,
            txCreatedAtMs);
    out.add(
        TxChange.newBuilder()
            .setTargetPointerKey(
                Keys.tableCommitJournalPointer(accountId, scopedTableId.getId(), txId))
            .setPayload(ByteString.copyFrom(journal.toByteArray()))
            .build());
    postCommitTasks.add(
        new PostCommitTask(
            List.copyOf(journal.getNamespacePathList()),
            journal.getTableName(),
            journal.getTableId(),
            journal.hasConnectorId() ? journal.getConnectorId() : null,
            journal.getMetadataLocation().isBlank() ? null : journal.getMetadataLocation(),
            journal.getTableUuid().isBlank() ? null : journal.getTableUuid(),
            List.copyOf(journal.getAddedSnapshotIdsList()),
            List.copyOf(journal.getRemovedSnapshotIdsList())));
    if (txId != null && !txId.isBlank()) {
      IcebergCommitReplayIndex replayIndex =
          buildCommitReplayIndex(txId, requestHash, postCommitTasks, txCreatedAtMs);
      out.add(
          TxChange.newBuilder()
              .setTargetPointerKey(Keys.tableCommitReplayPointer(accountId, txId))
              .setPayload(ByteString.copyFrom(replayIndex.toByteArray()))
              .build());
    }
    return new PlannedTransaction(List.copyOf(out), List.copyOf(postCommitTasks));
  }

  private record PlannedTransaction(List<TxChange> changes, List<PostCommitTask> postCommitTasks) {}

  private record PostCommitTask(
      List<String> namespacePath,
      String tableName,
      ResourceId tableId,
      ResourceId connectorId,
      String metadataLocation,
      String tableUuid,
      List<Long> addedSnapshotIds,
      List<Long> removedSnapshotIds) {}

  private record NormalizedIdentifier(List<String> namespacePath, String tableName, String key) {}

  private NormalizedIdentifier validateChange(TransactionCommitRequest.TableChange change) {
    if (change == null
        || change.identifier() == null
        || change.identifier().name() == null
        || change.identifier().name().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("table identifier is required")
          .asRuntimeException();
    }
    NormalizedIdentifier identifier = normalizeIdentifier(change);
    if (change.requirements() == null) {
      throw Status.INVALID_ARGUMENT
          .withDescription("requirements are required")
          .asRuntimeException();
    }
    if (change.updates() == null) {
      throw Status.INVALID_ARGUMENT.withDescription("updates are required").asRuntimeException();
    }
    for (Map<String, Object> requirement : change.requirements()) {
      String type = requirement == null ? null : stringValue(requirement.get("type"));
      if (type == null || !SUPPORTED_REQUIREMENTS.contains(type)) {
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "unsupported commit requirement type: " + (type == null ? "<missing>" : type))
            .asRuntimeException();
      }
    }
    for (Map<String, Object> update : change.updates()) {
      String action = update == null ? null : stringValue(update.get("action"));
      if (action == null || !SUPPORTED_UPDATES.contains(action)) {
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "unsupported commit update action: " + (action == null ? "<missing>" : action))
            .asRuntimeException();
      }
      validateSimpleUpdate(update, action);
    }
    return identifier;
  }

  private boolean hasAssertCreate(List<Map<String, Object>> requirements) {
    if (requirements == null) {
      return false;
    }
    for (Map<String, Object> requirement : requirements) {
      if ("assert-create"
          .equals(stringValue(requirement == null ? null : requirement.get("type")))) {
        return true;
      }
    }
    return false;
  }

  private boolean isDeltaReadOnlyCommitBlocked(Table table) {
    if (table == null || tableFormatSupport == null || config == null) {
      return false;
    }
    var deltaCompat = config.deltaCompat();
    if (deltaCompat.isEmpty()) {
      return false;
    }
    return deltaCompat.get().enabled()
        && deltaCompat.get().readOnly()
        && tableFormatSupport.isDelta(table);
  }

  private boolean containsAdvancedUpdates(List<Map<String, Object>> updates) {
    if (updates == null || updates.isEmpty()) {
      return false;
    }
    for (Map<String, Object> update : updates) {
      String action = stringValue(update == null ? null : update.get("action"));
      if (action == null) {
        continue;
      }
      if (!"set-properties".equals(action)
          && !"remove-properties".equals(action)
          && !"set-location".equals(action)) {
        return true;
      }
    }
    return false;
  }

  private boolean containsAdvancedRequirements(List<Map<String, Object>> requirements) {
    if (requirements == null || requirements.isEmpty()) {
      return false;
    }
    for (Map<String, Object> requirement : requirements) {
      String type = stringValue(requirement == null ? null : requirement.get("type"));
      if (type != null && !"assert-create".equals(type)) {
        return true;
      }
    }
    return false;
  }

  private Table applyUpdates(Table current, List<Map<String, Object>> updates) {
    Map<String, String> properties = new LinkedHashMap<>(current.getPropertiesMap());
    if (updates != null) {
      for (Map<String, Object> update : updates) {
        String action = stringValue(update == null ? null : update.get("action"));
        if ("set-properties".equals(action)) {
          Object raw = update.get("updates");
          if (raw instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
              properties.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
            }
          }
        } else if ("remove-properties".equals(action)) {
          Object raw = update.get("removals");
          if (raw instanceof List<?> removals) {
            for (Object removal : removals) {
              properties.remove(String.valueOf(removal));
            }
          }
        } else if ("set-location".equals(action)) {
          String location = stringValue(update.get("location"));
          if (location != null && !location.isBlank()) {
            properties.put("location", location);
            properties.put("storage_location", location);
          }
        }
      }
    }
    return current.toBuilder().putAllProperties(properties).build();
  }

  private void validateSimpleUpdate(Map<String, Object> update, String action) {
    if ("set-properties".equals(action)) {
      Object raw = update.get("updates");
      if (!(raw instanceof Map<?, ?> map)) {
        throw Status.INVALID_ARGUMENT
            .withDescription("set-properties requires updates")
            .asRuntimeException();
      }
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        if (entry.getKey() == null || entry.getValue() == null) {
          throw Status.INVALID_ARGUMENT
              .withDescription("set-properties requires non-null keys and values")
              .asRuntimeException();
        }
      }
    } else if ("remove-properties".equals(action)) {
      Object raw = update.get("removals");
      if (!(raw instanceof List<?> removals)) {
        throw Status.INVALID_ARGUMENT
            .withDescription("remove-properties requires removals")
            .asRuntimeException();
      }
      for (Object removal : removals) {
        if (removal == null) {
          throw Status.INVALID_ARGUMENT
              .withDescription("remove-properties requires non-null removals")
              .asRuntimeException();
        }
      }
    } else if ("set-location".equals(action)) {
      String location = stringValue(update.get("location"));
      if (location == null || location.isBlank()) {
        throw Status.INVALID_ARGUMENT
            .withDescription("set-location requires location")
            .asRuntimeException();
      }
    }
  }

  private NormalizedIdentifier normalizeIdentifier(TransactionCommitRequest.TableChange change) {
    List<String> namespacePath = new ArrayList<>();
    if (change.identifier().namespace() != null) {
      for (String segment : change.identifier().namespace()) {
        String normalized = segment == null ? null : segment.trim();
        if (normalized == null || normalized.isBlank()) {
          throw Status.INVALID_ARGUMENT
              .withDescription("namespace segments must be non-blank")
              .asRuntimeException();
        }
        namespacePath.add(normalized);
      }
    }
    String tableName = change.identifier().name().trim();
    if (tableName.isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("table identifier is required")
          .asRuntimeException();
    }
    String key = (namespacePath.isEmpty() ? "" : String.join(".", namespacePath) + ".") + tableName;
    return new NormalizedIdentifier(List.copyOf(namespacePath), tableName, key);
  }

  private ResourceId atomicCreateTableId(
      String accountId,
      String txId,
      ResourceId catalogId,
      ResourceId namespaceId,
      List<String> namespacePath,
      String tableName) {
    String seed =
        (txId == null ? "" : txId)
            + "|"
            + (catalogId == null ? "<catalog>" : catalogId.getId())
            + "|"
            + (namespacePath == null || namespacePath.isEmpty()
                ? (namespaceId == null ? "<namespace>" : namespaceId.getId())
                : String.join(".", namespacePath))
            + "|"
            + tableName;
    UUID deterministicId = UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8));
    return ResourceId.newBuilder()
        .setAccountId(accountId == null ? "" : accountId)
        .setId("tbl-" + deterministicId)
        .setKind(ResourceKind.RK_TABLE)
        .build();
  }

  private Table newCreateTableStub(
      ResourceId tableId,
      ResourceId catalogId,
      ResourceId namespaceId,
      String tableName,
      long createdAtMs) {
    return Table.newBuilder()
        .setResourceId(tableId)
        .setCatalogId(catalogId)
        .setNamespaceId(namespaceId)
        .setDisplayName(tableName)
        .setCreatedAt(Timestamps.fromMillis(Math.max(0L, createdAtMs)))
        .setUpstream(UpstreamRef.newBuilder().setFormat(TableFormat.TF_ICEBERG).build())
        .build();
  }

  private String firstDuplicateIdentifier(List<TransactionCommitRequest.TableChange> changes) {
    Set<String> seen = new LinkedHashSet<>();
    for (TransactionCommitRequest.TableChange change : changes) {
      if (change == null || change.identifier() == null || change.identifier().name() == null) {
        continue;
      }
      String key = normalizeIdentifier(change).key();
      if (!seen.add(key)) {
        return key;
      }
    }
    return null;
  }

  private String requestHash(List<TransactionCommitRequest.TableChange> changes) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(canonicalize(changes).getBytes(StandardCharsets.UTF_8));
      return Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest());
    } catch (Exception e) {
      throw new IllegalStateException("Unable to hash transaction request", e);
    }
  }

  private String requestHash(String canonicalValue) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(canonicalValue.getBytes(StandardCharsets.UTF_8));
      return Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest());
    } catch (Exception e) {
      throw new IllegalStateException("Unable to hash transaction request", e);
    }
  }

  private String canonicalRegisterRequest(
      List<String> namespacePath,
      String tableName,
      TableMetadataImportService.ImportedMetadata imported,
      Map<String, String> mergedProperties,
      boolean overwrite) {
    Map<String, Object> canonical = new LinkedHashMap<>();
    canonical.put("namespace", namespacePath);
    canonical.put("table", tableName);
    canonical.put("overwrite", overwrite);
    canonical.put("properties", mergedProperties == null ? Map.of() : mergedProperties);
    canonical.put(
        "metadata-location",
        imported == null || imported.icebergMetadata() == null
            ? null
            : imported.icebergMetadata().getMetadataLocation());
    canonical.put(
        "current-snapshot-id",
        imported == null || imported.currentSnapshot() == null
            ? null
            : imported.currentSnapshot().snapshotId());
    canonical.put(
        "snapshot-ids",
        imported == null || imported.snapshots() == null
            ? List.of()
            : imported.snapshots().stream()
                .map(TableMetadataImportService.ImportedSnapshot::snapshotId)
                .toList());
    return canonicalize(canonical);
  }

  private String canonicalize(Object value) {
    if (value == null) {
      return "null";
    }
    if (value instanceof TransactionCommitRequest.TableChange change) {
      return "{identifier:"
          + canonicalize(change.identifier())
          + ",requirements:"
          + canonicalize(change.requirements())
          + ",updates:"
          + canonicalize(change.updates())
          + "}";
    }
    if (value
        instanceof
        ai.floedb.floecat.gateway.iceberg.minimal.api.dto.TableIdentifierDto identifier) {
      return "{namespace:"
          + canonicalize(identifier.namespace())
          + ",name:"
          + canonicalize(identifier.name())
          + "}";
    }
    if (value instanceof Map<?, ?> map) {
      Map<String, Object> sorted = new TreeMap<>();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        sorted.put(String.valueOf(entry.getKey()), entry.getValue());
      }
      List<String> parts = new ArrayList<>();
      for (Map.Entry<String, Object> entry : sorted.entrySet()) {
        parts.add(entry.getKey() + ":" + canonicalize(entry.getValue()));
      }
      return "{" + String.join(",", parts) + "}";
    }
    if (value instanceof List<?> list) {
      List<String> parts = new ArrayList<>();
      for (Object item : list) {
        parts.add(canonicalize(item));
      }
      return "[" + String.join(",", parts) + "]";
    }
    return String.valueOf(value);
  }

  private Response mapGrpcFailure(StatusRuntimeException exception) {
    return switch (exception.getStatus().getCode()) {
      case NOT_FOUND -> IcebergErrorResponses.noSuchTable(descriptionOrCode(exception));
      case INVALID_ARGUMENT -> IcebergErrorResponses.validation(descriptionOrCode(exception));
      case ALREADY_EXISTS, ABORTED, FAILED_PRECONDITION ->
          IcebergErrorResponses.conflict(descriptionOrCode(exception));
      case UNAVAILABLE -> IcebergErrorResponses.commitStateUnknown(descriptionOrCode(exception));
      default -> IcebergErrorResponses.grpc(exception);
    };
  }

  private String descriptionOrCode(StatusRuntimeException exception) {
    return exception.getStatus().getDescription() == null
        ? exception.getStatus().getCode().name()
        : exception.getStatus().getDescription();
  }

  private boolean isMissingReplayState(StatusRuntimeException exception) {
    return exception != null
        && exception.getStatus().getCode() == Status.Code.UNAVAILABLE
        && descriptionOrCode(exception).startsWith("Replay validation state is missing");
  }

  private void abortQuietly(String txId, String reason) {
    try {
      backend.abortTransaction(txId, reason);
    } catch (RuntimeException exception) {
      LOG.debugf(exception, "Best-effort abort failed txId=%s reason=%s", txId, reason);
    }
  }

  private String firstNonBlank(String first, String second) {
    return first != null && !first.isBlank() ? first : second;
  }

  private String requestAccountId() {
    if (accountContext == null) {
      return null;
    }
    String accountId = accountContext.getAccountId();
    return accountId == null || accountId.isBlank() ? null : accountId;
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

  private String explicitIdempotencyKey(String idempotencyKey) {
    return idempotencyKey != null && !idempotencyKey.isBlank() ? idempotencyKey : null;
  }

  private java.time.Duration transactionLifetime() {
    return config == null ? null : config.idempotencyKeyLifetime();
  }

  private String suffixKey(String base, String suffix) {
    if (base == null || base.isBlank()) {
      return null;
    }
    return base + ":" + suffix;
  }

  private String stringValue(Object value) {
    return value == null ? null : String.valueOf(value);
  }

  private void processPostCommitTasks(String txId, List<PostCommitTask> items) {
    if (items == null || items.isEmpty()) {
      return;
    }
    try {
      for (PostCommitTask item : items) {
        if (item == null) {
          continue;
        }
        sideEffectService.pruneRemovedSnapshots(item.tableId(), item.removedSnapshotIds());
        sideEffectService.schedulePostCommitStatsSync(
            item.connectorId(), item.namespacePath(), item.tableName(), item.addedSnapshotIds());
      }
    } catch (RuntimeException e) {
      LOG.warnf(e, "Best-effort post-commit side effects failed for tx=%s", txId);
    }
  }

  private List<PostCommitTask> loadReplayPostCommitTasks(
      String accountId, String txId, String requestHash) {
    if (accountId == null
        || accountId.isBlank()
        || txId == null
        || txId.isBlank()
        || requestHash == null
        || requestHash.isBlank()) {
      return List.of();
    }
    try {
      IcebergCommitReplayIndex replayIndex =
          commitJournalService.getReplayIndex(accountId, txId).orElse(null);
      if (replayIndex == null || !requestHash.equals(replayIndex.getRequestHash())) {
        return List.of();
      }
      List<PostCommitTask> out = new ArrayList<>();
      for (IcebergCommitJournalEntry journal : replayIndex.getEntriesList()) {
        if (journal == null || !journal.hasTableId()) {
          continue;
        }
        out.add(
            new PostCommitTask(
                List.copyOf(journal.getNamespacePathList()),
                journal.getTableName(),
                journal.getTableId(),
                journal.hasConnectorId() ? journal.getConnectorId() : null,
                journal.getMetadataLocation().isBlank() ? null : journal.getMetadataLocation(),
                journal.getTableUuid().isBlank() ? null : journal.getTableUuid(),
                List.copyOf(journal.getAddedSnapshotIdsList()),
                List.copyOf(journal.getRemovedSnapshotIdsList())));
      }
      return List.copyOf(out);
    } catch (RuntimeException e) {
      LOG.warnf(e, "Skipping replay side effects for tx=%s accountId=%s", txId, accountId);
      return List.of();
    }
  }

  private List<PostCommitTask> loadReplayPostCommitTasks(
      String prefix,
      ResourceId catalogId,
      String replayAccountId,
      String txId,
      String requestHash,
      List<TransactionCommitRequest.TableChange> tableChanges) {
    List<PostCommitTask> tasks = loadReplayPostCommitTasks(replayAccountId, txId, requestHash);
    if (!tasks.isEmpty()) {
      return tasks;
    }
    return loadReplayPostCommitTasksFromJournals(
        prefix, catalogId, replayAccountId, txId, requestHash, tableChanges);
  }

  private List<PostCommitTask> loadImportedReplayPostCommitTasks(
      String prefix,
      ResourceId catalogId,
      String replayAccountId,
      String txId,
      String requestHash,
      List<String> namespacePath,
      String tableName) {
    List<PostCommitTask> tasks = loadReplayPostCommitTasks(replayAccountId, txId, requestHash);
    if (!tasks.isEmpty()) {
      return tasks;
    }
    IcebergCommitJournalEntry journal =
        loadImportedReplayJournal(
            prefix, catalogId, replayAccountId, txId, requestHash, namespacePath, tableName);
    if (journal == null || !journal.hasTableId()) {
      return List.of();
    }
    return List.of(postCommitTask(journal));
  }

  private Map<String, IcebergCommitJournalEntry> loadReplayValidationEntries(
      String accountId, String txId, String requestHash) {
    if (accountId == null
        || accountId.isBlank()
        || txId == null
        || txId.isBlank()
        || requestHash == null
        || requestHash.isBlank()) {
      return Map.of();
    }
    try {
      IcebergCommitReplayIndex replayIndex =
          commitJournalService.getReplayIndex(accountId, txId).orElse(null);
      if (replayIndex == null || !requestHash.equals(replayIndex.getRequestHash())) {
        return Map.of();
      }
      Map<String, IcebergCommitJournalEntry> out = new LinkedHashMap<>();
      for (IcebergCommitJournalEntry entry : replayIndex.getEntriesList()) {
        if (entry == null || entry.getTableName().isBlank()) {
          continue;
        }
        String key =
            (entry.getNamespacePathCount() == 0
                    ? ""
                    : String.join(".", entry.getNamespacePathList()) + ".")
                + entry.getTableName();
        out.put(key, entry);
      }
      return Map.copyOf(out);
    } catch (RuntimeException e) {
      LOG.warnf(e, "Skipping replay validation entries for tx=%s accountId=%s", txId, accountId);
      return Map.of();
    }
  }

  private Map<String, IcebergCommitJournalEntry> loadReplayValidationEntries(
      String prefix,
      ResourceId catalogId,
      String replayAccountId,
      String txId,
      String requestHash,
      List<TransactionCommitRequest.TableChange> tableChanges) {
    Map<String, IcebergCommitJournalEntry> replayEntries =
        loadReplayValidationEntries(replayAccountId, txId, requestHash);
    if (!replayEntries.isEmpty()) {
      return replayEntries;
    }
    return loadReplayValidationEntriesFromJournals(
        prefix, catalogId, replayAccountId, txId, requestHash, tableChanges);
  }

  private Map<String, IcebergCommitJournalEntry> loadImportedReplayValidationEntries(
      String prefix,
      ResourceId catalogId,
      String replayAccountId,
      String txId,
      String requestHash,
      List<String> namespacePath,
      String tableName) {
    Map<String, IcebergCommitJournalEntry> replayEntries =
        loadReplayValidationEntries(replayAccountId, txId, requestHash);
    if (!replayEntries.isEmpty()) {
      return replayEntries;
    }
    IcebergCommitJournalEntry journal =
        loadImportedReplayJournal(
            prefix, catalogId, replayAccountId, txId, requestHash, namespacePath, tableName);
    if (journal == null || journal.getTableName().isBlank()) {
      return Map.of();
    }
    String key =
        (journal.getNamespacePathCount() == 0
                ? ""
                : String.join(".", journal.getNamespacePathList()) + ".")
            + journal.getTableName();
    return Map.of(key, journal);
  }

  private Map<String, IcebergCommitJournalEntry> loadReplayValidationEntriesFromJournals(
      String prefix,
      ResourceId catalogId,
      String replayAccountId,
      String txId,
      String requestHash,
      List<TransactionCommitRequest.TableChange> tableChanges) {
    if (tableChanges == null || tableChanges.isEmpty()) {
      return Map.of();
    }
    Map<String, IcebergCommitJournalEntry> out = new LinkedHashMap<>();
    for (TransactionCommitRequest.TableChange change : tableChanges) {
      NormalizedIdentifier identifier = validateChange(change);
      IcebergCommitJournalEntry journal =
          loadReplayJournal(
              prefix, catalogId, replayAccountId, txId, requestHash, change, identifier);
      if (journal != null) {
        out.put(identifier.key(), journal);
      }
    }
    return Map.copyOf(out);
  }

  private List<PostCommitTask> loadReplayPostCommitTasksFromJournals(
      String prefix,
      ResourceId catalogId,
      String replayAccountId,
      String txId,
      String requestHash,
      List<TransactionCommitRequest.TableChange> tableChanges) {
    if (tableChanges == null || tableChanges.isEmpty()) {
      return List.of();
    }
    List<PostCommitTask> out = new ArrayList<>();
    for (TransactionCommitRequest.TableChange change : tableChanges) {
      NormalizedIdentifier identifier = validateChange(change);
      IcebergCommitJournalEntry journal =
          loadReplayJournal(
              prefix, catalogId, replayAccountId, txId, requestHash, change, identifier);
      if (journal != null && journal.hasTableId()) {
        out.add(postCommitTask(journal));
      }
    }
    return List.copyOf(out);
  }

  private IcebergCommitJournalEntry loadImportedReplayJournal(
      String prefix,
      ResourceId catalogId,
      String accountId,
      String txId,
      String requestHash,
      List<String> namespacePath,
      String tableName) {
    ResourceId namespaceId = backend.resolveNamespace(prefix, namespacePath).getResourceId();
    ResourceId tableId;
    try {
      tableId = backend.resolveTable(prefix, namespacePath, tableName).getResourceId();
    } catch (StatusRuntimeException exception) {
      if (exception.getStatus().getCode() != Status.Code.NOT_FOUND) {
        throw exception;
      }
      tableId =
          atomicCreateTableId(accountId, txId, catalogId, namespaceId, namespacePath, tableName);
    }
    return loadReplayJournal(
        accountId, scopeTableIdWithAccount(tableId, accountId), txId, requestHash);
  }

  private IcebergCommitJournalEntry loadReplayJournal(
      String prefix,
      ResourceId catalogId,
      String accountId,
      String txId,
      String requestHash,
      TransactionCommitRequest.TableChange change,
      NormalizedIdentifier identifier) {
    ResourceId namespaceId =
        backend.resolveNamespace(prefix, identifier.namespacePath()).getResourceId();
    ResourceId tableId;
    try {
      tableId =
          backend
              .resolveTable(prefix, identifier.namespacePath(), identifier.tableName())
              .getResourceId();
    } catch (StatusRuntimeException exception) {
      if (exception.getStatus().getCode() != Status.Code.NOT_FOUND
          || !hasAssertCreate(change.requirements())) {
        return null;
      }
      tableId =
          atomicCreateTableId(
              accountId,
              txId,
              catalogId,
              namespaceId,
              identifier.namespacePath(),
              identifier.tableName());
    }
    return loadReplayJournal(
        accountId, scopeTableIdWithAccount(tableId, accountId), txId, requestHash);
  }

  private IcebergCommitJournalEntry loadReplayJournal(
      String accountId, ResourceId tableId, String txId, String requestHash) {
    if (accountId == null
        || accountId.isBlank()
        || tableId == null
        || tableId.getId().isBlank()
        || txId == null
        || txId.isBlank()
        || requestHash == null
        || requestHash.isBlank()) {
      return null;
    }
    try {
      IcebergCommitJournalEntry journal =
          commitJournalService.get(accountId, tableId.getId(), txId).orElse(null);
      if (journal == null || !requestHash.equals(journal.getRequestHash())) {
        return null;
      }
      return journal;
    } catch (RuntimeException e) {
      LOG.warnf(
          e,
          "Skipping replay journal fallback for tx=%s accountId=%s tableId=%s",
          txId,
          accountId,
          tableId.getId());
      return null;
    }
  }

  private PostCommitTask postCommitTask(IcebergCommitJournalEntry journal) {
    return new PostCommitTask(
        List.copyOf(journal.getNamespacePathList()),
        journal.getTableName(),
        journal.getTableId(),
        journal.hasConnectorId() ? journal.getConnectorId() : null,
        journal.getMetadataLocation().isBlank() ? null : journal.getMetadataLocation(),
        journal.getTableUuid().isBlank() ? null : journal.getTableUuid(),
        List.copyOf(journal.getAddedSnapshotIdsList()),
        List.copyOf(journal.getRemovedSnapshotIdsList()));
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
      Table table,
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
    String metadataLocation = tableMetadataLocation(table);
    if (metadataLocation != null && !metadataLocation.isBlank()) {
      builder.setMetadataLocation(metadataLocation);
    }
    String tableUuid = tableUuid(table);
    if (tableUuid != null && !tableUuid.isBlank()) {
      builder.setTableUuid(tableUuid);
    }
    return builder.build();
  }

  private IcebergCommitReplayIndex buildCommitReplayIndex(
      String txId, String requestHash, List<PostCommitTask> tasks, long createdAtMs) {
    IcebergCommitReplayIndex.Builder builder =
        IcebergCommitReplayIndex.newBuilder()
            .setVersion(COMMIT_REPLAY_INDEX_VERSION)
            .setTxId(txId == null ? "" : txId)
            .setRequestHash(requestHash == null ? "" : requestHash);
    for (PostCommitTask task : tasks) {
      if (task == null || task.tableId() == null) {
        continue;
      }
      IcebergCommitJournalEntry.Builder entry =
          IcebergCommitJournalEntry.newBuilder()
              .setVersion(COMMIT_JOURNAL_VERSION)
              .setTxId(txId == null ? "" : txId)
              .setRequestHash(requestHash == null ? "" : requestHash)
              .setTableId(task.tableId())
              .setCreatedAtMs(Math.max(0L, createdAtMs));
      if (task.namespacePath() != null && !task.namespacePath().isEmpty()) {
        entry.addAllNamespacePath(task.namespacePath());
      }
      if (task.tableName() != null && !task.tableName().isBlank()) {
        entry.setTableName(task.tableName());
      }
      if (task.connectorId() != null && !task.connectorId().getId().isBlank()) {
        entry.setConnectorId(task.connectorId());
      }
      if (task.metadataLocation() != null && !task.metadataLocation().isBlank()) {
        entry.setMetadataLocation(task.metadataLocation());
      }
      if (task.tableUuid() != null && !task.tableUuid().isBlank()) {
        entry.setTableUuid(task.tableUuid());
      }
      if (task.addedSnapshotIds() != null && !task.addedSnapshotIds().isEmpty()) {
        entry.addAllAddedSnapshotIds(task.addedSnapshotIds());
      }
      if (task.removedSnapshotIds() != null && !task.removedSnapshotIds().isEmpty()) {
        entry.addAllRemovedSnapshotIds(task.removedSnapshotIds());
      }
      builder.addEntries(entry);
    }
    return builder.build();
  }

  private List<Long> addedSnapshotIds(List<Map<String, Object>> updates) {
    List<Long> out = new ArrayList<>();
    if (updates == null) {
      return out;
    }
    for (Map<String, Object> update : updates) {
      if (!"add-snapshot".equals(stringValue(update == null ? null : update.get("action")))) {
        continue;
      }
      Object snapshot = update.get("snapshot");
      if (snapshot instanceof Map<?, ?> snapshotMap) {
        Long value = longValue(snapshotMap.get("snapshot-id"));
        if (value != null) {
          out.add(value);
        }
      }
    }
    return out;
  }

  private List<Long> removedSnapshotIds(List<Map<String, Object>> updates) {
    List<Long> out = new ArrayList<>();
    if (updates == null) {
      return out;
    }
    for (Map<String, Object> update : updates) {
      if (!"remove-snapshots".equals(stringValue(update == null ? null : update.get("action")))) {
        continue;
      }
      Object ids = update.get("snapshot-ids");
      if (ids instanceof List<?> list) {
        for (Object value : list) {
          Long longValue = longValue(value);
          if (longValue != null) {
            out.add(longValue);
          }
        }
      }
    }
    return out;
  }

  private Long longValue(Object value) {
    if (value instanceof Number number) {
      return number.longValue();
    }
    if (value == null) {
      return null;
    }
    try {
      return Long.parseLong(String.valueOf(value));
    } catch (NumberFormatException ignored) {
      return null;
    }
  }

  private String tableMetadataLocation(Table table) {
    return table == null ? null : table.getPropertiesMap().get("metadata-location");
  }

  private String tableUuid(Table table) {
    return table == null ? null : table.getPropertiesMap().get("table-uuid");
  }

  private Response validateNullSnapshotRefRequirements(
      Table table, List<Map<String, Object>> requirements) {
    if (requirements == null || requirements.isEmpty()) {
      return null;
    }
    for (Map<String, Object> requirement : requirements) {
      if (requirement == null) {
        continue;
      }
      String type = stringValue(requirement.get("type"));
      if (!"assert-ref-snapshot-id".equals(type) || !requirement.containsKey("snapshot-id")) {
        continue;
      }
      if (requirement.get("snapshot-id") != null) {
        continue;
      }
      String refName = stringValue(requirement.get("ref"));
      if (refName == null || refName.isBlank()) {
        return IcebergErrorResponses.validation("assert-ref-snapshot-id requires ref");
      }
      if (hasSnapshotRef(table, refName)) {
        return IcebergErrorResponses.conflict("assert-ref-snapshot-id failed for ref " + refName);
      }
    }
    return null;
  }

  private boolean hasSnapshotRef(Table table, String refName) {
    if (table == null || refName == null || refName.isBlank()) {
      return false;
    }
    if ("main".equals(refName)) {
      Long currentSnapshotId = longValue(table.getPropertiesMap().get("current-snapshot-id"));
      if (currentSnapshotId != null && currentSnapshotId >= 0L) {
        return true;
      }
    }
    String encodedRefs = table.getPropertiesMap().get("metadata.refs");
    if (encodedRefs == null || encodedRefs.isBlank()) {
      return false;
    }
    return encodedRefs.contains(refName + "=") || encodedRefs.contains("\"" + refName + "\"");
  }
}
