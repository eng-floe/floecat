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
import ai.floedb.floecat.gateway.iceberg.minimal.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergCommitJournalEntry;
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
          "add-sort-order",
          "set-default-sort-order",
          "add-snapshot",
          "set-statistics",
          "remove-statistics",
          "set-partition-statistics",
          "remove-partition-statistics",
          "remove-snapshots",
          "set-snapshot-ref",
          "remove-snapshot-ref");

  private final TransactionBackend backend;
  private final IcebergMetadataCommitService metadataCommitService;
  private final ConnectorProvisioningService connectorProvisioningService;
  private final TableCommitJournalService commitJournalService;
  private final TableCommitSideEffectService sideEffectService;

  @Inject
  public TransactionCommitService(
      TransactionBackend backend,
      IcebergMetadataCommitService metadataCommitService,
      ConnectorProvisioningService connectorProvisioningService,
      TableCommitJournalService commitJournalService,
      TableCommitSideEffectService sideEffectService) {
    this.backend = backend;
    this.metadataCommitService = metadataCommitService;
    this.connectorProvisioningService = connectorProvisioningService;
    this.commitJournalService = commitJournalService;
    this.sideEffectService = sideEffectService;
  }

  public Response commit(String prefix, String idempotencyKey, TransactionCommitRequest request) {
    if (request == null || request.tableChanges() == null || request.tableChanges().isEmpty()) {
      return IcebergErrorResponses.validation("table-changes are required");
    }
    String duplicate = firstDuplicateIdentifier(request.tableChanges());
    if (duplicate != null) {
      return IcebergErrorResponses.validation(
          "duplicate table identifier in table-changes: " + duplicate);
    }

    String requestHash = requestHash(request.tableChanges());
    String beginKey = firstNonBlank(idempotencyKey, "req:" + requestHash);
    String txId = null;
    try {
      var begun = backend.beginTransaction(beginKey, requestHash);
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
        abortQuietly(txId, "transaction request-hash mismatch");
        return IcebergErrorResponses.conflict(
            "transaction request does not match existing transaction payload");
      }
      if (state == TransactionState.TS_APPLIED) {
        return Response.noContent().build();
      }
      if (state == TransactionState.TS_APPLY_FAILED_CONFLICT) {
        return IcebergErrorResponses.conflict("transaction commit failed");
      }

      PlannedTransaction planned =
          planChanges(prefix, txId, txCreatedAtMs, requestHash, request.tableChanges());
      List<TxChange> changes = planned.changes();
      LOG.infof(
          "Prepared transaction plan prefix=%s txId=%s tableChanges=%d txChanges=%d state=%s",
          prefix, txId, request.tableChanges().size(), changes.size(), state);
      if (state == TransactionState.TS_OPEN || state == TransactionState.TS_UNSPECIFIED) {
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

  private PlannedTransaction planChanges(
      String prefix,
      String txId,
      long txCreatedAtMs,
      String requestHash,
      List<TransactionCommitRequest.TableChange> tableChanges) {
    List<TxChange> out = new ArrayList<>();
    List<PostCommitTask> postCommitTasks = new ArrayList<>();
    for (TransactionCommitRequest.TableChange change : tableChanges) {
      validateChange(change);
      List<String> namespacePath =
          change.identifier().namespace() == null
              ? List.of()
              : List.copyOf(change.identifier().namespace());
      String tableName = change.identifier().name().trim();
      ResourceId catalogId = backend.resolveCatalog(prefix).getResourceId();
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
        String accountId =
            !namespaceId.getAccountId().isBlank()
                ? namespaceId.getAccountId()
                : catalogId.getAccountId();
        tableId =
            atomicCreateTableId(accountId, txId, catalogId, namespaceId, namespacePath, tableName);
        currentTable = newCreateTableStub(tableId, catalogId, namespaceId, tableName);
        expectedVersion = 0L;
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
      String accountId =
          firstNonBlank(
              tableId.getAccountId(),
              firstNonBlank(namespaceId.getAccountId(), catalogId.getAccountId()));
      ConnectorProvisioningService.ProvisionResult provisioned =
          connectorProvisioningService.resolveOrCreateForCommit(
              accountId, txId, namespacePath, namespaceId, catalogId, tableName, tableId, updated);
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
              tableId,
              namespacePath,
              tableName,
              provisioned.connectorId(),
              addedSnapshotIds(updates),
              removedSnapshotIds(updates),
              updated,
              txCreatedAtMs);
      out.add(
          TxChange.newBuilder()
              .setTargetPointerKey(Keys.tableCommitJournalPointer(accountId, tableId.getId(), txId))
              .setPayload(ByteString.copyFrom(journal.toByteArray()))
              .build());
      postCommitTasks.add(
          new PostCommitTask(
              List.copyOf(journal.getNamespacePathList()),
              journal.getTableName(),
              journal.getTableId(),
              journal.hasConnectorId() ? journal.getConnectorId() : null,
              List.copyOf(journal.getAddedSnapshotIdsList()),
              List.copyOf(journal.getRemovedSnapshotIdsList())));
    }
    return new PlannedTransaction(List.copyOf(out), List.copyOf(postCommitTasks));
  }

  private record PlannedTransaction(List<TxChange> changes, List<PostCommitTask> postCommitTasks) {}

  private record PostCommitTask(
      List<String> namespacePath,
      String tableName,
      ResourceId tableId,
      ResourceId connectorId,
      List<Long> addedSnapshotIds,
      List<Long> removedSnapshotIds) {}

  private void validateChange(TransactionCommitRequest.TableChange change) {
    if (change == null
        || change.identifier() == null
        || change.identifier().name() == null
        || change.identifier().name().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("table identifier is required")
          .asRuntimeException();
    }
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
    }
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
              if (entry.getKey() != null && entry.getValue() != null) {
                properties.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
              }
            }
          }
        } else if ("remove-properties".equals(action)) {
          Object raw = update.get("removals");
          if (raw instanceof List<?> removals) {
            for (Object removal : removals) {
              if (removal != null) {
                properties.remove(String.valueOf(removal));
              }
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
      ResourceId tableId, ResourceId catalogId, ResourceId namespaceId, String tableName) {
    return Table.newBuilder()
        .setResourceId(tableId)
        .setCatalogId(catalogId)
        .setNamespaceId(namespaceId)
        .setDisplayName(tableName)
        .setCreatedAt(Timestamps.fromMillis(System.currentTimeMillis()))
        .setUpstream(UpstreamRef.newBuilder().setFormat(TableFormat.TF_ICEBERG).build())
        .build();
  }

  private String firstDuplicateIdentifier(List<TransactionCommitRequest.TableChange> changes) {
    Set<String> seen = new LinkedHashSet<>();
    for (TransactionCommitRequest.TableChange change : changes) {
      String key =
          String.join(
                  ".",
                  change.identifier().namespace() == null
                      ? List.of()
                      : change.identifier().namespace())
              + "."
              + change.identifier().name();
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

  private void abortQuietly(String txId, String reason) {
    try {
      backend.abortTransaction(txId, reason);
    } catch (RuntimeException ignored) {
    }
  }

  private String firstNonBlank(String first, String second) {
    return first != null && !first.isBlank() ? first : second;
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
}
