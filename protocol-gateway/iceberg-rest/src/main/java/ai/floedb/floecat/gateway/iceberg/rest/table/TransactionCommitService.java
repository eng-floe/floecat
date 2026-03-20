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

import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.firstNonBlank;

import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.CatalogRef;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.NamespaceRef;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.ResourceResolver;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.compat.TableFormatSupport;
import ai.floedb.floecat.gateway.iceberg.rest.support.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.support.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.support.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.support.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.support.MetadataLocationUtil;
import ai.floedb.floecat.transaction.rpc.GetTransactionRequest;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TransactionCommitService {
  private static final String TX_REQUEST_HASH_PROPERTY = "iceberg.commit.request-hash";
  private static final List<Map<String, Object>> STAGE_CREATE_REQUIREMENTS =
      CommitUpdateInspector.assertCreateRequirements();
  private static final Logger LOG = Logger.getLogger(TransactionCommitService.class);
  @Inject IcebergGatewayConfig config;
  @Inject AccountContext accountContext;
  @Inject ResourceResolver resourceResolver;
  @Inject TableGatewaySupport tableGatewaySupport;
  @Inject TableFormatSupport tableFormatSupport;
  @Inject IcebergMetadataService icebergMetadataService;
  @Inject StagedTableRepository stagedTableRepository;
  @Inject TableLifecycleService tableLifecycleService;
  @Inject TransactionApplyService transactionApplyService;
  @Inject TransactionPlanningService transactionPlanningService;
  @Inject TableCommitResponseService tableCommitResponseService;
  @Inject GrpcServiceFacade grpcClient;

  public record CommitCommand(
      String prefix,
      String namespace,
      List<String> namespacePath,
      String table,
      String catalogName,
      ResourceId catalogId,
      ResourceId namespaceId,
      String idempotencyKey,
      String stageId,
      String transactionId,
      TableRequests.Commit request,
      TableGatewaySupport tableSupport) {}

  public Response createTable(
      NamespaceRef namespaceContext,
      String accessDelegationMode,
      String idempotencyKey,
      String transactionId,
      TableRequests.Create request,
      TableGatewaySupport tableSupport) {
    return tableLifecycleService.createTable(
        namespaceContext,
        accessDelegationMode,
        idempotencyKey,
        transactionId,
        request,
        tableSupport);
  }

  public Response registerTable(
      NamespaceRef namespaceContext,
      String idempotencyKey,
      TableRequests.Register req,
      TableGatewaySupport tableSupport) {
    return tableLifecycleService.registerTable(namespaceContext, idempotencyKey, req, tableSupport);
  }

  public Response commitTable(CommitCommand command) {
    if (command == null) {
      return IcebergErrorResponses.validation("Request body is required");
    }
    TableRequests.Commit req = command.request();
    if (req == null) {
      return IcebergErrorResponses.validation("Request body is required");
    }

    ai.floedb.floecat.catalog.rpc.Table preCommitTable = loadCurrentTable(command);
    if (isDeltaReadOnlyCommitBlocked(preCommitTable)) {
      return IcebergErrorResponses.conflict(
          "Delta compatibility mode is read-only; table commits are disabled for Delta tables");
    }

    Optional<StagedTableEntry> stagedEntryOpt = resolveStagedEntry(command);
    if (stagedEntryOpt.isPresent() && stagedEntryOpt.get().state() == StageState.ABORTED) {
      return IcebergErrorResponses.conflict(
          "stage " + stagedEntryOpt.get().key().stageId() + " was aborted");
    }
    TableRequests.Commit effectiveReq =
        mergeStagedCreateIntoCommit(command, req, stagedEntryOpt.orElse(null), preCommitTable);

    TransactionCommitRequest txRequest =
        new TransactionCommitRequest(
            List.of(
                new TransactionCommitRequest.TableChange(
                    new TableIdentifierDto(command.namespacePath(), command.table()),
                    effectiveReq.requirements(),
                    effectiveReq.updates())));

    Response txResponse =
        commit(command.prefix(), command.idempotencyKey(), txRequest, command.tableSupport());
    if (txResponse == null
        || txResponse.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
      return txResponse;
    }

    Response response =
        tableCommitResponseService.buildCommitResponse(
            command, effectiveReq, stagedEntryOpt.orElse(null));
    if (response != null && response.getStatus() == Response.Status.OK.getStatusCode()) {
      stagedEntryOpt.ifPresent(entry -> stagedTableRepository.deleteStage(entry.key()));
    }
    return response;
  }

  public Response commitCreate(
      String prefix,
      String idempotencyKey,
      List<String> namespacePath,
      String tableName,
      ResourceId catalogId,
      ResourceId namespaceId,
      TableRequests.Create request,
      TableGatewaySupport tableSupport) {
    return commitInternal(
        prefix,
        idempotencyKey,
        buildCreateRequest(namespacePath, tableName, catalogId, namespaceId, request, tableSupport),
        tableSupport,
        false);
  }

  TransactionCommitRequest buildCreateRequest(
      List<String> namespacePath,
      String tableName,
      ResourceId catalogId,
      ResourceId namespaceId,
      TableRequests.Create request,
      TableGatewaySupport tableSupport) {
    TableSpec spec;
    try {
      spec = tableSupport.buildCreateSpec(catalogId, namespaceId, tableName, request).build();
    } catch (Exception e) {
      throw e instanceof IllegalArgumentException
          ? (IllegalArgumentException) e
          : new IllegalArgumentException(e.getMessage(), e);
    }

    List<Map<String, Object>> updates = buildCreateUpdates(request, spec);
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(namespacePath, tableName),
                CommitUpdateInspector.assertCreateRequirements(),
                updates)));
  }

  public Response commit(
      String prefix,
      String idempotencyKey,
      TransactionCommitRequest request,
      TableGatewaySupport tableSupport) {
    return commitInternal(prefix, idempotencyKey, request, tableSupport, true);
  }

  private Response commitInternal(
      String prefix,
      String idempotencyKey,
      TransactionCommitRequest request,
      TableGatewaySupport tableSupport,
      boolean preMaterializeAssertCreate) {
    String accountId = accountContext.getAccountId();
    if (accountId == null || accountId.isBlank()) {
      return IcebergErrorResponses.validation("account context is required");
    }
    List<TransactionCommitRequest.TableChange> changes =
        request == null || request.tableChanges() == null ? List.of() : request.tableChanges();
    if (changes.isEmpty()) {
      return IcebergErrorResponses.validation("table-changes are required");
    }
    String duplicateIdentifier = firstDuplicateTableIdentifier(changes);
    if (duplicateIdentifier != null) {
      return IcebergErrorResponses.validation(
          "duplicate table identifier in table-changes: " + duplicateIdentifier);
    }
    CatalogRef catalogContext = resourceResolver.catalog(prefix);
    String catalogName = catalogContext.catalogName();
    ResourceId catalogId = catalogContext.catalogId();

    String requestHash = requestHash(changes);
    String beginIdempotency =
        firstNonBlank(idempotencyKey, "req:" + catalogName + ":" + requestHash);
    ai.floedb.floecat.transaction.rpc.BeginTransactionResponse begin;
    try {
      begin =
          grpcClient.beginTransaction(
              ai.floedb.floecat.transaction.rpc.BeginTransactionRequest.newBuilder()
                  .setIdempotency(
                      ai.floedb.floecat.common.rpc.IdempotencyKey.newBuilder()
                          .setKey(beginIdempotency == null ? "" : beginIdempotency))
                  .putProperties(TX_REQUEST_HASH_PROPERTY, requestHash)
                  .build());
    } catch (StatusRuntimeException beginFailure) {
      return transactionApplyService.mapPreCommitFailure(beginFailure);
    } catch (RuntimeException beginFailure) {
      return transactionApplyService.preCommitStateUnknown();
    }
    String txId = begin.getTransaction().getTxId();
    if (txId == null || txId.isBlank()) {
      return IcebergErrorResponses.failure(
          "Failed to begin transaction",
          "InternalServerError",
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    ai.floedb.floecat.transaction.rpc.GetTransactionResponse currentTxn;
    TransactionState currentState;
    try {
      currentTxn =
          grpcClient.getTransaction(GetTransactionRequest.newBuilder().setTxId(txId).build());
      currentState =
          currentTxn != null && currentTxn.hasTransaction()
              ? currentTxn.getTransaction().getState()
              : TransactionState.TS_UNSPECIFIED;
    } catch (StatusRuntimeException e) {
      transactionApplyService.abortQuietly(txId, "failed to load transaction");
      return transactionApplyService.mapPreCommitFailure(e);
    } catch (RuntimeException e) {
      transactionApplyService.abortQuietly(txId, "failed to load transaction");
      return transactionApplyService.preCommitStateUnknown();
    }
    if (currentTxn != null && currentTxn.hasTransaction()) {
      String existingRequestHash =
          currentTxn.getTransaction().getPropertiesMap().get(TX_REQUEST_HASH_PROPERTY);
      if (existingRequestHash != null
          && !existingRequestHash.isBlank()
          && !existingRequestHash.equals(requestHash)) {
        maybeAbortOpenTransaction(currentState, txId, "transaction request-hash mismatch");
        return IcebergErrorResponses.failure(
            "transaction request does not match existing transaction payload",
            "CommitFailedException",
            Response.Status.CONFLICT);
      }
    }

    String idempotencyBase = firstNonBlank(idempotencyKey, txId);
    long txCreatedAtMs =
        currentTxn != null
                && currentTxn.hasTransaction()
                && currentTxn.getTransaction().hasCreatedAt()
            ? Timestamps.toMillis(currentTxn.getTransaction().getCreatedAt())
            : begin.getTransaction().hasCreatedAt()
                ? Timestamps.toMillis(begin.getTransaction().getCreatedAt())
                : System.currentTimeMillis();
    TransactionPlanningService.PlanningResult planningResult =
        transactionPlanningService.prepareCommit(
            new TransactionPlanningService.PlanningCommand(
                accountId,
                txId,
                prefix,
                catalogName,
                catalogId,
                idempotencyBase,
                requestHash,
                txCreatedAtMs,
                currentState,
                changes,
                tableSupport,
                preMaterializeAssertCreate));
    if (planningResult.error() != null) {
      return planningResult.error();
    }

    return transactionApplyService.applyTransaction(
        txId,
        new CommitContext(
            currentState,
            idempotencyBase,
            planningResult.txChanges(),
            planningResult.outboxWorkItems()),
        tableSupport);
  }

  private List<Map<String, Object>> buildCreateUpdates(
      TableRequests.Create request, TableSpec spec) {
    TableMetadataBuilder.CreateRequestState state =
        TableMetadataBuilder.createRequestState(null, null, request, spec.getPropertiesMap());
    Map<String, String> props = new LinkedHashMap<>(state.properties());
    String tableLocation = blankToNull(props.remove("location"));
    Integer formatVersion = state.formatVersion();

    List<Map<String, Object>> updates = new ArrayList<>();
    if (tableLocation != null) {
      updates.add(
          Map.of("action", CommitUpdateInspector.ACTION_SET_LOCATION, "location", tableLocation));
    }
    updates.add(
        Map.of(
            "action",
            CommitUpdateInspector.ACTION_UPGRADE_FORMAT_VERSION,
            "format-version",
            formatVersion));
    updates.add(
        Map.of(
            "action",
            CommitUpdateInspector.ACTION_ADD_SCHEMA,
            "schema",
            state.schema(),
            "last-column-id",
            state.lastColumnId()));
    updates.add(
        Map.of(
            "action",
            CommitUpdateInspector.ACTION_SET_CURRENT_SCHEMA,
            "schema-id",
            state.schemaId()));
    updates.add(
        Map.of("action", CommitUpdateInspector.ACTION_ADD_SPEC, "spec", state.partitionSpec()));
    updates.add(
        Map.of(
            "action",
            CommitUpdateInspector.ACTION_SET_DEFAULT_SPEC,
            "spec-id",
            state.defaultSpecId()));
    updates.add(
        Map.of(
            "action",
            CommitUpdateInspector.ACTION_ADD_SORT_ORDER,
            "sort-order",
            state.sortOrder()));
    updates.add(
        Map.of(
            "action",
            CommitUpdateInspector.ACTION_SET_DEFAULT_SORT_ORDER,
            "sort-order-id",
            state.defaultSortOrderId()));
    if (!props.isEmpty()) {
      updates.add(Map.of("action", CommitUpdateInspector.ACTION_SET_PROPERTIES, "updates", props));
    }
    return List.copyOf(updates);
  }

  private String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value;
  }

  private ai.floedb.floecat.catalog.rpc.Table loadCurrentTable(CommitCommand command) {
    try {
      ResourceId tableId =
          tableGatewaySupport.resolveTableId(
              command.catalogName(), command.namespacePath(), command.table());
      return tableGatewaySupport.getTable(tableId);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return null;
      }
      throw e;
    }
  }

  private boolean isDeltaReadOnlyCommitBlocked(ai.floedb.floecat.catalog.rpc.Table table) {
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

  private TableRequests.Commit mergeStagedCreateIntoCommit(
      CommitCommand command,
      TableRequests.Commit req,
      StagedTableEntry stagedEntry,
      ai.floedb.floecat.catalog.rpc.Table tableState) {
    if (req == null || stagedEntry == null) {
      return req;
    }
    String stagedMetadataLocation =
        MetadataLocationUtil.metadataLocation(
            stagedEntry.request() == null ? null : stagedEntry.request().properties());
    if (tableState != null && hasCommittedSnapshot(tableState)) {
      return req;
    }
    if (tableState != null) {
      if (callerProvidesCreateInitialization(req)) {
        return injectStagedMetadataLocation(req, stagedMetadataLocation);
      }
      TransactionCommitRequest stagedRequest =
          buildCreateRequest(
              command.namespacePath(),
              command.table(),
              stagedEntry.catalogId(),
              stagedEntry.namespaceId(),
              stagedEntry.request(),
              command.tableSupport());
      var stagedChange = stagedRequest.tableChanges().get(0);
      List<Map<String, Object>> updates = new ArrayList<>(stagedChange.updates());
      if (req.updates() != null && !req.updates().isEmpty()) {
        updates.addAll(req.updates());
      }
      TableRequests.Commit merged =
          new TableRequests.Commit(
              req.requirements() == null ? List.of() : List.copyOf(req.requirements()),
              List.copyOf(updates));
      return injectStagedMetadataLocation(merged, stagedMetadataLocation);
    }
    if (callerProvidesCreateInitialization(req)) {
      return injectStagedMetadataLocation(req, stagedMetadataLocation);
    }
    TransactionCommitRequest stagedRequest =
        buildCreateRequest(
            command.namespacePath(),
            command.table(),
            stagedEntry.catalogId(),
            stagedEntry.namespaceId(),
            stagedEntry.request(),
            command.tableSupport());
    var stagedChange = stagedRequest.tableChanges().get(0);
    List<Map<String, Object>> requirements = new ArrayList<>(stagedChange.requirements());
    if (req.requirements() != null && !req.requirements().isEmpty()) {
      requirements.addAll(req.requirements());
    }
    List<Map<String, Object>> updates = new ArrayList<>(stagedChange.updates());
    if (req.updates() != null && !req.updates().isEmpty()) {
      updates.addAll(req.updates());
    }
    TableRequests.Commit merged =
        new TableRequests.Commit(List.copyOf(requirements), List.copyOf(updates));
    return injectStagedMetadataLocation(merged, stagedMetadataLocation);
  }

  private boolean hasCommittedSnapshot(ai.floedb.floecat.catalog.rpc.Table tableState) {
    if (tableState == null || tableState.getPropertiesMap().isEmpty()) {
      return false;
    }
    String currentSnapshotId = tableState.getPropertiesMap().get("current-snapshot-id");
    return currentSnapshotId != null && !currentSnapshotId.isBlank();
  }

  private TableRequests.Commit injectStagedMetadataLocation(
      TableRequests.Commit req, String stagedMetadataLocation) {
    if (req == null
        || stagedMetadataLocation == null
        || stagedMetadataLocation.isBlank()
        || CommitUpdateInspector.inspect(req).requestedMetadataLocation() != null) {
      return req;
    }
    List<Map<String, Object>> updates =
        req.updates() == null ? new ArrayList<>() : new ArrayList<>(req.updates());
    updates.add(
        Map.of(
            "action",
            CommitUpdateInspector.ACTION_SET_PROPERTIES,
            "updates",
            Map.of(MetadataLocationUtil.PRIMARY_KEY, stagedMetadataLocation)));
    return new TableRequests.Commit(req.requirements(), List.copyOf(updates));
  }

  private Optional<StagedTableEntry> resolveStagedEntry(CommitCommand command) {
    String accountId = accountContext.getAccountId();
    if (accountId == null || accountId.isBlank()) {
      return Optional.empty();
    }
    String stageId = firstNonBlank(command.stageId(), command.transactionId());
    if (stageId != null) {
      StagedTableKey key =
          new StagedTableKey(
              accountId, command.catalogName(), command.namespacePath(), command.table(), stageId);
      return stagedTableRepository.getStage(key);
    }
    return stagedTableRepository.findSingleStage(
        accountId, command.catalogName(), command.namespacePath(), command.table());
  }

  private boolean callerProvidesCreateInitialization(TableRequests.Commit req) {
    if (req == null || req.updates() == null || req.updates().isEmpty()) {
      return false;
    }
    for (Map<String, Object> update : req.updates()) {
      String action = update == null ? null : update.get("action") instanceof String s ? s : null;
      if (CommitUpdateInspector.isCreateInitializationAction(action)) {
        return true;
      }
    }
    return false;
  }

  private String firstDuplicateTableIdentifier(List<TransactionCommitRequest.TableChange> changes) {
    if (changes == null || changes.isEmpty()) {
      return null;
    }
    Set<String> seen = new java.util.LinkedHashSet<>();
    for (TransactionCommitRequest.TableChange change : changes) {
      if (change == null || change.identifier() == null) {
        continue;
      }
      String name = change.identifier().name();
      if (name == null || name.isBlank()) {
        continue;
      }
      List<String> namespacePath =
          change.identifier().namespace() == null
              ? List.of()
              : List.copyOf(change.identifier().namespace());
      String qualifiedName =
          namespacePath.isEmpty() ? name : String.join(".", namespacePath) + "." + name;
      if (!seen.add(qualifiedName)) {
        return qualifiedName;
      }
    }
    return null;
  }

  private String requestHash(List<TransactionCommitRequest.TableChange> changes) {
    List<Map<String, Object>> normalized = new ArrayList<>();
    if (changes != null) {
      for (TransactionCommitRequest.TableChange change : changes) {
        if (change == null) {
          normalized.add(Map.of());
          continue;
        }
        Map<String, Object> entry = new LinkedHashMap<>();
        var identifier = change.identifier();
        if (identifier != null) {
          Map<String, Object> idMap = new LinkedHashMap<>();
          idMap.put(
              "namespace",
              identifier.namespace() == null ? List.of() : List.copyOf(identifier.namespace()));
          idMap.put("name", identifier.name());
          entry.put("identifier", idMap);
        } else {
          entry.put("identifier", null);
        }
        entry.put(
            "requirements", change.requirements() == null ? List.of() : change.requirements());
        entry.put("updates", change.updates() == null ? List.of() : change.updates());
        normalized.add(entry);
      }
    }
    String canonical = canonicalize(normalized);
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(canonical.getBytes(StandardCharsets.UTF_8));
      return Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }

  private String canonicalize(Object value) {
    if (value == null) {
      return "null";
    }
    if (value instanceof Map<?, ?> map) {
      List<Map.Entry<?, ?>> entries = new ArrayList<>(map.entrySet());
      entries.sort(java.util.Comparator.comparing(entry -> String.valueOf(entry.getKey())));
      StringBuilder out = new StringBuilder("{");
      boolean first = true;
      for (Map.Entry<?, ?> entry : entries) {
        if (!first) {
          out.append(',');
        }
        first = false;
        out.append(escapeJsonString(String.valueOf(entry.getKey())))
            .append(':')
            .append(canonicalize(entry.getValue()));
      }
      return out.append('}').toString();
    }
    if (value instanceof List<?> list) {
      StringBuilder out = new StringBuilder("[");
      boolean first = true;
      for (Object entry : list) {
        if (!first) {
          out.append(',');
        }
        first = false;
        out.append(canonicalize(entry));
      }
      return out.append(']').toString();
    }
    if (value instanceof String str) {
      return escapeJsonString(str);
    }
    if (value instanceof Number || value instanceof Boolean) {
      return String.valueOf(value);
    }
    return escapeJsonString(String.valueOf(value));
  }

  private String escapeJsonString(String input) {
    String value = input == null ? "" : input;
    StringBuilder out = new StringBuilder("\"");
    for (int i = 0; i < value.length(); i++) {
      char ch = value.charAt(i);
      switch (ch) {
        case '\\' -> out.append("\\\\");
        case '"' -> out.append("\\\"");
        case '\b' -> out.append("\\b");
        case '\f' -> out.append("\\f");
        case '\n' -> out.append("\\n");
        case '\r' -> out.append("\\r");
        case '\t' -> out.append("\\t");
        default -> {
          if (ch < 0x20) {
            out.append(String.format("\\u%04x", (int) ch));
          } else {
            out.append(ch);
          }
        }
      }
    }
    return out.append('"').toString();
  }

  void maybeAbortOpenTransaction(TransactionState currentState, String txId, String reason) {
    if (currentState == TransactionState.TS_OPEN) {
      transactionApplyService.abortQuietly(txId, reason);
    }
  }

  public record CommitContext(
      TransactionState currentState,
      String idempotencyBase,
      List<ai.floedb.floecat.transaction.rpc.TxChange> txChanges,
      List<TableCommitOutboxService.WorkItem> outboxWorkItems) {}
}
