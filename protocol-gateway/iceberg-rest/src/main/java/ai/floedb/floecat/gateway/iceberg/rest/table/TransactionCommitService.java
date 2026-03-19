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
import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.maxFieldId;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.CatalogRef;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.NamespaceRef;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.ResourceResolver;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.compat.TableFormatSupport;
import ai.floedb.floecat.gateway.iceberg.rest.support.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.support.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.support.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.support.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.support.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.support.RefPropertyUtil;
import ai.floedb.floecat.gateway.iceberg.rest.support.SnapshotMetadataUtil;
import ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil;
import ai.floedb.floecat.gateway.iceberg.rest.table.IcebergMetadataService.ImportedMetadata;
import ai.floedb.floecat.gateway.iceberg.rest.table.IcebergMetadataService.ImportedSnapshot;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergCommitJournalEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergCommitOutboxEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.storage.kv.Keys;
import ai.floedb.floecat.transaction.rpc.GetTransactionRequest;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.iceberg.TableMetadata;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TransactionCommitService {
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};
  private static final int COMMIT_JOURNAL_VERSION = 1;
  private static final int COMMIT_OUTBOX_VERSION = 1;
  private static final String ICEBERG_METADATA_KEY = "iceberg";
  private static final String TX_REQUEST_HASH_PROPERTY = "iceberg.commit.request-hash";
  private static final int DEFAULT_COMMIT_CONFIRM_MAX_ATTEMPTS = 6;
  private static final long DEFAULT_COMMIT_CONFIRM_INITIAL_SLEEP_MS = 20L;
  private static final long DEFAULT_COMMIT_CONFIRM_MAX_SLEEP_MS = 200L;
  private static final List<Map<String, Object>> STAGE_CREATE_REQUIREMENTS =
      CommitUpdateInspector.assertCreateRequirements();
  private static final Set<TransactionState> CONFIRMABLE_COMMIT_STATES =
      Set.of(
          TransactionState.TS_UNSPECIFIED,
          TransactionState.TS_OPEN,
          TransactionState.TS_PREPARED,
          TransactionState.TS_APPLYING,
          TransactionState.TS_APPLY_FAILED_RETRYABLE);
  private static final Logger LOG = Logger.getLogger(TransactionCommitService.class);
  @Inject IcebergGatewayConfig config;
  @Inject AccountContext accountContext;
  @Inject ObjectMapper mapper;
  @Inject ResourceResolver resourceResolver;
  @Inject TableGatewaySupport tableGatewaySupport;
  @Inject TableFormatSupport tableFormatSupport;
  @Inject TableUpdatePlanner tableUpdatePlanner;
  @Inject TablePropertyService tablePropertyService;
  @Inject ConnectorProvisioningService connectorProvisioningService;
  @Inject IcebergMetadataService icebergMetadataService;
  @Inject MaterializeMetadataService materializeMetadataService;
  @Inject TableCommitJournalService commitJournalService;
  @Inject TableCommitOutboxService commitOutboxService;
  @Inject StagedTableService stagedTableService;
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
    if (request == null) {
      return IcebergErrorResponses.validation("Request body is required");
    }
    if (request.name() == null || request.name().isBlank()) {
      return IcebergErrorResponses.validation("name is required");
    }
    if (!hasSchema(request)) {
      return IcebergErrorResponses.validation("schema is required");
    }

    String tableName = request.name().trim();
    TableRequests.Create effectiveReq =
        applyDefaultLocationIfMissing(namespaceContext, tableName, request);
    if (Boolean.TRUE.equals(effectiveReq.stageCreate())) {
      return handleStageCreate(
          namespaceContext,
          tableName,
          effectiveReq,
          transactionId,
          idempotencyKey,
          accessDelegationMode,
          tableSupport);
    }

    Response txResponse;
    try {
      txResponse =
          commitCreate(
              namespaceContext.prefix(),
              idempotencyKey,
              namespaceContext.namespacePath(),
              tableName,
              namespaceContext.catalogId(),
              namespaceContext.namespaceId(),
              effectiveReq,
              tableSupport);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
    if (txResponse == null
        || txResponse.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
      return txResponse;
    }

    ai.floedb.floecat.catalog.rpc.Table created;
    try {
      ResourceId tableId =
          tableGatewaySupport.resolveTableId(
              namespaceContext.catalogName(), namespaceContext.namespacePath(), tableName);
      created = tableGatewaySupport.getTable(tableId);
    } catch (Exception e) {
      return IcebergErrorResponses.failure(
          "Failed to load created table",
          "InternalServerError",
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    List<StorageCredentialDto> credentials;
    try {
      credentials = tableSupport.credentialsForAccessDelegation(accessDelegationMode);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
    try {
      var resolved =
          icebergMetadataService.resolveMetadata(
              tableName,
              created,
              tableSupport,
              () ->
                  SnapshotLister.fetchSnapshots(
                      grpcClient, created.getResourceId(), SnapshotLister.Mode.ALL, null));
      LoadTableResultDto loadResult =
          TableResponseMapper.toLoadResult(
              resolved.metadataView(), tableSupport.defaultTableConfig(), credentials);
      LOG.infof(
          "Create table response namespace=%s table=%s metadata=%s location=%s configKeys=%s",
          namespaceContext.namespacePath(),
          tableName,
          loadResult.metadata() == null ? "<null>" : loadResult.metadata().metadataLocation(),
          loadResult.metadataLocation(),
          loadResult.config().keySet());
      return Response.ok(loadResult).build();
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
  }

  public Response registerTable(
      NamespaceRef namespaceContext,
      String idempotencyKey,
      TableRequests.Register req,
      TableGatewaySupport tableSupport) {
    if (req == null || req.metadataLocation() == null || req.metadataLocation().isBlank()) {
      return IcebergErrorResponses.validation("metadata-location is required");
    }
    if (req.name() == null || req.name().isBlank()) {
      return IcebergErrorResponses.validation("name is required");
    }
    String metadataLocation = req.metadataLocation().trim();
    String tableName = req.name().trim();

    Map<String, String> ioProperties =
        tableSupport.resolveRegisterFileIoProperties(req.properties());
    ImportedMetadata importedMetadata;
    try {
      importedMetadata = icebergMetadataService.importMetadata(metadataLocation, ioProperties);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }

    boolean overwrite = Boolean.TRUE.equals(req.overwrite());
    boolean tableExists = false;
    if (overwrite) {
      try {
        ResourceId existingTableId =
            tableGatewaySupport.resolveTableId(
                namespaceContext.catalogName(), namespaceContext.namespacePath(), tableName);
        tableGatewaySupport.getTable(existingTableId);
        tableExists = true;
      } catch (StatusRuntimeException e) {
        if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
          throw e;
        }
      }
    }
    Response commitResponse =
        overwrite && tableExists
            ? overwriteRegisteredTable(
                namespaceContext,
                tableName,
                metadataLocation,
                idempotencyKey,
                ioProperties,
                importedMetadata,
                tableSupport)
            : createRegisteredTable(
                namespaceContext,
                tableName,
                metadataLocation,
                idempotencyKey,
                ioProperties,
                importedMetadata,
                tableSupport);
    if (commitResponse != null) {
      return commitResponse;
    }

    ResourceId tableId =
        tableGatewaySupport.resolveTableId(
            namespaceContext.catalogName(), namespaceContext.namespacePath(), tableName);
    tableGatewaySupport.getTable(tableId);

    if (importedMetadata.metadataView() == null) {
      return IcebergErrorResponses.failure(
          "Failed to load canonical registered table metadata",
          "InternalServerError",
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return Response.ok(
            TableResponseMapper.toLoadResult(
                importedMetadata.metadataView(),
                tableSupport.defaultTableConfig(),
                tableSupport.defaultCredentials()))
        .build();
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

    Response response = buildCommitResponse(command, effectiveReq, stagedEntryOpt.orElse(null));
    if (response != null && response.getStatus() == Response.Status.OK.getStatusCode()) {
      stagedEntryOpt.ifPresent(entry -> stagedTableService.deleteStage(entry.key()));
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

  TransactionCommitRequest buildRegisterTransactionRequest(
      List<String> namespacePath,
      String tableName,
      Map<String, String> mergedProps,
      ImportedMetadata importedMetadata,
      List<Long> existingSnapshotIds,
      boolean assertCreate) {
    List<Map<String, Object>> requirements = new ArrayList<>();
    if (assertCreate) {
      requirements.addAll(CommitUpdateInspector.assertCreateRequirements());
    }
    List<Map<String, Object>> updates = new ArrayList<>();
    Map<String, String> props = mergedProps == null ? Map.of() : new LinkedHashMap<>(mergedProps);
    if (!props.isEmpty()) {
      updates.add(Map.of("action", CommitUpdateInspector.ACTION_SET_PROPERTIES, "updates", props));
    }
    String location = props.get("location");
    if (location != null && !location.isBlank()) {
      updates.add(
          Map.of("action", CommitUpdateInspector.ACTION_SET_LOCATION, "location", location));
    }
    List<Long> importedSnapshotIds = new ArrayList<>();
    for (ImportedSnapshot snapshot : snapshotsToImport(importedMetadata)) {
      Map<String, Object> snapshotMap = toSnapshotUpdate(snapshot, importedMetadata);
      if (!snapshotMap.isEmpty()) {
        updates.add(
            Map.of("action", CommitUpdateInspector.ACTION_ADD_SNAPSHOT, "snapshot", snapshotMap));
      }
      if (snapshot != null && snapshot.snapshotId() != null) {
        importedSnapshotIds.add(snapshot.snapshotId());
      }
    }
    if (existingSnapshotIds != null
        && !existingSnapshotIds.isEmpty()
        && !importedSnapshotIds.isEmpty()) {
      List<Long> removals =
          existingSnapshotIds.stream().filter(id -> !importedSnapshotIds.contains(id)).toList();
      if (!removals.isEmpty()) {
        updates.add(
            Map.of(
                "action", CommitUpdateInspector.ACTION_REMOVE_SNAPSHOTS, "snapshot-ids", removals));
      }
    }
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(namespacePath, tableName), requirements, updates)));
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
      return mapPreCommitFailure(beginFailure);
    } catch (RuntimeException beginFailure) {
      return preCommitStateUnknown();
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
      abortTransactionQuietly(txId, "failed to load transaction");
      return mapPreCommitFailure(e);
    } catch (RuntimeException e) {
      abortTransactionQuietly(txId, "failed to load transaction");
      return preCommitStateUnknown();
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
                : clockMillis();
    List<ai.floedb.floecat.transaction.rpc.TxChange> txChanges = new ArrayList<>();
    List<TableCommitOutboxService.WorkItem> outboxItems = new ArrayList<>();

    if (currentState == TransactionState.TS_APPLY_FAILED_CONFLICT) {
      return IcebergErrorResponses.failure(
          "transaction commit failed", "CommitFailedException", Response.Status.CONFLICT);
    }
    boolean alreadyApplied = currentState == TransactionState.TS_APPLIED;
    List<PlannedChange> planned = new ArrayList<>();
    for (TransactionCommitRequest.TableChange change : changes) {
      try {
        var identifier = change.identifier();
        if (identifier == null || identifier.name() == null || identifier.name().isBlank()) {
          maybeAbortOpenTransaction(currentState, txId, "table identifier is missing");
          return IcebergErrorResponses.validation("table identifier is required");
        }
        if (change.requirements() == null) {
          maybeAbortOpenTransaction(currentState, txId, "requirements are missing");
          return IcebergErrorResponses.validation("requirements are required");
        }
        if (change.updates() == null) {
          maybeAbortOpenTransaction(currentState, txId, "updates are missing");
          return IcebergErrorResponses.validation("updates are required");
        }
        boolean assertCreateRequested =
            hasRequirementType(
                change.requirements(), CommitUpdateInspector.REQUIREMENT_ASSERT_CREATE);
        Response requirementTypeError = validateKnownRequirementTypes(change.requirements());
        if (requirementTypeError != null) {
          maybeAbortOpenTransaction(currentState, txId, "requirements include unknown type");
          return requirementTypeError;
        }
        Response updateTypeError = validateKnownUpdateActions(change.updates());
        if (updateTypeError != null) {
          maybeAbortOpenTransaction(currentState, txId, "updates include unknown action");
          return updateTypeError;
        }
        List<String> namespacePath =
            identifier.namespace() == null ? List.of() : List.copyOf(identifier.namespace());
        String namespace = namespacePath.isEmpty() ? "" : String.join(".", namespacePath);
        ResourceId namespaceId = tableGatewaySupport.resolveNamespaceId(catalogName, namespacePath);
        TableRequests.Commit commitReq =
            new TableRequests.Commit(change.requirements(), change.updates());
        var command =
            new CommitCommand(
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
                commitReq,
                tableSupport);
        ResourceId tableId;
        ai.floedb.floecat.catalog.rpc.GetTableResponse tableResponse;
        try {
          tableId =
              tableGatewaySupport.resolveTableId(catalogName, namespacePath, identifier.name());
          tableResponse = tableGatewaySupport.getTableResponse(tableId);
        } catch (StatusRuntimeException e) {
          if (e.getStatus().getCode() != Status.Code.NOT_FOUND || !assertCreateRequested) {
            throw e;
          }
          tableId =
              atomicCreateTableId(
                  accountId, txId, catalogId, namespaceId, namespacePath, identifier.name());
          tableResponse = null;
        }
        Response assertCreateError =
            validateAssertCreateRequirement(change.requirements(), tableResponse);
        if (assertCreateError != null) {
          maybeAbortOpenTransaction(currentState, txId, "assert-create requirement failed");
          return assertCreateError;
        }
        ai.floedb.floecat.catalog.rpc.Table persistedTable =
            tableResponse == null || !tableResponse.hasTable()
                ? newCreateTableStub(tableId, catalogId, namespaceId, identifier.name())
                : tableResponse.getTable();
        long pointerVersion =
            tableResponse != null && tableResponse.hasMeta()
                ? tableResponse.getMeta().getPointerVersion()
                : 0L;
        Response nullRefRequirementError =
            validateNullSnapshotRefRequirements(
                tableSupport, persistedTable, change.requirements());
        if (nullRefRequirementError != null) {
          maybeAbortOpenTransaction(currentState, txId, "null snapshot-id ref requirement failed");
          return nullRefRequirementError;
        }
        ai.floedb.floecat.catalog.rpc.Table updated;
        TableMetadata committedMetadata = null;
        boolean shouldPlan = !alreadyApplied;
        if (!shouldPlan) {
          // TS_APPLIED replay reloads durable outbox work from the commit journal later.
          updated = persistedTable;
        } else {
          Supplier<ai.floedb.floecat.catalog.rpc.Table> workingTableSupplier = () -> persistedTable;
          Supplier<ai.floedb.floecat.catalog.rpc.Table> requirementTableSupplier =
              () -> persistedTable;
          var plan =
              tableUpdatePlanner.planTransaction(
                  command, workingTableSupplier, requirementTableSupplier, tableId);
          if (plan.hasError()) {
            maybeAbortOpenTransaction(currentState, txId, "transaction planning failed");
            return plan.error();
          }
          updated = plan.table();
          PreMaterializedTable preMaterialized =
              preMaterializeTableBeforeCommit(
                  namespace,
                  identifier.name(),
                  tableId,
                  updated,
                  change.requirements() == null ? List.of() : List.copyOf(change.requirements()),
                  change.updates() == null ? List.of() : List.copyOf(change.updates()),
                  tableSupport,
                  preMaterializeAssertCreate);
          updated = preMaterialized.table();
          committedMetadata = preMaterialized.tableMetadata();
        }
        planned.add(
            new PlannedChange(
                namespacePath,
                namespaceId,
                identifier.name(),
                tableId,
                updated,
                committedMetadata,
                change.updates() == null ? List.of() : List.copyOf(change.updates()),
                pointerVersion));
      } catch (StatusRuntimeException e) {
        if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
          maybeAbortOpenTransaction(currentState, txId, "table not found during planning");
          return IcebergErrorResponses.noSuchTable(
              "Table not found while planning transaction change");
        }
        maybeAbortOpenTransaction(currentState, txId, "transaction planning failed");
        throw e;
      } catch (RuntimeException e) {
        maybeAbortOpenTransaction(currentState, txId, "transaction planning failed");
        throw e;
      }
    }

    for (var plan : planned) {
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
        maybeAbortOpenTransaction(currentState, txId, "connector provisioning failed");
        return connectorResolution.error();
      }
      tableForTx = connectorResolution.table();
      ResourceId connectorId = connectorResolution.connectorId();
      if (!connectorResolution.connectorTxChanges().isEmpty()) {
        txChanges.addAll(connectorResolution.connectorTxChanges());
      }
      CommitUpdateInspector.Parsed parsedUpdates =
          CommitUpdateInspector.inspectUpdates(plan.updates());
      List<Long> addedSnapshotIds = parsedUpdates.addedSnapshotIds();
      List<Long> removedSnapshotIds = parsedUpdates.removedSnapshotIds();
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
        maybeAbortOpenTransaction(currentState, txId, "snapshot metadata planning failed");
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
              connectorId,
              addedSnapshotIds,
              removedSnapshotIds,
              tableForTx,
              txCreatedAtMs);
      String pendingKey =
          Keys.tableCommitOutboxPendingPointer(
              txCreatedAtMs, accountId, scopedTableId.getId(), txId);
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
    }

    List<TableCommitOutboxService.WorkItem> outboxWorkItems =
        alreadyApplied
            ? loadReplayWorkItems(accountId, txId, txCreatedAtMs, requestHash, planned)
            : new ArrayList<>();
    if (!alreadyApplied) {
      outboxWorkItems = outboxItems;
    }

    boolean applied = isCommitAccepted(currentState);
    if (!applied) {
      if (currentState == TransactionState.TS_OPEN) {
        try {
          grpcClient.prepareTransaction(
              ai.floedb.floecat.transaction.rpc.PrepareTransactionRequest.newBuilder()
                  .setTxId(txId)
                  .addAllChanges(txChanges)
                  .setIdempotency(
                      ai.floedb.floecat.common.rpc.IdempotencyKey.newBuilder()
                          .setKey(idempotencyBase == null ? "" : idempotencyBase + ":prepare"))
                  .build());
        } catch (RuntimeException e) {
          abortTransactionQuietly(txId, "transaction prepare failed");
          if (e instanceof StatusRuntimeException prepareFailure) {
            return mapPreCommitFailure(prepareFailure);
          }
          return preCommitStateUnknown();
        }
      }
      try {
        ai.floedb.floecat.transaction.rpc.CommitTransactionRequest.Builder commitRequest =
            ai.floedb.floecat.transaction.rpc.CommitTransactionRequest.newBuilder().setTxId(txId);
        if (currentState != TransactionState.TS_APPLY_FAILED_RETRYABLE) {
          commitRequest.setIdempotency(
              ai.floedb.floecat.common.rpc.IdempotencyKey.newBuilder()
                  .setKey(idempotencyBase == null ? "" : idempotencyBase + ":commit"));
        }
        var commitResponse = grpcClient.commitTransaction(commitRequest.build());
        TransactionState commitState =
            commitResponse != null && commitResponse.hasTransaction()
                ? commitResponse.getTransaction().getState()
                : TransactionState.TS_UNSPECIFIED;
        if (isCommitAccepted(commitState)) {
          applied = true;
          // Commit applied; continue with best-effort outbox processing.
        } else {
          if (isDeterministicFailedState(commitState)) {
            return IcebergErrorResponses.failure(
                "transaction commit did not reach applied state",
                "CommitFailedException",
                Response.Status.CONFLICT);
          }
          if (shouldConfirmAmbiguousCommitState(commitState) && waitForAppliedState(txId)) {
            applied = true;
          } else {
            return IcebergErrorResponses.failure(
                "transaction commit did not reach applied state",
                "CommitStateUnknownException",
                Response.Status.SERVICE_UNAVAILABLE);
          }
        }
      } catch (StatusRuntimeException commitFailure) {
        if (isDeterministicCommitFailure(commitFailure)) {
          return IcebergErrorResponses.failure(
              "transaction commit failed", "CommitFailedException", Response.Status.CONFLICT);
        }
        if (isRetryableCommitAbort(commitFailure)) {
          if (waitForAppliedState(txId)) {
            applied = true;
            // Apply eventually succeeded; continue with best-effort outbox processing.
          } else {
            return IcebergErrorResponses.failure(
                "transaction commit failed",
                "CommitStateUnknownException",
                Response.Status.SERVICE_UNAVAILABLE);
          }
        } else {
          Response mapped = mapCommitFailureByStatus(commitFailure.getStatus().getCode());
          return mapped == null ? preCommitStateUnknown() : mapped;
        }
      } catch (RuntimeException commitFailure) {
        return IcebergErrorResponses.failure(
            "transaction commit failed",
            "CommitStateUnknownException",
            Response.Status.INTERNAL_SERVER_ERROR);
      }
    }

    if (!applied) {
      return IcebergErrorResponses.failure(
          "transaction commit did not reach applied state",
          "CommitStateUnknownException",
          Response.Status.SERVICE_UNAVAILABLE);
    }

    if (!outboxWorkItems.isEmpty()) {
      try {
        commitOutboxService.processPendingNow(tableSupport, outboxWorkItems);
      } catch (RuntimeException e) {
        LOG.warnf(e, "Best-effort outbox processing failed for tx=%s", txId);
      }
    }

    return Response.noContent().build();
  }

  private List<Map<String, Object>> buildCreateUpdates(
      TableRequests.Create request, TableSpec spec) {
    Map<String, Object> schema = requireObject(request.schema(), "schema");
    Integer schemaId = requireNonNegativeInt(schema, "schema-id", "schema");
    Integer lastColumnId =
        firstNonNegativeInt(
            asInt(schema.get("last-column-id")),
            maxFieldId(schema, "fields", "id", "field-id", "source-id"));
    if (lastColumnId == null) {
      throw new IllegalArgumentException("schema requires last-column-id");
    }

    Map<String, Object> partitionSpec =
        request.partitionSpec() == null || request.partitionSpec().isNull()
            ? defaultPartitionSpec()
            : requireObject(request.partitionSpec(), "partition-spec");
    Integer specId = requireNonNegativeInt(partitionSpec, "spec-id", "partition-spec");

    Map<String, Object> sortOrder =
        request.writeOrder() == null || request.writeOrder().isNull()
            ? defaultSortOrder()
            : requireObject(request.writeOrder(), "write-order");
    Integer sortOrderId = requireNonNegativeInt(sortOrder, "order-id", "write-order");

    Map<String, String> props = new LinkedHashMap<>(spec.getPropertiesMap());
    String tableLocation = blankToNull(props.remove("location"));
    Integer formatVersion = firstNonNegativeInt(asInt(props.remove("format-version")), 2);
    props.putIfAbsent("last-sequence-number", "0");

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
            schema,
            "last-column-id",
            lastColumnId));
    updates.add(
        Map.of("action", CommitUpdateInspector.ACTION_SET_CURRENT_SCHEMA, "schema-id", schemaId));
    updates.add(Map.of("action", CommitUpdateInspector.ACTION_ADD_SPEC, "spec", partitionSpec));
    updates.add(Map.of("action", CommitUpdateInspector.ACTION_SET_DEFAULT_SPEC, "spec-id", specId));
    updates.add(
        Map.of("action", CommitUpdateInspector.ACTION_ADD_SORT_ORDER, "sort-order", sortOrder));
    updates.add(
        Map.of(
            "action",
            CommitUpdateInspector.ACTION_SET_DEFAULT_SORT_ORDER,
            "sort-order-id",
            sortOrderId));
    if (!props.isEmpty()) {
      updates.add(Map.of("action", CommitUpdateInspector.ACTION_SET_PROPERTIES, "updates", props));
    }
    return List.copyOf(updates);
  }

  private Map<String, Object> requireObject(JsonNode node, String fieldName) {
    if (node == null || node.isNull()) {
      throw new IllegalArgumentException(fieldName + " is required");
    }
    if (!node.isObject()) {
      throw new IllegalArgumentException(fieldName + " must be an object");
    }
    return new LinkedHashMap<>(mapper.convertValue(node, MAP_TYPE));
  }

  private Integer requireNonNegativeInt(Map<String, Object> map, String key, String fieldName) {
    Integer value = asInt(map.get(key));
    if (value == null || value < 0) {
      throw new IllegalArgumentException(fieldName + " requires " + key);
    }
    return value;
  }

  @SafeVarargs
  private final Integer firstNonNegativeInt(Integer... values) {
    if (values == null) {
      return null;
    }
    for (Integer value : values) {
      if (value != null && value >= 0) {
        return value;
      }
    }
    return null;
  }

  private Integer asInt(Object value) {
    if (value instanceof Integer i) {
      return i;
    }
    if (value instanceof Number n) {
      return n.intValue();
    }
    if (value instanceof String s) {
      try {
        return Integer.parseInt(s);
      } catch (NumberFormatException ignored) {
        return null;
      }
    }
    return null;
  }

  private String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value;
  }

  private Map<String, Object> defaultPartitionSpec() {
    return new LinkedHashMap<>(Map.of("spec-id", 0, "fields", List.of()));
  }

  private Map<String, Object> defaultSortOrder() {
    return new LinkedHashMap<>(Map.of("order-id", 0, "fields", List.of()));
  }

  private Response handleStageCreate(
      NamespaceRef namespaceContext,
      String tableName,
      TableRequests.Create request,
      String transactionId,
      String idempotencyKey,
      String accessDelegationMode,
      TableGatewaySupport tableSupport) {
    if (request == null) {
      return IcebergErrorResponses.validation("stage-create requires a request body");
    }
    TableRequests.Create effectiveReq =
        applyDefaultLocationIfMissing(namespaceContext, tableName, request);
    if (effectiveReq.location() == null || effectiveReq.location().isBlank()) {
      LOG.warnf(
          "Stage-create request missing location prefix=%s namespace=%s table=%s payload=%s",
          namespaceContext.prefix(),
          namespaceContext.namespacePath(),
          tableName,
          safeSerializeCreate(request));
    }
    String accountId = accountContext.getAccountId();
    if (accountId == null || accountId.isBlank()) {
      return IcebergErrorResponses.validation("account context is required");
    }
    String stageId = firstNonBlank(transactionId, idempotencyKey);
    if (stageId == null || stageId.isBlank()) {
      stageId = UUID.randomUUID().toString();
    }
    try {
      LOG.infof(
          "Stage-create request payload prefix=%s namespace=%s table=%s stageId=%s location=%s"
              + " properties=%s",
          namespaceContext.prefix(),
          namespaceContext.namespacePath(),
          tableName,
          stageId,
          effectiveReq.location(),
          effectiveReq.properties());
      ai.floedb.floecat.catalog.rpc.Table stubTable =
          ai.floedb.floecat.catalog.rpc.Table.newBuilder()
              .setCatalogId(namespaceContext.catalogId())
              .setNamespaceId(namespaceContext.namespaceId())
              .setDisplayName(tableName)
              .build();
      BootstrapResult bootstrap = bootstrapStageCreate(tableName, stubTable, effectiveReq);
      effectiveReq = bootstrap.request();
      TableSpec spec =
          tableSupport
              .buildCreateSpec(
                  namespaceContext.catalogId(),
                  namespaceContext.namespaceId(),
                  tableName,
                  effectiveReq)
              .build();
      StagedTableEntry entry =
          new StagedTableEntry(
              new StagedTableKey(
                  accountId,
                  namespaceContext.catalogName(),
                  namespaceContext.namespacePath(),
                  tableName,
                  stageId),
              namespaceContext.catalogId(),
              namespaceContext.namespaceId(),
              effectiveReq,
              spec,
              STAGE_CREATE_REQUIREMENTS,
              StageState.STAGED,
              null,
              null,
              idempotencyKey);
      StagedTableEntry stored = stagedTableService.saveStage(entry);
      LOG.infof(
          "Stored stage-create payload account=%s catalog=%s namespace=%s table=%s stageId=%s"
              + " txnHeader=%s",
          accountContext.getAccountId(),
          namespaceContext.catalogName(),
          namespaceContext.namespacePath(),
          tableName,
          stored.key().stageId(),
          transactionId);
      List<StorageCredentialDto> credentials;
      try {
        credentials = tableSupport.credentialsForAccessDelegation(accessDelegationMode);
      } catch (IllegalArgumentException e) {
        stagedTableService.deleteStage(stored.key());
        return IcebergErrorResponses.validation(e.getMessage());
      }
      LoadTableResultDto loadResult =
          TableResponseMapper.toLoadResult(
              bootstrap.metadataView(), tableSupport.defaultTableConfig(), credentials);
      LOG.infof(
          "Stage-create metadata resolved stageId=%s location=%s",
          stored.key().stageId(), loadResult.metadataLocation());
      LOG.infof(
          "Stage-create response stageId=%s metadataLocation=%s configKeys=%s",
          stored.key().stageId(), loadResult.metadataLocation(), loadResult.config().keySet());
      return Response.ok(loadResult).build();
    } catch (IllegalArgumentException | com.fasterxml.jackson.core.JsonProcessingException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
  }

  private TableRequests.Create applyDefaultLocationIfMissing(
      NamespaceRef namespaceContext, String tableName, TableRequests.Create request) {
    if (request == null) {
      return null;
    }
    if (request.location() != null && !request.location().isBlank()) {
      return request;
    }
    String resolved = tableGatewaySupport.defaultTableLocation(namespaceContext, tableName);
    if (resolved == null || resolved.isBlank()) {
      return request;
    }
    return new TableRequests.Create(
        request.name(),
        request.schema(),
        resolved,
        request.properties(),
        request.partitionSpec(),
        request.writeOrder(),
        request.stageCreate());
  }

  private String safeSerializeCreate(TableRequests.Create request) {
    if (request == null) {
      return "<null>";
    }
    try {
      return mapper.writeValueAsString(request);
    } catch (Exception e) {
      return String.valueOf(request);
    }
  }

  private boolean hasSchema(TableRequests.Create request) {
    return request != null && request.schema() != null && !request.schema().isNull();
  }

  private BootstrapResult bootstrapStageCreate(
      String tableName, ai.floedb.floecat.catalog.rpc.Table table, TableRequests.Create request) {
    if (request == null) {
      throw new IllegalArgumentException("create request is required");
    }
    String tableLocation = request.location();
    if (tableLocation == null || tableLocation.isBlank()) {
      throw new IllegalArgumentException("stage-create requires a table location");
    }
    Map<String, String> props = new LinkedHashMap<>();
    if (request.properties() != null && !request.properties().isEmpty()) {
      props.putAll(request.properties());
    }
    props.putIfAbsent("table-uuid", UUID.randomUUID().toString());
    String tableUuid = props.get("table-uuid");
    if (!props.containsKey(MetadataLocationUtil.PRIMARY_KEY)) {
      MetadataLocationUtil.setMetadataLocation(
          props, MetadataLocationUtil.bootstrapMetadataLocation(tableLocation, tableUuid));
    }
    TableRequests.Create effectiveRequest =
        new TableRequests.Create(
            request.name(),
            request.schema(),
            request.location(),
            Map.copyOf(props),
            request.partitionSpec(),
            request.writeOrder(),
            request.stageCreate());
    TableMetadataView metadataView =
        TableMetadataBuilder.fromCreateRequest(tableName, table, effectiveRequest);
    return new BootstrapResult(effectiveRequest, metadataView);
  }

  private record BootstrapResult(TableRequests.Create request, TableMetadataView metadataView) {}

  private Response createRegisteredTable(
      NamespaceRef namespaceContext,
      String tableName,
      String metadataLocation,
      String idempotencyKey,
      Map<String, String> ioProperties,
      ImportedMetadata importedMetadata,
      TableGatewaySupport tableSupport) {
    Response response =
        commit(
            namespaceContext.prefix(),
            idempotencyKey,
            buildRegisterTransactionRequest(
                namespaceContext.namespacePath(),
                tableName,
                mergeImportedProperties(null, importedMetadata, metadataLocation, ioProperties),
                importedMetadata,
                List.of(),
                true),
            tableSupport);
    if (response != null && response.getStatus() == Response.Status.CONFLICT.getStatusCode()) {
      return IcebergErrorResponses.conflict("Table already exists");
    }
    return response != null && response.getStatus() != Response.Status.NO_CONTENT.getStatusCode()
        ? response
        : null;
  }

  private Response overwriteRegisteredTable(
      NamespaceRef namespaceContext,
      String tableName,
      String metadataLocation,
      String idempotencyKey,
      Map<String, String> ioProperties,
      ImportedMetadata importedMetadata,
      TableGatewaySupport tableSupport) {
    ResourceId tableId =
        tableGatewaySupport.resolveTableId(
            namespaceContext.catalogName(), namespaceContext.namespacePath(), tableName);
    ai.floedb.floecat.catalog.rpc.Table existing;
    try {
      existing = tableGatewaySupport.getTable(tableId);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return IcebergErrorResponses.noSuchTable(
            "Table "
                + String.join(".", namespaceContext.namespacePath())
                + "."
                + tableName
                + " not found");
      }
      throw e;
    }
    List<Long> existingSnapshotIds = listSnapshotIds(tableId);
    Map<String, String> props =
        mergeImportedProperties(
            existing.getPropertiesMap(), importedMetadata, metadataLocation, ioProperties);
    Response response =
        commit(
            namespaceContext.prefix(),
            idempotencyKey,
            buildRegisterTransactionRequest(
                namespaceContext.namespacePath(),
                tableName,
                props,
                importedMetadata,
                existingSnapshotIds,
                false),
            tableSupport);
    if (response != null && response.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
      return response;
    }

    if (importedMetadata.metadataView() == null) {
      return IcebergErrorResponses.failure(
          "Failed to load canonical registered table metadata",
          "InternalServerError",
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return Response.ok(
            TableResponseMapper.toLoadResult(
                importedMetadata.metadataView(),
                tableSupport.defaultTableConfig(),
                tableSupport.defaultCredentials()))
        .build();
  }

  private List<ImportedSnapshot> snapshotsToImport(ImportedMetadata importedMetadata) {
    if (importedMetadata == null) {
      return List.of();
    }
    if (importedMetadata.snapshots() != null) {
      return importedMetadata.snapshots();
    }
    if (importedMetadata.currentSnapshot() == null) {
      return List.of();
    }
    return List.of(importedMetadata.currentSnapshot());
  }

  private Map<String, Object> toSnapshotUpdate(
      ImportedSnapshot snapshot, ImportedMetadata importedMetadata) {
    if (snapshot == null) {
      return Map.of();
    }
    Map<String, Object> snapshotMap = new LinkedHashMap<>();
    if (snapshot.snapshotId() != null) {
      snapshotMap.put("snapshot-id", snapshot.snapshotId());
    }
    if (snapshot.parentSnapshotId() != null) {
      snapshotMap.put("parent-snapshot-id", snapshot.parentSnapshotId());
    }
    if (snapshot.sequenceNumber() != null) {
      snapshotMap.put("sequence-number", snapshot.sequenceNumber());
    }
    if (snapshot.timestampMs() != null) {
      snapshotMap.put("timestamp-ms", snapshot.timestampMs());
    }
    String manifestList = firstManifestList(snapshot.manifestLists());
    if (manifestList != null) {
      snapshotMap.put("manifest-list", manifestList);
    }
    if (snapshot.summary() != null && !snapshot.summary().isEmpty()) {
      snapshotMap.put("summary", snapshot.summary());
    }
    if (snapshot.schemaId() != null) {
      snapshotMap.put("schema-id", snapshot.schemaId());
    }
    if (importedMetadata != null
        && importedMetadata.schemaJson() != null
        && !importedMetadata.schemaJson().isBlank()) {
      snapshotMap.put("schema-json", importedMetadata.schemaJson());
    }
    return snapshotMap;
  }

  private List<Long> listSnapshotIds(ResourceId tableId) {
    if (tableId == null) {
      return List.of();
    }
    try {
      return grpcClient
          .listSnapshots(ListSnapshotsRequest.newBuilder().setTableId(tableId).build())
          .getSnapshotsList()
          .stream()
          .map(Snapshot::getSnapshotId)
          .toList();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return List.of();
      }
      throw e;
    }
  }

  private Map<String, String> mergeImportedProperties(
      Map<String, String> existing,
      ImportedMetadata importedMetadata,
      String metadataLocation,
      Map<String, String> registerIoProperties) {
    Map<String, String> merged = new LinkedHashMap<>();
    if (existing != null && !existing.isEmpty()) {
      merged.putAll(existing);
    }
    if (importedMetadata != null && importedMetadata.properties() != null) {
      merged.putAll(importedMetadata.properties());
    }
    if (importedMetadata != null
        && importedMetadata.tableLocation() != null
        && !importedMetadata.tableLocation().isBlank()) {
      merged.put("location", importedMetadata.tableLocation());
    }
    if (registerIoProperties != null && !registerIoProperties.isEmpty()) {
      merged.putAll(registerIoProperties);
    }
    MetadataLocationUtil.setMetadataLocation(merged, metadataLocation);
    return merged;
  }

  private static String firstManifestList(List<String> manifestLists) {
    if (manifestLists == null || manifestLists.isEmpty()) {
      return null;
    }
    for (String manifestList : manifestLists) {
      if (manifestList != null && !manifestList.isBlank()) {
        return manifestList;
      }
    }
    return null;
  }

  private record StageCommitResult(
      ai.floedb.floecat.catalog.rpc.Table table,
      LoadTableResultDto loadResult,
      boolean tableCreated) {}

  private record SnapshotChangePlan(
      List<ai.floedb.floecat.transaction.rpc.TxChange> txChanges, Response error) {}

  private record PreMaterializedTable(
      ai.floedb.floecat.catalog.rpc.Table table, TableMetadata tableMetadata, Response error) {}

  private record PlannedChange(
      List<String> namespacePath,
      ResourceId namespaceId,
      String tableName,
      ResourceId tableId,
      ai.floedb.floecat.catalog.rpc.Table table,
      TableMetadata tableMetadata,
      List<Map<String, Object>> updates,
      long expectedVersion) {}

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

  private Response buildCommitResponse(
      CommitCommand command, TableRequests.Commit req, StagedTableEntry stagedEntry) {
    ResourceId tableId =
        tableGatewaySupport.resolveTableId(
            command.catalogName(), command.namespacePath(), command.table());
    ai.floedb.floecat.catalog.rpc.Table committedTable = tableGatewaySupport.getTable(tableId);
    TableGatewaySupport tableSupport = command.tableSupport();
    StageCommitResult stagedResponse =
        buildStagedCommitResult(command, req, stagedEntry, committedTable);

    CommitTableResponseDto finalResponse =
        buildFinalResponse(committedTable, stagedResponse, req, tableSupport);
    if (finalResponse == null
        || finalResponse.metadata() == null
        || finalResponse.metadataLocation() == null
        || finalResponse.metadataLocation().isBlank()) {
      return IcebergErrorResponses.failure(
          "Commit response missing metadata",
          "InternalServerError",
          Response.Status.INTERNAL_SERVER_ERROR);
    }

    return Response.ok(finalResponse).build();
  }

  private StageCommitResult buildStagedCommitResult(
      CommitCommand command,
      TableRequests.Commit req,
      StagedTableEntry stagedEntry,
      ai.floedb.floecat.catalog.rpc.Table table) {
    if (stagedEntry == null || stagedEntry.request() == null || table == null) {
      return null;
    }
    if (CommitUpdateInspector.inspect(req).containsSnapshotUpdates()) {
      return null;
    }
    TableGatewaySupport tableSupport = command.tableSupport();
    List<StorageCredentialDto> credentials =
        tableSupport == null ? List.of() : tableSupport.defaultCredentials();
    LoadTableResultDto loadResult =
        TableResponseMapper.toLoadResult(
            TableMetadataBuilder.fromCreateRequest(command.table(), table, stagedEntry.request()),
            tableSupport == null ? Map.of() : tableSupport.defaultTableConfig(),
            credentials);
    return new StageCommitResult(table, loadResult, false);
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
    if (tableState != null) {
      return req;
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
      return stagedTableService.getStage(key);
    }
    return stagedTableService.findSingleStage(
        accountId, command.catalogName(), command.namespacePath(), command.table());
  }

  private CommitTableResponseDto buildFinalResponse(
      ai.floedb.floecat.catalog.rpc.Table committedTable,
      StageCommitResult stageMaterialization,
      TableRequests.Commit req,
      TableGatewaySupport tableSupport) {
    CommitUpdateInspector.Parsed parsed = CommitUpdateInspector.inspect(req);
    if (!parsed.containsSnapshotUpdates() && stageMaterialization != null) {
      CommitTableResponseDto stagedResponse = preferStageMetadata(null, stageMaterialization);
      return preferRequestedMetadata(stagedResponse, parsed.requestedMetadataLocation());
    }
    IcebergMetadata metadata =
        icebergMetadataService.resolveCurrentIcebergMetadata(committedTable, tableSupport);
    var resolved =
        icebergMetadataService.resolveMetadata(
            committedTable == null ? null : committedTable.getDisplayName(),
            committedTable,
            metadata,
            tableSupport.defaultFileIoProperties(),
            () ->
                ai.floedb.floecat.gateway.iceberg.rest.catalog.SnapshotLister.fetchSnapshots(
                    grpcClient,
                    committedTable.getResourceId(),
                    ai.floedb.floecat.gateway.iceberg.rest.catalog.SnapshotLister.Mode.ALL,
                    metadata));
    CommitTableResponseDto finalResponse =
        resolved.metadataView() == null
            ? null
            : ensureMetadataLocation(
                TableResponseMapper.toCommitResponse(resolved.metadataView()),
                committedTable,
                resolved.icebergMetadata());
    if (!parsed.containsSnapshotUpdates()) {
      finalResponse = preferStageMetadata(finalResponse, stageMaterialization);
    }
    return preferRequestedMetadata(finalResponse, parsed.requestedMetadataLocation());
  }

  private CommitTableResponseDto preferStageMetadata(
      CommitTableResponseDto response, StageCommitResult stageMaterialization) {
    if (stageMaterialization == null || stageMaterialization.loadResult() == null) {
      return response;
    }
    LoadTableResultDto staged = stageMaterialization.loadResult();
    TableMetadataView stagedMetadata = staged.metadata();
    String stagedLocation = staged.metadataLocation();
    if ((stagedLocation == null || stagedLocation.isBlank())
        && stagedMetadata != null
        && stagedMetadata.metadataLocation() != null
        && !stagedMetadata.metadataLocation().isBlank()) {
      stagedLocation = stagedMetadata.metadataLocation();
    }
    if (stagedLocation == null || stagedLocation.isBlank()) {
      if (stagedMetadata == null) {
        return response;
      }
      boolean responseIncomplete =
          response == null
              || response.metadata() == null
              || response.metadata().formatVersion() == null
              || response.metadata().schemas() == null
              || response.metadata().schemas().isEmpty();
      if (!responseIncomplete) {
        return response;
      }
      return commitResponse(stagedMetadata);
    }
    String originalLocation = response == null ? "<null>" : response.metadataLocation();
    LOG.infof(
        "Stage metadata evaluation stagedLocation=%s originalLocation=%s",
        stagedLocation, originalLocation);
    if (stagedMetadata != null) {
      stagedMetadata = stagedMetadata.withMetadataLocation(stagedLocation);
    }
    if (response != null
        && stagedLocation.equals(response.metadataLocation())
        && Objects.equals(stagedMetadata, response.metadata())) {
      return response;
    }
    LOG.infof("Preferring staged metadata location %s over %s", stagedLocation, originalLocation);
    return commitResponse(stagedMetadata);
  }

  private CommitTableResponseDto preferRequestedMetadata(
      CommitTableResponseDto response, String requestedMetadataLocation) {
    if (requestedMetadataLocation == null || requestedMetadataLocation.isBlank()) {
      return response;
    }
    TableMetadataView metadata = response == null ? null : response.metadata();
    if (metadata != null) {
      metadata = metadata.withMetadataLocation(requestedMetadataLocation);
    }
    if (response != null && requestedMetadataLocation.equals(response.metadataLocation())) {
      if (Objects.equals(metadata, response.metadata())) {
        return response;
      }
    }
    return commitResponse(metadata);
  }

  private CommitTableResponseDto commitResponse(TableMetadataView metadata) {
    if (metadata == null) {
      return new CommitTableResponseDto(null, null);
    }
    return new CommitTableResponseDto(metadata.metadataLocation(), metadata);
  }

  private CommitTableResponseDto ensureMetadataLocation(
      CommitTableResponseDto response,
      ai.floedb.floecat.catalog.rpc.Table committedTable,
      IcebergMetadata metadata) {
    if (response == null) {
      return null;
    }
    String resolved =
        firstNonBlank(
            response.metadataLocation(),
            response.metadata() == null ? null : response.metadata().metadataLocation(),
            committedTable == null
                ? null
                : MetadataLocationUtil.metadataLocation(committedTable.getPropertiesMap()),
            metadata == null ? null : metadata.getMetadataLocation());
    if (resolved == null || resolved.isBlank()) {
      return response;
    }
    TableMetadataView metadataView = response.metadata();
    if (metadataView != null && !resolved.equals(metadataView.metadataLocation())) {
      metadataView = metadataView.withMetadataLocation(resolved);
    }
    if (resolved.equals(response.metadataLocation())
        && Objects.equals(metadataView, response.metadata())) {
      return response;
    }
    return new CommitTableResponseDto(resolved, metadataView);
  }

  private boolean callerProvidesCreateInitialization(TableRequests.Commit req) {
    if (req == null || req.updates() == null || req.updates().isEmpty()) {
      return false;
    }
    for (Map<String, Object> update : req.updates()) {
      if (isCreateInitializationUpdate(update)) {
        return true;
      }
    }
    return false;
  }

  private boolean isCreateInitializationUpdate(Map<String, Object> update) {
    if (update == null) {
      return false;
    }
    Object action = update.get("action");
    if (!(action instanceof String value)) {
      return false;
    }
    return CommitUpdateInspector.isCreateInitializationAction(value);
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
    TableRequests.Commit commitRequest =
        new TableRequests.Commit(
            requirements == null ? List.of() : List.copyOf(requirements),
            updates == null ? List.of() : List.copyOf(updates));
    CommitUpdateInspector.Parsed parsed = CommitUpdateInspector.inspect(commitRequest);
    boolean assertCreateRequested =
        hasRequirementType(requirements, CommitUpdateInspector.REQUIREMENT_ASSERT_CREATE);
    boolean firstWriteAssertCreate = assertCreateRequested && parsed.containsSnapshotUpdates();
    if (!preMaterializeAssertCreate && assertCreateRequested && !firstWriteAssertCreate) {
      return new PreMaterializedTable(plannedTable, null, null);
    }
    if (icebergMetadataService == null || materializeMetadataService == null) {
      return new PreMaterializedTable(plannedTable, null, null);
    }
    TableMetadata baseMetadata = null;
    String baseMetadataLocation = null;
    try {
      var resolved =
          icebergMetadataService.resolveMetadata(
              tableName,
              plannedTable,
              null,
              tableSupport == null ? Map.of() : tableSupport.defaultFileIoProperties(),
              List::of);
      if (resolved != null && resolved.tableMetadata() != null) {
        baseMetadata = resolved.tableMetadata();
        baseMetadataLocation = metadataLocation(resolved.tableMetadata());
      }
    } catch (RuntimeException e) {
      LOG.debugf(
          e,
          "Skipping base metadata resolution during pre-materialization tableId=%s table=%s",
          tableId == null ? "<missing>" : tableId.getId(),
          tableName);
    }
    if (baseMetadata == null) {
      if (firstWriteAssertCreate) {
        baseMetadata =
            icebergMetadataService.bootstrapTableMetadataFromCommit(plannedTable, commitRequest);
        baseMetadataLocation = metadataLocation(baseMetadata);
      } else {
        baseMetadata =
            icebergMetadataService.bootstrapTableMetadata(
                tableName,
                plannedTable,
                new LinkedHashMap<>(plannedTable.getPropertiesMap()),
                null,
                List.of());
        baseMetadataLocation = metadataLocation(baseMetadata);
      }
    }
    if (baseMetadata == null) {
      return new PreMaterializedTable(plannedTable, null, null);
    }

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
    if (requestedMetadataLocation != null
        && !requestedMetadataLocation.isBlank()
        && !firstWriteAssertCreate) {
      return new PreMaterializedTable(canonicalizedTable, canonicalMetadata, null);
    }

    MaterializeMetadataService.MaterializeResult materialized;
    try {
      materialized =
          materializeMetadataService.materialize(
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

      ai.floedb.floecat.catalog.rpc.Snapshot snapshot;
      try {
        snapshot =
            fetchOrBuildSnapshotPayload(
                scopedTableId, table, committedMetadata, tableSupport, snapshotId, snapshotMap);
      } catch (StatusRuntimeException e) {
        return new SnapshotChangePlan(List.of(), mapPreCommitFailure(e));
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

  private ai.floedb.floecat.catalog.rpc.Snapshot fetchOrBuildSnapshotPayload(
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
        ai.floedb.floecat.catalog.rpc.Snapshot existing = resp.getSnapshot();
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

    ai.floedb.floecat.catalog.rpc.Snapshot.Builder builder =
        ai.floedb.floecat.catalog.rpc.Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId);
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
    if (resolvedOperation != null && !resolvedOperation.isBlank()) {
      // Persist Iceberg-only snapshot fields in format_metadata rather than the generic catalog
      // snapshot schema.
    }
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

  private boolean isCommitAccepted(TransactionState state) {
    return state == TransactionState.TS_APPLIED;
  }

  private boolean isDeterministicFailedState(TransactionState state) {
    return state == TransactionState.TS_APPLY_FAILED_CONFLICT
        || state == TransactionState.TS_ABORTED;
  }

  private boolean isDeterministicCommitFailure(StatusRuntimeException failure) {
    Status.Code code = failure.getStatus().getCode();
    if (code == Status.Code.ABORTED) {
      ErrorCode detailCode = extractFloecatErrorCode(failure);
      if (detailCode == ErrorCode.MC_ABORT_RETRYABLE) {
        return false;
      }
      if (detailCode == ErrorCode.MC_CONFLICT || detailCode == ErrorCode.MC_PRECONDITION_FAILED) {
        return true;
      }
      return true;
    }
    return code == Status.Code.FAILED_PRECONDITION || code == Status.Code.ALREADY_EXISTS;
  }

  private boolean isRetryableCommitAbort(StatusRuntimeException failure) {
    if (failure.getStatus().getCode() != Status.Code.ABORTED) {
      return false;
    }
    return extractFloecatErrorCode(failure) == ErrorCode.MC_ABORT_RETRYABLE;
  }

  private ErrorCode extractFloecatErrorCode(StatusRuntimeException exception) {
    var statusProto = StatusProto.fromThrowable(exception);
    if (statusProto == null) {
      return null;
    }
    for (com.google.protobuf.Any detail : statusProto.getDetailsList()) {
      if (!detail.is(Error.class)) {
        continue;
      }
      try {
        return detail.unpack(Error.class).getCode();
      } catch (Exception ignored) {
        // Continue scanning details.
      }
    }
    return null;
  }

  private boolean waitForAppliedState(String txId) {
    if (txId == null || txId.isBlank()) {
      return false;
    }
    int maxAttempts =
        Math.max(
            1,
            ConfigProvider.getConfig()
                .getOptionalValue("floecat.gateway.commit.confirm.max-attempts", Integer.class)
                .orElse(DEFAULT_COMMIT_CONFIRM_MAX_ATTEMPTS));
    long sleepMillis =
        Math.max(
            0L,
            ConfigProvider.getConfig()
                .getOptionalValue("floecat.gateway.commit.confirm.initial-sleep-ms", Long.class)
                .orElse(DEFAULT_COMMIT_CONFIRM_INITIAL_SLEEP_MS));
    long maxSleepMillis =
        Math.max(
            sleepMillis,
            ConfigProvider.getConfig()
                .getOptionalValue("floecat.gateway.commit.confirm.max-sleep-ms", Long.class)
                .orElse(DEFAULT_COMMIT_CONFIRM_MAX_SLEEP_MS));

    for (int attempt = 0; attempt < maxAttempts; attempt++) {
      try {
        var current =
            grpcClient.getTransaction(GetTransactionRequest.newBuilder().setTxId(txId).build());
        if (current != null && current.hasTransaction()) {
          TransactionState state = current.getTransaction().getState();
          if (state == TransactionState.TS_APPLIED) {
            return true;
          }
          if (state == TransactionState.TS_APPLY_FAILED_CONFLICT
              || state == TransactionState.TS_ABORTED) {
            return false;
          }
        }
      } catch (RuntimeException ignored) {
        return false;
      }
      if (attempt + 1 < maxAttempts && sleepMillis > 0L) {
        try {
          Thread.sleep(sleepMillis);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return false;
        }
        sleepMillis = Math.min(maxSleepMillis, Math.max(1L, sleepMillis * 2L));
      }
    }
    return false;
  }

  private boolean shouldConfirmAmbiguousCommitState(TransactionState state) {
    return CONFIRMABLE_COMMIT_STATES.contains(state);
  }

  private Response mapPreCommitFailure(StatusRuntimeException failure) {
    Status.Code code = failure.getStatus().getCode();
    if (code == Status.Code.NOT_FOUND) {
      return IcebergErrorResponses.failure(
          "transaction commit failed", "NoSuchTableException", Response.Status.NOT_FOUND);
    }
    if (code == Status.Code.FAILED_PRECONDITION || code == Status.Code.ALREADY_EXISTS) {
      return IcebergErrorResponses.failure(
          "transaction commit failed", "CommitFailedException", Response.Status.CONFLICT);
    }
    if (code == Status.Code.ABORTED) {
      ErrorCode detailCode = extractFloecatErrorCode(failure);
      if (detailCode == ErrorCode.MC_ABORT_RETRYABLE) {
        return IcebergErrorResponses.failure(
            "transaction commit failed",
            "CommitStateUnknownException",
            Response.Status.SERVICE_UNAVAILABLE);
      }
      return IcebergErrorResponses.failure(
          "transaction commit failed", "CommitFailedException", Response.Status.CONFLICT);
    }
    Response mapped = mapCommitFailureByStatus(code);
    return mapped == null ? preCommitStateUnknown() : mapped;
  }

  private Response mapCommitFailureByStatus(Status.Code code) {
    if (code == Status.Code.INVALID_ARGUMENT) {
      return IcebergErrorResponses.failure(
          "transaction commit failed", "ValidationException", Response.Status.BAD_REQUEST);
    }
    if (code == Status.Code.UNAVAILABLE) {
      return IcebergErrorResponses.failure(
          "transaction commit failed",
          "CommitStateUnknownException",
          Response.Status.SERVICE_UNAVAILABLE);
    }
    if (code == Status.Code.UNAUTHENTICATED) {
      return IcebergErrorResponses.failure(
          "transaction commit failed", "UnauthorizedException", Response.Status.UNAUTHORIZED);
    }
    if (code == Status.Code.PERMISSION_DENIED) {
      return IcebergErrorResponses.failure(
          "transaction commit failed", "ForbiddenException", Response.Status.FORBIDDEN);
    }
    if (code == Status.Code.UNKNOWN) {
      return IcebergErrorResponses.failure(
          "transaction commit failed", "CommitStateUnknownException", Response.Status.BAD_GATEWAY);
    }
    if (code == Status.Code.DEADLINE_EXCEEDED) {
      return IcebergErrorResponses.failure(
          "transaction commit failed",
          "CommitStateUnknownException",
          Response.Status.GATEWAY_TIMEOUT);
    }
    return null;
  }

  private Response preCommitStateUnknown() {
    return IcebergErrorResponses.failure(
        "transaction commit failed",
        "CommitStateUnknownException",
        Response.Status.INTERNAL_SERVER_ERROR);
  }

  private void abortTransactionQuietly(String txId, String reason) {
    if (txId == null || txId.isBlank()) {
      return;
    }
    try {
      grpcClient.abortTransaction(
          ai.floedb.floecat.transaction.rpc.AbortTransactionRequest.newBuilder()
              .setTxId(txId)
              .setReason(reason == null ? "" : reason)
              .build());
    } catch (RuntimeException e) {
      LOG.debugf(e, "Best-effort abort failed for tx=%s", txId);
    }
  }

  private void maybeAbortOpenTransaction(
      TransactionState currentState, String txId, String reason) {
    if (currentState == TransactionState.TS_OPEN) {
      abortTransactionQuietly(txId, reason);
    }
  }

  private Response validateKnownRequirementTypes(List<Map<String, Object>> requirements) {
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
    return null;
  }

  private Response validateAssertCreateRequirement(
      List<Map<String, Object>> requirements,
      ai.floedb.floecat.catalog.rpc.GetTableResponse tableResponse) {
    if (!hasRequirementType(requirements, CommitUpdateInspector.REQUIREMENT_ASSERT_CREATE)) {
      return null;
    }
    if (tableResponse != null && tableResponse.hasTable()) {
      return IcebergErrorResponses.failure(
          "assert-create failed", "CommitFailedException", Response.Status.CONFLICT);
    }
    return null;
  }

  private Response validateKnownUpdateActions(List<Map<String, Object>> updates) {
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

  private boolean hasRequirementType(List<Map<String, Object>> requirements, String type) {
    if (requirements == null || requirements.isEmpty() || type == null || type.isBlank()) {
      return false;
    }
    for (Map<String, Object> requirement : requirements) {
      String requirementType =
          requirement == null ? null : requirement.get("type") instanceof String s ? s : null;
      if (type.equals(requirementType)) {
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
    UUID deterministicId = UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8));
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
      if (hasSnapshotRef(tableSupport, table, refName)) {
        return IcebergErrorResponses.failure(
            "assert-ref-snapshot-id failed for ref " + refName,
            "CommitFailedException",
            Response.Status.CONFLICT);
      }
    }
    return null;
  }

  private boolean hasSnapshotRef(
      TableGatewaySupport tableSupport, ai.floedb.floecat.catalog.rpc.Table table, String refName) {
    if (refName == null || refName.isBlank()) {
      return false;
    }
    try {
      var metadata =
          icebergMetadataService == null
              ? null
              : icebergMetadataService.resolveCurrentIcebergMetadata(table, tableSupport);
      if (metadata != null) {
        if (metadata.getRefsMap().containsKey(refName)
            && metadata.getRefsOrThrow(refName).getSnapshotId() >= 0L) {
          return true;
        }
        // currentSnapshotId defaults to 0 in proto3 when unset; only positive values are
        // unambiguous here. Explicit version 0 is handled via refs/properties.
        if ("main".equals(refName) && metadata.getCurrentSnapshotId() > 0L) {
          return true;
        }
      }
    } catch (RuntimeException ignored) {
      // Fall back to table properties.
    }
    if (table == null) {
      return false;
    }
    if ("main".equals(refName)
        && TableMappingUtil.asLong(table.getPropertiesMap().get("current-snapshot-id")) != null
        && TableMappingUtil.asLong(table.getPropertiesMap().get("current-snapshot-id")) >= 0L) {
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
