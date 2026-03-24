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
import ai.floedb.floecat.gateway.iceberg.rest.table.IcebergMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.table.StageState;
import ai.floedb.floecat.gateway.iceberg.rest.table.StagedTableEntry;
import ai.floedb.floecat.gateway.iceberg.rest.table.StagedTableKey;
import ai.floedb.floecat.gateway.iceberg.rest.table.StagedTableRepository;
import ai.floedb.floecat.gateway.iceberg.rest.table.TableLifecycleService;
import ai.floedb.floecat.transaction.rpc.GetTransactionRequest;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@ApplicationScoped
public class TransactionCommitService {
  private static final String TX_REQUEST_HASH_PROPERTY = "iceberg.commit.request-hash";
  @Inject IcebergGatewayConfig config;
  @Inject AccountContext accountContext;
  @Inject ResourceResolver resourceResolver;
  @Inject TableGatewaySupport tableGatewaySupport;
  @Inject TableFormatSupport tableFormatSupport;
  @Inject IcebergMetadataService icebergMetadataService;
  @Inject StagedTableRepository stagedTableRepository;
  @Inject TableLifecycleService tableLifecycleService;
  @Inject TransactionExecutor transactionExecutor;
  @Inject CommitPlanBuilder commitPlanBuilder;
  @Inject CreateCommitNormalizer createCommitNormalizer;
  @Inject TransactionAborter transactionAborter;
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
        createCommitNormalizer.normalizeFirstWriteCommit(
            accountContext.getAccountId(),
            command.catalogName(),
            command.namespacePath(),
            command.table(),
            command.catalogId(),
            command.namespaceId(),
            preCommitTable != null,
            preCommitTable,
            req,
            stagedEntryOpt.orElse(null),
            command.tableSupport());

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
    stagedEntryOpt.ifPresent(entry -> stagedTableRepository.deleteStage(entry.key()));
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
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to build create table specification", e);
    }

    List<Map<String, Object>> updates = CreateUpdateFactory.fromCreateRequest(request, spec);
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
    Response identifierValidationError = validateTableIdentifiers(changes);
    if (identifierValidationError != null) {
      return identifierValidationError;
    }
    String duplicateIdentifier = firstDuplicateTableIdentifier(changes);
    if (duplicateIdentifier != null) {
      return IcebergErrorResponses.validation(
          "duplicate table identifier in table-changes: " + duplicateIdentifier);
    }
    CatalogRef catalogContext = resourceResolver.catalog(prefix);
    String catalogName = catalogContext.catalogName();
    ResourceId catalogId = catalogContext.catalogId();

    String requestHash = CommitRequestHasher.hash(changes);
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
      return transactionExecutor.mapPrepareFailure(beginFailure);
    } catch (RuntimeException beginFailure) {
      return transactionExecutor.stateUnknown();
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
      transactionAborter.abortQuietly(txId, "failed to load transaction");
      return transactionExecutor.mapPrepareFailure(e);
    } catch (RuntimeException e) {
      transactionAborter.abortQuietly(txId, "failed to load transaction");
      return transactionExecutor.stateUnknown();
    }
    if (currentTxn != null && currentTxn.hasTransaction()) {
      String existingRequestHash =
          currentTxn.getTransaction().getPropertiesMap().get(TX_REQUEST_HASH_PROPERTY);
      if (existingRequestHash != null
          && !existingRequestHash.isBlank()
          && !existingRequestHash.equals(requestHash)) {
        transactionAborter.abortIfOpen(currentState, txId, "transaction request-hash mismatch");
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
    CommitRequestContext context =
        new CommitRequestContext(
            accountId,
            txId,
            prefix,
            catalogName,
            catalogId,
            idempotencyBase,
            requestHash,
            txCreatedAtMs,
            currentState,
            tableSupport,
            changes,
            preMaterializeAssertCreate);
    CommitPlan plan;
    try {
      plan = commitPlanBuilder.build(context);
    } catch (WebApplicationException e) {
      transactionAborter.abortIfOpen(currentState, txId, "transaction commit planning failed");
      return e.getResponse();
    } catch (StatusRuntimeException e) {
      transactionAborter.abortIfOpen(currentState, txId, "transaction commit planning failed");
      return transactionExecutor.mapPrepareFailure(e);
    } catch (RuntimeException e) {
      transactionAborter.abortIfOpen(currentState, txId, "transaction commit planning failed");
      return transactionExecutor.stateUnknown();
    }

    return transactionExecutor.execute(context, plan, tableSupport);
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
    if (table == null) {
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

  private Optional<StagedTableEntry> resolveStagedEntry(CommitCommand command) {
    String accountId = accountContext.getAccountId();
    if (accountId == null || accountId.isBlank()) {
      return Optional.empty();
    }
    // Stage-create uses the Iceberg transaction header as the persisted stage identifier.
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

  private String firstDuplicateTableIdentifier(List<TransactionCommitRequest.TableChange> changes) {
    if (changes == null || changes.isEmpty()) {
      return null;
    }
    Set<String> seen = new LinkedHashSet<>();
    for (TransactionCommitRequest.TableChange change : changes) {
      String qualifiedName = canonicalTableIdentifier(change);
      if (!seen.add(qualifiedName)) {
        return qualifiedName;
      }
    }
    return null;
  }

  private Response validateTableIdentifiers(List<TransactionCommitRequest.TableChange> changes) {
    if (changes == null) {
      return null;
    }
    for (TransactionCommitRequest.TableChange change : changes) {
      if (change == null || change.identifier() == null) {
        return IcebergErrorResponses.validation("table identifier is required");
      }
      if (change.identifier().name() == null || change.identifier().name().isBlank()) {
        return IcebergErrorResponses.validation("table identifier is required");
      }
      List<String> namespace = change.identifier().namespace();
      if (namespace == null) {
        continue;
      }
      for (String segment : namespace) {
        if (segment == null || segment.isBlank()) {
          return IcebergErrorResponses.validation(
              "table identifier namespace entries must be non-blank");
        }
      }
    }
    return null;
  }

  private String canonicalTableIdentifier(TransactionCommitRequest.TableChange change) {
    TableIdentifierDto identifier = change.identifier();
    List<String> namespacePath = new ArrayList<>();
    if (identifier.namespace() != null) {
      for (String segment : identifier.namespace()) {
        namespacePath.add(segment.trim().toLowerCase(Locale.ROOT));
      }
    }
    String name = identifier.name().trim().toLowerCase(Locale.ROOT);
    return namespacePath.isEmpty() ? name : String.join(".", namespacePath) + "." + name;
  }
}
