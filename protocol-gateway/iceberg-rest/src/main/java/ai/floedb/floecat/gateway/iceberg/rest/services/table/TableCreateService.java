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

import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.firstNonBlank;

import ai.floedb.floecat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.IcebergHttpUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.NamespaceRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.NamespaceClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StageState;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableEntry;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableKey;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableCreateService {
  private static final Logger LOG = Logger.getLogger(TableCreateService.class);
  private static final List<Map<String, Object>> STAGE_CREATE_REQUIREMENTS =
      List.of(Map.of("type", "assert-create"));

  @Inject TableLifecycleService tableLifecycleService;
  @Inject NamespaceClient namespaceClient;
  @Inject IcebergGatewayConfig config;
  @Inject StagedTableService stagedTableService;
  @Inject AccountContext accountContext;
  @Inject ObjectMapper mapper;

  public Response create(
      NamespaceRequestContext namespaceContext,
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

    TableSpec.Builder spec;
    try {
      spec =
          tableSupport.buildCreateSpec(
              namespaceContext.catalogId(),
              namespaceContext.namespaceId(),
              tableName,
              effectiveReq);
    } catch (IllegalArgumentException | JsonProcessingException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
    Table created = tableLifecycleService.createTable(spec, idempotencyKey);
    Map<String, String> tableConfig = tableSupport.defaultTableConfig();
    List<StorageCredentialDto> credentials;
    try {
      credentials = tableSupport.credentialsForAccessDelegation(accessDelegationMode);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
    LoadTableResultDto loadResult;
    try {
      loadResult =
          TableResponseMapper.toLoadResultFromCreate(
              tableName, created, effectiveReq, tableConfig, credentials);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }

    var responseMetadata = loadResult.metadata();
    String responseMetadataLocation = loadResult.metadataLocation();
    if (responseMetadata != null && responseMetadataLocation != null) {
      responseMetadata = responseMetadata.withMetadataLocation(responseMetadataLocation);
    }
    Map<String, String> responseConfig =
        loadResult.config() == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(loadResult.config());
    if (responseMetadataLocation != null && !responseMetadataLocation.isBlank()) {
      String metadataDirectory =
          MetadataLocationUtil.canonicalMetadataDirectory(responseMetadataLocation);
      if (metadataDirectory != null && !metadataDirectory.isBlank()) {
        responseConfig.put("write.metadata.path", metadataDirectory);
      }
    }
    loadResult =
        new LoadTableResultDto(
            responseMetadataLocation,
            responseMetadata,
            Map.copyOf(responseConfig),
            loadResult.storageCredentials());
    LOG.infof(
        "Create table response namespace=%s table=%s metadata=%s location=%s configKeys=%s",
        namespaceContext.namespacePath(),
        tableName,
        responseMetadata == null ? "<null>" : responseMetadata.metadataLocation(),
        responseMetadataLocation,
        loadResult.config().keySet());

    Response.ResponseBuilder builder = Response.ok(loadResult);
    if (responseMetadataLocation != null) {
      builder.header(
          HttpHeaders.ETAG, IcebergHttpUtil.etagForMetadataLocation(responseMetadataLocation));
    }
    return builder.build();
  }

  private Response handleStageCreate(
      NamespaceRequestContext namespaceContext,
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
    String stageId = resolveStageId(transactionId, idempotencyKey);
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
      TableSpec.Builder specBuilder =
          tableSupport.buildCreateSpec(
              namespaceContext.catalogId(),
              namespaceContext.namespaceId(),
              tableName,
              effectiveReq);
      TableSpec spec = specBuilder.build();
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
      Table stubTable =
          Table.newBuilder()
              .setCatalogId(namespaceContext.catalogId())
              .setNamespaceId(namespaceContext.namespaceId())
              .setDisplayName(tableName)
              .build();
      List<StorageCredentialDto> credentials;
      try {
        credentials = tableSupport.credentialsForAccessDelegation(accessDelegationMode);
      } catch (IllegalArgumentException e) {
        return IcebergErrorResponses.validation(e.getMessage());
      }
      LoadTableResultDto loadResult =
          TableResponseMapper.toLoadResultFromCreate(
              tableName, stubTable, effectiveReq, tableSupport.defaultTableConfig(), credentials);
      LOG.infof(
          "Stage-create metadata resolved stageId=%s location=%s",
          stored.key().stageId(), loadResult.metadataLocation());
      LOG.infof(
          "Stage-create response stageId=%s metadataLocation=%s configKeys=%s",
          stored.key().stageId(), loadResult.metadataLocation(), loadResult.config().keySet());
      return Response.ok(loadResult).build();
    } catch (IllegalArgumentException | JsonProcessingException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
  }

  private TableRequests.Create applyDefaultLocationIfMissing(
      NamespaceRequestContext namespaceContext, String tableName, TableRequests.Create request) {
    if (request == null) {
      return null;
    }
    if (request.location() != null && !request.location().isBlank()) {
      return request;
    }
    String resolved = resolveDefaultLocation(namespaceContext, tableName);
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

  private String resolveStageId(String transactionId, String idempotencyKey) {
    if (transactionId != null && !transactionId.isBlank()) {
      return transactionId.trim();
    }
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      return idempotencyKey.trim();
    }
    return UUID.randomUUID().toString();
  }

  private String resolveDefaultLocation(
      NamespaceRequestContext namespaceContext, String tableName) {
    if (namespaceContext == null || tableName == null || tableName.isBlank()) {
      return null;
    }
    String namespaceLocation = resolveNamespaceLocation(namespaceContext);
    if (namespaceLocation != null && !namespaceLocation.isBlank()) {
      return joinLocation(namespaceLocation, List.of(tableName));
    }
    String warehouse = config.defaultWarehousePath().orElse(null);
    if (warehouse == null || warehouse.isBlank()) {
      return null;
    }
    return joinLocation(warehouse, joinNamespaceParts(namespaceContext.namespacePath(), tableName));
  }

  private String resolveNamespaceLocation(NamespaceRequestContext namespaceContext) {
    if (namespaceContext == null || namespaceContext.namespaceId() == null) {
      return null;
    }
    try {
      Namespace namespace =
          namespaceClient
              .getNamespace(
                  GetNamespaceRequest.newBuilder()
                      .setNamespaceId(namespaceContext.namespaceId())
                      .build())
              .getNamespace();
      if (namespace == null) {
        return null;
      }
      Map<String, String> props = namespace.getPropertiesMap();
      if (props == null || props.isEmpty()) {
        return null;
      }
      return firstNonBlank(props.get("location"), props.get("warehouse"));
    } catch (Exception e) {
      LOG.debugf(
          e, "Failed to resolve namespace location for %s", namespaceContext.namespacePath());
      return null;
    }
  }

  private List<String> joinNamespaceParts(List<String> namespacePath, String tableName) {
    List<String> parts = namespacePath == null ? new ArrayList<>() : new ArrayList<>(namespacePath);
    if (tableName != null && !tableName.isBlank()) {
      parts.add(tableName);
    }
    return parts;
  }

  private String joinLocation(String base, List<String> parts) {
    if (base == null || base.isBlank()) {
      return base;
    }
    String normalized = base;
    while (normalized.endsWith("/") && normalized.length() > 1) {
      normalized = normalized.substring(0, normalized.length() - 1);
    }
    String suffix =
        parts == null
            ? ""
            : parts.stream()
                .filter(part -> part != null && !part.isBlank())
                .reduce("", (left, right) -> left.isEmpty() ? right : left + "/" + right);
    if (suffix.isBlank()) {
      return normalized;
    }
    return normalized + "/" + suffix;
  }

  private String safeSerializeCreate(TableRequests.Create request) {
    if (request == null) {
      return "<null>";
    }
    try {
      return mapper.writeValueAsString(request);
    } catch (JsonProcessingException e) {
      return String.valueOf(request);
    }
  }

  private boolean hasSchema(TableRequests.Create request) {
    if (request == null) {
      return false;
    }
    return request.schema() != null && !request.schema().isNull();
  }
}
