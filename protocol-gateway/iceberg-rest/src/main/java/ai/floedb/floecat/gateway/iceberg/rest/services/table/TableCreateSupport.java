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
import ai.floedb.floecat.gateway.iceberg.rest.catalog.NamespaceRef;
import ai.floedb.floecat.gateway.iceberg.rest.common.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StageState;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableEntry;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableKey;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableService;
import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableCreateSupport {
  private static final Logger LOG = Logger.getLogger(TableCreateSupport.class);
  private static final List<Map<String, Object>> STAGE_CREATE_REQUIREMENTS =
      CommitUpdateInspector.assertCreateRequirements();

  @Inject GrpcServiceFacade grpcClient;
  @Inject IcebergGatewayConfig config;
  @Inject StagedTableService stagedTableService;
  @Inject AccountContext accountContext;

  TableRequests.Create applyDefaultLocationIfMissing(
      NamespaceRef namespaceContext, String tableName, TableRequests.Create request) {
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

  Response handleStageCreate(
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
    if (request.location() == null || request.location().isBlank()) {
      LOG.warnf(
          "Stage-create request missing location namespace=%s table=%s stageId=%s"
              + " locationPresent=%s propertyKeyCount=%d propertyKeys=%s",
          namespaceContext.namespacePath(),
          tableName,
          resolveStageId(transactionId, idempotencyKey),
          false,
          propertyKeyCount(request),
          safePropertyKeys(request));
    }
    String accountId = accountContext.getAccountId();
    if (accountId == null || accountId.isBlank()) {
      return IcebergErrorResponses.validation("account context is required");
    }
    String stageId = resolveStageId(transactionId, idempotencyKey);
    try {
      LOG.infof(
          "Stage-create request namespace=%s table=%s stageId=%s locationPresent=%s"
              + " propertyKeyCount=%d propertyKeys=%s",
          namespaceContext.namespacePath(),
          tableName,
          stageId,
          request.location() != null && !request.location().isBlank(),
          propertyKeyCount(request),
          safePropertyKeys(request));
      TableSpec spec =
          tableSupport
              .buildCreateSpec(
                  namespaceContext.catalogId(), namespaceContext.namespaceId(), tableName, request)
              .build();
      StagedTableEntry stored =
          stagedTableService.saveStage(
              new StagedTableEntry(
                  new StagedTableKey(
                      accountId,
                      namespaceContext.catalogName(),
                      namespaceContext.namespacePath(),
                      tableName,
                      stageId),
                  namespaceContext.catalogId(),
                  namespaceContext.namespaceId(),
                  sanitizeStageRequest(request),
                  spec,
                  STAGE_CREATE_REQUIREMENTS,
                  StageState.STAGED,
                  null,
                  null,
                  idempotencyKey));
      LOG.infof(
          "Stored stage-create payload account=%s catalog=%s namespace=%s table=%s stageId=%s"
              + " txnHeader=%s",
          accountContext.getAccountId(),
          namespaceContext.catalogName(),
          namespaceContext.namespacePath(),
          tableName,
          stored.key().stageId(),
          transactionId);
      Table stubTable = stageCreateTable(spec);
      List<StorageCredentialDto> credentials =
          tableSupport.credentialsForAccessDelegation(stubTable, accessDelegationMode);
      LoadTableResultDto loadResult =
          TableResponseMapper.toLoadResultFromCreate(
              tableName,
              stubTable,
              request,
              tableSupport.defaultTableConfig(stubTable),
              credentials);
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

  private static Table stageCreateTable(TableSpec spec) {
    if (spec == null) {
      return Table.getDefaultInstance();
    }
    Table.Builder table =
        Table.newBuilder()
            .setCatalogId(spec.getCatalogId())
            .setNamespaceId(spec.getNamespaceId())
            .setDisplayName(spec.getDisplayName())
            .putAllProperties(spec.getPropertiesMap());
    if (spec.hasUpstream()) {
      table.setUpstream(spec.getUpstream());
    }
    return table.build();
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

  private String resolveDefaultLocation(NamespaceRef namespaceContext, String tableName) {
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

  private String resolveNamespaceLocation(NamespaceRef namespaceContext) {
    if (namespaceContext == null || namespaceContext.namespaceId() == null) {
      return null;
    }
    try {
      Namespace namespace =
          grpcClient
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

  private static int propertyKeyCount(TableRequests.Create request) {
    return request == null || request.properties() == null ? 0 : request.properties().size();
  }

  private static Set<String> safePropertyKeys(TableRequests.Create request) {
    Map<String, String> properties = sanitizedStageProperties(request);
    if (properties.isEmpty()) {
      return Set.of();
    }
    Set<String> keys = new LinkedHashSet<>(properties.keySet());
    return keys.isEmpty() ? Set.of() : Set.copyOf(keys);
  }

  private static TableRequests.Create sanitizeStageRequest(TableRequests.Create request) {
    if (request == null) {
      return null;
    }
    return new TableRequests.Create(
        request.name(),
        request.schema(),
        request.location(),
        sanitizedStageProperties(request),
        request.partitionSpec(),
        request.writeOrder(),
        request.stageCreate());
  }

  private static Map<String, String> sanitizedStageProperties(TableRequests.Create request) {
    if (request == null || request.properties() == null || request.properties().isEmpty()) {
      return Map.of();
    }
    java.util.LinkedHashMap<String, String> sanitized = new java.util.LinkedHashMap<>();
    request
        .properties()
        .forEach(
            (key, value) -> {
              if (key == null || isSensitivePropertyKey(key)) {
                return;
              }
              sanitized.put(key, value);
            });
    return sanitized.isEmpty() ? Map.of() : Map.copyOf(sanitized);
  }

  private static boolean isSensitivePropertyKey(String key) {
    String normalized = key == null ? "" : key.trim().toLowerCase(Locale.ROOT);
    if (normalized.isEmpty()) {
      return true;
    }
    if (FileIoFactory.isFileIoProperty(normalized)) {
      return true;
    }
    return normalized.contains("secret")
        || normalized.contains("token")
        || normalized.contains("password")
        || normalized.contains("credential")
        || normalized.contains("access-key")
        || normalized.contains("secret-key");
  }
}
