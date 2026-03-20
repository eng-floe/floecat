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

import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.asInteger;
import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.asLong;
import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.asObjectMap;
import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.asString;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.support.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.support.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rest.support.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.FieldMask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

@ApplicationScoped
public class TableUpdatePlanner {

  @Inject IcebergMetadataService icebergMetadataService;
  @Inject TablePropertyService tablePropertyService;
  @Inject SnapshotUpdateService snapshotUpdateService;
  @Inject ObjectMapper mapper;

  public UpdatePlan planUpdates(
      TransactionCommitService.CommitCommand command,
      Supplier<Table> tableSupplier,
      Supplier<Table> requirementTableSupplier,
      ResourceId tableId) {
    return planInternal(command, tableSupplier, requirementTableSupplier, false);
  }

  public UpdatePlan planUpdates(
      TransactionCommitService.CommitCommand command,
      Supplier<Table> tableSupplier,
      ResourceId tableId) {
    return planUpdates(command, tableSupplier, tableSupplier, tableId);
  }

  public UpdatePlan planTransactionUpdates(
      TransactionCommitService.CommitCommand command,
      Supplier<Table> tableSupplier,
      Supplier<Table> requirementTableSupplier,
      ResourceId tableId) {
    return planInternal(command, tableSupplier, requirementTableSupplier, true);
  }

  private UpdatePlan planInternal(
      TransactionCommitService.CommitCommand command,
      Supplier<Table> tableSupplier,
      Supplier<Table> requirementTableSupplier,
      boolean validateSnapshotUpdates) {
    TableRequests.Commit req = command.request();
    TableSpec.Builder spec = TableSpec.newBuilder();
    FieldMask.Builder mask = FieldMask.newBuilder();
    if (req == null) {
      return UpdatePlan.failure(spec, mask, validationError("Request body is required"));
    }
    if (req.requirements() == null) {
      return UpdatePlan.failure(spec, mask, validationError("requirements are required"));
    }
    if (req.updates() == null) {
      return UpdatePlan.failure(spec, mask, validationError("updates are required"));
    }
    Response requirementError =
        validateRequirements(
            command.tableSupport(),
            req.requirements(),
            requirementTableSupplier,
            IcebergErrorResponses::validation,
            this::commitConflictError);
    if (requirementError != null) {
      return UpdatePlan.failure(spec, mask, requirementError);
    }
    Map<String, String> mergedProps = null;
    Response locationError =
        tablePropertyService.applyLocationUpdate(spec, mask, tableSupplier, req.updates());
    if (locationError != null) {
      return UpdatePlan.failure(spec, mask, locationError);
    }
    String unsupported = unsupportedUpdateAction(req);
    if (unsupported != null) {
      return UpdatePlan.failure(
          spec, mask, validationError("unsupported commit update action: " + unsupported));
    }
    if (validateSnapshotUpdates) {
      Response snapshotValidation = snapshotUpdateService.validateSnapshotUpdates(req.updates());
      if (snapshotValidation != null) {
        return UpdatePlan.failure(spec, mask, snapshotValidation);
      }
    }
    var propertyResult =
        tablePropertyService.applyCommitPropertyUpdates(tableSupplier, mergedProps, req.updates());
    if (propertyResult.hasError()) {
      return UpdatePlan.failure(spec, mask, propertyResult.error());
    }
    mergedProps = propertyResult.properties();
    applyTableDefinitionSpecUpdates(spec, mask, req.updates());
    mergedProps = stripFileIoProperties(mergedProps);
    if (mergedProps != null) {
      spec.clearProperties().putAllProperties(mergedProps);
      mask.addPaths("properties");
    }
    return UpdatePlan.success(spec, mask);
  }

  public UpdatePlan planTransactionUpdates(
      TransactionCommitService.CommitCommand command,
      Supplier<Table> tableSupplier,
      ResourceId tableId) {
    return planTransactionUpdates(command, tableSupplier, tableSupplier, tableId);
  }

  public PlanResult planTransaction(
      TransactionCommitService.CommitCommand command,
      Supplier<Table> tableSupplier,
      Supplier<Table> requirementTableSupplier,
      ResourceId tableId) {
    var updatePlan =
        planTransactionUpdates(command, tableSupplier, requirementTableSupplier, tableId);
    if (updatePlan.hasError()) {
      return new PlanResult(null, updatePlan.error());
    }
    Table current = tableSupplier.get();
    Table next = applySpec(current, updatePlan.spec(), updatePlan.mask());
    return new PlanResult(next, null);
  }

  public PlanResult planTransaction(
      TransactionCommitService.CommitCommand command,
      Supplier<Table> tableSupplier,
      ResourceId tableId) {
    return planTransaction(command, tableSupplier, tableSupplier, tableId);
  }

  public record PlanResult(Table table, Response error) {
    public boolean hasError() {
      return error != null;
    }
  }

  public record UpdatePlan(TableSpec.Builder spec, FieldMask.Builder mask, Response error) {
    static UpdatePlan success(TableSpec.Builder spec, FieldMask.Builder mask) {
      return new UpdatePlan(spec, mask, null);
    }

    static UpdatePlan failure(TableSpec.Builder spec, FieldMask.Builder mask, Response error) {
      return new UpdatePlan(spec, mask, error);
    }

    public boolean hasError() {
      return error != null;
    }
  }

  private Response validationError(String message) {
    return IcebergErrorResponses.validation(message);
  }

  Response validateRequirements(
      TableGatewaySupport tableSupport,
      List<Map<String, Object>> requirements,
      Supplier<Table> tableSupplier,
      Function<String, Response> validationErrorFactory,
      Function<String, Response> conflictErrorFactory) {
    if (requirements == null || requirements.isEmpty()) {
      return null;
    }
    Table table = tableSupplier.get();
    IcebergMetadata[] metadataHolder = new IcebergMetadata[1];
    boolean[] metadataLoaded = new boolean[] {false};
    Supplier<IcebergMetadata> metadataSupplier =
        () -> {
          if (!metadataLoaded[0]) {
            metadataHolder[0] =
                icebergMetadataService == null
                    ? null
                    : icebergMetadataService.resolveCurrentIcebergMetadata(table, tableSupport);
            metadataLoaded[0] = true;
          }
          return metadataHolder[0];
        };

    for (Map<String, Object> requirement : requirements) {
      if (requirement == null) {
        return validationErrorFactory.apply("commit requirement entry cannot be null");
      }
      String type = asString(requirement.get("type"));
      if (type == null || type.isBlank()) {
        return validationErrorFactory.apply("commit requirement missing type");
      }
      switch (type) {
        case "assert-create" -> {
          // no-op
        }
        case "assert-table-uuid" -> {
          String expected = asString(requirement.get("uuid"));
          if (expected == null || expected.isBlank()) {
            return validationErrorFactory.apply("assert-table-uuid requires uuid");
          }
          IcebergMetadata metadata = metadataSupplier.get();
          String metadataUuid =
              metadata != null
                      && metadata.getTableUuid() != null
                      && !metadata.getTableUuid().isBlank()
                  ? metadata.getTableUuid()
                  : null;
          if (metadataUuid != null) {
            if (expected.equals(metadataUuid)) {
              continue;
            }
            return conflictErrorFactory.apply("assert-table-uuid failed");
          }
          String fallbackUuid = resolveTableUuid(table);
          if (fallbackUuid != null && expected.equals(fallbackUuid)) {
            continue;
          }
          return conflictErrorFactory.apply("assert-table-uuid failed");
        }
        case "assert-current-schema-id" -> {
          Integer expected = asInteger(requirement.get("current-schema-id"));
          if (expected == null) {
            return validationErrorFactory.apply(
                "assert-current-schema-id requires current-schema-id");
          }
          Integer actual =
              resolveIntFromMetadataOrProperty(
                  table,
                  metadataSupplier,
                  "current-schema-id",
                  IcebergMetadata::getCurrentSchemaId);
          if (!Objects.equals(expected, actual)) {
            return conflictErrorFactory.apply("assert-current-schema-id failed");
          }
        }
        case "assert-last-assigned-field-id" -> {
          Integer expected = asInteger(requirement.get("last-assigned-field-id"));
          if (expected == null) {
            return validationErrorFactory.apply(
                "assert-last-assigned-field-id requires last-assigned-field-id");
          }
          Integer actual =
              resolveIntFromMetadataOrProperty(
                  table, metadataSupplier, "last-column-id", IcebergMetadata::getLastColumnId);
          if (!Objects.equals(expected, actual)) {
            return conflictErrorFactory.apply("assert-last-assigned-field-id failed");
          }
        }
        case "assert-last-assigned-partition-id" -> {
          Integer expected = asInteger(requirement.get("last-assigned-partition-id"));
          if (expected == null) {
            return validationErrorFactory.apply(
                "assert-last-assigned-partition-id requires last-assigned-partition-id");
          }
          Integer actual =
              resolveIntFromMetadataOrProperty(
                  table,
                  metadataSupplier,
                  "last-partition-id",
                  IcebergMetadata::getLastPartitionId);
          if (!Objects.equals(expected, actual)) {
            return conflictErrorFactory.apply("assert-last-assigned-partition-id failed");
          }
        }
        case "assert-default-spec-id" -> {
          Integer expected = asInteger(requirement.get("default-spec-id"));
          if (expected == null) {
            return validationErrorFactory.apply("assert-default-spec-id requires default-spec-id");
          }
          Integer actual =
              resolveIntFromMetadataOrProperty(
                  table, metadataSupplier, "default-spec-id", IcebergMetadata::getDefaultSpecId);
          if (!Objects.equals(expected, actual)) {
            return conflictErrorFactory.apply("assert-default-spec-id failed");
          }
        }
        case "assert-default-sort-order-id" -> {
          Integer expected = asInteger(requirement.get("default-sort-order-id"));
          if (expected == null) {
            return validationErrorFactory.apply(
                "assert-default-sort-order-id requires default-sort-order-id");
          }
          Integer actual =
              resolveIntFromMetadataOrProperty(
                  table,
                  metadataSupplier,
                  "default-sort-order-id",
                  IcebergMetadata::getDefaultSortOrderId);
          if (!Objects.equals(expected, actual)) {
            return conflictErrorFactory.apply("assert-default-sort-order-id failed");
          }
        }
        case "assert-ref-snapshot-id" -> {
          String refName = asString(requirement.get("ref"));
          if (refName == null || refName.isBlank()) {
            return validationErrorFactory.apply("assert-ref-snapshot-id requires ref");
          }
          Long expected = asLong(requirement.get("snapshot-id"));
          if (expected == null) {
            continue;
          }
          IcebergMetadata metadata = metadataSupplier.get();
          Long actual = resolveRefSnapshotId(table, metadata, refName);
          if (actual == null) {
            continue;
          }
          if (!Objects.equals(expected, actual)) {
            return conflictErrorFactory.apply("assert-ref-snapshot-id failed for ref " + refName);
          }
        }
        default -> {
          return validationErrorFactory.apply("unsupported commit requirement: " + type);
        }
      }
    }
    return null;
  }

  private Response commitConflictError(String message) {
    return IcebergErrorResponses.failure(
        message, "CommitFailedException", Response.Status.CONFLICT);
  }

  private String unsupportedUpdateAction(TableRequests.Commit req) {
    if (req == null || req.updates() == null) {
      return null;
    }
    for (Map<String, Object> update : req.updates()) {
      if (update == null) {
        return "<missing>";
      }
      String action = CommitUpdateInspector.actionOf(update);
      if (action == null || action.isBlank()) {
        return "<missing>";
      }
      if (!CommitUpdateInspector.isSupportedUpdateAction(action)) {
        return action;
      }
    }
    return null;
  }

  private void applyTableDefinitionSpecUpdates(
      TableSpec.Builder spec, FieldMask.Builder mask, List<Map<String, Object>> updates) {
    if (updates == null || updates.isEmpty()) {
      return;
    }
    for (Map<String, Object> update : updates) {
      if (update == null) {
        continue;
      }
      String action = CommitUpdateInspector.actionOf(update);
      if (!CommitUpdateInspector.ACTION_ADD_SCHEMA.equals(action)) {
        continue;
      }
      Map<String, Object> schema = asObjectMap(update.get("schema"));
      if (schema == null || schema.isEmpty()) {
        continue;
      }
      try {
        String schemaJson = mapper.writeValueAsString(schema);
        if (!schemaJson.isBlank()) {
          spec.setSchemaJson(schemaJson);
          mask.addPaths("schema_json");
        }
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException("Invalid add-schema payload", e);
      }
    }
  }

  private Map<String, String> stripFileIoProperties(Map<String, String> mergedProps) {
    if (mergedProps == null) {
      return null;
    }
    if (mergedProps.isEmpty()) {
      return mergedProps;
    }
    mergedProps.keySet().removeIf(FileIoFactory::isFileIoProperty);
    return mergedProps;
  }

  private Table applySpec(Table current, TableSpec.Builder spec, FieldMask.Builder mask) {
    if (mask == null || mask.getPathsCount() == 0) {
      return current;
    }
    Table.Builder out = current.toBuilder();
    Set<String> paths = new HashSet<>(mask.getPathsList());
    if (paths.contains("display_name") && spec.hasDisplayName()) {
      out.setDisplayName(spec.getDisplayName());
    }
    if (paths.contains("description") && spec.hasDescription()) {
      out.setDescription(spec.getDescription());
    }
    if (paths.contains("schema_json") && spec.hasSchemaJson()) {
      out.setSchemaJson(spec.getSchemaJson());
    }
    if (paths.contains("catalog_id") && spec.hasCatalogId()) {
      out.setCatalogId(spec.getCatalogId());
    }
    if (paths.contains("namespace_id") && spec.hasNamespaceId()) {
      out.setNamespaceId(spec.getNamespaceId());
    }
    if ((paths.contains("upstream") || paths.contains("upstream.uri")) && spec.hasUpstream()) {
      out.setUpstream(spec.getUpstream());
    }
    if (paths.contains("properties")) {
      out.clearProperties().putAllProperties(spec.getPropertiesMap());
    }
    return out.build();
  }

  private static String resolveTableUuid(Table table) {
    if (table == null) {
      return null;
    }
    String fromProps = table.getPropertiesMap().get("table-uuid");
    if (fromProps != null && !fromProps.isBlank()) {
      return fromProps;
    }
    if (table.hasResourceId()) {
      String id = table.getResourceId().getId();
      if (id != null && !id.isBlank()) {
        return id;
      }
    }
    return null;
  }

  private static Long resolveRefSnapshotId(Table table, IcebergMetadata metadata, String refName) {
    if (metadata != null) {
      if (metadata.getRefsMap().containsKey(refName)) {
        long snapshotId = metadata.getRefsOrThrow(refName).getSnapshotId();
        if (snapshotId >= 0L) {
          return snapshotId;
        }
      }
      if ("main".equals(refName)) {
        long currentSnapshotId = metadata.getCurrentSnapshotId();
        if (currentSnapshotId > 0L) {
          return currentSnapshotId;
        }
      }
    }
    if ("main".equals(refName) && table != null) {
      return asLong(table.getPropertiesMap().get("current-snapshot-id"));
    }
    return null;
  }

  private static Integer resolveIntFromMetadataOrProperty(
      Table table,
      Supplier<IcebergMetadata> metadataSupplier,
      String propertyKey,
      Function<IcebergMetadata, Integer> metadataExtractor) {
    Integer propertyValue =
        propertyInt(table == null ? null : table.getPropertiesMap(), propertyKey);
    IcebergMetadata metadata = metadataSupplier.get();
    if (metadata == null) {
      return propertyValue;
    }
    Integer metadataValue = metadataExtractor.apply(metadata);
    if (metadataValue == null || metadataValue < 0) {
      return propertyValue;
    }
    if (propertyValue != null && metadataValue < propertyValue) {
      return propertyValue;
    }
    return metadataValue;
  }

  private static Integer propertyInt(Map<String, String> props, String key) {
    if (props == null || key == null || key.isBlank()) {
      return null;
    }
    return asInteger(props.get(key));
  }
}
