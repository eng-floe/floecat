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

import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asObjectMap;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asString;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergError;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.CommitRequirementService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.SnapshotMetadataService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.FieldMask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@ApplicationScoped
public class TableUpdatePlanner {

  @Inject CommitRequirementService commitRequirementService;
  @Inject TablePropertyService tablePropertyService;
  @Inject SnapshotMetadataService snapshotMetadataService;
  @Inject ObjectMapper mapper;

  public UpdatePlan planUpdates(
      TableCommitService.CommitCommand command,
      Supplier<Table> tableSupplier,
      Supplier<Table> requirementTableSupplier,
      ResourceId tableId) {
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
        commitRequirementService.validateRequirements(
            command.tableSupport(),
            req.requirements(),
            requirementTableSupplier,
            this::validationError,
            this::conflictError);
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

  public UpdatePlan planUpdates(
      TableCommitService.CommitCommand command, Supplier<Table> tableSupplier, ResourceId tableId) {
    return planUpdates(command, tableSupplier, tableSupplier, tableId);
  }

  public UpdatePlan planTransactionUpdates(
      TableCommitService.CommitCommand command,
      Supplier<Table> tableSupplier,
      Supplier<Table> requirementTableSupplier,
      ResourceId tableId) {
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
        commitRequirementService.validateRequirements(
            command.tableSupport(),
            req.requirements(),
            requirementTableSupplier,
            this::validationError,
            this::conflictError);
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
    Response snapshotValidation = snapshotMetadataService.validateSnapshotUpdates(req.updates());
    if (snapshotValidation != null) {
      return UpdatePlan.failure(spec, mask, snapshotValidation);
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
      TableCommitService.CommitCommand command, Supplier<Table> tableSupplier, ResourceId tableId) {
    return planTransactionUpdates(command, tableSupplier, tableSupplier, tableId);
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
    return Response.status(Response.Status.BAD_REQUEST)
        .entity(new IcebergErrorResponse(new IcebergError(message, "ValidationException", 400)))
        .build();
  }

  private Response conflictError(String message) {
    return Response.status(Response.Status.CONFLICT)
        .entity(new IcebergErrorResponse(new IcebergError(message, "CommitFailedException", 409)))
        .build();
  }

  private String unsupportedUpdateAction(TableRequests.Commit req) {
    if (req == null || req.updates() == null) {
      return null;
    }
    for (Map<String, Object> update : req.updates()) {
      if (update == null) {
        return "<missing>";
      }
      String action = asString(update == null ? null : update.get("action"));
      if (action == null || action.isBlank()) {
        return "<missing>";
      }
      if (!"set-properties".equals(action)
          && !"remove-properties".equals(action)
          && !"set-location".equals(action)
          && !"add-snapshot".equals(action)
          && !"remove-snapshots".equals(action)
          && !"set-snapshot-ref".equals(action)
          && !"remove-snapshot-ref".equals(action)
          && !"assign-uuid".equals(action)
          && !"upgrade-format-version".equals(action)
          && !"add-schema".equals(action)
          && !"set-current-schema".equals(action)
          && !"add-spec".equals(action)
          && !"set-default-spec".equals(action)
          && !"add-sort-order".equals(action)
          && !"set-default-sort-order".equals(action)
          && !"remove-partition-specs".equals(action)
          && !"remove-schemas".equals(action)
          && !"set-statistics".equals(action)
          && !"remove-statistics".equals(action)
          && !"set-partition-statistics".equals(action)
          && !"remove-partition-statistics".equals(action)
          && !"add-encryption-key".equals(action)
          && !"remove-encryption-key".equals(action)) {
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
      String action = asString(update.get("action"));
      if (!"add-schema".equals(action)) {
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

  // TableMappingUtil provides common parsing helpers.
}
