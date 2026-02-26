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

import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asInteger;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asLong;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asString;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.firstNonNull;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergError;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.RefPropertyUtil;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.CommitRequirementService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.SnapshotMetadataService;
import com.google.protobuf.FieldMask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

@ApplicationScoped
public class TableUpdatePlanner {

  @Inject CommitRequirementService commitRequirementService;
  @Inject TablePropertyService tablePropertyService;
  @Inject SnapshotMetadataService snapshotMetadataService;

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
    if (tablePropertyService.hasPropertyUpdates(req)) {
      if (mergedProps == null) {
        mergedProps = tablePropertyService.ensurePropertyMap(tableSupplier, null);
      }
      Response updateError = tablePropertyService.applyPropertyUpdates(mergedProps, req.updates());
      if (updateError != null) {
        return UpdatePlan.failure(spec, mask, updateError);
      }
    }
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
    mergedProps = applySnapshotPropertyUpdates(mergedProps, tableSupplier, req.updates());
    mergedProps = applyRefPropertyUpdates(mergedProps, tableSupplier, req.updates());
    mergedProps = applyTableDefinitionPropertyUpdates(mergedProps, tableSupplier, req.updates());
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
    if (tablePropertyService.hasPropertyUpdates(req)) {
      if (mergedProps == null) {
        mergedProps = tablePropertyService.ensurePropertyMap(tableSupplier, null);
      }
      Response updateError = tablePropertyService.applyPropertyUpdates(mergedProps, req.updates());
      if (updateError != null) {
        return UpdatePlan.failure(spec, mask, updateError);
      }
    }
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
    mergedProps = applySnapshotPropertyUpdates(mergedProps, tableSupplier, req.updates());
    mergedProps = applyRefPropertyUpdates(mergedProps, tableSupplier, req.updates());
    mergedProps = applyTableDefinitionPropertyUpdates(mergedProps, tableSupplier, req.updates());
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

  private Map<String, String> applyRefPropertyUpdates(
      Map<String, String> mergedProps,
      Supplier<Table> tableSupplier,
      List<Map<String, Object>> updates) {
    if (updates == null || updates.isEmpty()) {
      return mergedProps;
    }
    Map<String, Map<String, Object>> refs = loadStoredRefs(mergedProps, tableSupplier);
    boolean mutated = false;
    for (Map<String, Object> update : updates) {
      if (update == null) {
        continue;
      }
      String action = asString(update.get("action"));
      if ("set-snapshot-ref".equals(action)) {
        String refName = asString(update.get("ref-name"));
        Long snapshotId = asLong(update.get("snapshot-id"));
        if (refName == null || refName.isBlank() || snapshotId == null || snapshotId <= 0) {
          continue;
        }
        Map<String, Object> refMap = new LinkedHashMap<>();
        refMap.put("snapshot-id", snapshotId);
        String type = asString(update.get("type"));
        if (type != null && !type.isBlank()) {
          refMap.put("type", type.toLowerCase(Locale.ROOT));
        }
        Long maxRefAge =
            asLong(firstNonNull(update.get("max-ref-age-ms"), update.get("max_ref_age_ms")));
        if (maxRefAge != null) {
          refMap.put("max-ref-age-ms", maxRefAge);
        }
        Long maxSnapshotAge =
            asLong(
                firstNonNull(update.get("max-snapshot-age-ms"), update.get("max_snapshot_age_ms")));
        if (maxSnapshotAge != null) {
          refMap.put("max-snapshot-age-ms", maxSnapshotAge);
        }
        Integer minSnapshots =
            asInteger(
                firstNonNull(
                    update.get("min-snapshots-to-keep"), update.get("min_snapshots_to_keep")));
        if (minSnapshots != null) {
          refMap.put("min-snapshots-to-keep", minSnapshots);
        }
        refs.put(refName, refMap);
        mutated = true;
      } else if ("remove-snapshot-ref".equals(action)) {
        String refName = asString(update.get("ref-name"));
        if (refName != null && refs.remove(refName) != null) {
          mutated = true;
        }
      }
    }
    if (!mutated) {
      return mergedProps;
    }
    Map<String, String> targetProps =
        mergedProps == null
            ? new LinkedHashMap<>(tableSupplier.get().getPropertiesMap())
            : mergedProps;
    if (refs.isEmpty()) {
      targetProps.remove(RefPropertyUtil.PROPERTY_KEY);
    } else {
      targetProps.put(RefPropertyUtil.PROPERTY_KEY, RefPropertyUtil.encode(refs));
    }
    return targetProps;
  }

  private Map<String, String> applySnapshotPropertyUpdates(
      Map<String, String> mergedProps,
      Supplier<Table> tableSupplier,
      List<Map<String, Object>> updates) {
    if (updates == null || updates.isEmpty()) {
      return mergedProps;
    }
    Long latestSnapshotId = null;
    Long latestSequence = null;
    for (Map<String, Object> update : updates) {
      if (update == null) {
        continue;
      }
      String action = asString(update.get("action"));
      if (!"add-snapshot".equals(action)) {
        continue;
      }
      @SuppressWarnings("unchecked")
      Map<String, Object> snapshot =
          update.get("snapshot") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
      if (snapshot == null || snapshot.isEmpty()) {
        continue;
      }
      Long snapshotId = asLong(snapshot.get("snapshot-id"));
      if (snapshotId != null && snapshotId > 0) {
        latestSnapshotId = snapshotId;
      }
      Long sequenceNumber = asLong(snapshot.get("sequence-number"));
      if (sequenceNumber != null && sequenceNumber > 0) {
        latestSequence =
            latestSequence == null ? sequenceNumber : Math.max(latestSequence, sequenceNumber);
      }
    }
    if (latestSnapshotId == null || latestSnapshotId <= 0) {
      return mergedProps;
    }
    Map<String, String> targetProps =
        mergedProps == null
            ? new LinkedHashMap<>(tableSupplier.get().getPropertiesMap())
            : mergedProps;
    targetProps.put("current-snapshot-id", Long.toString(latestSnapshotId));
    if (latestSequence != null && latestSequence > 0) {
      Long existing = asLong(targetProps.get("last-sequence-number"));
      if (existing == null || existing < latestSequence) {
        targetProps.put("last-sequence-number", Long.toString(latestSequence));
      }
    }
    return targetProps;
  }

  private Map<String, String> applyTableDefinitionPropertyUpdates(
      Map<String, String> mergedProps,
      Supplier<Table> tableSupplier,
      List<Map<String, Object>> updates) {
    if (updates == null || updates.isEmpty()) {
      return mergedProps;
    }
    Map<String, String> targetProps =
        mergedProps == null
            ? new LinkedHashMap<>(tableSupplier.get().getPropertiesMap())
            : mergedProps;
    boolean mutated = false;
    for (Map<String, Object> update : updates) {
      if (update == null) {
        continue;
      }
      String action = asString(update.get("action"));
      if ("upgrade-format-version".equals(action)) {
        Integer version = asInteger(update.get("format-version"));
        if (version != null && version > 0) {
          targetProps.put("format-version", Integer.toString(version));
          mutated = true;
        }
      } else if ("add-schema".equals(action)) {
        Integer lastColumnId = asInteger(update.get("last-column-id"));
        if (lastColumnId != null && lastColumnId >= 0) {
          targetProps.put("last-column-id", Integer.toString(lastColumnId));
          mutated = true;
        }
      } else if ("set-current-schema".equals(action)) {
        Integer schemaId = asInteger(update.get("schema-id"));
        if (schemaId != null && schemaId >= 0) {
          targetProps.put("current-schema-id", Integer.toString(schemaId));
          mutated = true;
        }
      } else if ("add-spec".equals(action)) {
        @SuppressWarnings("unchecked")
        Map<String, Object> spec =
            update.get("spec") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
        if (spec != null) {
          Integer partitionFieldMax = maxPartitionFieldId(spec);
          if (partitionFieldMax != null && partitionFieldMax >= 0) {
            targetProps.put("last-partition-id", Integer.toString(partitionFieldMax));
            mutated = true;
          }
        }
      } else if ("set-default-spec".equals(action)) {
        Integer specId = asInteger(update.get("spec-id"));
        if (specId != null && specId >= 0) {
          targetProps.put("default-spec-id", Integer.toString(specId));
          mutated = true;
        }
      } else if ("set-default-sort-order".equals(action)) {
        Integer orderId = asInteger(update.get("sort-order-id"));
        if (orderId != null && orderId >= 0) {
          targetProps.put("default-sort-order-id", Integer.toString(orderId));
          mutated = true;
        }
      } else if ("set-location".equals(action)) {
        String location = asString(update.get("location"));
        if (location != null && !location.isBlank()) {
          targetProps.put("location", location);
          mutated = true;
        }
      }
    }
    if (!mutated) {
      return mergedProps;
    }
    return targetProps;
  }

  @SuppressWarnings("unchecked")
  private Integer maxPartitionFieldId(Map<String, Object> spec) {
    if (spec == null || spec.isEmpty()) {
      return null;
    }
    Object rawFields = spec.get("fields");
    if (!(rawFields instanceof List<?> fields) || fields.isEmpty()) {
      return 0;
    }
    Integer max = null;
    for (Object fieldObj : fields) {
      if (!(fieldObj instanceof Map<?, ?> fieldMap)) {
        continue;
      }
      Integer fieldId = asInteger(((Map<String, Object>) fieldMap).get("field-id"));
      if (fieldId == null) {
        continue;
      }
      max = max == null ? fieldId : Math.max(max, fieldId);
    }
    return max == null ? 0 : max;
  }

  private Map<String, Map<String, Object>> loadStoredRefs(
      Map<String, String> mergedProps, Supplier<Table> tableSupplier) {
    String encoded =
        mergedProps != null
            ? mergedProps.get(RefPropertyUtil.PROPERTY_KEY)
            : tableSupplier.get().getPropertiesMap().get(RefPropertyUtil.PROPERTY_KEY);
    return RefPropertyUtil.decode(encoded);
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
