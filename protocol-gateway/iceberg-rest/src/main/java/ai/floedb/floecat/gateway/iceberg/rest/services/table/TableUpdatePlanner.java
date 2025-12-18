package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.RefPropertyUtil;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.CommitRequirementService;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.SnapshotMetadataService;
import com.google.protobuf.FieldMask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

@ApplicationScoped
public class TableUpdatePlanner {

  @Inject CommitRequirementService commitRequirementService;
  @Inject TableLifecycleService tableLifecycleService;
  @Inject TablePropertyService tablePropertyService;
  @Inject SnapshotMetadataService snapshotMetadataService;

  public UpdatePlan planUpdates(
      TableCommitService.CommitCommand command, Supplier<Table> tableSupplier, ResourceId tableId) {
    TableRequests.Commit req = command.request();
    TableSpec.Builder spec = TableSpec.newBuilder();
    FieldMask.Builder mask = FieldMask.newBuilder();
    if (req == null) {
      return UpdatePlan.success(spec, mask);
    }
    Response requirementError =
        commitRequirementService.validateRequirements(
            command.tableSupport(),
            req.requirements(),
            tableSupplier,
            this::validationError,
            this::conflictError);
    if (requirementError != null) {
      return UpdatePlan.failure(spec, mask, requirementError);
    }
    if (req.name() != null) {
      spec.setDisplayName(req.name());
      mask.addPaths("display_name");
    }
    if (req.namespace() != null && !req.namespace().isEmpty()) {
      var targetNs =
          tableLifecycleService.resolveNamespaceId(
              command.catalogName(), new ArrayList<>(req.namespace()));
      spec.setNamespaceId(targetNs);
      mask.addPaths("namespace_id");
    }
    if (req.schemaJson() != null && !req.schemaJson().isBlank()) {
      spec.setSchemaJson(req.schemaJson());
      mask.addPaths("schema_json");
    }
    Map<String, String> mergedProps = null;
    if (req.properties() != null && !req.properties().isEmpty()) {
      mergedProps = new LinkedHashMap<>(req.properties());
      tablePropertyService.stripMetadataLocation(mergedProps);
      if (mergedProps.isEmpty()) {
        mergedProps = null;
      }
    }
    if (tablePropertyService.hasPropertyUpdates(req)) {
      if (mergedProps == null) {
        mergedProps = new LinkedHashMap<>(tableSupplier.get().getPropertiesMap());
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
    Response snapshotError =
        snapshotMetadataService.applySnapshotUpdates(
            command.tableSupport(),
            tableId,
            command.namespacePath(),
            command.table(),
            tableSupplier,
            req.updates(),
            command.idempotencyKey());
    if (snapshotError != null) {
      return UpdatePlan.failure(spec, mask, snapshotError);
    }
    mergedProps = applyRefPropertyUpdates(mergedProps, tableSupplier, req.updates());
    mergedProps = stripFileIoProperties(mergedProps);
    if (mergedProps != null) {
      spec.clearProperties().putAllProperties(mergedProps);
      mask.addPaths("properties");
    }
    return UpdatePlan.success(spec, mask);
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
        .entity(
            new ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse(
                new ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergError(
                    message, "ValidationException", 400)))
        .build();
  }

  private Response conflictError(String message) {
    return Response.status(Response.Status.CONFLICT)
        .entity(
            new ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse(
                new ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergError(
                    message, "CommitFailedException", 409)))
        .build();
  }

  private String unsupportedUpdateAction(TableRequests.Commit req) {
    if (req == null || req.updates() == null) {
      return null;
    }
    for (Map<String, Object> update : req.updates()) {
      String action = asString(update == null ? null : update.get("action"));
      if (action == null) {
        continue;
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

  private static String asString(Object value) {
    return value == null ? null : String.valueOf(value);
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

  private static Object firstNonNull(Object first, Object second) {
    return first != null ? first : second;
  }

  private static Long asLong(Object value) {
    if (value instanceof Number number) {
      return number.longValue();
    }
    if (value instanceof String str) {
      try {
        return Long.parseLong(str);
      } catch (NumberFormatException ignored) {
        return null;
      }
    }
    return null;
  }

  private static Integer asInteger(Object value) {
    if (value instanceof Number number) {
      return number.intValue();
    }
    if (value instanceof String str) {
      try {
        return Integer.parseInt(str);
      } catch (NumberFormatException ignored) {
        return null;
      }
    }
    return null;
  }
}
