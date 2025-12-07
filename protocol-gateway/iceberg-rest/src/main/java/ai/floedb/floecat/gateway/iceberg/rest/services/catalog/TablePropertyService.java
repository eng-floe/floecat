package ai.floedb.floecat.gateway.iceberg.rest.services.catalog;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergError;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import com.google.protobuf.FieldMask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TablePropertyService {
  private static final Logger LOG = Logger.getLogger(TablePropertyService.class);

  public void stripMetadataLocation(Map<String, String> props) {
    if (props == null || props.isEmpty()) {
      return;
    }
    boolean removed = false;
    if (props.remove("metadata-location") != null) {
      removed = true;
    }
    if (props.remove("metadata_location") != null) {
      removed = true;
    }
    if (removed) {
      LOG.debug("Ignored commit metadata-location property override");
    }
  }

  public boolean hasPropertyUpdates(TableRequests.Commit req) {
    if (req == null || req.updates() == null || req.updates().isEmpty()) {
      return false;
    }
    for (Map<String, Object> update : req.updates()) {
      String action = asString(update == null ? null : update.get("action"));
      if ("set-properties".equals(action) || "remove-properties".equals(action)) {
        return true;
      }
    }
    return false;
  }

  public Response applyPropertyUpdates(
      Map<String, String> properties, List<Map<String, Object>> updates) {
    if (updates == null) {
      return null;
    }
    for (Map<String, Object> update : updates) {
      if (update == null) {
        return validationError("commit update entry cannot be null");
      }
      String action = asString(update.get("action"));
      if (action == null) {
        return validationError("commit update missing action");
      }
      switch (action) {
        case "set-properties" -> {
          Map<String, String> toSet = new LinkedHashMap<>(asStringMap(update.get("updates")));
          if (toSet.isEmpty()) {
            return validationError("set-properties requires updates");
          }
          stripMetadataLocation(toSet);
          if (!toSet.isEmpty()) {
            properties.putAll(toSet);
          }
        }
        case "remove-properties" -> {
          List<String> removals = asStringList(update.get("removals"));
          if (removals.isEmpty()) {
            return validationError("remove-properties requires removals");
          }
          removals.forEach(properties::remove);
        }
        default -> {
          // ignore
        }
      }
    }
    return null;
  }

  public Map<String, String> ensurePropertyMap(
      Supplier<Table> tableSupplier, Map<String, String> current) {
    if (current != null) {
      return current;
    }
    Table table = tableSupplier.get();
    if (table == null || table.getPropertiesMap().isEmpty()) {
      return new LinkedHashMap<>();
    }
    return new LinkedHashMap<>(table.getPropertiesMap());
  }

  public Table tableWithPropertyOverrides(
      Supplier<Table> tableSupplier, Map<String, String> propertyOverrides) {
    Table table = tableSupplier.get();
    if (propertyOverrides == null || propertyOverrides.isEmpty() || table == null) {
      return table;
    }
    return table.toBuilder().clearProperties().putAllProperties(propertyOverrides).build();
  }

  public Response applyLocationUpdate(
      TableSpec.Builder spec,
      FieldMask.Builder mask,
      Supplier<Table> tableSupplier,
      List<Map<String, Object>> updates) {
    if (updates == null || updates.isEmpty()) {
      return null;
    }
    String location = null;
    for (Map<String, Object> update : updates) {
      String action = asString(update == null ? null : update.get("action"));
      if (!"set-location".equals(action)) {
        continue;
      }
      if (location != null) {
        return validationError("set-location may only be specified once");
      }
      String value = asString(update.get("location"));
      if (value == null || value.isBlank()) {
        return validationError("set-location requires location");
      }
      location = value;
    }
    if (location == null) {
      return null;
    }
    Table existing = tableSupplier.get();
    if (existing == null || !existing.hasUpstream()) {
      LOG.debug("Skipping set-location update for table without upstream reference");
      return null;
    }
    UpstreamRef upstream = existing.getUpstream();
    if (!upstream.hasConnectorId() || upstream.getConnectorId().getId().isBlank()) {
      LOG.debug(
          "Skipping set-location update for table without upstream connector (will be applied once"
              + " connector is registered)");
      return null;
    }
    UpstreamRef.Builder builder = upstream.toBuilder().setUri(location);
    spec.setUpstream(builder.build());
    mask.addPaths("upstream.uri");
    return null;
  }

  private Response validationError(String message) {
    return Response.status(Response.Status.BAD_REQUEST)
        .entity(new IcebergErrorResponse(new IcebergError(message, "ValidationException", 400)))
        .build();
  }

  private static Map<String, String> asStringMap(Object value) {
    if (!(value instanceof Map<?, ?> map)) {
      return Map.of();
    }
    Map<String, String> result = new LinkedHashMap<>();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (entry.getKey() == null || entry.getValue() == null) {
        continue;
      }
      result.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return result;
  }

  private static List<String> asStringList(Object value) {
    if (!(value instanceof List<?> list)) {
      return List.of();
    }
    return list.stream()
        .filter(v -> v != null && !v.toString().isBlank())
        .map(Object::toString)
        .toList();
  }

  private static String asString(Object value) {
    return value == null ? null : String.valueOf(value);
  }
}
