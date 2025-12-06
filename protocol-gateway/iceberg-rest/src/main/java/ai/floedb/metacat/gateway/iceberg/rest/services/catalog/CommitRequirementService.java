package ai.floedb.metacat.gateway.iceberg.rest.services.catalog;

import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.gateway.iceberg.rpc.IcebergMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import org.jboss.logging.Logger;

@ApplicationScoped
public class CommitRequirementService {
  private static final Logger LOG = Logger.getLogger(CommitRequirementService.class);

  public Response validateRequirements(
      TableGatewaySupport tableSupport,
      List<Map<String, Object>> requirements,
      Supplier<Table> tableSupplier,
      Function<String, Response> validationErrorFactory,
      Function<String, Response> conflictErrorFactory) {
    if (requirements == null || requirements.isEmpty()) {
      return null;
    }
    Table table = tableSupplier.get();
    Map<String, String> props = table.getPropertiesMap();
    IcebergMetadata[] metadataHolder = new IcebergMetadata[1];
    Supplier<IcebergMetadata> metadataSupplier =
        () -> {
          if (metadataHolder[0] == null) {
            metadataHolder[0] = tableSupport.loadCurrentMetadata(table);
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
          String propertyUuid =
              Optional.ofNullable(props.get("table-uuid"))
                  .filter(value -> !value.isBlank())
                  .orElse(null);
          if (propertyUuid != null && expected.equals(propertyUuid)) {
            continue;
          }
          IcebergMetadata metadata = metadataSupplier.get();
          String metadataUuid =
              metadata != null
                      && metadata.getTableUuid() != null
                      && !metadata.getTableUuid().isBlank()
                  ? metadata.getTableUuid()
                  : null;
          if (metadataUuid != null && expected.equals(metadataUuid)) {
            continue;
          }
          String fallbackUuid = table.hasResourceId() ? table.getResourceId().getId() : "";
          if (expected.equals(fallbackUuid)) {
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
          IcebergMetadata metadata = metadataSupplier.get();
          Integer actual =
              metadata != null
                  ? metadata.getCurrentSchemaId()
                  : propertyInt(props, "current-schema-id");
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
          IcebergMetadata metadata = metadataSupplier.get();
          Integer actual =
              metadata != null
                  ? metadata.getLastColumnId()
                  : propertyInt(props, "last-assigned-field-id");
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
          IcebergMetadata metadata = metadataSupplier.get();
          Integer actual =
              metadata != null
                  ? metadata.getLastPartitionId()
                  : propertyInt(props, "last-assigned-partition-id");
          if (!Objects.equals(expected, actual)) {
            return conflictErrorFactory.apply("assert-last-assigned-partition-id failed");
          }
        }
        case "assert-default-spec-id" -> {
          Integer expected = asInteger(requirement.get("default-spec-id"));
          if (expected == null) {
            return validationErrorFactory.apply("assert-default-spec-id requires default-spec-id");
          }
          IcebergMetadata metadata = metadataSupplier.get();
          Integer actual =
              metadata != null
                  ? metadata.getDefaultSpecId()
                  : propertyInt(props, "default-spec-id");
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
          IcebergMetadata metadata = metadataSupplier.get();
          Integer actual =
              metadata != null
                  ? metadata.getDefaultSortOrderId()
                  : propertyInt(props, "default-sort-order-id");
          if (!Objects.equals(expected, actual)) {
            return conflictErrorFactory.apply("assert-default-sort-order-id failed");
          }
        }
        case "assert-ref-snapshot-id" -> {
          String refName = asString(requirement.get("ref"));
          Long expected = asLong(requirement.get("snapshot-id"));
          if (refName == null || refName.isBlank()) {
            return validationErrorFactory.apply("assert-ref-snapshot-id requires ref");
          }
          if (expected == null) {
            LOG.debugf(
                "Skipping assert-ref-snapshot-id requirement for table %s ref %s because"
                    + " snapshot-id was not provided",
                table.hasResourceId() ? table.getResourceId().getId() : "<unknown>", refName);
            continue;
          }
          Long actual = null;
          IcebergMetadata metadata = metadataSupplier.get();
          if (metadata != null && metadata.getRefsMap().containsKey(refName)) {
            actual = metadata.getRefsMap().get(refName).getSnapshotId();
          } else if ("main".equals(refName)) {
            if (metadata != null && metadata.getCurrentSnapshotId() > 0) {
              actual = metadata.getCurrentSnapshotId();
            } else {
              actual = propertyLong(props, "current-snapshot-id");
            }
          }
          if (!Objects.equals(actual, expected)) {
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

  private static String asString(Object value) {
    return value == null ? null : value.toString();
  }

  private static Integer asInteger(Object value) {
    if (value instanceof Number number) {
      return number.intValue();
    }
    if (value instanceof String text && !text.isBlank()) {
      return Integer.parseInt(text);
    }
    return null;
  }

  private static Long asLong(Object value) {
    if (value instanceof Number number) {
      return number.longValue();
    }
    if (value instanceof String text && !text.isBlank()) {
      return Long.parseLong(text);
    }
    return null;
  }

  private static Integer propertyInt(Map<String, String> props, String key) {
    if (props == null) {
      return null;
    }
    String value = props.get(key);
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static Long propertyLong(Map<String, String> props, String key) {
    if (props == null) {
      return null;
    }
    String value = props.get(key);
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
