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

import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.asInteger;
import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.asLong;
import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.asString;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.support.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.support.RefPropertyUtil;
import ai.floedb.floecat.gateway.iceberg.rest.table.IcebergMetadataService;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

@ApplicationScoped
public class CommitRequirementValidator {
  @Inject IcebergMetadataService icebergMetadataService;

  public Response validate(
      TableGatewaySupport tableSupport,
      java.util.List<Map<String, Object>> requirements,
      Supplier<Table> tableSupplier) {
    return validate(
        tableSupport,
        requirements,
        tableSupplier,
        IcebergErrorResponses::validation,
        this::commitConflictError);
  }

  public Response validateNullRefRequirements(
      TableGatewaySupport tableSupport,
      Table table,
      java.util.List<Map<String, Object>> requirements) {
    if (requirements == null || requirements.isEmpty()) {
      return null;
    }
    for (Map<String, Object> requirement : requirements) {
      if (requirement == null) {
        continue;
      }
      String type = asString(requirement.get("type"));
      if (!"assert-ref-snapshot-id".equals(type) || !requirement.containsKey("snapshot-id")) {
        continue;
      }
      if (requirement.get("snapshot-id") != null) {
        continue;
      }
      String refName = asString(requirement.get("ref"));
      if (refName == null || refName.isBlank()) {
        return IcebergErrorResponses.validation("assert-ref-snapshot-id requires ref");
      }
      if (snapshotRefExists(tableSupport, table, refName)) {
        return IcebergErrorResponses.failure(
            "assert-ref-snapshot-id failed for ref " + refName,
            "CommitFailedException",
            Response.Status.CONFLICT);
      }
    }
    return null;
  }

  Response validate(
      TableGatewaySupport tableSupport,
      java.util.List<Map<String, Object>> requirements,
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
        case "assert-create" -> {}
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
          Long actual = resolveRefSnapshotId(table, metadataSupplier.get(), refName);
          if (actual == null || !Objects.equals(expected, actual)) {
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

  private boolean snapshotRefExists(TableGatewaySupport tableSupport, Table table, String refName) {
    if (refName == null || refName.isBlank()) {
      return false;
    }
    try {
      IcebergMetadata metadata =
          icebergMetadataService == null
              ? null
              : icebergMetadataService.resolveCurrentIcebergMetadata(table, tableSupport);
      if (snapshotRefExistsInMetadata(metadata, refName)) {
        return true;
      }
    } catch (RuntimeException ignored) {
    }
    if (table == null) {
      return false;
    }
    if (hasCurrentMainSnapshot(table.getPropertiesMap(), refName)) {
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
    Long snapshotId = asLong(refs.get(refName).get("snapshot-id"));
    return snapshotId != null && snapshotId >= 0L;
  }

  private boolean snapshotRefExistsInMetadata(IcebergMetadata metadata, String refName) {
    if (metadata == null) {
      return false;
    }
    if (metadata.getRefsMap().containsKey(refName)
        && metadata.getRefsOrThrow(refName).getSnapshotId() >= 0L) {
      return true;
    }
    return "main".equals(refName) && metadata.getCurrentSnapshotId() > 0L;
  }

  private boolean hasCurrentMainSnapshot(Map<String, String> properties, String refName) {
    if (!"main".equals(refName) || properties == null) {
      return false;
    }
    Long snapshotId = asLong(properties.get("current-snapshot-id"));
    return snapshotId != null && snapshotId >= 0L;
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
