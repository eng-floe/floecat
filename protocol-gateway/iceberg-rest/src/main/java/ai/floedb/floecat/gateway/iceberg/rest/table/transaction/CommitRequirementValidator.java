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
import ai.floedb.floecat.gateway.iceberg.rest.table.IcebergMetadataService;
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
    Table table = tableSupplier.get();
    try {
      return validate(
          CurrentTableState.load(table, tableSupport, icebergMetadataService),
          requirements,
          IcebergErrorResponses::validation,
          this::commitConflictError);
    } catch (IllegalStateException e) {
      return authoritativeMetadataMismatch(e.getMessage());
    }
  }

  public Response validate(
      CurrentTableState currentState, java.util.List<Map<String, Object>> requirements) {
    return validate(
        currentState, requirements, IcebergErrorResponses::validation, this::commitConflictError);
  }

  Response validate(
      TableGatewaySupport tableSupport,
      java.util.List<Map<String, Object>> requirements,
      Supplier<Table> tableSupplier,
      Function<String, Response> validationErrorFactory,
      Function<String, Response> conflictErrorFactory) {
    Table table = tableSupplier.get();
    try {
      return validate(
          CurrentTableState.load(table, tableSupport, icebergMetadataService),
          requirements,
          validationErrorFactory,
          conflictErrorFactory);
    } catch (IllegalStateException e) {
      return authoritativeMetadataMismatch(e.getMessage());
    }
  }

  public Response validateNullRefRequirements(
      TableGatewaySupport tableSupport,
      Table table,
      java.util.List<Map<String, Object>> requirements) {
    try {
      return validateNullRefRequirements(
          CurrentTableState.load(table, tableSupport, icebergMetadataService), requirements);
    } catch (IllegalStateException e) {
      return authoritativeMetadataMismatch(e.getMessage());
    }
  }

  public Response validateNullRefRequirements(
      CurrentTableState currentState, java.util.List<Map<String, Object>> requirements) {
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
      try {
        if (currentState != null && currentState.snapshotRefExists(refName)) {
          return IcebergErrorResponses.failure(
              "assert-ref-snapshot-id failed for ref " + refName,
              "CommitFailedException",
              Response.Status.CONFLICT);
        }
      } catch (IllegalStateException e) {
        return IcebergErrorResponses.failure(
            e.getMessage(), "InternalServerError", Response.Status.INTERNAL_SERVER_ERROR);
      }
    }
    return null;
  }

  Response validate(
      CurrentTableState currentState,
      java.util.List<Map<String, Object>> requirements,
      Function<String, Response> validationErrorFactory,
      Function<String, Response> conflictErrorFactory) {
    if (requirements == null || requirements.isEmpty()) {
      return null;
    }

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
          String actual;
          try {
            actual = currentState == null ? null : currentState.tableUuid();
          } catch (IllegalStateException e) {
            return authoritativeMetadataMismatch(e.getMessage());
          }
          if (expected.equals(actual)) {
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
          Integer actual;
          try {
            actual = currentState == null ? null : currentState.currentSchemaId();
          } catch (IllegalStateException e) {
            return authoritativeMetadataMismatch(e.getMessage());
          }
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
          Integer actual;
          try {
            actual = currentState == null ? null : currentState.lastAssignedFieldId();
          } catch (IllegalStateException e) {
            return authoritativeMetadataMismatch(e.getMessage());
          }
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
          Integer actual;
          try {
            actual = currentState == null ? null : currentState.lastAssignedPartitionId();
          } catch (IllegalStateException e) {
            return authoritativeMetadataMismatch(e.getMessage());
          }
          if (!Objects.equals(expected, actual)) {
            return conflictErrorFactory.apply("assert-last-assigned-partition-id failed");
          }
        }
        case "assert-default-spec-id" -> {
          Integer expected = asInteger(requirement.get("default-spec-id"));
          if (expected == null) {
            return validationErrorFactory.apply("assert-default-spec-id requires default-spec-id");
          }
          Integer actual;
          try {
            actual = currentState == null ? null : currentState.defaultSpecId();
          } catch (IllegalStateException e) {
            return authoritativeMetadataMismatch(e.getMessage());
          }
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
          Integer actual;
          try {
            actual = currentState == null ? null : currentState.defaultSortOrderId();
          } catch (IllegalStateException e) {
            return authoritativeMetadataMismatch(e.getMessage());
          }
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
          Long actual;
          try {
            actual = currentState == null ? null : currentState.refSnapshotId(refName);
          } catch (IllegalStateException e) {
            return authoritativeMetadataMismatch(e.getMessage());
          }
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

  private Response authoritativeMetadataMismatch(String message) {
    return IcebergErrorResponses.failure(
        message, "InternalServerError", Response.Status.INTERNAL_SERVER_ERROR);
  }
}
