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

package ai.floedb.floecat.gateway.iceberg.rest.services.table.transaction;

import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.common.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.common.RefPropertyUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import jakarta.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class TransactionCommitRequestSupport {
  private TransactionCommitRequestSupport() {}

  static String firstDuplicateTableIdentifier(List<TransactionCommitRequest.TableChange> changes) {
    if (changes == null || changes.isEmpty()) {
      return null;
    }
    Set<String> seen = new java.util.LinkedHashSet<>();
    for (TransactionCommitRequest.TableChange change : changes) {
      if (change == null || change.identifier() == null) {
        continue;
      }
      String name = change.identifier().name();
      if (name == null || name.isBlank()) {
        continue;
      }
      List<String> namespacePath =
          change.identifier().namespace() == null
              ? List.of()
              : List.copyOf(change.identifier().namespace());
      String qualifiedName =
          namespacePath.isEmpty() ? name : String.join(".", namespacePath) + "." + name;
      if (!seen.add(qualifiedName)) {
        return qualifiedName;
      }
    }
    return null;
  }

  static String requestHash(List<TransactionCommitRequest.TableChange> changes) {
    List<Map<String, Object>> normalized = new ArrayList<>();
    if (changes != null) {
      for (TransactionCommitRequest.TableChange change : changes) {
        if (change == null) {
          normalized.add(Map.of());
          continue;
        }
        Map<String, Object> entry = new LinkedHashMap<>();
        var identifier = change.identifier();
        if (identifier != null) {
          Map<String, Object> idMap = new LinkedHashMap<>();
          idMap.put(
              "namespace",
              identifier.namespace() == null ? List.of() : List.copyOf(identifier.namespace()));
          idMap.put("name", identifier.name());
          entry.put("identifier", idMap);
        } else {
          entry.put("identifier", null);
        }
        entry.put(
            "requirements", change.requirements() == null ? List.of() : change.requirements());
        entry.put("updates", change.updates() == null ? List.of() : change.updates());
        normalized.add(entry);
      }
    }
    String canonical = canonicalize(normalized);
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(canonical.getBytes(StandardCharsets.UTF_8));
      return Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }

  static String canonicalizeForTests(Object value) {
    return canonicalize(value);
  }

  static Response validateKnownRequirementTypes(List<Map<String, Object>> requirements) {
    for (Map<String, Object> requirement : requirements) {
      if (requirement == null) {
        return IcebergErrorResponses.validation("commit requirement entry cannot be null");
      }
      Object typeObj = requirement.get("type");
      String type = typeObj instanceof String value ? value : null;
      if (type == null || type.isBlank()) {
        return IcebergErrorResponses.validation("commit requirement missing type");
      }
      if (!CommitUpdateInspector.isSupportedRequirementType(type)) {
        return IcebergErrorResponses.validation("unsupported commit requirement: " + type);
      }
    }
    return null;
  }

  static Response validateKnownUpdateActions(List<Map<String, Object>> updates) {
    for (Map<String, Object> update : updates) {
      if (update == null) {
        return IcebergErrorResponses.validation("unsupported commit update action: <missing>");
      }
      Object actionObj = update.get("action");
      String action = actionObj instanceof String value ? value : null;
      if (action == null || action.isBlank()) {
        return IcebergErrorResponses.validation("unsupported commit update action: <missing>");
      }
      if (!CommitUpdateInspector.isSupportedUpdateAction(action)) {
        return IcebergErrorResponses.validation("unsupported commit update action: " + action);
      }
    }
    return null;
  }

  static Response validateAssertCreateRequirement(
      List<Map<String, Object>> requirements,
      ai.floedb.floecat.catalog.rpc.GetTableResponse tableResponse) {
    if (!hasRequirementType(requirements, CommitUpdateInspector.REQUIREMENT_ASSERT_CREATE)) {
      return null;
    }
    if (tableResponse != null && tableResponse.hasTable()) {
      return IcebergErrorResponses.failure(
          "assert-create failed", "CommitFailedException", Response.Status.CONFLICT);
    }
    return null;
  }

  static Response validateNullSnapshotRefRequirements(
      TableGatewaySupport tableSupport,
      ai.floedb.floecat.catalog.rpc.Table table,
      List<Map<String, Object>> requirements) {
    if (requirements == null || requirements.isEmpty()) {
      return null;
    }
    for (Map<String, Object> requirement : requirements) {
      if (requirement == null) {
        continue;
      }
      Object typeObj = requirement.get("type");
      String type = typeObj instanceof String value ? value : null;
      if (!"assert-ref-snapshot-id".equals(type) || !requirement.containsKey("snapshot-id")) {
        continue;
      }
      if (requirement.get("snapshot-id") != null) {
        continue;
      }
      Object refObj = requirement.get("ref");
      String refName = refObj instanceof String value ? value : null;
      if (refName == null || refName.isBlank()) {
        return IcebergErrorResponses.validation("assert-ref-snapshot-id requires ref");
      }
      if (hasSnapshotRef(tableSupport, table, refName)) {
        return IcebergErrorResponses.failure(
            "assert-ref-snapshot-id failed for ref " + refName,
            "CommitFailedException",
            Response.Status.CONFLICT);
      }
    }
    return null;
  }

  static boolean hasRequirementType(List<Map<String, Object>> requirements, String type) {
    if (requirements == null || requirements.isEmpty() || type == null || type.isBlank()) {
      return false;
    }
    for (Map<String, Object> requirement : requirements) {
      String requirementType =
          requirement == null ? null : requirement.get("type") instanceof String s ? s : null;
      if (type.equals(requirementType)) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasSnapshotRef(
      TableGatewaySupport tableSupport, ai.floedb.floecat.catalog.rpc.Table table, String refName) {
    if (refName == null || refName.isBlank()) {
      return false;
    }
    if (table == null) {
      return false;
    }
    if ("main".equals(refName)
        && TableMappingUtil.asLong(table.getPropertiesMap().get("current-snapshot-id")) != null
        && TableMappingUtil.asLong(table.getPropertiesMap().get("current-snapshot-id")) >= 0L) {
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
    Long snapshotId = TableMappingUtil.asLong(refs.get(refName).get("snapshot-id"));
    return snapshotId != null && snapshotId >= 0L;
  }

  private static String canonicalize(Object value) {
    if (value == null) {
      return "null";
    }
    if (value instanceof Map<?, ?> map) {
      List<Map.Entry<?, ?>> entries = new ArrayList<>(map.entrySet());
      entries.sort(java.util.Comparator.comparing(entry -> String.valueOf(entry.getKey())));
      StringBuilder out = new StringBuilder("{");
      boolean first = true;
      for (Map.Entry<?, ?> entry : entries) {
        if (!first) {
          out.append(',');
        }
        first = false;
        out.append(escapeJsonString(String.valueOf(entry.getKey())))
            .append(':')
            .append(canonicalize(entry.getValue()));
      }
      return out.append('}').toString();
    }
    if (value instanceof List<?> list) {
      StringBuilder out = new StringBuilder("[");
      boolean first = true;
      for (Object entry : list) {
        if (!first) {
          out.append(',');
        }
        first = false;
        out.append(canonicalize(entry));
      }
      return out.append(']').toString();
    }
    if (value instanceof String str) {
      return escapeJsonString(str);
    }
    if (value instanceof Number || value instanceof Boolean) {
      return String.valueOf(value);
    }
    return escapeJsonString(String.valueOf(value));
  }

  private static String escapeJsonString(String input) {
    String value = input == null ? "" : input;
    StringBuilder out = new StringBuilder("\"");
    for (int i = 0; i < value.length(); i++) {
      char ch = value.charAt(i);
      switch (ch) {
        case '\\' -> out.append("\\\\");
        case '"' -> out.append("\\\"");
        case '\b' -> out.append("\\b");
        case '\f' -> out.append("\\f");
        case '\n' -> out.append("\\n");
        case '\r' -> out.append("\\r");
        case '\t' -> out.append("\\t");
        default -> {
          if (ch < 0x20) {
            out.append(String.format("\\u%04x", (int) ch));
          } else {
            out.append(ch);
          }
        }
      }
    }
    return out.append('"').toString();
  }
}
