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

package ai.floedb.floecat.gateway.iceberg.rest.services.compat;

import ai.floedb.floecat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadataLogEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSchema;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSnapshotLogEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSortOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

@ApplicationScoped
public class DeltaIcebergMetadataTranslator {
  private static final ObjectMapper JSON = new ObjectMapper();

  public IcebergMetadata translate(Table table, List<Snapshot> snapshots) {
    List<Snapshot> ordered = orderedSnapshots(snapshots);
    Snapshot current = ordered.isEmpty() ? null : ordered.get(ordered.size() - 1);
    long currentSnapshotId = current == null ? -1L : current.getSnapshotId();
    long lastUpdatedMs = current == null ? System.currentTimeMillis() : snapshotMillis(current);
    long lastSequence = ordered.stream().mapToLong(this::snapshotSequence).max().orElse(0L);
    String metadataLocation =
        DeltaMetadataLocationSynthesizer.syntheticLocation(table, Math.max(0L, currentSnapshotId));
    String tableUuid =
        table != null && table.hasResourceId() ? table.getResourceId().getId() : "delta-table";
    int schemaId = current != null && current.getSchemaId() > 0 ? current.getSchemaId() : 0;
    NormalizedSchema normalizedSchema = normalizeSchema(schemaJson(current, table), schemaId);

    IcebergMetadata.Builder builder =
        IcebergMetadata.newBuilder()
            .setTableUuid(tableUuid)
            .setFormatVersion(2)
            .setMetadataLocation(metadataLocation)
            .setLastUpdatedMs(lastUpdatedMs)
            .setCurrentSnapshotId(currentSnapshotId)
            .setLastSequenceNumber(lastSequence)
            .setCurrentSchemaId(normalizedSchema.schemaId())
            .setDefaultSpecId(0)
            .setLastPartitionId(0)
            .setDefaultSortOrderId(0)
            .addSortOrders(IcebergSortOrder.newBuilder().setSortOrderId(0).build())
            .addMetadataLog(
                IcebergMetadataLogEntry.newBuilder()
                    .setTimestampMs(lastUpdatedMs)
                    .setFile(metadataLocation)
                    .build());

    if (current != null && currentSnapshotId >= 0) {
      builder.putRefs(
          "main",
          IcebergRef.newBuilder().setSnapshotId(currentSnapshotId).setType("branch").build());
    }

    builder.setLastColumnId(normalizedSchema.lastColumnId());
    builder.addSchemas(
        IcebergSchema.newBuilder()
            .setSchemaId(normalizedSchema.schemaId())
            .setSchemaJson(normalizedSchema.schemaJson())
            .setLastColumnId(normalizedSchema.lastColumnId())
            .build());

    if (current != null && current.hasPartitionSpec()) {
      PartitionSpecInfo spec = current.getPartitionSpec();
      builder.addPartitionSpecs(spec);
      builder.setDefaultSpecId(spec.getSpecId());
    }

    for (Snapshot snapshot : ordered) {
      builder.addSnapshotLog(
          IcebergSnapshotLogEntry.newBuilder()
              .setSnapshotId(snapshot.getSnapshotId())
              .setTimestampMs(snapshotMillis(snapshot))
              .build());
    }
    return builder.build();
  }

  private List<Snapshot> orderedSnapshots(List<Snapshot> snapshots) {
    if (snapshots == null || snapshots.isEmpty()) {
      return List.of();
    }
    return snapshots.stream()
        .sorted(
            Comparator.comparingLong(this::snapshotSequence)
                .thenComparingLong(Snapshot::getSnapshotId))
        .toList();
  }

  private long snapshotSequence(Snapshot snapshot) {
    if (snapshot == null) {
      return 0L;
    }
    long seq = snapshot.getSequenceNumber();
    return seq > 0 ? seq : snapshot.getSnapshotId();
  }

  private long snapshotMillis(Snapshot snapshot) {
    if (snapshot == null || !snapshot.hasUpstreamCreatedAt()) {
      return System.currentTimeMillis();
    }
    return snapshot.getUpstreamCreatedAt().getSeconds() * 1000L;
  }

  private String schemaJson(Snapshot snapshot, Table table) {
    if (snapshot != null
        && snapshot.getSchemaJson() != null
        && !snapshot.getSchemaJson().isBlank()) {
      return snapshot.getSchemaJson();
    }
    if (table != null && table.getSchemaJson() != null && !table.getSchemaJson().isBlank()) {
      return table.getSchemaJson();
    }
    return "{\"schema-id\":0,\"type\":\"struct\",\"fields\":[],\"last-column-id\":0}";
  }

  private NormalizedSchema normalizeSchema(String rawSchemaJson, int desiredSchemaId) {
    try {
      Map<String, Object> root =
          JSON.readValue(rawSchemaJson, new TypeReference<Map<String, Object>>() {});
      int schemaId = positiveInt(root.get("schema-id"), desiredSchemaId);
      IdAllocator ids = new IdAllocator(maxFieldIdInSchema(root.get("fields")) + 1);
      List<Map<String, Object>> fields = normalizeFields(root.get("fields"), ids);
      int existingLastColumnId = positiveInt(root.get("last-column-id"), 0);
      int maxFieldId = maxFieldId(fields);
      int lastColumnId = Math.max(existingLastColumnId, maxFieldId);
      root.put("schema-id", schemaId);
      root.put("last-column-id", lastColumnId);
      root.put("type", "struct");
      root.put("fields", fields);
      return new NormalizedSchema(schemaId, lastColumnId, JSON.writeValueAsString(root));
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to normalize Delta schema JSON", e);
    }
  }

  private List<Map<String, Object>> normalizeFields(Object rawFields, IdAllocator ids)
      throws JsonProcessingException {
    if (!(rawFields instanceof List<?> list)) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>(list.size());
    for (Object entry : list) {
      if (!(entry instanceof Map<?, ?> mapLike)) {
        continue;
      }
      Map<String, Object> field = new LinkedHashMap<>();
      for (Map.Entry<?, ?> e : mapLike.entrySet()) {
        if (e.getKey() != null) {
          field.put(e.getKey().toString(), e.getValue());
        }
      }
      int id = ids.assign(positiveInt(field.get("id"), 0));
      field.put("id", id);
      String name = stringValue(field.get("name"));
      if (name == null || name.isBlank()) {
        name = "field_" + id;
      }
      field.put("name", name);
      Object nullable = field.get("nullable");
      if (!field.containsKey("required")) {
        boolean required = (nullable instanceof Boolean b) && !b;
        field.put("required", required);
      }
      if (!field.containsKey("type")) {
        field.put("type", "string");
      } else if (field.get("type") instanceof Map<?, ?> mapType) {
        field.put("type", normalizeNestedType(mapType, ids));
      } else if (field.get("type") instanceof String typeName) {
        field.put("type", normalizeTypeName(typeName));
      }
      out.add(field);
    }
    return out;
  }

  private Object normalizeNestedType(Map<?, ?> mapType, IdAllocator ids)
      throws JsonProcessingException {
    Map<String, Object> normalized = new LinkedHashMap<>();
    for (Map.Entry<?, ?> entry : mapType.entrySet()) {
      if (entry.getKey() == null) {
        continue;
      }
      String key = entry.getKey().toString();
      Object value = entry.getValue();
      if ("fields".equals(key)) {
        normalized.put(key, normalizeFields(value, ids));
        continue;
      }
      if ("elementType".equals(key)
          || "keyType".equals(key)
          || "valueType".equals(key)
          || "element".equals(key)
          || "key".equals(key)
          || "value".equals(key)) {
        if (value instanceof Map<?, ?> nestedMap) {
          normalized.put(key, normalizeNestedType(nestedMap, ids));
        } else if (value instanceof String nestedTypeName) {
          normalized.put(key, normalizeTypeName(nestedTypeName));
        } else {
          normalized.put(key, value);
        }
        continue;
      }
      if ("type".equals(key) && value instanceof String typeName) {
        normalized.put(key, normalizeTypeName(typeName));
      } else {
        normalized.put(key, value);
      }
    }
    return normalized;
  }

  private String normalizeTypeName(String rawType) {
    if (rawType == null || rawType.isBlank()) {
      return "string";
    }
    String type = rawType.trim();
    String lower = type.toLowerCase(Locale.ROOT);
    return switch (lower) {
      case "byte", "short", "integer" -> "int";
      case "real" -> "float";
      case "str" -> "string";
      default -> type;
    };
  }

  private int positiveInt(Object value, int fallback) {
    if (value instanceof Number number) {
      int asInt = number.intValue();
      return asInt > 0 ? asInt : fallback;
    }
    return fallback;
  }

  private String stringValue(Object value) {
    return value == null ? null : value.toString();
  }

  private int maxFieldIdInSchema(Object rawFields) {
    if (!(rawFields instanceof List<?> list)) {
      return 0;
    }
    int max = 0;
    for (Object entry : list) {
      if (!(entry instanceof Map<?, ?> mapLike)) {
        continue;
      }
      Object id = mapLike.get("id");
      max = Math.max(max, positiveInt(id, 0));
      Object type = mapLike.get("type");
      if (type instanceof Map<?, ?> typeMap) {
        max = Math.max(max, maxFieldIdInType(typeMap));
      }
    }
    return max;
  }

  private int maxFieldIdInType(Map<?, ?> typeMap) {
    int max = 0;
    Object fields = typeMap.get("fields");
    max = Math.max(max, maxFieldIdInSchema(fields));
    for (String key : List.of("elementType", "keyType", "valueType", "element", "key", "value")) {
      Object nested = typeMap.get(key);
      if (nested instanceof Map<?, ?> nestedMap) {
        max = Math.max(max, maxFieldIdInType(nestedMap));
      }
    }
    return max;
  }

  private int maxFieldId(List<Map<String, Object>> fields) {
    int max = 0;
    for (Map<String, Object> field : fields) {
      max = Math.max(max, positiveInt(field.get("id"), 0));
      Object type = field.get("type");
      if (type instanceof Map<?, ?> typeMap) {
        max = Math.max(max, maxFieldIdInType(typeMap));
      }
    }
    return max;
  }

  private static final class IdAllocator {
    private int next;
    private final Set<Integer> seen = new HashSet<>();

    private IdAllocator(int initial) {
      this.next = Math.max(1, initial);
    }

    private int assign(int existing) {
      if (existing > 0 && !seen.contains(existing)) {
        seen.add(existing);
        next = Math.max(next, existing + 1);
        return existing;
      }
      while (seen.contains(next)) {
        next++;
      }
      int assigned = next++;
      seen.add(assigned);
      return assigned;
    }
  }

  private record NormalizedSchema(int schemaId, int lastColumnId, String schemaJson) {}
}
