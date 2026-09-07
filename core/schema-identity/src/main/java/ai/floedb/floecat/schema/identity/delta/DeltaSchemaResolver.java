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

package ai.floedb.floecat.schema.identity.delta;

import ai.floedb.floecat.schema.identity.ColumnPath;
import ai.floedb.floecat.schema.identity.ResolvedSchema;
import ai.floedb.floecat.schema.identity.SchemaNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

/** Resolves Delta schema JSON into format-neutral source identity facts. */
public final class DeltaSchemaResolver {
  public static final String COLUMN_ID = "delta.columnMapping.id";
  public static final String PHYSICAL_NAME = "delta.columnMapping.physicalName";
  public static final String NESTED_IDS = "delta.columnMapping.nested.ids";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private DeltaSchemaResolver() {}

  /**
   * Resolves one Delta schema version.
   *
   * <p>The caller must supply the <em>effective</em> mapping mode after checking the Delta
   * protocol, not merely the value of {@code delta.columnMapping.mode}. Mapping metadata is
   * authoritative only when that effective mode is enabled. This method reports source IDs but
   * never assigns Floecat canonical IDs, so using it cannot change the existing connector or
   * planner contract.
   */
  public static DeltaResolvedSchema resolve(String schemaJson, ColumnMappingMode effectiveMode) {
    Objects.requireNonNull(effectiveMode, "effectiveMode");
    if (schemaJson == null || schemaJson.isBlank()) {
      return new DeltaResolvedSchema(ResolvedSchema.of(List.of()), effectiveMode);
    }

    JsonNode root;
    try {
      root = MAPPER.readTree(schemaJson);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse Delta schema JSON", e);
    }

    Walk walk = new Walk(effectiveMode);
    walk.struct(root, ColumnPath.ROOT, Optional.of(ColumnPath.ROOT));
    walk.verify();
    return new DeltaResolvedSchema(ResolvedSchema.of(walk.nodes), effectiveMode);
  }

  private static final class Walk {
    private final ColumnMappingMode mode;
    private final List<SchemaNode> nodes = new ArrayList<>();
    private final List<NestedIds> nestedIdHolders = new ArrayList<>();
    private final List<String> problems = new ArrayList<>();

    private Walk(ColumnMappingMode mode) {
      this.mode = mode;
    }

    private void struct(
        JsonNode structNode, ColumnPath logicalPrefix, Optional<ColumnPath> physicalPrefix) {
      if (structNode == null || !structNode.isObject()) {
        problems.add(describe(logicalPrefix) + " is not a Delta struct object");
        return;
      }
      if (!"struct".equals(structNode.path("type").asText(""))) {
        problems.add(describe(logicalPrefix) + " is not tagged as a Delta struct");
        return;
      }
      JsonNode fields = structNode.get("fields");
      if (fields == null || !fields.isArray()) {
        problems.add(describe(logicalPrefix) + " has no fields array");
        return;
      }

      int ordinal = 0;
      for (JsonNode field : fields) {
        ordinal++;
        if (!field.isObject()) {
          problems.add(
              "field " + ordinal + " under " + describe(logicalPrefix) + " is not an object");
          continue;
        }
        JsonNode nameNode = field.get("name");
        if (nameNode == null || !nameNode.isTextual() || nameNode.textValue().isEmpty()) {
          problems.add("field " + ordinal + " under " + describe(logicalPrefix) + " has no name");
          continue;
        }

        String name = nameNode.textValue();
        ColumnPath path = logicalPrefix.field(name);
        Optional<String> physicalName = physicalName(field, path);
        Optional<ColumnPath> physicalPath =
            physicalName.isEmpty()
                ? Optional.empty()
                : physicalPrefix.map(prefix -> prefix.field(physicalName.orElseThrow()));
        JsonNode type = field.get("type");
        if (!validType(type, path)) {
          continue;
        }

        SchemaNode node =
            new SchemaNode(path, ordinal, !isContainer(type), fieldId(field, path), physicalPath);
        nodes.add(node);
        walkType(type, node, physicalPath, nestedIds(field, path), physicalName.orElse(name));
      }
    }

    private void walkType(
        JsonNode type,
        SchemaNode parent,
        Optional<ColumnPath> physicalPath,
        NestedIds nestedIds,
        String nestedKey) {
      if (type.isTextual()) {
        return;
      }
      switch (type.path("type").asText("")) {
        case "struct" -> struct(type, parent.path(), physicalPath);
        case "array" -> {
          JsonNode elementType = requiredType(type, "elementType", parent.path());
          if (elementType == null) {
            return;
          }
          String elementKey = nestedKey + ".element";
          Optional<ColumnPath> elementPhysical = physicalPath.map(ColumnPath::arrayElement);
          SchemaNode element =
              new SchemaNode(
                  parent.path().arrayElement(),
                  1,
                  !isContainer(elementType),
                  nestedIds.take(elementKey),
                  elementPhysical);
          nodes.add(element);
          walkType(elementType, element, elementPhysical, nestedIds, elementKey);
        }
        case "map" -> {
          JsonNode keyType = requiredType(type, "keyType", parent.path());
          JsonNode valueType = requiredType(type, "valueType", parent.path());
          if (keyType == null || valueType == null) {
            return;
          }

          String keyKey = nestedKey + ".key";
          Optional<ColumnPath> keyPhysical = physicalPath.map(ColumnPath::mapKey);
          SchemaNode key =
              new SchemaNode(
                  parent.path().mapKey(),
                  1,
                  !isContainer(keyType),
                  nestedIds.take(keyKey),
                  keyPhysical);
          nodes.add(key);
          walkType(keyType, key, keyPhysical, nestedIds, keyKey);

          String valueKey = nestedKey + ".value";
          Optional<ColumnPath> valuePhysical = physicalPath.map(ColumnPath::mapValue);
          SchemaNode value =
              new SchemaNode(
                  parent.path().mapValue(),
                  2,
                  !isContainer(valueType),
                  nestedIds.take(valueKey),
                  valuePhysical);
          nodes.add(value);
          walkType(valueType, value, valuePhysical, nestedIds, valueKey);
        }
        default -> problems.add("unsupported complex type at " + describe(parent.path()));
      }
    }

    private JsonNode requiredType(JsonNode owner, String field, ColumnPath path) {
      JsonNode type = owner.get(field);
      if (!validType(type, path)) {
        problems.add(describe(path) + " has no valid " + field);
        return null;
      }
      return type;
    }

    private boolean validType(JsonNode type, ColumnPath path) {
      if (type == null || type.isNull()) {
        problems.add(describe(path) + " has no type");
        return false;
      }
      if (type.isTextual()) {
        if (type.textValue().isEmpty()) {
          problems.add(describe(path) + " has an empty type");
          return false;
        }
        return true;
      }
      if (!type.isObject() || type.path("type").asText("").isEmpty()) {
        problems.add(describe(path) + " has a malformed complex type");
        return false;
      }
      return true;
    }

    private Optional<String> physicalName(JsonNode field, ColumnPath path) {
      if (!mode.isEnabled()) {
        return Optional.empty();
      }
      JsonNode value = metadata(field).get(PHYSICAL_NAME);
      if (value == null || !value.isTextual() || value.textValue().isEmpty()) {
        problems.add(describe(path) + " has no " + PHYSICAL_NAME + " in mapping mode " + mode);
        return Optional.empty();
      }
      return Optional.of(value.textValue());
    }

    private OptionalInt fieldId(JsonNode field, ColumnPath path) {
      if (!mode.isEnabled()) {
        return OptionalInt.empty();
      }
      JsonNode value = metadata(field).get(COLUMN_ID);
      OptionalInt id = positiveInt(value);
      if (id.isEmpty()) {
        problems.add(describe(path) + " has no positive " + COLUMN_ID + " in mapping mode " + mode);
      }
      return id;
    }

    private NestedIds nestedIds(JsonNode field, ColumnPath path) {
      if (!mode.isEnabled()) {
        return NestedIds.empty();
      }
      JsonNode value = metadata(field).get(NESTED_IDS);
      if (value == null || value.isNull()) {
        return NestedIds.empty();
      }
      if (!value.isObject()) {
        problems.add(describe(path) + " has a non-object " + NESTED_IDS);
        return NestedIds.empty();
      }

      Map<String, Integer> ids = new LinkedHashMap<>();
      value
          .fields()
          .forEachRemaining(
              entry -> {
                OptionalInt id = positiveInt(entry.getValue());
                if (entry.getKey().isEmpty() || id.isEmpty()) {
                  problems.add(
                      describe(path) + " has an invalid nested ID for '" + entry.getKey() + "'");
                } else {
                  ids.put(entry.getKey(), id.getAsInt());
                }
              });
      NestedIds holder = new NestedIds(path, ids);
      nestedIdHolders.add(holder);
      return holder;
    }

    private void verify() {
      for (NestedIds holder : nestedIdHolders) {
        for (String key : holder.unconsumed()) {
          problems.add(describe(holder.owner) + " has an unused nested ID for '" + key + "'");
        }
      }
      if (!problems.isEmpty()) {
        throw new IllegalArgumentException("Invalid Delta schema: " + String.join("; ", problems));
      }
    }

    private static JsonNode metadata(JsonNode field) {
      JsonNode metadata = field.get("metadata");
      return metadata != null && metadata.isObject() ? metadata : MAPPER.createObjectNode();
    }

    private static OptionalInt positiveInt(JsonNode value) {
      return value != null
              && value.isIntegralNumber()
              && value.canConvertToInt()
              && value.intValue() > 0
          ? OptionalInt.of(value.intValue())
          : OptionalInt.empty();
    }

    private static String describe(ColumnPath path) {
      return path.isRoot() ? "<root>" : "'" + path.display() + "'";
    }
  }

  private static final class NestedIds {
    private final ColumnPath owner;
    private final Map<String, Integer> ids;
    private final Set<String> consumed = new LinkedHashSet<>();

    private NestedIds(ColumnPath owner, Map<String, Integer> ids) {
      this.owner = owner;
      this.ids = Map.copyOf(ids);
    }

    private static NestedIds empty() {
      return new NestedIds(ColumnPath.ROOT, Map.of());
    }

    private OptionalInt take(String key) {
      Integer id = ids.get(key);
      if (id == null) {
        return OptionalInt.empty();
      }
      consumed.add(key);
      return OptionalInt.of(id);
    }

    private Set<String> unconsumed() {
      Set<String> result = new LinkedHashSet<>(ids.keySet());
      result.removeAll(consumed);
      return result;
    }
  }

  private static boolean isContainer(JsonNode type) {
    if (type == null || !type.isObject()) {
      return false;
    }
    return switch (type.path("type").asText("")) {
      case "struct", "array", "map" -> true;
      default -> false;
    };
  }
}
