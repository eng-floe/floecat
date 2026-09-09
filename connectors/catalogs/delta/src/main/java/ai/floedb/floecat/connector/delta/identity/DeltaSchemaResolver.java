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

package ai.floedb.floecat.connector.delta.identity;

import ai.floedb.floecat.schema.identity.ColumnPath;
import ai.floedb.floecat.schema.identity.ResolvedSchema;
import ai.floedb.floecat.schema.identity.SchemaNode;
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

/**
 * Resolves a Delta schema into format-neutral source identity facts.
 *
 * <p>Delta Kernel owns the parse: this resolver walks the {@link StructType} the kernel produced
 * rather than re-reading the schema JSON, so Delta's schema shape is interpreted in exactly one
 * place. Structural faults — a missing {@code elementType}, an unparseable type tag — are therefore
 * reported by the kernel's own {@code IllegalArgumentException} before this walk begins. What this
 * resolver validates is the column-mapping metadata layered on top of that shape.
 */
public final class DeltaSchemaResolver {
  public static final String COLUMN_ID = "delta.columnMapping.id";
  public static final String PHYSICAL_NAME = "delta.columnMapping.physicalName";
  public static final String NESTED_IDS = "delta.columnMapping.nested.ids";

  private DeltaSchemaResolver() {}

  /**
   * Resolves one Delta schema version from its JSON, parsing it with Delta Kernel.
   *
   * <p>Callers holding a kernel schema already should use {@link #resolve(StructType,
   * ColumnMappingMode)} instead of re-serializing it.
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
    return resolve(DataTypeJsonSerDe.deserializeStructType(schemaJson), effectiveMode);
  }

  /**
   * Resolves one Delta schema version from the kernel schema.
   *
   * <p>See {@link #resolve(String, ColumnMappingMode)} for the effective-mode contract.
   */
  public static DeltaResolvedSchema resolve(StructType schema, ColumnMappingMode effectiveMode) {
    Objects.requireNonNull(effectiveMode, "effectiveMode");
    if (schema == null) {
      return new DeltaResolvedSchema(ResolvedSchema.of(List.of()), effectiveMode);
    }

    Walk walk = new Walk(effectiveMode);
    walk.struct(schema, ColumnPath.ROOT, Optional.of(ColumnPath.ROOT));
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
        StructType structType, ColumnPath logicalPrefix, Optional<ColumnPath> physicalPrefix) {
      int ordinal = 0;
      for (StructField field : structType.fields()) {
        ordinal++;
        // The kernel accepts an empty field name, but a path element cannot carry one, so this
        // field can never be addressed or reconciled — report it rather than failing structurally.
        String name = field.getName();
        if (name == null || name.isEmpty()) {
          problems.add("field " + ordinal + " under " + describe(logicalPrefix) + " has no name");
          continue;
        }

        ColumnPath path = logicalPrefix.field(name);
        Optional<String> physicalName = physicalName(field, path);
        Optional<ColumnPath> physicalPath =
            physicalName.isEmpty()
                ? Optional.empty()
                : physicalPrefix.map(prefix -> prefix.field(physicalName.orElseThrow()));
        DataType type = field.getDataType();

        SchemaNode node =
            new SchemaNode(path, ordinal, !isContainer(type), fieldId(field, path), physicalPath);
        nodes.add(node);
        walkType(type, node, physicalPath, nestedIds(field, path), physicalName.orElse(name));
      }
    }

    private void walkType(
        DataType type,
        SchemaNode parent,
        Optional<ColumnPath> physicalPath,
        NestedIds nestedIds,
        String nestedKey) {
      if (type instanceof StructType structType) {
        struct(structType, parent.path(), physicalPath);
      } else if (type instanceof ArrayType arrayType) {
        String elementKey = nestedKey + ".element";
        Optional<ColumnPath> elementPhysical = physicalPath.map(ColumnPath::arrayElement);
        DataType elementType = arrayType.getElementType();
        SchemaNode element =
            new SchemaNode(
                parent.path().arrayElement(),
                1,
                !isContainer(elementType),
                nestedIds.take(elementKey),
                elementPhysical);
        nodes.add(element);
        walkType(elementType, element, elementPhysical, nestedIds, elementKey);
      } else if (type instanceof MapType mapType) {
        String keyKey = nestedKey + ".key";
        Optional<ColumnPath> keyPhysical = physicalPath.map(ColumnPath::mapKey);
        DataType keyType = mapType.getKeyType();
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
        DataType valueType = mapType.getValueType();
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
    }

    private Optional<String> physicalName(StructField field, ColumnPath path) {
      if (!mode.isEnabled()) {
        return Optional.empty();
      }
      Object value = metadata(field).get(PHYSICAL_NAME);
      if (!(value instanceof String name) || name.isEmpty()) {
        problems.add(describe(path) + " has no " + PHYSICAL_NAME + " in mapping mode " + mode);
        return Optional.empty();
      }
      return Optional.of(name);
    }

    private OptionalInt fieldId(StructField field, ColumnPath path) {
      if (!mode.isEnabled()) {
        return OptionalInt.empty();
      }
      OptionalInt id = positiveInt(metadata(field).get(COLUMN_ID));
      if (id.isEmpty()) {
        problems.add(describe(path) + " has no positive " + COLUMN_ID + " in mapping mode " + mode);
      }
      return id;
    }

    private NestedIds nestedIds(StructField field, ColumnPath path) {
      if (!mode.isEnabled()) {
        return NestedIds.empty();
      }
      Object value = metadata(field).get(NESTED_IDS);
      if (value == null) {
        return NestedIds.empty();
      }
      // The kernel keeps a non-object value as-is rather than rejecting it, so a residual string
      // left behind by an older writer reaches us here.
      if (!(value instanceof FieldMetadata nested)) {
        problems.add(describe(path) + " has a non-object " + NESTED_IDS);
        return NestedIds.empty();
      }

      Map<String, Integer> ids = new LinkedHashMap<>();
      nested
          .getEntries()
          .forEach(
              (key, entry) -> {
                OptionalInt id = positiveInt(entry);
                if (key.isEmpty() || id.isEmpty()) {
                  problems.add(describe(path) + " has an invalid nested ID for '" + key + "'");
                } else {
                  ids.put(key, id.getAsInt());
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

    private static FieldMetadata metadata(StructField field) {
      FieldMetadata metadata = field.getMetadata();
      return metadata == null ? FieldMetadata.empty() : metadata;
    }

    private static OptionalInt positiveInt(Object value) {
      if (!(value instanceof Number number) || value instanceof Double || value instanceof Float) {
        return OptionalInt.empty();
      }
      long id = number.longValue();
      return id > 0L && id <= Integer.MAX_VALUE ? OptionalInt.of((int) id) : OptionalInt.empty();
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

  private static boolean isContainer(DataType type) {
    return type instanceof StructType || type instanceof ArrayType || type instanceof MapType;
  }
}
