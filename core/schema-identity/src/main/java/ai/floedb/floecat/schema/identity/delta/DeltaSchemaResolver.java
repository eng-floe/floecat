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

import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.schema.identity.ColumnPath;
import ai.floedb.floecat.schema.identity.NodeKind;
import ai.floedb.floecat.schema.identity.ResolvedSchema;
import ai.floedb.floecat.schema.identity.SchemaNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

/**
 * Produces a {@link ResolvedSchema} from Delta schema JSON, extracting whatever native ids Delta
 * supplied.
 *
 * <p>This is a producer, not the canonical identity abstraction. It knows about physical names,
 * column mapping modes and nested ids so that nothing downstream has to.
 *
 * <h2>Two native id sources</h2>
 *
 * Delta stamps {@code delta.columnMapping.id} onto struct fields. Collection interiors are not
 * struct fields and so carry no such metadata; Delta assigns their ids separately through {@code
 * delta.columnMapping.nested.ids}, a map held on the nearest enclosing struct field. Both draw from
 * one id namespace, which this resolver validates as such.
 *
 * <p>Nested-id keys are rooted at the enclosing field's <em>physical</em> name and extended once
 * per collection level, matching what Delta's own writer produces:
 *
 * <pre>
 *   items ARRAY&lt;STRUCT&lt;x INT&gt;&gt;      physical name col-abc
 *     items         delta.columnMapping.id
 *     items[]       nested.ids["col-abc.element"]
 *     items[].x     x's own delta.columnMapping.id      (chain restarts at a struct field)
 *
 *   attrs MAP&lt;STRING, ARRAY&lt;LONG&gt;&gt;   physical name col-xyz
 *     attrs         delta.columnMapping.id
 *     attrs.key     nested.ids["col-xyz.key"]
 *     attrs{}       nested.ids["col-xyz.value"]
 *     attrs{}[]     nested.ids["col-xyz.value.element"]
 * </pre>
 *
 * <p>Nested ids are written only by writers that need them, chiefly for Iceberg compatibility, so a
 * collection interior with no native id is legal even in a fully mapped table. Such a node reports
 * an empty {@link SchemaNode#nativeFieldId()} and gets its canonical id from the identity layer.
 *
 * <p>The schema JSON is parsed directly, with no dependency on Delta Kernel: callers in the
 * catalog-access stack hold a schema string rather than a Kernel snapshot.
 */
public final class DeltaSchemaResolver {

  public static final String COLUMN_MAPPING_ID_KEY = "delta.columnMapping.id";
  public static final String COLUMN_MAPPING_PHYSICAL_NAME_KEY = "delta.columnMapping.physicalName";
  public static final String COLUMN_MAPPING_NESTED_IDS_KEY = "delta.columnMapping.nested.ids";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private DeltaSchemaResolver() {}

  /**
   * Resolves Delta schema JSON without knowing whether column mapping governs the table.
   *
   * <p><b>Yields no authoritative identity.</b> Every node comes back with an empty {@code
   * nativeFieldId} and an empty {@code sourcePhysicalPath}, so the result can never be seeded from
   * native ids. Not knowing the effective mode means not knowing whether any mapping metadata in
   * the schema governs, and guessing would risk adopting ids that do not identify anything.
   *
   * <p>What still works is lookup: residual physical names are consulted when resolving Parquet
   * paths and statistics names, and statistics names match against either spelling. Use this
   * overload for discovery and diagnostics; use {@link #resolve(String, ColumnMappingMode)}
   * wherever the table's effective mode is known, which is anywhere identity matters.
   */
  public static DeltaResolvedSchema resolve(String schemaJson) {
    return resolve(schemaJson, null);
  }

  /**
   * Resolves Delta schema JSON for a table whose column mapping mode is known. A blank schema
   * yields an empty result; a malformed or ambiguous one throws.
   *
   * <p>Resolution fails rather than proceeding ambiguously. Rejected: JSON that parses but is not a
   * Delta struct schema, two nodes sharing a canonical or physical path, two nodes sharing a native
   * id (ordinary and nested ids occupy one namespace), nested-id metadata that is malformed or
   * names a node that does not exist, and — in a mapped table — a struct field missing its id or
   * its physical name.
   *
   * <p>Only an active mapping mode produces authoritative identity. Under {@link
   * ColumnMappingMode#NONE} Delta resolves columns by display name, so any {@code
   * delta.columnMapping.*} metadata still present in the schema is inert residue: it is ignored
   * entirely, and every node comes back with no native id and no source physical path.
   *
   * @param effectiveMappingMode the mode that actually governs this table, or null when it is not
   *     known. This is <em>not</em> simply the {@code delta.columnMapping.mode} property: Delta
   *     honours that property only when the table's reader and writer protocol versions and
   *     features support column mapping, so a table can carry the property without the feature
   *     being active. Determining that is the caller's job — the catalog-access layer reads the
   *     protocol action alongside the metadata and already has what it needs — and this module
   *     deliberately stays out of protocol negotiation. Passing the raw property where the feature
   *     is not enabled would make this resolver treat inert metadata as governing identity.
   */
  public static DeltaResolvedSchema resolve(
      String schemaJson, ColumnMappingMode effectiveMappingMode) {
    Walk walk = new Walk(effectiveMappingMode);
    if (schemaJson != null && !schemaJson.isBlank()) {
      JsonNode parsed;
      try {
        parsed = MAPPER.readTree(schemaJson);
      } catch (Exception e) {
        throw new IllegalArgumentException("Failed to parse Delta schema JSON", e);
      }
      walk.struct(
          parsed, ColumnPath.ROOT, Optional.of(ColumnPath.ROOT), walk.statsRoot, walk.parquetRoot);
      walk.verify();
    }
    return new DeltaResolvedSchema(
        ResolvedSchema.of(TableFormat.TF_DELTA, walk.nodes),
        effectiveMappingMode,
        walk.statsRoot,
        walk.parquetRoot);
  }

  /** Single traversal building the node list and both Delta-specific indexes. */
  private static final class Walk {
    private final List<SchemaNode> nodes = new ArrayList<>();
    private final DeltaStatsNameIndex statsRoot = new DeltaStatsNameIndex();
    private final DeltaParquetPathIndex parquetRoot = new DeltaParquetPathIndex();
    private final List<String> problems = new ArrayList<>();
    private final List<NestedIds> nestedIdHolders = new ArrayList<>();
    private final ColumnMappingMode effectiveMode;

    private Walk(ColumnMappingMode effectiveMode) {
      this.effectiveMode = effectiveMode;
    }

    /** True when column mapping actually governs this table's identity. */
    private boolean mapped() {
      return effectiveMode != null && effectiveMode.isEnabled();
    }

    private void verify() {
      for (NestedIds holder : nestedIdHolders) {
        for (String key : holder.unconsumed()) {
          problems.add(
              "field "
                  + describe(holder.owner)
                  + " has a nested id for '"
                  + key
                  + "' that matches no node in its type");
        }
      }
      if (!problems.isEmpty()) {
        throw new IllegalArgumentException(
            "Invalid Delta column mapping metadata: " + String.join("; ", problems));
      }
    }

    private void struct(
        JsonNode structNode,
        ColumnPath logicalPrefix,
        Optional<ColumnPath> physicalPrefix,
        DeltaStatsNameIndex parentStats,
        DeltaParquetPathIndex parentParquet) {
      if (structNode == null || !structNode.isObject()) {
        problems.add(describe(logicalPrefix) + " is not a Delta struct object");
        return;
      }
      String tag = structNode.path("type").asText("");
      if (!tag.isEmpty() && !"struct".equals(tag)) {
        problems.add(
            describe(logicalPrefix) + " has type '" + tag + "' where a Delta struct was expected");
        return;
      }
      JsonNode fields = structNode.get("fields");
      if (fields == null || !fields.isArray()) {
        // An explicit empty list is a legal empty struct; a missing or non-array one is malformed
        // and must not be read as "no columns".
        problems.add(
            describe(logicalPrefix) + " has no 'fields' array and is not a Delta struct schema");
        return;
      }
      int ordinal = 0;
      for (JsonNode field : fields) {
        String name = field.path("name").asText("");
        if (name.isEmpty()) {
          problems.add("a field under " + describe(logicalPrefix) + " has no name");
          continue;
        }
        ordinal++;
        ColumnPath path = logicalPrefix.field(name);
        Optional<String> authoritative = authoritativePhysicalName(field, path);
        // Two different names. The lookup name drives the Parquet and statistics indexes and must
        // work under any mode, so with the mode unknown it still consults residual metadata. Only
        // an authoritative name — one written under an active mapping mode — becomes identity.
        String lookupName =
            mapped()
                ? authoritative.orElse(name)
                : effectiveMode == null ? rawPhysicalName(field).orElse(name) : name;
        Optional<ColumnPath> sourcePhysicalPath =
            authoritative.isPresent()
                ? physicalPrefix.map(prefix -> prefix.field(authoritative.orElseThrow()))
                : Optional.empty();
        JsonNode typeNode = field.get("type");

        DeltaParquetPathIndex parquet = parentParquet.structChild(lookupName, name, mapped());
        DeltaStatsNameIndex stats =
            parentStats == null ? null : parentStats.child(name, lookupName, effectiveMode);

        SchemaNode node =
            register(
                path,
                sourcePhysicalPath,
                NodeKind.FIELD,
                ordinal,
                typeNode,
                authoritativeFieldId(field, path),
                stats,
                parquet);

        type(
            typeNode,
            node,
            sourcePhysicalPath,
            nestedIds(field, path),
            authoritative.orElse(name),
            stats,
            parquet);
      }
    }

    /**
     * Descends a field's type. {@code nestedIds} and {@code nestedKey} carry the nested-id map of
     * the nearest enclosing struct field and the key accumulated so far; both restart at the next
     * struct field.
     */
    private void type(
        JsonNode typeNode,
        SchemaNode parent,
        Optional<ColumnPath> parentPhysicalPath,
        NestedIds nestedIds,
        String nestedKey,
        DeltaStatsNameIndex parentStats,
        DeltaParquetPathIndex parentParquet) {
      if (typeNode == null || !typeNode.isObject()) {
        return;
      }
      switch (typeNode.path("type").asText("")) {
        case "struct" ->
            struct(typeNode, parent.path(), parentPhysicalPath, parentStats, parentParquet);
        case "array" -> {
          String key = nestedKey + "." + DeltaParquetPathIndex.ELEMENT;
          Optional<ColumnPath> sourcePhysicalPath =
              parentPhysicalPath.map(ColumnPath::arrayElement);
          DeltaParquetPathIndex parquet = parentParquet.elementChild();
          SchemaNode element =
              register(
                  parent.path().arrayElement(),
                  sourcePhysicalPath,
                  NodeKind.ARRAY_ELEMENT,
                  1,
                  typeNode.get("elementType"),
                  nestedIds.take(key),
                  null,
                  parquet);
          // Delta collects no statistics inside a collection, so the stats index stops here.
          type(
              typeNode.get("elementType"),
              element,
              sourcePhysicalPath,
              nestedIds,
              key,
              null,
              parquet);
        }
        case "map" -> {
          String keyKey = nestedKey + "." + DeltaParquetPathIndex.KEY;
          Optional<ColumnPath> keyPhysical = parentPhysicalPath.map(ColumnPath::mapKey);
          DeltaParquetPathIndex keyParquet = parentParquet.keyChild();
          SchemaNode mapKey =
              register(
                  parent.path().mapKey(),
                  keyPhysical,
                  NodeKind.MAP_KEY,
                  1,
                  typeNode.get("keyType"),
                  nestedIds.take(keyKey),
                  null,
                  keyParquet);
          type(typeNode.get("keyType"), mapKey, keyPhysical, nestedIds, keyKey, null, keyParquet);

          String valueKey = nestedKey + "." + DeltaParquetPathIndex.VALUE;
          Optional<ColumnPath> valuePhysical = parentPhysicalPath.map(ColumnPath::mapValue);
          DeltaParquetPathIndex valueParquet = parentParquet.valueChild();
          SchemaNode mapValue =
              register(
                  parent.path().mapValue(),
                  valuePhysical,
                  NodeKind.MAP_VALUE,
                  2,
                  typeNode.get("valueType"),
                  nestedIds.take(valueKey),
                  null,
                  valueParquet);
          type(
              typeNode.get("valueType"),
              mapValue,
              valuePhysical,
              nestedIds,
              valueKey,
              null,
              valueParquet);
        }
        default -> {}
      }
    }

    private SchemaNode register(
        ColumnPath path,
        Optional<ColumnPath> sourcePhysicalPath,
        NodeKind kind,
        int ordinal,
        JsonNode typeNode,
        OptionalInt nativeFieldId,
        DeltaStatsNameIndex stats,
        DeltaParquetPathIndex parquet) {
      SchemaNode node =
          new SchemaNode(
              path, kind, ordinal, !isContainer(typeNode), nativeFieldId, sourcePhysicalPath);
      nodes.add(node);
      parquet.setNode(node);
      if (stats != null) {
        stats.setNode(node);
      }
      return node;
    }

    /** Reads a physical name out of the metadata without judging whether it governs the table. */
    private static Optional<String> rawPhysicalName(JsonNode field) {
      JsonNode metadata = field.get("metadata");
      if (metadata != null && metadata.isObject()) {
        String physical = metadata.path(COLUMN_MAPPING_PHYSICAL_NAME_KEY).asText("");
        if (!physical.isBlank()) {
          return Optional.of(physical);
        }
      }
      return Optional.empty();
    }

    /**
     * The physical name that <em>authoritatively</em> identifies a field — that is, one written
     * under an active column mapping mode. Empty under any other mode, however much residual
     * metadata the schema happens to carry.
     */
    private Optional<String> authoritativePhysicalName(JsonNode field, ColumnPath path) {
      if (!mapped()) {
        return Optional.empty();
      }
      Optional<String> physical = rawPhysicalName(field);
      if (physical.isEmpty()) {
        problems.add(
            "field "
                + describe(path)
                + " has no "
                + COLUMN_MAPPING_PHYSICAL_NAME_KEY
                + " but the table uses column mapping mode "
                + effectiveMode);
      }
      return physical;
    }

    /**
     * The field's authoritative native id. Read only under an active mapping mode: outside one,
     * Delta resolves columns by display name and any {@code delta.columnMapping.id} left in the
     * schema is inert residue that must not be mistaken for governing identity.
     */
    private OptionalInt authoritativeFieldId(JsonNode field, ColumnPath path) {
      if (!mapped()) {
        return OptionalInt.empty();
      }
      JsonNode metadata = field.get("metadata");
      if (metadata != null && metadata.isObject()) {
        JsonNode id = metadata.get(COLUMN_MAPPING_ID_KEY);
        if (id != null && !id.isNull()) {
          if (!id.canConvertToInt() || id.asInt(0) <= 0) {
            problems.add(
                "field " + describe(path) + " has a non-positive " + COLUMN_MAPPING_ID_KEY);
            return OptionalInt.empty();
          }
          return OptionalInt.of(id.asInt());
        }
      }
      problems.add(
          "field "
              + describe(path)
              + " has no "
              + COLUMN_MAPPING_ID_KEY
              + " but the table uses column mapping mode "
              + effectiveMode);
      return OptionalInt.empty();
    }

    private NestedIds nestedIds(JsonNode field, ColumnPath path) {
      if (!mapped()) {
        return NestedIds.empty();
      }
      JsonNode metadata = field.get("metadata");
      if (metadata == null || !metadata.isObject()) {
        return NestedIds.empty();
      }
      JsonNode nested = metadata.get(COLUMN_MAPPING_NESTED_IDS_KEY);
      if (nested == null || nested.isNull()) {
        return NestedIds.empty();
      }
      if (!nested.isObject()) {
        problems.add(
            "field " + describe(path) + " has a non-object " + COLUMN_MAPPING_NESTED_IDS_KEY);
        return NestedIds.empty();
      }
      Map<String, Integer> ids = new LinkedHashMap<>();
      nested
          .fields()
          .forEachRemaining(
              entry -> {
                JsonNode value = entry.getValue();
                if (value == null || !value.canConvertToInt() || value.asInt(0) <= 0) {
                  problems.add(
                      "field "
                          + describe(path)
                          + " has a non-positive nested id for '"
                          + entry.getKey()
                          + "'");
                  return;
                }
                ids.put(entry.getKey(), value.asInt());
              });
      NestedIds holder = new NestedIds(path, ids);
      nestedIdHolders.add(holder);
      return holder;
    }

    private static String describe(ColumnPath path) {
      return path.isRoot() ? "<root>" : "'" + path.display() + "'";
    }
  }

  /** One field's {@code delta.columnMapping.nested.ids} map, tracking which keys were used. */
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
      Set<String> remaining = new LinkedHashSet<>(ids.keySet());
      remaining.removeAll(consumed);
      return remaining;
    }
  }

  private static boolean isContainer(JsonNode typeNode) {
    if (typeNode == null || !typeNode.isObject()) {
      return false;
    }
    String tag = typeNode.path("type").asText("");
    return "struct".equals(tag) || "array".equals(tag) || "map".equals(tag);
  }
}
