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
package ai.floedb.floecat.extensions.floedb.hints;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import ai.floedb.floecat.systemcatalog.hint.HintClearContext;
import ai.floedb.floecat.systemcatalog.hint.HintClearDecision;
import com.google.protobuf.FieldMask;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Floe-specific decision helper that knows how to interpret schema changes when clearing hints.
 *
 * <p>It reads the before/after {@link Table} payload (and optionally the update {@link FieldMask})
 * to determine whether the relation identity/upstream changed, in which case it clears both
 * relation and column hints. If the change is limited to column schema, it computes the column IDs
 * that were added/updated/removed and clears exactly those columns; failing that it clears all
 * column hints as a conservative fallback.
 */
public final class FloeHintClearPolicy {

  private static final Set<String> RELATION_MASK_PATHS = Set.of("display_name", "namespace_id");
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();

  private static final String SCHEMA_FIELD = "schema_json";

  public HintClearDecision decide(HintClearContext context) {
    if (context == null) {
      return dropAll();
    }
    if (context.beforeView() != null || context.afterView() != null) {
      return decideForView(context.beforeView(), context.afterView(), context.mask());
    }
    Table before = context.beforeTable();
    Table after = context.afterTable();
    if (before == null || after == null) {
      return dropAll();
    }
    if (isRelationChange(before, after, context.mask())) {
      return dropAll();
    }
    if (!touchedSchema(context.mask())) {
      return new HintClearDecision(false, false, Set.of(), Set.of(), Set.of());
    }
    Optional<Set<Long>> columnIds = computeChangedColumnIds(before, after);
    if (columnIds.isEmpty()) {
      return clearColumnsFully();
    }
    Set<Long> ids = columnIds.get();
    if (ids.isEmpty()) {
      return new HintClearDecision(false, false, Set.of(), Set.of(), Set.of());
    }
    return new HintClearDecision(false, false, Set.of(), Set.of(), ids);
  }

  private HintClearDecision decideForView(View before, View after, FieldMask mask) {
    // TODO(mrouvroy): improve View hint cleanup
    return dropAll();
  }

  private boolean isRelationChange(Table before, Table after, FieldMask mask) {
    if (maskTargetsRelation(mask)) {
      return true;
    }
    if (!equals(before.getDisplayName(), after.getDisplayName())) {
      return true;
    }
    if (!before.getNamespaceId().equals(after.getNamespaceId())) {
      return true;
    }
    if (!before.getUpstream().equals(after.getUpstream())) {
      return true;
    }
    return false;
  }

  private boolean maskTargetsRelation(FieldMask mask) {
    if (mask == null) {
      return false;
    }
    for (String path : mask.getPathsList()) {
      if (path == null) {
        continue;
      }
      String normalized = normalizePath(path);
      if (RELATION_MASK_PATHS.contains(normalized) || normalized.startsWith("upstream")) {
        return true;
      }
    }
    return false;
  }

  private String normalizePath(String path) {
    return path == null ? "" : path.trim().toLowerCase();
  }

  private boolean touchedSchema(FieldMask mask) {
    if (mask == null) {
      return true; // treat missing mask as unknown, trigger schema diff
    }
    for (String path : mask.getPathsList()) {
      if (path == null) {
        continue;
      }
      if (path.trim().equalsIgnoreCase(SCHEMA_FIELD)) {
        return true;
      }
    }
    return false;
  }

  private Optional<Set<Long>> computeChangedColumnIds(Table before, Table after) {
    Optional<SchemaDescriptor> beforeSchema = parseSchema(before.getSchemaJson());
    Optional<SchemaDescriptor> afterSchema = parseSchema(after.getSchemaJson());
    if (afterSchema.isEmpty()) {
      return Optional.empty();
    }
    Optional<Map<Long, SchemaColumn>> afterColumns = mapColumns(afterSchema.get());
    if (afterColumns.isEmpty()) {
      return Optional.empty();
    }
    Map<Long, SchemaColumn> afterMap = afterColumns.get();
    Map<Long, SchemaColumn> beforeMap = beforeSchema.flatMap(this::mapColumns).orElseGet(Map::of);
    Set<Long> changed = new LinkedHashSet<>();
    for (Map.Entry<Long, SchemaColumn> entry : afterMap.entrySet()) {
      SchemaColumn beforeColumn = beforeMap.get(entry.getKey());
      if (beforeColumn == null || !entry.getValue().equals(beforeColumn)) {
        changed.add(entry.getKey());
      }
    }
    for (Long id : beforeMap.keySet()) {
      if (!afterMap.containsKey(id)) {
        changed.add(id);
      }
    }
    return Optional.of(changed);
  }

  private Optional<SchemaDescriptor> parseSchema(String schemaJson) {
    if (schemaJson == null || schemaJson.isBlank()) {
      return Optional.of(SchemaDescriptor.getDefaultInstance());
    }
    SchemaDescriptor.Builder builder = SchemaDescriptor.newBuilder();
    try {
      JSON_PARSER.merge(schemaJson, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      return Optional.empty();
    }
  }

  private Optional<Map<Long, SchemaColumn>> mapColumns(SchemaDescriptor descriptor) {
    Map<Long, SchemaColumn> result = new LinkedHashMap<>();
    for (SchemaColumn column : descriptor.getColumnsList()) {
      long id = stableColumnId(column);
      if (id <= 0) {
        return Optional.empty();
      }
      result.put(id, column);
    }
    return Optional.of(result);
  }

  private long stableColumnId(SchemaColumn column) {
    if (column.getId() != 0L) {
      return column.getId();
    }
    return -1;
  }

  private HintClearDecision dropAll() {
    return new HintClearDecision(true, true, Set.of(), Set.of(), Set.of());
  }

  private HintClearDecision clearColumnsFully() {
    return new HintClearDecision(false, true, Set.of(), Set.of(), Set.of());
  }

  private boolean equals(String a, String b) {
    return a == null ? b == null : a.equals(b);
  }
}
