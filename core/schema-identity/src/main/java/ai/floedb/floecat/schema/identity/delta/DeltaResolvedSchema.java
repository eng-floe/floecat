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
import ai.floedb.floecat.schema.identity.LegacyDottedKeyIndex;
import ai.floedb.floecat.schema.identity.ResolvedSchema;
import ai.floedb.floecat.schema.identity.SchemaNode;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** A resolved Delta schema and its mapping-aware statistics-name lookup. */
public record DeltaResolvedSchema(ResolvedSchema schema, ColumnMappingMode effectiveMappingMode) {

  public DeltaResolvedSchema {
    Objects.requireNonNull(schema, "schema");
    Objects.requireNonNull(effectiveMappingMode, "effectiveMappingMode");
  }

  /**
   * Resolves Delta's multi-part statistics name to its logical schema node. Statistics use physical
   * field names when column mapping is enabled and logical field names otherwise.
   */
  public Optional<SchemaNode> nodeForStatsNames(List<String> names) {
    if (names == null || names.isEmpty()) {
      return Optional.empty();
    }
    ColumnPath path = ColumnPath.ROOT;
    for (String name : names) {
      if (name == null || name.isEmpty()) {
        return Optional.empty();
      }
      path = path.field(name);
    }
    return effectiveMappingMode.isEnabled()
        ? schema.bySourcePhysicalPath(path)
        : schema.byPath(path);
  }

  /**
   * Resolves a Parquet primitive to its logical schema node using the identity that the effective
   * mapping mode makes authoritative.
   *
   * <p>Under {@code ID} the footer's field ID is the only trustworthy identity, because physical
   * names are free to differ from anything the log records. Under {@code NAME} the footer carries
   * physical names, and without mapping it carries logical ones, both of which the statistics-name
   * lookup already handles.
   */
  public Optional<SchemaNode> nodeForFooterColumn(List<String> parquetPath, Integer fieldId) {
    return switch (effectiveMappingMode) {
      case NONE, NAME -> nodeForStatsNames(parquetPath);
      case ID -> fieldId == null ? Optional.empty() : schema.byNativeFieldId(fieldId);
    };
  }

  /**
   * Translates logical column keys into the keys that Delta file statistics use for the same
   * columns.
   *
   * <p>Column mapping makes statistics physical, so the two namespaces coincide only when mapping
   * is off. Ambiguity is rejected in whichever namespaces the answer has to cross, and is judged
   * against the whole schema rather than the requested subset: a key is ambiguous because of what
   * the table contains, not because of what this caller happened to ask for.
   *
   * <ul>
   *   <li>A requested key naming more than one logical column is dropped, so an ambiguous request
   *       can never fan out into several columns' statistics.
   *   <li>Under column mapping, a column whose physical key names more than one column is dropped
   *       too, since the statistics themselves could not then be attributed.
   * </ul>
   *
   * <p>Unknown keys are dropped as well, so the result only ever names columns of this schema. See
   * {@link LegacyDottedKeyIndex}.
   */
  public Set<String> statsKeysFor(Set<String> logicalKeys) {
    Objects.requireNonNull(logicalKeys, "logicalKeys");

    Map<String, SchemaNode> requested = requestedColumns(logicalKeys);
    if (!effectiveMappingMode.isEnabled()) {
      return Collections.unmodifiableSet(new LinkedHashSet<>(requested.keySet()));
    }

    Set<String> unambiguousPhysicalKeys = unambiguousPhysicalKeys();
    LinkedHashSet<String> statsKeys = new LinkedHashSet<>();
    for (SchemaNode node : requested.values()) {
      node.sourcePhysicalPath()
          .map(ColumnPath::legacyDottedKey)
          .filter(unambiguousPhysicalKeys::contains)
          .ifPresent(statsKeys::add);
    }
    return Collections.unmodifiableSet(statsKeys);
  }

  /** The requested columns that exactly one logical column of this schema is named by. */
  private Map<String, SchemaNode> requestedColumns(Set<String> logicalKeys) {
    LegacyDottedKeyIndex<SchemaNode> logicalIndex = LegacyDottedKeyIndex.create();
    for (SchemaNode node : schema.nodes()) {
      logicalIndex.add(node.path(), node);
    }
    LinkedHashMap<String, SchemaNode> requested = new LinkedHashMap<>();
    logicalIndex
        .values()
        .forEach(
            (key, node) -> {
              if (logicalKeys.contains(key)) {
                requested.put(key, node);
              }
            });
    return requested;
  }

  /** The physical keys that name exactly one column of this schema. */
  private Set<String> unambiguousPhysicalKeys() {
    LegacyDottedKeyIndex<ColumnPath> physicalIndex = LegacyDottedKeyIndex.create();
    for (SchemaNode node : schema.nodes()) {
      node.sourcePhysicalPath().ifPresent(path -> physicalIndex.add(path, path));
    }
    return physicalIndex.keys();
  }
}
