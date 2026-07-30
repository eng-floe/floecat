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

package ai.floedb.floecat.systemcatalog.util;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.types.rpc.LogicalType;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Predicates over {@link SchemaColumn} rows of a mapped table schema. */
public final class SchemaColumns {

  private SchemaColumns() {}

  private static final String KEY_SUFFIX = ".key";

  /**
   * Drops the synthetic placeholder rows the nested-type traversal emits for container children
   * that have no user-facing identity of their own: list elements (path {@code parent[]}), map
   * values ({@code parent{}}), and map keys ({@code parent.key} where {@code parent} is a MAP-typed
   * row — a real struct field named {@code key} keeps its row). These placeholders exist so schema
   * paths and stats paths cover the same node set; user-facing catalog surfaces (e.g. {@code
   * information_schema.columns}) must not present them as columns. Struct-field rows ({@code
   * parent.child}) are real named fields and are kept, as is any top-level row whose path is its
   * own name.
   */
  public static List<SchemaColumn> withoutSyntheticNodes(List<SchemaColumn> columns) {
    Set<String> arrayPaths = new HashSet<>();
    Set<String> mapPaths = new HashSet<>();
    for (SchemaColumn column : columns) {
      if (column.getType().getKind() == LogicalType.Kind.TK_ARRAY) {
        arrayPaths.add(column.getPhysicalPath());
      } else if (column.getType().getKind() == LogicalType.Kind.TK_MAP) {
        mapPaths.add(column.getPhysicalPath());
      }
    }
    return columns.stream()
        .filter(column -> !isSyntheticContainerNode(column, arrayPaths, mapPaths))
        .toList();
  }

  private static boolean isSyntheticContainerNode(
      SchemaColumn column, Set<String> arrayPaths, Set<String> mapPaths) {
    String path = column.getPhysicalPath();
    // Synthetic rows are always nested: a top-level row's path is its own name, so a user column
    // named "items[]" or "m{}" is a real column rather than a container placeholder.
    if (path.equals(column.getName())) {
      return false;
    }
    // Nested rows classify by TYPED PARENT, not by suffix alone: a struct field whose source
    // name literally ends in "[]"/"{}" (e.g. STRUCT<"items[]": INT>, path s.items[]) has no
    // ARRAY/MAP-typed row at the would-be parent path and keeps its row.
    if (path.endsWith("[]")) {
      return arrayPaths.contains(path.substring(0, path.length() - 2));
    }
    if (path.endsWith("{}")) {
      return mapPaths.contains(path.substring(0, path.length() - 2));
    }
    return path.endsWith(KEY_SUFFIX)
        && mapPaths.contains(path.substring(0, path.length() - KEY_SUFFIX.length()));
  }

  /**
   * Keeps only top-level columns (rows whose physical path equals their name). Planner-facing
   * relation payloads must use this, not {@link #withoutSyntheticNodes}: ordinals are 1-based
   * within the parent, so any nested row — struct children included — shares its ordinal (and
   * therefore its downstream attnum) with some top-level column. Nested typing reaches planners
   * through the per-column type tree.
   */
  public static List<SchemaColumn> topLevelOnly(List<SchemaColumn> columns) {
    return columns.stream()
        .filter(c -> c.getPhysicalPath().isEmpty() || c.getPhysicalPath().equals(c.getName()))
        .toList();
  }
}
