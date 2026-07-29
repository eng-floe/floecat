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
   * parent.child}) are real named fields and are kept.
   */
  public static List<SchemaColumn> withoutSyntheticNodes(List<SchemaColumn> columns) {
    Set<String> mapPaths = new HashSet<>();
    for (SchemaColumn column : columns) {
      if (column.getType().getKind() == LogicalType.Kind.TK_MAP) {
        mapPaths.add(column.getPhysicalPath());
      }
    }
    return columns.stream().filter(column -> !isSyntheticContainerNode(column, mapPaths)).toList();
  }

  private static boolean isSyntheticContainerNode(SchemaColumn column, Set<String> mapPaths) {
    String path = column.getPhysicalPath();
    if (path.endsWith("[]") || path.endsWith("{}")) {
      return true;
    }
    return path.endsWith(KEY_SUFFIX)
        && mapPaths.contains(path.substring(0, path.length() - KEY_SUFFIX.length()));
  }
}
