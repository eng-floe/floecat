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

package ai.floedb.floecat.schema.identity;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** A complete source schema version, indexed by each identity supplied by the format. */
public final class ResolvedSchema {
  private final List<SchemaNode> nodes;
  private final Map<ColumnPath, SchemaNode> byPath;
  private final Map<ColumnPath, SchemaNode> byPhysicalPath;
  private final Map<Integer, SchemaNode> byNativeId;

  public static ResolvedSchema of(List<SchemaNode> nodes) {
    return new ResolvedSchema(nodes);
  }

  private ResolvedSchema(List<SchemaNode> nodes) {
    this.nodes = List.copyOf(Objects.requireNonNull(nodes, "nodes"));
    this.byPath = new LinkedHashMap<>();
    this.byPhysicalPath = new LinkedHashMap<>();
    this.byNativeId = new LinkedHashMap<>();
    for (SchemaNode node : this.nodes) {
      requireUnique(byPath, node.path(), node, "logical path " + node.path());
      node.sourcePhysicalPath()
          .ifPresent(path -> requireUnique(byPhysicalPath, path, node, "physical path " + path));
      node.nativeFieldId()
          .ifPresent(id -> requireUnique(byNativeId, id, node, "native field ID " + id));
    }
  }

  public List<SchemaNode> nodes() {
    return nodes;
  }

  public Optional<SchemaNode> byPath(ColumnPath path) {
    return Optional.ofNullable(byPath.get(path));
  }

  public Optional<SchemaNode> bySourcePhysicalPath(ColumnPath path) {
    return Optional.ofNullable(byPhysicalPath.get(path));
  }

  public Optional<SchemaNode> byNativeFieldId(int id) {
    return Optional.ofNullable(byNativeId.get(id));
  }

  private static <K> void requireUnique(
      Map<K, SchemaNode> index, K key, SchemaNode node, String description) {
    SchemaNode existing = index.putIfAbsent(key, node);
    if (existing != null) {
      throw new IllegalArgumentException(
          "Duplicate "
              + description
              + " for "
              + existing.path().display()
              + " and "
              + node.path().display());
    }
  }
}
