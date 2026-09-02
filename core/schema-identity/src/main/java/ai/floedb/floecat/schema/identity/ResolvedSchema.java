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

import ai.floedb.floecat.catalog.rpc.TableFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A table's complete schema tree, resolved into format-neutral nodes.
 *
 * <p>This is what a format producer emits and what the identity layer consumes. It carries no
 * canonical ids: assigning those is a separate step, because the source format supplies them for
 * all nodes (Iceberg), some nodes (Delta with column mapping), or none (Delta without).
 *
 * <p>Scope is one schema version. A rename moves a node's path while its canonical id should
 * persist, so nothing here may be cached across schema versions — carrying identity forward is the
 * identity layer's job.
 */
public final class ResolvedSchema {

  private final TableFormat format;
  private final List<SchemaNode> nodes;
  private final Map<ColumnPath, SchemaNode> byPath;
  private final Map<ColumnPath, SchemaNode> bySourcePhysicalPath;
  private final Map<Integer, SchemaNode> byNativeFieldId;

  private ResolvedSchema(
      TableFormat format,
      List<SchemaNode> nodes,
      Map<ColumnPath, SchemaNode> byPath,
      Map<ColumnPath, SchemaNode> bySourcePhysicalPath,
      Map<Integer, SchemaNode> byNativeFieldId) {
    this.format = format;
    this.nodes = List.copyOf(nodes);
    this.byPath = Collections.unmodifiableMap(byPath);
    this.bySourcePhysicalPath = Collections.unmodifiableMap(bySourcePhysicalPath);
    this.byNativeFieldId = Collections.unmodifiableMap(byNativeFieldId);
  }

  /**
   * Builds a resolved schema, rejecting anything ambiguous.
   *
   * <p>Two nodes may not share a canonical path, a physical path, or a native field id. A producer
   * that would emit such a schema has misread its source, and resolving it either way would attach
   * one node's statistics to another.
   */
  public static ResolvedSchema of(TableFormat format, List<SchemaNode> nodes) {
    Objects.requireNonNull(format, "format");
    Objects.requireNonNull(nodes, "nodes");
    Map<ColumnPath, SchemaNode> byPath = new LinkedHashMap<>();
    Map<ColumnPath, SchemaNode> bySourcePhysicalPath = new LinkedHashMap<>();
    Map<Integer, SchemaNode> byNativeFieldId = new LinkedHashMap<>();
    List<String> problems = new ArrayList<>();

    for (SchemaNode node : nodes) {
      SchemaNode pathClash = byPath.putIfAbsent(node.path(), node);
      if (pathClash != null) {
        problems.add("duplicate canonical path '" + node.path().display() + "'");
      }
      node.sourcePhysicalPath()
          .ifPresent(
              physical -> {
                SchemaNode clash = bySourcePhysicalPath.putIfAbsent(physical, node);
                if (clash != null) {
                  problems.add(
                      "duplicate physical path '"
                          + physical.display()
                          + "' shared by '"
                          + clash.path().display()
                          + "' and '"
                          + node.path().display()
                          + "'");
                }
              });
      node.nativeFieldId()
          .ifPresent(
              id -> {
                SchemaNode clash = byNativeFieldId.putIfAbsent(id, node);
                if (clash != null) {
                  problems.add(
                      "duplicate native field id "
                          + id
                          + " shared by '"
                          + clash.path().display()
                          + "' and '"
                          + node.path().display()
                          + "'");
                }
              });
    }
    if (!problems.isEmpty()) {
      throw new IllegalArgumentException(
          "Ambiguous " + format + " schema: " + String.join("; ", problems));
    }
    return new ResolvedSchema(format, nodes, byPath, bySourcePhysicalPath, byNativeFieldId);
  }

  public TableFormat format() {
    return format;
  }

  /** Every node, in pre-order traversal with top-level columns first. */
  public List<SchemaNode> nodes() {
    return nodes;
  }

  public Optional<SchemaNode> byPath(ColumnPath path) {
    return Optional.ofNullable(byPath.get(path));
  }

  public Optional<SchemaNode> bySourcePhysicalPath(ColumnPath sourcePhysicalPath) {
    return Optional.ofNullable(bySourcePhysicalPath.get(sourcePhysicalPath));
  }

  public Optional<SchemaNode> byNativeFieldId(int nativeFieldId) {
    return nativeFieldId <= 0
        ? Optional.empty()
        : Optional.ofNullable(byNativeFieldId.get(nativeFieldId));
  }

  /** True when the source format supplied an id for every node, as Iceberg always does. */
  public boolean hasTotalNativeIds() {
    return nodes.stream().allMatch(SchemaNode::hasNativeFieldId);
  }

  /** The nodes Floecat must assign ids for itself. Empty when {@link #hasTotalNativeIds()}. */
  public List<SchemaNode> nodesWithoutNativeIds() {
    return nodes.stream().filter(node -> !node.hasNativeFieldId()).toList();
  }
}
