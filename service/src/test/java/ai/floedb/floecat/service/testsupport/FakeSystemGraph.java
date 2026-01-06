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

package ai.floedb.floecat.service.testsupport;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.GraphNode;
import java.util.*;

public final class FakeSystemGraph {

  public final Map<ResourceId, GraphNode> nodesById = new HashMap<>();
  public final Map<String, ResourceId> tablesByName = new HashMap<>();
  public final Map<String, ResourceId> viewsByName = new HashMap<>();
  public final Map<String, ResourceId> namespacesByName = new HashMap<>();

  public final List<GraphNode> relations = new ArrayList<>();
  final List<GraphNode> namespaces = new ArrayList<>();
  final List<GraphNode> functions = new ArrayList<>();
  final List<GraphNode> types = new ArrayList<>();

  Optional<ResourceId> resolveTable(NameRef ref, String ek, String ev) {
    return Optional.ofNullable(tablesByName.get(ref.getName()));
  }

  Optional<ResourceId> resolveView(NameRef ref, String ek, String ev) {
    return Optional.ofNullable(viewsByName.get(ref.getName()));
  }

  Optional<ResourceId> resolveNamespace(NameRef ref, String ek, String ev) {
    return Optional.ofNullable(namespacesByName.get(ref.getName()));
  }

  Optional<ResourceId> resolveName(NameRef ref, String ek, String ev) {
    return resolveTable(ref, ek, ev)
        .or(() -> resolveView(ref, ek, ev))
        .or(() -> resolveNamespace(ref, ek, ev));
  }

  Optional<GraphNode> resolve(ResourceId id, String ek, String ev) {
    return Optional.ofNullable(nodesById.get(id));
  }

  List<GraphNode> listRelations(ResourceId cat, String ek, String ev) {
    return List.copyOf(relations);
  }

  List<GraphNode> listRelationsInNamespace(ResourceId cat, ResourceId ns, String ek, String ev) {
    return List.copyOf(relations);
  }

  List<GraphNode> listFunctions(ResourceId ns, String ek, String ev) {
    return List.copyOf(functions);
  }

  List<GraphNode> listTypes(ResourceId cat, String ek, String ev) {
    return List.copyOf(types);
  }

  List<GraphNode> listNamespaces(ResourceId cat, String ek, String ev) {
    return List.copyOf(namespaces);
  }

  Optional<NameRef> tableName(ResourceId id, String ek, String ev) {
    return Optional.of(NameRef.newBuilder().setName("system").build());
  }

  Optional<NameRef> viewName(ResourceId id, String ek, String ev) {
    return Optional.of(NameRef.newBuilder().setName("system").build());
  }

  Optional<NameRef> namespaceName(ResourceId id, String ek, String ev) {
    return Optional.of(NameRef.newBuilder().setName("system").build());
  }
}
