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
import ai.floedb.floecat.query.rpc.SnapshotPin;
import java.util.*;

public final class FakeUserGraph {

  public final Map<String, ResourceId> tablesByName = new HashMap<>();
  public final Map<String, ResourceId> viewsByName = new HashMap<>();
  public final Map<String, ResourceId> namespacesByName = new HashMap<>();
  public final Map<ResourceId, GraphNode> nodesById = new HashMap<>();
  public final List<GraphNode> relations = new ArrayList<>();

  Optional<ResourceId> tryResolveTable(String cid, NameRef ref) {
    return Optional.ofNullable(tablesByName.get(ref.getName()));
  }

  Optional<ResourceId> tryResolveView(String cid, NameRef ref) {
    return Optional.ofNullable(viewsByName.get(ref.getName()));
  }

  Optional<ResourceId> tryResolveNamespace(String cid, NameRef ref) {
    return Optional.ofNullable(namespacesByName.get(ref.getName()));
  }

  Optional<ResourceId> tryResolveName(String cid, NameRef ref) {
    ResourceId t = tablesByName.get(ref.getName());
    ResourceId v = viewsByName.get(ref.getName());
    if (t != null && v != null) throw new IllegalArgumentException("ambiguous");
    return Optional.ofNullable(t != null ? t : v);
  }

  Optional<GraphNode> resolve(ResourceId id) {
    return Optional.ofNullable(nodesById.get(id));
  }

  List<GraphNode> listRelations(ResourceId cat) {
    return List.copyOf(relations);
  }

  SnapshotPin snapshotPinFor(String cid, ResourceId id, Object o, Optional<?> ts) {
    return SnapshotPin.newBuilder().setSnapshotId(1).build();
  }

  Optional<NameRef> tableName(ResourceId id) {
    return Optional.of(NameRef.newBuilder().setName("user").build());
  }

  Optional<NameRef> viewName(ResourceId id) {
    return Optional.of(NameRef.newBuilder().setName("user").build());
  }

  Optional<NameRef> namespaceName(ResourceId id) {
    return Optional.of(NameRef.newBuilder().setName("user").build());
  }
}
