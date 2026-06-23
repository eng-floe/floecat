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

package ai.floedb.floecat.systemcatalog.informationschema;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.TopologyGraph;
import ai.floedb.floecat.scanner.spi.TopologyNames;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class TestRefCatalogOverlay extends TestCatalogOverlay {

  private final List<TopologyGraph.NamespaceRef> namespaceRefs = new ArrayList<>();
  private final Map<ResourceId, List<TopologyGraph.RelationRef>> relationRefsByNamespace =
      new HashMap<>();
  private final Map<ResourceId, Set<String>> relationNamesByNamespace = new HashMap<>();

  private String namespaceRefsFailure;
  private String namespaceRefsByNameFailure;
  private String relationRefsFailure;
  private String relationRefsByNameFailure;
  private Set<String> namespaceNames = Set.of();

  TestRefCatalogOverlay withNamespaceRef(
      ResourceId namespaceId, String name, ResourceId catalogId, List<String> pathSegments) {
    namespaceRefs.add(new TopologyGraph.NamespaceRef(namespaceId, name, catalogId, pathSegments));
    return this;
  }

  TestRefCatalogOverlay withRelationRef(
      ResourceId namespaceId, ResourceId relationId, String name, ResourceKind kind) {
    relationRefsByNamespace
        .computeIfAbsent(namespaceId, ignored -> new ArrayList<>())
        .add(new TopologyGraph.RelationRef(relationId, name, kind));
    return this;
  }

  TestRefCatalogOverlay failNamespaceRefs(String message) {
    namespaceRefsFailure = message;
    return this;
  }

  TestRefCatalogOverlay failNamespaceRefsByName(String message) {
    namespaceRefsByNameFailure = message;
    return this;
  }

  TestRefCatalogOverlay failRelationRefs(String message) {
    relationRefsFailure = message;
    return this;
  }

  TestRefCatalogOverlay failRelationRefsByName(String message) {
    relationRefsByNameFailure = message;
    return this;
  }

  Set<String> namespaceNames() {
    return namespaceNames;
  }

  Set<String> relationNames(ResourceId namespaceId) {
    return relationNamesByNamespace.getOrDefault(namespaceId, Set.of());
  }

  @Override
  public boolean supportsLightweightRefs() {
    return true;
  }

  @Override
  public List<TopologyGraph.NamespaceRef> listNamespaceRefs(ResourceId catalogId) {
    if (namespaceRefsFailure != null) {
      throw new AssertionError(namespaceRefsFailure);
    }
    return List.copyOf(namespaceRefs);
  }

  @Override
  public List<TopologyGraph.NamespaceRef> listNamespaceRefsByName(
      ResourceId catalogId, Set<String> names) {
    if (namespaceRefsByNameFailure != null) {
      throw new AssertionError(namespaceRefsByNameFailure);
    }
    namespaceNames = Set.copyOf(names);
    return namespaceRefs.stream()
        .filter(ns -> names.contains(TopologyNames.namespaceName(ns.pathSegments(), ns.name())))
        .toList();
  }

  @Override
  public List<TopologyGraph.RelationRef> listRelationRefs(
      ResourceId catalogId, ResourceId namespaceId) {
    if (relationRefsFailure != null) {
      throw new AssertionError(relationRefsFailure);
    }
    return List.copyOf(relationRefsByNamespace.getOrDefault(namespaceId, List.of()));
  }

  @Override
  public List<TopologyGraph.RelationRef> listRelationRefsByName(
      ResourceId catalogId, ResourceId namespaceId, Set<String> names) {
    if (relationRefsByNameFailure != null) {
      throw new AssertionError(relationRefsByNameFailure);
    }
    relationNamesByNamespace.put(namespaceId, Set.copyOf(names));
    return relationRefsByNamespace.getOrDefault(namespaceId, List.of()).stream()
        .filter(ref -> names.contains(ref.name()))
        .toList();
  }
}
