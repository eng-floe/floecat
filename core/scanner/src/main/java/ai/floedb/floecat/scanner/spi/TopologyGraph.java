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

package ai.floedb.floecat.scanner.spi;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import java.util.List;
import java.util.Set;

/**
 * Lightweight topology view of a catalog: names, IDs, and kinds -- available entirely from pointer
 * metadata (no blob fetch, no S3). {@link CatalogOverlay} exposes these ref-listing methods to
 * scanners and traversal callers; this interface remains the shared ref shape and cache
 * invalidation hook.
 *
 * <p>Scanners that only need to enumerate objects should prefer {@link
 * CatalogOverlay#listRelationRefs} / {@link CatalogOverlay#listNamespaceRefs} over full object
 * listing methods that force object materialization.
 */
public interface TopologyGraph {

  /** Returns all namespace refs visible in the given catalog. */
  default List<NamespaceRef> listNamespaceRefs(ResourceId catalogId) {
    return java.util.List.of();
  }

  /** Returns namespace refs whose information_schema name matches the given set. */
  default List<NamespaceRef> listNamespaceRefsByName(ResourceId catalogId, Set<String> names) {
    return listNamespaceRefs(catalogId).stream()
        .filter(r -> names.contains(TopologyNames.namespaceName(r.pathSegments(), r.name())))
        .collect(java.util.stream.Collectors.toList());
  }

  /** Returns all relation refs (tables + views) in the given namespace. */
  default List<RelationRef> listRelationRefs(ResourceId catalogId, ResourceId namespaceId) {
    return java.util.List.of();
  }

  /**
   * Returns relation refs whose names match the given set. Allows predicate pushdown for {@code
   * WHERE table_name IN (...)} filters.
   */
  default List<RelationRef> listRelationRefsByName(
      ResourceId catalogId, ResourceId namespaceId, Set<String> names) {
    return listRelationRefs(catalogId, namespaceId).stream()
        .filter(r -> names.contains(r.name()))
        .collect(java.util.stream.Collectors.toList());
  }

  /**
   * Evicts a table or view from the topology cache using the reverse map to locate its namespace.
   * Call BEFORE the storage delete so the reverse-map lookup still works.
   */
  default void evict(ResourceId resourceId) {}

  /**
   * Evicts the cached relation list for a specific namespace. Call after a table/view create/delete
   * when the namespace ID is known directly.
   */
  default void evictRelationRefs(ResourceId namespaceId) {}

  /**
   * Evicts the cached namespace list for a specific catalog. Call after a namespace create/delete.
   */
  default void evictNamespaceRefs(ResourceId catalogId) {}

  record NamespaceRef(ResourceId id, String name, ResourceId catalogId, List<String> pathSegments) {
    public NamespaceRef(ResourceId id, String name) {
      this(id, name, null, List.of());
    }

    public NamespaceRef {
      pathSegments = pathSegments == null ? List.of() : List.copyOf(pathSegments);
    }
  }

  record RelationRef(ResourceId id, String name, ResourceKind kind) {}
}
