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
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.spi.SystemScanRequest;
import ai.floedb.floecat.scanner.spi.TopologyGraph;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

final class NamespaceScanSupport {
  private NamespaceScanSupport() {}

  static List<NamespaceEntry> entries(SystemObjectScanContext ctx) {
    return entries(ctx, SystemScanRequest.empty(), null);
  }

  static List<NamespaceEntry> entries(
      SystemObjectScanContext ctx, SystemScanRequest request, String schemaColumnName) {
    if (request.constraints().isAlwaysFalse()) {
      return List.of();
    }
    Set<String> schemaNames =
        schemaColumnName == null
            ? null
            : request.constraints().values(schemaColumnName).orElse(null);
    if (ctx.supportsLightweightRefs()) {
      List<TopologyGraph.NamespaceRef> refs;
      if (schemaNames == null) {
        refs = ctx.listNamespaceRefs();
      } else if (canUseDirectNamespaceLookup(schemaNames)) {
        refs = ctx.listNamespaceRefsByName(schemaNames);
      } else {
        refs =
            ctx.listNamespaceRefs().stream()
                .filter(ns -> schemaNames.contains(schemaName(ns)))
                .toList();
      }
      return refs.stream()
          .map(ns -> new NamespaceEntry(ns.id(), catalogIdFor(ctx, ns), schemaName(ns)))
          .toList();
    }
    Stream<NamespaceNode> namespaces = ctx.listNamespaces().stream();
    if (schemaNames != null) {
      namespaces = namespaces.filter(ns -> schemaNames.contains(schemaName(ns)));
    }
    return namespaces
        .map(ns -> new NamespaceEntry(ns.id(), ns.catalogId(), schemaName(ns)))
        .toList();
  }

  static List<TopologyGraph.RelationRef> relationRefs(
      SystemObjectScanContext ctx,
      ResourceId namespaceId,
      SystemScanRequest request,
      String relationNameColumn) {
    if (request.constraints().isAlwaysFalse()) {
      return List.of();
    }
    Set<String> relationNames =
        relationNameColumn == null
            ? null
            : request.constraints().values(relationNameColumn).orElse(null);
    if (relationNames == null) {
      return ctx.listRelationRefs(namespaceId);
    }
    return ctx.listRelationRefsByName(namespaceId, relationNames);
  }

  static List<RelationNode> relations(
      SystemObjectScanContext ctx,
      ResourceId namespaceId,
      SystemScanRequest request,
      String relationNameColumn) {
    if (request.constraints().isAlwaysFalse()) {
      return List.of();
    }
    Set<String> relationNames =
        relationNameColumn == null
            ? null
            : request.constraints().values(relationNameColumn).orElse(null);
    if (ctx.supportsLightweightRefs() && relationNames != null) {
      return ctx.listRelationRefsByName(namespaceId, relationNames).stream()
          .map(ref -> ctx.tryResolve(ref.id()))
          .flatMap(Optional::stream)
          .filter(RelationNode.class::isInstance)
          .map(RelationNode.class::cast)
          .toList();
    }
    Stream<RelationNode> relations = ctx.listRelations(namespaceId).stream();
    if (relationNames != null) {
      relations = relations.filter(rel -> relationNames.contains(rel.displayName()));
    }
    return relations.toList();
  }

  private static ResourceId catalogIdFor(
      SystemObjectScanContext ctx, TopologyGraph.NamespaceRef namespace) {
    ResourceId catalogId = namespace.catalogId();
    if (catalogId == null || catalogId.getId().isBlank()) {
      return ctx.queryDefaultCatalogId();
    }
    return catalogId;
  }

  private static String schemaName(NamespaceNode namespace) {
    return NameRefUtil.namespaceName(namespace.pathSegments(), namespace.displayName());
  }

  private static String schemaName(TopologyGraph.NamespaceRef namespace) {
    return NameRefUtil.namespaceName(namespace.pathSegments(), namespace.name());
  }

  private static boolean canUseDirectNamespaceLookup(Set<String> schemaNames) {
    return schemaNames.stream().noneMatch(name -> name != null && name.contains("."));
  }

  record NamespaceEntry(ResourceId id, ResourceId catalogId, String schemaName) {}
}
