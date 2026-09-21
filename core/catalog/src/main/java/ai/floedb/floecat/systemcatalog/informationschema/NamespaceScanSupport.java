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
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.spi.SystemScanRequest;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.util.List;
import java.util.Set;

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
    List<CatalogGraphView.NamespaceRef> refs;
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

  static List<CatalogGraphView.RelationRef> relationRefs(
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
    List<RelationNode> relations =
        (relationNames == null
                ? ctx.listRelationRefs(namespaceId)
                : ctx.listRelationRefsByName(namespaceId, relationNames))
            .stream()
                .map(ref -> ctx.tryResolve(ref.id()))
                .flatMap(java.util.Optional::stream)
                .filter(RelationNode.class::isInstance)
                .map(RelationNode.class::cast)
                .toList();
    if (relationNames != null) {
      return relations.stream().filter(rel -> relationNames.contains(rel.displayName())).toList();
    }
    return relations;
  }

  private static ResourceId catalogIdFor(
      SystemObjectScanContext ctx, CatalogGraphView.NamespaceRef namespace) {
    ResourceId catalogId = namespace.catalogId();
    if (catalogId == null || catalogId.getId().isBlank()) {
      return ctx.queryDefaultCatalogId();
    }
    return catalogId;
  }

  private static String schemaName(CatalogGraphView.NamespaceRef namespace) {
    return NameRefUtil.namespaceName(namespace.pathSegments(), namespace.name());
  }

  private static boolean canUseDirectNamespaceLookup(Set<String> schemaNames) {
    return schemaNames.stream().noneMatch(name -> name != null && name.contains("."));
  }

  record NamespaceEntry(ResourceId id, ResourceId catalogId, String schemaName) {}
}
