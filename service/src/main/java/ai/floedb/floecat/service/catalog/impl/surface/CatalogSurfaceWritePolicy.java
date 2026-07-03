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
package ai.floedb.floecat.service.catalog.impl.surface;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.spi.TopologyGraph;
import ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.systemcatalog.graph.SystemResourceIdGenerator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Catalog Surface visibility and write eligibility policy. */
public final class CatalogSurfaceWritePolicy {

  private static final String PATH_DELIM = "\u001F";

  private final CatalogOverlay overlay;

  public CatalogSurfaceWritePolicy(CatalogOverlay overlay) {
    this.overlay = Objects.requireNonNull(overlay, "catalog overlay is required");
  }

  public CatalogNode requireVisibleCatalog(ResourceId catalogId, String field, String corr) {
    return CatalogSurfaceSupport.requireVisibleCatalog(overlay, catalogId, field, corr);
  }

  public CatalogNode requireWritableCatalog(ResourceId catalogId, String corr) {
    return requireWritableCatalog(catalogId, "catalog_id", corr);
  }

  public CatalogNode requireWritableCatalog(ResourceId catalogId, String field, String corr) {
    CatalogSurfaceSupport.ensureKind(catalogId, ResourceKind.RK_CATALOG, field, corr);
    if (SystemResourceIdGenerator.isSystemId(catalogId)) {
      throw GrpcErrors.permissionDenied(
          corr, SYSTEM_OBJECT_IMMUTABLE, Map.of("id", catalogId.getId(), "kind", "catalog"));
    }
    return requireVisibleCatalog(catalogId, field, corr);
  }

  public NamespaceNode requireVisibleNamespace(ResourceId namespaceId, String corr) {
    return CatalogSurfaceSupport.requireVisibleNamespace(overlay, namespaceId, corr);
  }

  public NamespaceNode requireWritableNamespace(ResourceId namespaceId, String corr) {
    return requireWritableNamespace(namespaceId, "namespace_id", corr);
  }

  public NamespaceNode requireWritableNamespace(ResourceId namespaceId, String field, String corr) {
    CatalogSurfaceSupport.ensureKind(namespaceId, ResourceKind.RK_NAMESPACE, field, corr);
    if (SystemResourceIdGenerator.isSystemId(namespaceId)) {
      throw GrpcErrors.permissionDenied(
          corr, SYSTEM_OBJECT_IMMUTABLE, Map.of("id", namespaceId.getId(), "kind", "namespace"));
    }
    var namespace = requireVisibleNamespace(namespaceId, corr);
    if (namespace.origin() == GraphNodeOrigin.SYSTEM) {
      throw GrpcErrors.permissionDenied(
          corr, SYSTEM_OBJECT_IMMUTABLE, Map.of("id", namespaceId.getId(), "kind", "namespace"));
    }
    return namespace;
  }

  /**
   * Delete-path guard for catalogs. Rejects system catalogs (immutable) but, unlike {@link
   * #requireWritableCatalog}, does <b>not</b> require the catalog to currently resolve through the
   * overlay: an unresolved target is left for the repository fallback, which deletes idempotently
   * (and enforces any caller precondition). System catalogs are identified purely by id, so no
   * overlay lookup is needed here.
   */
  public void requireDeletableCatalog(ResourceId catalogId, String corr) {
    CatalogSurfaceSupport.ensureKind(catalogId, ResourceKind.RK_CATALOG, "catalog_id", corr);
    if (SystemResourceIdGenerator.isSystemId(catalogId)) {
      throw GrpcErrors.permissionDenied(
          corr, SYSTEM_OBJECT_IMMUTABLE, Map.of("id", catalogId.getId(), "kind", "catalog"));
    }
  }

  /**
   * Delete-path guard for namespaces. Rejects system namespaces (immutable) but, unlike {@link
   * #requireWritableNamespace}, does <b>not</b> throw when the namespace fails to resolve: an
   * unresolved target is left for the repository fallback, which deletes idempotently (and enforces
   * any caller precondition). The origin-based immutability check is applied only when the
   * namespace resolves — system namespaces are backed by the in-memory system graph and always
   * resolve, so a target that does not resolve is provably not a system object.
   */
  public void requireDeletableNamespace(ResourceId namespaceId, String corr) {
    CatalogSurfaceSupport.ensureKind(namespaceId, ResourceKind.RK_NAMESPACE, "namespace_id", corr);
    if (SystemResourceIdGenerator.isSystemId(namespaceId)) {
      throw GrpcErrors.permissionDenied(
          corr, SYSTEM_OBJECT_IMMUTABLE, Map.of("id", namespaceId.getId(), "kind", "namespace"));
    }
    overlay
        .resolve(namespaceId)
        .filter(NamespaceNode.class::isInstance)
        .map(NamespaceNode.class::cast)
        .filter(ns -> ns.origin() == GraphNodeOrigin.SYSTEM)
        .ifPresent(
            ns -> {
              throw GrpcErrors.permissionDenied(
                  corr,
                  SYSTEM_OBJECT_IMMUTABLE,
                  Map.of("id", namespaceId.getId(), "kind", "namespace"));
            });
  }

  public void requireNamespaceInCatalog(
      NamespaceNode namespace, ResourceId namespaceId, ResourceId catalogId, String corr) {
    CatalogSurfaceSupport.requireNamespaceInCatalog(namespace, namespaceId, catalogId, corr);
  }

  public TableNode requireVisibleTable(ResourceId tableId, String corr) {
    if (tableId == null) {
      throw GrpcErrors.notFound(corr, TABLE, Map.of("id", "<missing_table_id>"));
    }
    CatalogSurfaceSupport.ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", corr);
    return overlay
        .resolve(tableId)
        .filter(TableNode.class::isInstance)
        .map(TableNode.class::cast)
        .orElseThrow(() -> GrpcErrors.notFound(corr, TABLE, Map.of("id", tableId.getId())));
  }

  public TableNode requireWritableTable(ResourceId tableId, String corr) {
    TableNode node = requireVisibleTable(tableId, corr);
    enforceWritableTableNode(node, tableId, corr);
    return node;
  }

  public void requireWritableTableForDelete(ResourceId tableId, String corr, boolean callerCares) {
    GraphNode node = resolveTableNode(tableId, corr, callerCares);
    if (node == null) {
      return;
    }
    enforceWritableTableNode(node, tableId, corr);
  }

  public ViewNode requireVisibleView(ResourceId viewId, String corr) {
    if (viewId == null) {
      throw GrpcErrors.notFound(corr, VIEW, Map.of("id", "<missing_view_id>"));
    }
    CatalogSurfaceSupport.ensureKind(viewId, ResourceKind.RK_VIEW, "view_id", corr);
    return overlay
        .resolve(viewId)
        .filter(ViewNode.class::isInstance)
        .map(ViewNode.class::cast)
        .orElseThrow(() -> GrpcErrors.notFound(corr, VIEW, Map.of("id", viewId.getId())));
  }

  public ViewNode requireWritableView(ResourceId viewId, String corr) {
    ViewNode node = requireVisibleView(viewId, corr);
    enforceWritableViewNode(node, viewId, corr);
    return node;
  }

  public void requireWritableViewForDelete(ResourceId viewId, String corr, boolean callerCares) {
    GraphNode node = resolveViewNode(viewId, corr, callerCares);
    if (node == null) {
      return;
    }
    enforceWritableViewNode(node, viewId, corr);
  }

  public void requireNamespacePathWriteEligible(
      ResourceId catalogId, List<String> fullPath, String corr) {
    var sysMatch = systemNamespacePathMatch(catalogId, fullPath, listSystemNamespaces(catalogId));
    if (sysMatch == SystemPathMatch.EXACT) {
      throw GrpcErrors.alreadyExists(
          corr,
          NAMESPACE_ALREADY_EXISTS,
          Map.of(
              "display_name", fullPath.get(fullPath.size() - 1),
              "catalog_id", catalogId.getId(),
              "path", String.join(".", fullPath)));
    } else if (sysMatch == SystemPathMatch.UNDER_SYSTEM) {
      throw GrpcErrors.permissionDenied(
          corr,
          SYSTEM_OBJECT_IMMUTABLE,
          Map.of("catalog_id", catalogId.getId(), "path", String.join(".", fullPath)));
    }
  }

  public void requireRelationNameWriteEligible(
      ResourceId namespaceId,
      ResourceId catalogId,
      String displayName,
      ResourceId currentResourceId,
      MessageKey alreadyExistsKey,
      String corr) {
    String relationName = CatalogSurfaceSupport.normalizeName(displayName);
    for (TopologyGraph.RelationRef relation :
        overlay.listRelationRefsByName(catalogId, namespaceId, Set.of(relationName))) {
      if (!relationName.equals(CatalogSurfaceSupport.normalizeName(relation.name()))) {
        continue;
      }
      if (currentResourceId != null && currentResourceId.equals(relation.id())) {
        continue;
      }
      throw GrpcErrors.alreadyExists(
          corr,
          alreadyExistsKey,
          Map.of(
              "display_name",
              relationName,
              "catalog_id",
              catalogId.getId(),
              "namespace_id",
              namespaceId.getId()));
    }
  }

  private GraphNode resolveTableNode(ResourceId tableId, String corr, boolean throwOnError) {
    if (throwOnError) {
      return requireVisibleTable(tableId, corr);
    }
    if (tableId == null) {
      throw GrpcErrors.notFound(corr, TABLE, Map.of("id", "<missing_table_id>"));
    }
    CatalogSurfaceSupport.ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", corr);

    try {
      return overlay.resolve(tableId).orElse(null);
    } catch (RuntimeException e) {
      return null;
    }
  }

  private GraphNode resolveViewNode(ResourceId viewId, String corr, boolean throwOnError) {
    if (throwOnError) {
      return requireVisibleView(viewId, corr);
    }
    if (viewId == null) {
      throw GrpcErrors.notFound(corr, VIEW, Map.of("id", "<missing_view_id>"));
    }
    CatalogSurfaceSupport.ensureKind(viewId, ResourceKind.RK_VIEW, "view_id", corr);

    try {
      return overlay.resolve(viewId).orElse(null);
    } catch (RuntimeException e) {
      return null;
    }
  }

  private void enforceWritableTableNode(GraphNode node, ResourceId tableId, String corr) {
    if (node instanceof UserTableNode) {
      return;
    }

    if (node != null && node.origin() == GraphNodeOrigin.SYSTEM) {
      throw GrpcErrors.permissionDenied(
          corr, SYSTEM_OBJECT_IMMUTABLE, Map.of("id", tableId.getId(), "kind", "table"));
    }

    throw GrpcErrors.notFound(corr, TABLE, Map.of("id", tableId.getId()));
  }

  private void enforceWritableViewNode(GraphNode node, ResourceId viewId, String corr) {
    if (node instanceof ViewNode vn) {
      if (isSystemViewNode(vn)) {
        throw GrpcErrors.permissionDenied(
            corr, SYSTEM_OBJECT_IMMUTABLE, Map.of("id", viewId.getId(), "kind", "view"));
      }
      return;
    }
    throw GrpcErrors.notFound(corr, VIEW, Map.of("id", viewId.getId()));
  }

  private List<NamespaceNode> listSystemNamespaces(ResourceId catalogId) {
    if (catalogId == null) {
      return List.of();
    }
    return overlay.listSystemNamespaces(catalogId);
  }

  private SystemPathMatch systemNamespacePathMatch(
      ResourceId catalogId, List<String> fullPath, List<NamespaceNode> sysNamespaces) {
    if (catalogId == null || fullPath == null || fullPath.isEmpty()) {
      return SystemPathMatch.NONE;
    }

    var fullNorm = new ArrayList<String>(fullPath.size());
    for (var seg : fullPath) {
      fullNorm.add(CatalogSurfaceSupport.normalizeName(seg));
    }

    var sysFullPaths = new java.util.HashSet<String>();
    for (var n : sysNamespaces) {
      if (n == null) {
        continue;
      }

      var leaf = n.displayName();
      if (leaf == null || leaf.isBlank()) {
        continue;
      }

      var sb = new StringBuilder();
      boolean first = true;
      for (var seg : n.pathSegments()) {
        var s = CatalogSurfaceSupport.normalizeName(seg);
        if (!first) {
          sb.append(PATH_DELIM);
        }
        sb.append(s);
        first = false;
      }
      var leafNorm = CatalogSurfaceSupport.normalizeName(leaf);
      if (!first) {
        sb.append(PATH_DELIM);
      }
      sb.append(leafNorm);

      sysFullPaths.add(sb.toString());
    }

    var sb = new StringBuilder();
    for (int i = 0; i < fullNorm.size(); i++) {
      if (i > 0) {
        sb.append(PATH_DELIM);
      }
      sb.append(fullNorm.get(i));

      if (sysFullPaths.contains(sb.toString())) {
        return (i == fullNorm.size() - 1) ? SystemPathMatch.EXACT : SystemPathMatch.UNDER_SYSTEM;
      }
    }

    return SystemPathMatch.NONE;
  }

  private static boolean isSystemViewNode(GraphNode node) {
    if (node == null || node.id() == null) {
      return false;
    }
    return node.origin() == GraphNodeOrigin.SYSTEM;
  }

  private enum SystemPathMatch {
    NONE,
    EXACT,
    UNDER_SYSTEM
  }
}
