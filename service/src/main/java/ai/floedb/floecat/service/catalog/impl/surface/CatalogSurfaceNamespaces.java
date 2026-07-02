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

import ai.floedb.floecat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.GetNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.floecat.catalog.rpc.ListNamespacesResponse;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.systemcatalog.graph.SystemResourceIdGenerator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Catalog Surface policy for namespace RPCs. */
public final class CatalogSurfaceNamespaces {

  private static final String PATH_DELIM = "\u001F";

  private final NamespaceRepository namespaceRepo;
  private final CatalogOverlay overlay;

  public CatalogSurfaceNamespaces(NamespaceRepository namespaceRepo, CatalogOverlay overlay) {
    this.namespaceRepo = namespaceRepo;
    this.overlay = overlay;
  }

  public ListNamespacesResponse listNamespaces(
      ListNamespacesRequest request, String accountId, String corr) {
    final ResourceId catalogId;
    final List<String> parentPath;

    if (request.hasNamespaceId()) {
      var parentNode =
          CatalogSurfaceSupport.requireVisibleNamespace(overlay, request.getNamespaceId(), corr);
      catalogId = parentNode.catalogId();
      parentPath = append(parentNode.pathSegments(), parentNode.displayName());
    } else if (request.hasCatalogId()) {
      catalogId =
          CatalogSurfaceSupport.requireVisibleCatalog(
                  overlay, request.getCatalogId(), "catalog_id", corr)
              .id();
      parentPath = new ArrayList<>(request.getPathList());
    } else {
      throw GrpcErrors.invalidArgument(corr, SELECTOR_REQUIRED, Map.of());
    }

    final boolean recursive = request.getRecursive();
    if (request.getChildrenOnly() && recursive) {
      throw GrpcErrors.invalidArgument(
          corr, null, Map.of("children_only", "true", "recursive", "true"));
    }

    final String namePrefix = request.getNamePrefix().trim();
    final List<NamespaceNode> sysNamespaces = listSystemNamespaces(catalogId);

    var pageIn = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);
    final int want = Math.max(1, pageIn.limit);
    var result =
        CatalogSurfaceNamespacePager.list(
            namespaceRepo,
            accountId,
            catalogId,
            parentPath,
            namePrefix,
            recursive,
            want,
            pageIn.token,
            sysNamespaces,
            CatalogSurfaceNamespaces::toProto,
            corr);

    var page = MutationOps.pageOut(result.nextToken(), result.totalSize());
    return ListNamespacesResponse.newBuilder()
        .addAllNamespaces(result.items())
        .setPage(page)
        .build();
  }

  public GetNamespaceResponse getNamespace(GetNamespaceRequest request, String corr) {
    var nsId = request.getNamespaceId();

    var nsOpt = namespaceRepo.getById(nsId);
    if (nsOpt.isPresent()) {
      return GetNamespaceResponse.newBuilder().setNamespace(nsOpt.get()).build();
    }

    var node =
        overlay
            .resolve(nsId)
            .filter(NamespaceNode.class::isInstance)
            .map(NamespaceNode.class::cast)
            .orElseThrow(() -> GrpcErrors.notFound(corr, NAMESPACE, Map.of("id", nsId.getId())));

    return GetNamespaceResponse.newBuilder().setNamespace(toProto(node)).build();
  }

  public CatalogNode requireWritableCatalog(ResourceId catalogId, String field, String corr) {
    return CatalogSurfaceSupport.requireWritableCatalog(overlay, catalogId, field, corr);
  }

  public void requireWritableNamespace(ResourceId namespaceId, String corr) {
    CatalogSurfaceSupport.ensureKind(namespaceId, ResourceKind.RK_NAMESPACE, "namespace_id", corr);
    if (namespaceId != null && SystemResourceIdGenerator.isSystemId(namespaceId)) {
      throw GrpcErrors.permissionDenied(
          corr, SYSTEM_OBJECT_IMMUTABLE, Map.of("id", namespaceId.getId(), "kind", "namespace"));
    }
  }

  public void rejectSystemNamespacePathCollision(
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

  private List<NamespaceNode> listSystemNamespaces(ResourceId catalogId) {
    if (catalogId == null) {
      return List.of();
    }
    return overlay.listSystemNamespaces(catalogId);
  }

  private enum SystemPathMatch {
    NONE,
    EXACT,
    UNDER_SYSTEM
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

  private static ArrayList<String> append(List<String> parents, String last) {
    var pp = new ArrayList<String>(parents.size() + 1);
    pp.addAll(parents);
    pp.add(last);
    return pp;
  }

  private static Namespace toProto(NamespaceNode n) {
    return Namespace.newBuilder()
        .setResourceId(n.id())
        .setCatalogId(n.catalogId())
        .setDisplayName(n.displayName())
        .clearParents()
        .addAllParents(n.pathSegments())
        .putAllProperties(n.properties())
        .build();
  }
}
