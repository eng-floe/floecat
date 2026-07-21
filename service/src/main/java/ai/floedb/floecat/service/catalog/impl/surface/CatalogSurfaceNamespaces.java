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
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Catalog Surface policy for namespace RPCs. */
public final class CatalogSurfaceNamespaces {

  private final NamespaceRepository namespaceRepo;
  private final CatalogOverlay overlay;
  private final CatalogSurfaceWritePolicy writePolicy;

  public CatalogSurfaceNamespaces(NamespaceRepository namespaceRepo, CatalogOverlay overlay) {
    this.namespaceRepo = namespaceRepo;
    this.overlay = overlay;
    this.writePolicy = new CatalogSurfaceWritePolicy(overlay);
  }

  public ListNamespacesResponse listNamespaces(
      ListNamespacesRequest request, String accountId, String corr) {
    final ResourceId catalogId;
    final List<String> parentPath;

    if (request.hasNamespaceId()) {
      var parentNode = writePolicy.requireVisibleNamespace(request.getNamespaceId(), corr);
      catalogId = parentNode.catalogId();
      parentPath = append(parentNode.pathSegments(), parentNode.displayName());
    } else if (request.hasCatalogId()) {
      catalogId =
          writePolicy.requireVisibleCatalog(request.getCatalogId(), "catalog_id", corr).id();
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
            want,
            pageIn.token,
            new CatalogSurfaceNamespacePageSource(
                namespaceRepo,
                accountId,
                catalogId,
                parentPath,
                namePrefix,
                recursive,
                sysNamespaces),
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

    var node = writePolicy.requireVisibleNamespace(nsId, corr);

    return GetNamespaceResponse.newBuilder().setNamespace(toProto(node)).build();
  }

  private List<NamespaceNode> listSystemNamespaces(ResourceId catalogId) {
    if (catalogId == null) {
      return List.of();
    }
    return overlay.listSystemNamespaces(catalogId);
  }

  private static ArrayList<String> append(List<String> parents, String last) {
    var pp = new ArrayList<String>(parents.size() + 1);
    pp.addAll(parents);
    pp.add(last);
    return pp;
  }

  static Namespace toProto(NamespaceNode n) {
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
