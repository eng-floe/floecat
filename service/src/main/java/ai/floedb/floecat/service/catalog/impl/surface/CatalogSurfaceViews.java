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

import ai.floedb.floecat.catalog.rpc.GetViewRequest;
import ai.floedb.floecat.catalog.rpc.GetViewResponse;
import ai.floedb.floecat.catalog.rpc.ListViewsRequest;
import ai.floedb.floecat.catalog.rpc.ListViewsResponse;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Catalog Surface policy for view RPCs. */
public class CatalogSurfaceViews {

  private static final String VIEW_TOKEN_PREFIX = "view:";

  private final ViewRepository viewRepo;
  private final CatalogOverlay overlay;

  public CatalogSurfaceViews(ViewRepository viewRepo, CatalogOverlay overlay) {
    this.viewRepo = viewRepo;
    this.overlay = overlay;
  }

  public ListViewsResponse listViews(ListViewsRequest request, String accountId, String corr) {
    var repo = viewRepo();
    var namespaceId = request.getNamespaceId();
    NamespaceNode nsNode =
        CatalogSurfaceSupport.requireVisibleNamespace(overlay, namespaceId, corr);
    var catalogId = nsNode.catalogId();

    var pageIn = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);
    final int want = Math.max(1, pageIn.limit);
    final boolean isServiceToken =
        pageIn.token != null && pageIn.token.startsWith(VIEW_TOKEN_PREFIX);
    final String resumeAfterRel =
        isServiceToken ? CatalogSurfaceSupport.decodeToken(VIEW_TOKEN_PREFIX, pageIn.token) : "";
    String repoCursor = isServiceToken ? "" : pageIn.token;

    var out = new ArrayList<View>(want);
    String lastEmittedRel = "";

    String repoNext = "";
    if (nsNode.origin() != GraphNodeOrigin.SYSTEM && !isServiceToken) {
      var next = new StringBuilder();
      final List<View> scanned;
      try {
        scanned =
            repo.list(accountId, catalogId.getId(), namespaceId.getId(), want, repoCursor, next);
      } catch (IllegalArgumentException badToken) {
        throw GrpcErrors.invalidArgument(
            corr, PAGE_TOKEN_INVALID, Map.of("page_token", repoCursor));
      }

      out.addAll(scanned);
      repoNext = next.toString();
    }

    int sysCount = 0;
    final boolean repoExhausted = repoNext.isBlank();
    if (repoExhausted) {
      var sysNodes =
          overlay.listSystemRelationsInNamespace(catalogId, namespaceId).stream()
              .filter(ViewNode.class::isInstance)
              .map(ViewNode.class::cast)
              .toList();
      sysCount = sysNodes.size();

      if (out.size() < want && sysCount > 0) {
        record SysItem(ViewNode node, String rel) {}

        var sysItems =
            sysNodes.stream()
                .map(node -> new SysItem(node, relativeViewKey(node)))
                .filter(it -> it.rel() != null && !it.rel().isBlank())
                .sorted(Comparator.comparing(SysItem::rel))
                .toList();

        for (var it : sysItems) {
          if (!resumeAfterRel.isBlank() && it.rel().compareTo(resumeAfterRel) <= 0) {
            continue;
          }
          if (out.size() >= want) {
            break;
          }
          out.add(viewFromSystemNode(it.node()));
          lastEmittedRel = it.rel();
        }
      }
    } else {
      sysCount =
          (int)
              overlay.listSystemRelationsInNamespace(catalogId, namespaceId).stream()
                  .filter(ViewNode.class::isInstance)
                  .count();
    }

    String nextToken = repoNext;
    if (nextToken.isBlank() && out.size() == want && sysCount > 0) {
      nextToken = CatalogSurfaceSupport.encodeToken(VIEW_TOKEN_PREFIX, lastEmittedRel);
    }

    int repoCount =
        (nsNode.origin() == GraphNodeOrigin.SYSTEM)
            ? 0
            : repo.count(accountId, catalogId.getId(), namespaceId.getId());

    var page = MutationOps.pageOut(nextToken, repoCount + sysCount);

    return ListViewsResponse.newBuilder().addAllViews(out).setPage(page).build();
  }

  public GetViewResponse getView(GetViewRequest request, String corr) {
    var viewId = request.getViewId();
    ViewNode node = requireVisibleView(viewId, corr);
    var view = viewFromOverlayNodeOrRepo(node, viewId, corr);

    return GetViewResponse.newBuilder().setView(view).build();
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

  public CatalogNode requireWritableCatalog(ResourceId catalogId, String field, String corr) {
    return CatalogSurfaceSupport.requireWritableCatalog(overlay, catalogId, field, corr);
  }

  public NamespaceNode requireWritableNamespace(ResourceId namespaceId, String field, String corr) {
    return CatalogSurfaceSupport.requireWritableNamespace(overlay, namespaceId, field, corr);
  }

  public void requireNamespaceInCatalog(
      NamespaceNode namespace, ResourceId namespaceId, ResourceId catalogId, String corr) {
    CatalogSurfaceSupport.requireNamespaceInCatalog(namespace, namespaceId, catalogId, corr);
  }

  public void requireWritableViewForDelete(ResourceId viewId, String corr, boolean callerCares) {
    GraphNode node = resolveViewNode(viewId, corr, callerCares);
    if (node == null) {
      return;
    }
    enforceWritableViewNode(node, viewId, corr);
  }

  private GraphNode resolveViewNode(ResourceId viewId, String corr, boolean throwOnError) {
    if (viewId == null) {
      throw GrpcErrors.notFound(corr, VIEW, Map.of("id", "<missing_view_id>"));
    }
    CatalogSurfaceSupport.ensureKind(viewId, ResourceKind.RK_VIEW, "view_id", corr);

    try {
      return overlay.resolve(viewId).orElse(null);
    } catch (RuntimeException e) {
      if (throwOnError) {
        throw e;
      }
      return null;
    }
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

  private View viewFromOverlayNodeOrRepo(ViewNode node, ResourceId viewId, String corr) {
    if (isSystemViewNode(node)) {
      return viewFromSystemNode(node);
    }

    return viewRepo()
        .getById(viewId)
        .orElseThrow(() -> GrpcErrors.notFound(corr, VIEW, Map.of("id", viewId.getId())));
  }

  private static View viewFromSystemNode(ViewNode node) {
    View.Builder builder =
        View.newBuilder()
            .setResourceId(node.id())
            .setCatalogId(node.catalogId())
            .setNamespaceId(node.namespaceId())
            .setDisplayName(node.displayName())
            .putAllProperties(node.properties());
    builder.addAllSqlDefinitions(node.sqlDefinitions());
    return builder.build();
  }

  private ViewRepository viewRepo() {
    return Objects.requireNonNull(viewRepo, "view repository is required for view reads");
  }

  private static boolean isSystemViewNode(GraphNode node) {
    if (node == null || node.id() == null) {
      return false;
    }
    return node.origin() == GraphNodeOrigin.SYSTEM;
  }

  private static String relativeViewKey(ViewNode node) {
    if (node == null) {
      return "";
    }
    var name = node.displayName();
    if (name == null) {
      name = "";
    }
    return CatalogSurfaceSupport.normalizeName(name);
  }
}
