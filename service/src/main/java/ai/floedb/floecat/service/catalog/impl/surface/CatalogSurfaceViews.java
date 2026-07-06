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
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import java.util.Map;
import java.util.Objects;

/** Catalog Surface policy for view RPCs. */
public final class CatalogSurfaceViews {

  private final ViewRepository viewRepo;
  private final CatalogOverlay overlay;
  private final CatalogSurfaceWritePolicy writePolicy;

  public CatalogSurfaceViews(ViewRepository viewRepo, CatalogOverlay overlay) {
    this.viewRepo = Objects.requireNonNull(viewRepo, "view repository is required");
    this.overlay = overlay;
    this.writePolicy = new CatalogSurfaceWritePolicy(overlay);
  }

  public ListViewsResponse listViews(ListViewsRequest request, String accountId, String corr) {
    var namespaceId = request.getNamespaceId();
    NamespaceNode nsNode = writePolicy.requireVisibleNamespace(namespaceId, corr);

    var pageIn = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);
    final int want = Math.max(1, pageIn.limit);
    var result =
        CatalogSurfaceRelationPager.list(
            want,
            pageIn.token,
            new CatalogSurfaceViewPageSource(viewRepo, overlay, accountId, nsNode, namespaceId),
            corr);

    var page = MutationOps.pageOut(result.nextToken(), result.totalSize());
    return ListViewsResponse.newBuilder().addAllViews(result.items()).setPage(page).build();
  }

  public GetViewResponse getView(GetViewRequest request, String corr) {
    var viewId = request.getViewId();
    ViewNode node = writePolicy.requireVisibleView(viewId, corr);
    var view = viewFromOverlayNodeOrRepo(node, viewId, corr);

    return GetViewResponse.newBuilder().setView(view).build();
  }

  private View viewFromOverlayNodeOrRepo(ViewNode node, ResourceId viewId, String corr) {
    if (isSystemViewNode(node)) {
      // GetView reports the system view's canonical catalog id, unlike ListViews which rewrites it
      // to the catalog being browsed (see CatalogSurfaceViewPageSource#mapSystemNode). The rewrite
      // is a presentation of the "symlink" — a system view is visible under many user catalogs at
      // once — and a by-id lookup carries no requested-catalog context (GetViewRequest is only a
      // view_id; ResourceId has no catalog field), so there is no single catalog to rewrite to.
      // The canonical id is therefore the truthful answer here.
      return viewFromSystemNode(node);
    }

    return viewRepo
        .getById(viewId)
        .orElseThrow(() -> GrpcErrors.notFound(corr, VIEW, Map.of("id", viewId.getId())));
  }

  /**
   * Builds a {@link View} from a system view node using the node's canonical (system) catalog id.
   * Callers that present the view within a specific user catalog — e.g. {@code ListViews} — rewrite
   * {@code catalog_id} afterward; {@code GetView} does not, as a by-id lookup has no such context.
   */
  static View viewFromSystemNode(ViewNode node) {
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

  private static boolean isSystemViewNode(ViewNode node) {
    if (node == null || node.id() == null) {
      return false;
    }
    return node.origin() == GraphNodeOrigin.SYSTEM;
  }
}
