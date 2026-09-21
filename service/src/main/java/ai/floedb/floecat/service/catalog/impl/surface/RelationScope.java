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

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.KIND;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.PAGE_TOKEN_INVALID;

import ai.floedb.floecat.catalog.rpc.ListRelationsRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import ai.floedb.floecat.scanner.utils.CatalogContext;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * What a relation listing walks: the namespaces in scope crossed with the kinds requested,
 * flattened into one ordered list of segments. Paging is then a walk over that list.
 *
 * <p>Namespaces come back as refs, so a node is hydrated only for a segment a page actually reads.
 */
public final class RelationScope {

  /** Listing order. A kind outside this list is not a relation. */
  private static final List<ResourceKind> KIND_ORDER =
      List.of(ResourceKind.RK_TABLE, ResourceKind.RK_VIEW);

  /** Separates path segments, the namespace from the kind, and keeps segment keys unambiguous. */
  private static final char KEY_SEPARATOR = '\0';

  /** One namespace's relations of one kind: the unit a page token resumes at. */
  record Segment(CatalogGraphView.NamespaceRef namespace, ResourceKind kind) {
    String key() {
      return namespaceKey(namespace) + KEY_SEPARATOR + kind.getNumber();
    }
  }

  private final CatalogGraphView graphView;
  private final CatalogContext context;
  private final CatalogSurfaceWritePolicy writePolicy;

  RelationScope(
      CatalogGraphView graphView, CatalogContext context, CatalogSurfaceWritePolicy writePolicy) {
    this.graphView = graphView;
    this.context = context;
    this.writePolicy = writePolicy;
  }

  /** The kinds a request selects, in listing order. Any kind outside table/view is rejected. */
  public static List<ResourceKind> requestedKinds(List<ResourceKind> requested, String corr) {
    if (requested == null || requested.isEmpty()) {
      return KIND_ORDER;
    }
    for (ResourceKind kind : requested) {
      if (!KIND_ORDER.contains(kind)) {
        throw GrpcErrors.invalidArgument(corr, KIND, Map.of("field", "kinds"));
      }
    }
    return KIND_ORDER.stream().filter(requested::contains).toList();
  }

  /**
   * Scoping matches ListNamespaces: a catalog scope starts at its top-level namespaces, a namespace
   * scope at that namespace, and {@code recursive} adds everything below.
   */
  List<Segment> segments(ListRelationsRequest request, List<ResourceKind> kinds, String corr) {
    var segments = new ArrayList<Segment>();
    for (var namespace : namespaces(request, corr)) {
      for (ResourceKind kind : kinds) {
        segments.add(new Segment(namespace, kind));
      }
    }
    return segments;
  }

  /**
   * Where a cursor resumes: the first segment at or after its key. A namespace dropped between
   * pages resumes at the next one, but a key naming a kind outside the filter was minted for a
   * different request and is rejected rather than silently paging past the end.
   */
  static int indexAtOrAfter(
      List<Segment> segments, String key, List<ResourceKind> kinds, String corr) {
    if (key.isEmpty()) {
      return 0;
    }
    int separator = key.lastIndexOf(KEY_SEPARATOR);
    if (separator < 0) {
      throw invalidToken(key, corr);
    }
    int kindNumber;
    try {
      kindNumber = Integer.parseInt(key.substring(separator + 1));
    } catch (NumberFormatException notANumber) {
      throw invalidToken(key, corr);
    }
    if (kinds.stream().noneMatch(kind -> kind.getNumber() == kindNumber)) {
      throw invalidToken(key, corr);
    }
    for (int i = 0; i < segments.size(); i++) {
      if (segments.get(i).key().compareTo(key) >= 0) {
        return i;
      }
    }
    return segments.size();
  }

  private static RuntimeException invalidToken(String key, String corr) {
    return GrpcErrors.invalidArgument(corr, PAGE_TOKEN_INVALID, Map.of("page_token", key));
  }

  private List<CatalogGraphView.NamespaceRef> namespaces(
      ListRelationsRequest request, String corr) {
    if (request.hasCatalogId()) {
      ResourceId catalogId =
          writePolicy.requireVisibleCatalog(request.getCatalogId(), "catalog_id", corr).id();
      var all = graphView.listNamespaceRefs(catalogId, context);
      return sortedByKey(
          request.getRecursive()
              ? all
              : all.stream()
                  .filter(ns -> CatalogSurfaceSupport.namespaceParentPath(ns).isEmpty())
                  .toList());
    }

    CatalogGraphView.NamespaceRef rootRef =
        graphView
            .namespaceRef(request.getNamespaceId(), context)
            .orElseThrow(
                () ->
                    GrpcErrors.notFound(
                        corr,
                        ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey
                            .NAMESPACE,
                        Map.of("id", request.getNamespaceId().getId())));
    if (!request.getRecursive()) {
      return List.of(rootRef);
    }

    List<String> rootPath = CatalogSurfaceSupport.namespacePath(rootRef);
    var subtree = new ArrayList<CatalogGraphView.NamespaceRef>();
    subtree.add(rootRef);
    for (var candidate : graphView.listNamespaceRefs(rootRef.catalogId(), context)) {
      if (!candidate.id().equals(rootRef.id())
          && isDescendant(CatalogSurfaceSupport.namespacePath(candidate), rootPath)) {
        subtree.add(candidate);
      }
    }
    return sortedByKey(subtree);
  }

  private static List<CatalogGraphView.NamespaceRef> sortedByKey(
      List<CatalogGraphView.NamespaceRef> namespaces) {
    return namespaces.stream().sorted(Comparator.comparing(RelationScope::namespaceKey)).toList();
  }

  private static String namespaceKey(CatalogGraphView.NamespaceRef namespace) {
    return String.join(
        String.valueOf(KEY_SEPARATOR), CatalogSurfaceSupport.namespacePath(namespace));
  }

  private static boolean isDescendant(List<String> candidatePath, List<String> rootPath) {
    if (candidatePath == null || candidatePath.size() < rootPath.size()) {
      return false;
    }
    for (int i = 0; i < rootPath.size(); i++) {
      if (!rootPath.get(i).equals(candidatePath.get(i))) {
        return false;
      }
    }
    return true;
  }
}
