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
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import ai.floedb.floecat.scanner.utils.CatalogContext;
import ai.floedb.floecat.service.common.PageTokens;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

final class CatalogSurfaceSupport {

  private CatalogSurfaceSupport() {}

  static NamespaceNode requireVisibleNamespace(
      CatalogGraphView graphView, ResourceId namespaceId, CatalogContext context, String corr) {
    if (namespaceId == null) {
      throw GrpcErrors.notFound(corr, NAMESPACE, Map.of("id", "<missing_namespace_id>"));
    }
    ensureKind(namespaceId, ResourceKind.RK_NAMESPACE, "namespace_id", corr);
    return graphView
        .resolve(namespaceId, context)
        .filter(NamespaceNode.class::isInstance)
        .map(NamespaceNode.class::cast)
        .orElseThrow(() -> GrpcErrors.notFound(corr, NAMESPACE, Map.of("id", namespaceId.getId())));
  }

  static CatalogNode requireVisibleCatalog(
      CatalogGraphView graphView,
      ResourceId catalogId,
      String field,
      CatalogContext context,
      String corr) {
    ensureKind(catalogId, ResourceKind.RK_CATALOG, field, corr);
    return graphView
        .resolve(catalogId, context)
        .filter(CatalogNode.class::isInstance)
        .map(CatalogNode.class::cast)
        .orElseThrow(() -> GrpcErrors.notFound(corr, CATALOG, Map.of("id", catalogId.getId())));
  }

  static void requireNamespaceInCatalog(
      NamespaceNode namespace, ResourceId namespaceId, ResourceId catalogId, String corr) {
    var namespaceCatalogId = namespace.catalogId();
    if (namespaceCatalogId == null || !namespaceCatalogId.getId().equals(catalogId.getId())) {
      throw GrpcErrors.invalidArgument(
          corr,
          NAMESPACE_CATALOG_MISMATCH,
          Map.of(
              "namespace_id", namespaceId.getId(),
              "namespace.catalog_id", namespaceCatalogId == null ? "" : namespaceCatalogId.getId(),
              "catalog_id", catalogId.getId()));
    }
  }

  static void ensureKind(ResourceId resourceId, ResourceKind expected, String field, String corr) {
    if (resourceId == null || resourceId.getKind() != expected) {
      throw GrpcErrors.invalidArgument(corr, KIND, Map.of("field", field));
    }
  }

  static String normalizeName(String in) {
    if (in == null) {
      return "";
    }

    String t = Normalizer.normalize(in.trim(), Normalizer.Form.NFKC);
    t = t.replaceAll("\\s+", " ");
    return t;
  }

  /** Returns the namespace path in the canonical parent-plus-leaf form. */
  static List<String> namespacePath(CatalogGraphView.NamespaceRef namespace) {
    var path = new ArrayList<>(namespace.pathSegments());
    if (namespace.name() != null && !namespace.name().isBlank()) {
      path.add(namespace.name());
    }
    return List.copyOf(path);
  }

  static List<String> namespaceParentPath(CatalogGraphView.NamespaceRef namespace) {
    List<String> path = namespacePath(namespace);
    return path.isEmpty() ? path : path.subList(0, path.size() - 1);
  }

  static String encodeToken(String prefix, String resumeAfterRel) {
    return PageTokens.encode(prefix, resumeAfterRel);
  }

  static String decodeToken(String prefix, String token, String corr) {
    return PageTokens.decode(prefix, token, corr);
  }
}
