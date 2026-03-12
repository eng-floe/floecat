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

package ai.floedb.floecat.service.catalog.impl;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.systemcatalog.graph.SystemResourceIdGenerator;
import java.util.Map;

final class CatalogOverlayGuards {

  private CatalogOverlayGuards() {}

  static CatalogNode requireVisibleCatalogNode(
      CatalogOverlay overlay, ResourceId catalogId, String corr) {
    if (catalogId == null) {
      throw GrpcErrors.notFound(corr, CATALOG, Map.of("id", "<missing_catalog_id>"));
    }
    ensureKind(catalogId, ResourceKind.RK_CATALOG, "catalog_id", corr);
    return overlay
        .resolve(catalogId)
        .filter(CatalogNode.class::isInstance)
        .map(CatalogNode.class::cast)
        .orElseThrow(() -> GrpcErrors.notFound(corr, CATALOG, Map.of("id", catalogId.getId())));
  }

  static NamespaceNode requireVisibleNamespaceNode(
      CatalogOverlay overlay, ResourceId namespaceId, String corr) {
    if (namespaceId == null) {
      throw GrpcErrors.notFound(corr, NAMESPACE, Map.of("id", "<missing_namespace_id>"));
    }
    ensureKind(namespaceId, ResourceKind.RK_NAMESPACE, "namespace_id", corr);
    return overlay
        .resolve(namespaceId)
        .filter(NamespaceNode.class::isInstance)
        .map(NamespaceNode.class::cast)
        .orElseThrow(() -> GrpcErrors.notFound(corr, NAMESPACE, Map.of("id", namespaceId.getId())));
  }

  static NamespaceNode requireWritableNamespaceNode(
      CatalogOverlay overlay, ResourceId namespaceId, String corr) {
    NamespaceNode ns = requireVisibleNamespaceNode(overlay, namespaceId, corr);
    if (ns.origin() == GraphNodeOrigin.SYSTEM) {
      throw GrpcErrors.permissionDenied(
          corr, SYSTEM_OBJECT_IMMUTABLE, Map.of("id", namespaceId.getId(), "kind", "namespace"));
    }
    return ns;
  }

  static boolean isSystemCatalogId(ResourceId catalogId) {
    return catalogId != null && SystemResourceIdGenerator.isSystemId(catalogId);
  }

  static void rejectSystemCatalogMutation(ResourceId catalogId, String corr) {
    if (isSystemCatalogId(catalogId)) {
      throw GrpcErrors.permissionDenied(
          corr, SYSTEM_OBJECT_IMMUTABLE, Map.of("id", catalogId.getId(), "kind", "catalog"));
    }
  }

  static void rejectSystemNamespaceMutation(ResourceId namespaceId, String corr) {
    if (namespaceId != null && SystemResourceIdGenerator.isSystemId(namespaceId)) {
      throw GrpcErrors.permissionDenied(
          corr, SYSTEM_OBJECT_IMMUTABLE, Map.of("id", namespaceId.getId(), "kind", "namespace"));
    }
  }

  private static void ensureKind(
      ResourceId resourceId, ResourceKind expected, String field, String corr) {
    if (resourceId == null || resourceId.getKind() != expected) {
      throw GrpcErrors.invalidArgument(corr, KIND, Map.of("field", field));
    }
  }
}
