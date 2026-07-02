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
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.systemcatalog.graph.SystemResourceIdGenerator;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Base64;
import java.util.Map;

final class CatalogSurfaceSupport {

  private CatalogSurfaceSupport() {}

  static NamespaceNode requireVisibleNamespace(
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

  static CatalogNode requireVisibleCatalog(
      CatalogOverlay overlay, ResourceId catalogId, String field, String corr) {
    ensureKind(catalogId, ResourceKind.RK_CATALOG, field, corr);
    return overlay
        .resolve(catalogId)
        .filter(CatalogNode.class::isInstance)
        .map(CatalogNode.class::cast)
        .orElseThrow(() -> GrpcErrors.notFound(corr, CATALOG, Map.of("id", catalogId.getId())));
  }

  static CatalogNode requireWritableCatalog(
      CatalogOverlay overlay, ResourceId catalogId, String field, String corr) {
    var catalog = requireVisibleCatalog(overlay, catalogId, field, corr);
    if (SystemResourceIdGenerator.isSystemId(catalog.id())) {
      throw GrpcErrors.permissionDenied(
          corr, SYSTEM_OBJECT_IMMUTABLE, Map.of("id", catalog.id().getId(), "kind", "catalog"));
    }
    return catalog;
  }

  static NamespaceNode requireWritableNamespace(
      CatalogOverlay overlay, ResourceId namespaceId, String field, String corr) {
    ensureKind(namespaceId, ResourceKind.RK_NAMESPACE, field, corr);
    var namespace = requireVisibleNamespace(overlay, namespaceId, corr);
    if (namespace.origin() == GraphNodeOrigin.SYSTEM) {
      throw GrpcErrors.permissionDenied(
          corr, SYSTEM_OBJECT_IMMUTABLE, Map.of("id", namespaceId.getId(), "kind", "namespace"));
    }
    return namespace;
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

  static String encodeToken(String prefix, String resumeAfterRel) {
    if (resumeAfterRel == null) {
      resumeAfterRel = "";
    }
    if (resumeAfterRel.isBlank()) {
      return prefix;
    }
    return prefix
        + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(resumeAfterRel.getBytes(StandardCharsets.UTF_8));
  }

  static String decodeToken(String prefix, String token) {
    if (token == null || token.isBlank() || !token.startsWith(prefix)) {
      return "";
    }
    if (token.length() == prefix.length()) {
      return "";
    }
    var s = token.substring(prefix.length());
    var bytes = Base64.getUrlDecoder().decode(s);
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
