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

package ai.floedb.floecat.systemcatalog.spi.types;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.ResourceIdUtils;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.scanner.spi.MetadataResolutionContext;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Lookup helper keyed by namespace identity and local type name. */
public final class SystemTypeLookup implements TypeLookup {

  private final Map<String, ResourceId> namespaceIdByCanonicalName;
  private final Map<TypeKey, TypeNode> byIdentityAndName;

  public SystemTypeLookup(MetadataResolutionContext ctx) {
    Objects.requireNonNull(ctx, "ctx");
    this.namespaceIdByCanonicalName = namespaceIndex(ctx.overlay().listNamespaces(ctx.catalogId()));
    this.byIdentityAndName = typeIndex(ctx.overlay().listTypes(ctx.catalogId()));
  }

  @Override
  public Optional<TypeNode> findByName(String namespace, String name) {
    String canonicalNamespace = canonical(namespace);
    String localName = localName(canonical(name));
    if (canonicalNamespace.isEmpty() || localName.isEmpty()) {
      return Optional.empty();
    }
    ResourceId namespaceId = namespaceIdByCanonicalName.get(canonicalNamespace);
    if (namespaceId == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(byIdentityAndName.get(new TypeKey(namespaceId, localName)));
  }

  private static Map<String, ResourceId> namespaceIndex(List<NamespaceNode> namespaces) {
    Map<String, ResourceId> index = new HashMap<>();
    for (NamespaceNode namespace : namespaces) {
      ResourceId namespaceId = namespace.id();
      if (!ResourceIdUtils.hasIdentity(namespaceId)) {
        continue;
      }
      String canonicalName = NameRefUtil.canonical(namespace.toNameRef());
      if (canonicalName.isEmpty()) {
        continue;
      }
      ResourceId previous = index.putIfAbsent(canonicalName, namespaceId);
      if (previous != null && !previous.equals(namespaceId)) {
        throw new IllegalStateException(
            "Duplicate namespace name in lookup scope: "
                + canonicalName
                + " ("
                + previous.getId()
                + ", "
                + namespaceId.getId()
                + ")");
      }
    }
    return Map.copyOf(index);
  }

  private static Map<TypeKey, TypeNode> typeIndex(List<TypeNode> types) {
    Map<TypeKey, TypeNode> index = new HashMap<>();
    for (TypeNode type : types) {
      ResourceId namespaceId = type.namespaceId();
      if (!ResourceIdUtils.hasIdentity(namespaceId)) {
        continue;
      }
      String localName = localName(canonical(type.displayName()));
      if (localName.isEmpty()) {
        continue;
      }
      TypeKey key = new TypeKey(namespaceId, localName);
      TypeNode previous = index.putIfAbsent(key, type);
      if (previous != null && !Objects.equals(previous.id(), type.id())) {
        throw new IllegalStateException("Duplicate type in lookup scope: " + key);
      }
    }
    return Map.copyOf(index);
  }

  private static String localName(String canonicalName) {
    int idx = canonicalName.lastIndexOf('.');
    return idx >= 0 ? canonicalName.substring(idx + 1) : canonicalName;
  }

  private static String canonical(String value) {
    if (value == null) {
      return "";
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? "" : trimmed.toLowerCase();
  }

  private record TypeKey(ResourceId namespaceId, String localName) {}
}
