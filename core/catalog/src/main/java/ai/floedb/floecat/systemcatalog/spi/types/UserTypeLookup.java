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
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Lookup helper for user-defined types scoped to the current catalog. */
final class UserTypeLookup implements TypeLookup {

  private final Map<String, List<ResourceId>> namespaceIdsByCanonicalName;
  private final Map<TypeKey, TypeNode> byIdentityAndName;

  UserTypeLookup(MetadataResolutionContext ctx) {
    Objects.requireNonNull(ctx, "ctx");
    List<TypeNode> userTypes = userTypes(ctx.overlay().listTypes(ctx.catalogId()));
    if (userTypes.isEmpty()) {
      this.namespaceIdsByCanonicalName = Map.of();
      this.byIdentityAndName = Map.of();
      return;
    }

    Set<ResourceId> referencedNamespaceIds = referencedNamespaceIds(userTypes);
    Map<ResourceId, String> canonicalNamespaceById =
        resolveNamespaceCanonicalNames(ctx, referencedNamespaceIds);
    if (canonicalNamespaceById.isEmpty()) {
      this.namespaceIdsByCanonicalName = Map.of();
      this.byIdentityAndName = Map.of();
      return;
    }

    this.namespaceIdsByCanonicalName = namespaceIndex(canonicalNamespaceById);
    this.byIdentityAndName = typeIndex(userTypes, canonicalNamespaceById.keySet());
  }

  @Override
  public Optional<TypeNode> findByName(String namespace, String name) {
    String canonicalNamespace = canonical(namespace);
    String localName = localName(canonical(name));
    if (canonicalNamespace.isEmpty() || localName.isEmpty()) {
      return Optional.empty();
    }
    List<ResourceId> namespaceIds = namespaceIdsByCanonicalName.get(canonicalNamespace);
    if (namespaceIds == null || namespaceIds.isEmpty()) {
      return Optional.empty();
    }
    for (ResourceId namespaceId : namespaceIds) {
      TypeNode node = byIdentityAndName.get(new TypeKey(namespaceId, localName));
      if (node != null) {
        return Optional.of(node);
      }
    }
    return Optional.empty();
  }

  private static List<TypeNode> userTypes(List<TypeNode> types) {
    List<TypeNode> result = new ArrayList<>();
    for (TypeNode type : types) {
      ResourceId typeId = type.id();
      ResourceId namespaceId = type.namespaceId();
      if (!ResourceIdUtils.hasIdentity(typeId) || !ResourceIdUtils.hasIdentity(namespaceId)) {
        continue;
      }
      if (SystemNodeRegistry.SYSTEM_ACCOUNT.equals(typeId.getAccountId())) {
        continue;
      }
      result.add(type);
    }
    return List.copyOf(result);
  }

  private static Set<ResourceId> referencedNamespaceIds(List<TypeNode> types) {
    Set<ResourceId> ids = new LinkedHashSet<>();
    for (TypeNode type : types) {
      ResourceId namespaceId = type.namespaceId();
      if (ResourceIdUtils.hasIdentity(namespaceId)) {
        ids.add(namespaceId);
      }
    }
    return Set.copyOf(ids);
  }

  private static Map<ResourceId, String> resolveNamespaceCanonicalNames(
      MetadataResolutionContext ctx, Set<ResourceId> namespaceIds) {
    Map<ResourceId, String> canonicalById = new HashMap<>();
    for (ResourceId namespaceId : namespaceIds) {
      Optional<NamespaceNode> namespace =
          ctx.overlay()
              .resolve(namespaceId)
              .filter(NamespaceNode.class::isInstance)
              .map(NamespaceNode.class::cast);
      if (namespace.isEmpty()) {
        continue;
      }
      NamespaceNode ns = namespace.get();
      if (!ResourceIdUtils.hasIdentity(ns.id())) {
        continue;
      }
      if (!Objects.equals(ns.catalogId(), ctx.catalogId())) {
        continue;
      }
      if (SystemNodeRegistry.SYSTEM_ACCOUNT.equals(ns.id().getAccountId())) {
        continue;
      }
      String canonicalName = NameRefUtil.canonical(ns.toNameRef());
      if (canonicalName.isEmpty()) {
        continue;
      }
      canonicalById.put(ns.id(), canonicalName);
    }
    return Map.copyOf(canonicalById);
  }

  private static Map<String, List<ResourceId>> namespaceIndex(
      Map<ResourceId, String> canonicalNamespaceById) {
    Map<String, List<ResourceId>> index = new HashMap<>();
    for (Map.Entry<ResourceId, String> entry : canonicalNamespaceById.entrySet()) {
      List<ResourceId> ids = index.computeIfAbsent(entry.getValue(), ignored -> new ArrayList<>(1));
      ids.add(entry.getKey());
    }
    Map<String, List<ResourceId>> frozen = new HashMap<>(index.size());
    index.forEach((name, ids) -> frozen.put(name, List.copyOf(ids)));
    return Map.copyOf(frozen);
  }

  private static Map<TypeKey, TypeNode> typeIndex(
      List<TypeNode> types, Set<ResourceId> allowedNamespaceIds) {
    Map<TypeKey, TypeNode> index = new HashMap<>();
    for (TypeNode type : types) {
      ResourceId namespaceId = type.namespaceId();
      if (!ResourceIdUtils.hasIdentity(namespaceId)) {
        continue;
      }
      if (!allowedNamespaceIds.contains(namespaceId)) {
        continue;
      }
      if (SystemNodeRegistry.SYSTEM_ACCOUNT.equals(type.id().getAccountId())) {
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
