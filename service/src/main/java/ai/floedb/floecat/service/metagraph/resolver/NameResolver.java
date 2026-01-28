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

package ai.floedb.floecat.service.metagraph.resolver;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Repository-backed name resolution helpers.
 *
 * <p>This class provides the *atomic* operations for: - catalog lookup - namespace lookup - table
 * lookup - view lookup
 *
 * <p>MetadataGraph and FullyQualifiedResolver delegate to this class so the main façade stays
 * focused on caching and orchestration.
 *
 * <p>No caching — pure repository calls.
 */
@ApplicationScoped
public final class NameResolver {

  // ----------------------------------------------------------------------
  // Result wrapper for resolved relations
  // ----------------------------------------------------------------------

  public record ResolvedRelation(ResourceId resourceId, NameRef canonicalName) {}

  // ----------------------------------------------------------------------
  // Dependencies
  // ----------------------------------------------------------------------

  private final CatalogRepository catalogRepository;
  private final NamespaceRepository namespaceRepository;
  private final TableRepository tableRepository;
  private final ViewRepository viewRepository;

  @Inject
  public NameResolver(
      CatalogRepository catalogRepository,
      NamespaceRepository namespaceRepository,
      TableRepository tableRepository,
      ViewRepository viewRepository) {

    this.catalogRepository = catalogRepository;
    this.namespaceRepository = namespaceRepository;
    this.tableRepository = tableRepository;
    this.viewRepository = viewRepository;
  }

  // ----------------------------------------------------------------------
  // Strong resolution (throws on missing)
  // ----------------------------------------------------------------------

  public ResourceId resolveCatalogId(String cid, String accountId, String catalogName) {
    return catalogRepository
        .getByName(accountId, catalogName)
        .map(Catalog::getResourceId)
        .orElseThrow(() -> GrpcErrors.notFound(cid, CATALOG, Map.of("id", catalogName)));
  }

  public ResourceId resolveNamespaceId(String cid, String accountId, NameRef ref) {
    Catalog catalog = catalogByName(cid, accountId, ref.getCatalog());
    List<String> fullPath = namespacePath(ref);

    return namespaceRepository
        .getByPath(accountId, catalog.getResourceId().getId(), fullPath)
        .map(Namespace::getResourceId)
        .orElseThrow(
            () ->
                GrpcErrors.notFound(
                    cid,
                    NAMESPACE_BY_PATH_MISSING,
                    Map.of(
                        "catalog_id", catalog.getResourceId().getId(),
                        "path", String.join(".", fullPath))));
  }

  public ResourceId resolveTableId(String cid, String accountId, NameRef ref) {
    Catalog catalog = catalogByName(cid, accountId, ref.getCatalog());
    Namespace namespace = namespaceByPath(cid, accountId, catalog, ref.getPathList());

    return tableRepository
        .getByName(
            accountId,
            catalog.getResourceId().getId(),
            namespace.getResourceId().getId(),
            ref.getName())
        .map(Table::getResourceId)
        .orElseThrow(
            () ->
                GrpcErrors.notFound(
                    cid,
                    TABLE_BY_NAME_MISSING,
                    Map.of(
                        "catalog", ref.getCatalog(),
                        "path", String.join(".", ref.getPathList()),
                        "name", ref.getName())));
  }

  public ResourceId resolveViewId(String cid, String accountId, NameRef ref) {
    Catalog catalog = catalogByName(cid, accountId, ref.getCatalog());
    Namespace namespace = namespaceByPath(cid, accountId, catalog, ref.getPathList());

    return viewRepository
        .getByName(
            accountId,
            catalog.getResourceId().getId(),
            namespace.getResourceId().getId(),
            ref.getName())
        .map(View::getResourceId)
        .orElseThrow(
            () ->
                GrpcErrors.notFound(
                    cid,
                    VIEW_BY_NAME_MISSING,
                    Map.of(
                        "catalog", ref.getCatalog(),
                        "path", String.join(".", ref.getPathList()),
                        "name", ref.getName())));
  }

  // ----------------------------------------------------------------------
  // Weak resolution (Optional)
  // ----------------------------------------------------------------------

  public Optional<ResolvedRelation> resolveTableRelation(String accountId, NameRef ref) {
    if (!validCatalog(ref) || !validName(ref)) return Optional.empty();

    Optional<Catalog> catalogOpt = catalogRepository.getByName(accountId, ref.getCatalog());
    if (catalogOpt.isEmpty()) return Optional.empty();
    Catalog catalog = catalogOpt.get();

    Optional<Namespace> nsOpt =
        namespaceRepository.getByPath(
            accountId, catalog.getResourceId().getId(), ref.getPathList());
    if (nsOpt.isEmpty()) return Optional.empty();
    Namespace ns = nsOpt.get();

    return tableRepository
        .getByName(
            accountId, catalog.getResourceId().getId(), ns.getResourceId().getId(), ref.getName())
        .map(
            t ->
                new ResolvedRelation(
                    t.getResourceId(),
                    canonicalName(catalog, ns, t.getDisplayName(), t.getResourceId())));
  }

  public Optional<ResolvedRelation> resolveViewRelation(String accountId, NameRef ref) {
    if (!validCatalog(ref) || !validName(ref)) return Optional.empty();

    Optional<Catalog> catalogOpt = catalogRepository.getByName(accountId, ref.getCatalog());
    if (catalogOpt.isEmpty()) return Optional.empty();
    Catalog catalog = catalogOpt.get();

    Optional<Namespace> nsOpt =
        namespaceRepository.getByPath(
            accountId, catalog.getResourceId().getId(), ref.getPathList());
    if (nsOpt.isEmpty()) return Optional.empty();
    Namespace ns = nsOpt.get();

    return viewRepository
        .getByName(
            accountId, catalog.getResourceId().getId(), ns.getResourceId().getId(), ref.getName())
        .map(
            v ->
                new ResolvedRelation(
                    v.getResourceId(),
                    canonicalName(catalog, ns, v.getDisplayName(), v.getResourceId())));
  }

  // ----------------------------------------------------------------------
  // Repository helpers
  // ----------------------------------------------------------------------

  private Catalog catalogByName(String cid, String accountId, String name) {
    return catalogRepository
        .getByName(accountId, name)
        .orElseThrow(() -> GrpcErrors.notFound(cid, CATALOG, Map.of("id", name)));
  }

  private Namespace namespaceByPath(
      String cid, String accountId, Catalog catalog, List<String> parents) {

    return namespaceRepository
        .getByPath(accountId, catalog.getResourceId().getId(), parents)
        .orElseThrow(
            () ->
                GrpcErrors.notFound(
                    cid,
                    NAMESPACE_BY_PATH_MISSING,
                    Map.of(
                        "catalog_id", catalog.getResourceId().getId(),
                        "path", String.join(".", parents))));
  }

  // ----------------------------------------------------------------------
  // Path helpers
  // ----------------------------------------------------------------------

  private List<String> namespacePath(NameRef ref) {
    List<String> out = new ArrayList<>(ref.getPathList());
    if (ref.getName() != null && !ref.getName().isBlank()) {
      if (out.isEmpty() || !out.get(out.size() - 1).equals(ref.getName())) {
        out.add(ref.getName());
      }
    }
    return out;
  }

  // canonical: namespace parents + its own display name
  private NameRef canonicalName(Catalog catalog, Namespace ns, String displayName, ResourceId id) {

    List<String> path = new ArrayList<>(ns.getParentsList());
    if (!ns.getDisplayName().isBlank()) {
      path.add(ns.getDisplayName());
    }

    return NameRef.newBuilder()
        .setCatalog(catalog.getDisplayName())
        .addAllPath(path)
        .setName(displayName)
        .setResourceId(id)
        .build();
  }

  // ----------------------------------------------------------------------
  // Validation helpers
  // ----------------------------------------------------------------------

  private boolean validCatalog(NameRef ref) {
    return ref.getCatalog() != null && !ref.getCatalog().isBlank();
  }

  private boolean validName(NameRef ref) {
    return ref.getName() != null && !ref.getName().isBlank();
  }

  // ----------------------------------------------------------------------
  // Listing helpers
  // ----------------------------------------------------------------------

  public List<ResourceId> listNamespaces(String accountId, String catalogId) {
    return namespaceRepository.listIds(accountId, catalogId);
  }

  public List<ResourceId> listTableIds(String accountId, String catalogId) {
    List<ResourceId> out = new ArrayList<>();
    List<ResourceId> nsIds = namespaceRepository.listIds(accountId, catalogId);

    for (ResourceId ns : nsIds) {
      List<Table> tables =
          tableRepository.list(
              accountId, catalogId, ns.getId(), Integer.MAX_VALUE, "", new StringBuilder());
      for (Table t : tables) out.add(t.getResourceId());
    }
    return out;
  }

  public List<ResourceId> listTableIdsInNamespace(
      String accountId, String catalogId, String namespaceId) {
    List<Table> tables =
        tableRepository.list(
            accountId, catalogId, namespaceId, Integer.MAX_VALUE, "", new StringBuilder());
    return tables.stream().map(Table::getResourceId).toList();
  }

  public List<ResourceId> listViewIds(String accountId, String catalogId) {
    List<ResourceId> out = new ArrayList<>();
    List<ResourceId> nsIds = namespaceRepository.listIds(accountId, catalogId);

    for (ResourceId ns : nsIds) {
      List<View> views =
          viewRepository.list(
              accountId, catalogId, ns.getId(), Integer.MAX_VALUE, "", new StringBuilder());
      for (View t : views) out.add(t.getResourceId());
    }
    return out;
  }

  public List<ResourceId> listViewIdsInNamespace(
      String accountId, String catalogId, String namespaceId) {
    List<View> views =
        viewRepository.list(
            accountId, catalogId, namespaceId, Integer.MAX_VALUE, "", new StringBuilder());
    return views.stream().map(View::getResourceId).toList();
  }
}
