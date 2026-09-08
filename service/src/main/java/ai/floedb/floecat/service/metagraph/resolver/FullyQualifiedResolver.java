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

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.TopologyGraph.NamespaceRef;
import ai.floedb.floecat.scanner.spi.TopologyGraph.RelationRef;
import ai.floedb.floecat.service.error.impl.GeneratedErrorMessages;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository.CatalogRef;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Implements directory-style fully-qualified resolution semantics used by MetadataGraph.
 *
 * <p>This class: - resolves table/view lists without pagination (ResolveFQ list) - resolves
 * tables/views under a namespace prefix (ResolveFQ prefix) - never touches the metadata graph or
 * nodes; purely repository-driven
 *
 * <p>MetadataGraph depends on this for consistent resolver behavior.
 */
public class FullyQualifiedResolver {

  private final CatalogRepository catalogRepository;
  private final NamespaceRepository namespaceRepository;
  private final RelationAccess tables;
  private final RelationAccess views;

  public FullyQualifiedResolver(
      CatalogRepository catalogRepository,
      NamespaceRepository namespaceRepository,
      TableRepository tableRepository,
      ViewRepository viewRepository) {

    this.catalogRepository = catalogRepository;
    this.namespaceRepository = namespaceRepository;
    this.tables =
        new RelationAccess(
            ResourceKind.RK_TABLE,
            GeneratedErrorMessages.MessageKey.TABLE_NAME_MISSING,
            tableRepository::getRefByName,
            tableRepository::listRefs,
            tableRepository::count);
    this.views =
        new RelationAccess(
            ResourceKind.RK_VIEW,
            GeneratedErrorMessages.MessageKey.VIEW_NAME_MISSING,
            viewRepository::getRefByName,
            viewRepository::listRefs,
            viewRepository::count);
  }

  // ----------------------------------------------------------------------
  // Table/View lists (no pagination)
  // ----------------------------------------------------------------------

  public ResolveResult resolveTableList(
      String cid, String accountId, List<NameRef> names, int limit, String pageToken) {
    return resolveList(cid, accountId, names, limit, pageToken, tables);
  }

  public ResolveResult resolveViewList(
      String cid, String accountId, List<NameRef> names, int limit, String pageToken) {
    return resolveList(cid, accountId, names, limit, pageToken, views);
  }

  // ----------------------------------------------------------------------
  // Prefix resolution: list tables/views under a namespace prefix
  // ----------------------------------------------------------------------

  public ResolveResult resolveTablesByPrefix(
      String cid, String accountId, NameRef prefix, int limit, String token) {
    return resolveByPrefix(cid, accountId, prefix, limit, token, tables);
  }

  public ResolveResult resolveViewsByPrefix(
      String cid, String accountId, NameRef prefix, int limit, String token) {
    return resolveByPrefix(cid, accountId, prefix, limit, token, views);
  }

  /**
   * Counts user tables under a prefix without fetching any rows. Unlike {@link
   * #resolveTablesByPrefix}, this skips the row listing entirely — callers that only need the total
   * (e.g. combined system+user counts on a system-phase page) should use this rather than a one-row
   * probe whose rows and cursor are discarded.
   */
  public int countTablesByPrefix(String cid, String accountId, NameRef prefix) {
    return countByPrefix(accountId, prefix, tables);
  }

  /**
   * Counts user views under a prefix without fetching any rows. See {@link #countTablesByPrefix}.
   */
  public int countViewsByPrefix(String cid, String accountId, NameRef prefix) {
    return countByPrefix(accountId, prefix, views);
  }

  // ----------------------------------------------------------------------
  // Internal helpers (canonical entry resolution)
  // ----------------------------------------------------------------------

  private Optional<QualifiedRelation> resolveEntry(
      String cid, String accountId, NameRef ref, RelationAccess relations) {
    validateNameRef(cid, ref);
    validateRelationName(cid, ref, relations);

    Optional<CatalogRef> catalogOpt = catalogByName(accountId, ref.getCatalog());
    if (catalogOpt.isEmpty()) {
      return Optional.empty();
    }
    CatalogRef catalog = catalogOpt.get();

    Optional<NamespaceRef> nsOpt = namespaceByPath(catalog, ref.getPathList());
    if (nsOpt.isEmpty()) {
      return Optional.empty();
    }
    NamespaceRef ns = nsOpt.get();

    return relations
        .get(accountId, catalog.id().getId(), ns.id().getId(), ref.getName())
        .map(relation -> qualify(catalog, ns, relation, relations.kind()));
  }

  // ----------------------------------------------------------------------
  // Repository calls
  // ----------------------------------------------------------------------

  private ResolveResult resolveList(
      String cid,
      String accountId,
      List<NameRef> names,
      int limit,
      String pageToken,
      RelationAccess relations) {
    validateListToken(cid, pageToken);
    if (names == null || names.isEmpty()) {
      return new ResolveResult(List.of(), 0, "");
    }

    int max = Math.min(names.size(), normalizeLimit(limit));
    List<QualifiedRelation> out = new ArrayList<>(max);
    for (int i = 0; i < max; i++) {
      resolveEntry(cid, accountId, names.get(i), relations).ifPresent(out::add);
    }
    return new ResolveResult(out, out.size(), "");
  }

  private ResolveResult resolveByPrefix(
      String cid,
      String accountId,
      NameRef prefix,
      int limit,
      String token,
      RelationAccess relations) {
    Optional<ResolvedScope> scope = resolveScope(accountId, prefix, namespacePath(prefix));
    if (scope.isEmpty()) {
      return new ResolveResult(List.of(), 0, "");
    }

    CatalogRef catalog = scope.orElseThrow().catalog();
    NamespaceRef namespace = scope.orElseThrow().namespace();
    StringBuilder next = new StringBuilder();
    List<RelationRef> refs =
        listRefs(relations, cid, accountId, catalog, namespace, limit, token, next);
    List<QualifiedRelation> qualified =
        refs.stream().map(ref -> qualify(catalog, namespace, ref, relations.kind())).toList();
    return new ResolveResult(
        qualified,
        relations.count(accountId, catalog.id().getId(), namespace.id().getId()),
        next.toString());
  }

  private int countByPrefix(String accountId, NameRef prefix, RelationAccess relations) {
    return resolveScope(accountId, prefix, namespacePath(prefix))
        .map(
            scope ->
                relations.count(
                    accountId, scope.catalog().id().getId(), scope.namespace().id().getId()))
        .orElse(0);
  }

  private Optional<ResolvedScope> resolveScope(
      String accountId, NameRef ref, List<String> namespacePath) {
    return catalogByName(accountId, ref.getCatalog())
        .flatMap(
            catalog ->
                namespaceByPath(catalog, namespacePath)
                    .map(namespace -> new ResolvedScope(catalog, namespace)));
  }

  private Optional<CatalogRef> catalogByName(String accountId, String name) {
    return catalogRepository.getRefByName(accountId, name);
  }

  private Optional<NamespaceRef> namespaceByPath(CatalogRef catalog, List<String> path) {
    return namespaceRepository.getRefByPath(
        catalog.id().getAccountId(), catalog.id().getId(), path);
  }

  private List<RelationRef> listRefs(
      RelationAccess relations,
      String cid,
      String accountId,
      CatalogRef catalog,
      NamespaceRef namespace,
      int limit,
      String token,
      StringBuilder nextOut) {
    try {
      return relations.list(
          accountId,
          catalog.id().getId(),
          namespace.id().getId(),
          normalizeLimit(limit),
          token,
          nextOut);
    } catch (IllegalArgumentException ex) {
      throw GrpcErrors.invalidArgument(
          cid, GeneratedErrorMessages.MessageKey.PAGE_TOKEN_INVALID, Map.of("page_token", token));
    }
  }

  // ----------------------------------------------------------------------
  // Validation helpers
  // ----------------------------------------------------------------------

  private void validateListToken(String cid, String token) {
    if (token != null && !token.isBlank()) {
      throw GrpcErrors.invalidArgument(
          cid, GeneratedErrorMessages.MessageKey.PAGE_TOKEN_INVALID, Map.of("page_token", token));
    }
  }

  private int normalizeLimit(int limit) {
    return Math.max(1, limit > 0 ? limit : 50);
  }

  private void validateNameRef(String cid, NameRef ref) {
    if (ref == null || ref.getCatalog().isBlank()) {
      throw GrpcErrors.invalidArgument(
          cid, GeneratedErrorMessages.MessageKey.CATALOG_MISSING, Map.of());
    }
  }

  private void validateRelationName(String cid, NameRef ref, RelationAccess relations) {
    if (ref.getName().isBlank()) {
      throw GrpcErrors.invalidArgument(
          cid, relations.nameMissingKey(), Map.of("name", ref.getName()));
    }
  }

  private QualifiedRelation qualify(
      CatalogRef catalog, NamespaceRef namespace, RelationRef relation, ResourceKind expectedKind) {
    ResourceId id = relation.id();
    if (id == null
        || id.getId().isBlank()
        || id.getAccountId().isBlank()
        || id.getKind() != expectedKind
        || relation.kind() != expectedKind) {
      throw new IllegalStateException("non-canonical relation ref in fq resolver");
    }
    NameRef canonical =
        NameRef.newBuilder()
            .setCatalog(catalog.name())
            .addAllPath(namespace.pathSegments())
            .setName(relation.name())
            .setResourceId(id)
            .build();
    return new QualifiedRelation(canonical, id);
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

  public record QualifiedRelation(NameRef name, ResourceId resourceId) {}

  public record ResolveResult(List<QualifiedRelation> relations, int totalSize, String nextToken) {}

  private record ResolvedScope(CatalogRef catalog, NamespaceRef namespace) {}

  private record RelationAccess(
      ResourceKind kind,
      GeneratedErrorMessages.MessageKey nameMissingKey,
      RelationLookup lookup,
      RelationListing listing,
      RelationCount counter) {

    Optional<RelationRef> get(String accountId, String catalogId, String namespaceId, String name) {
      return lookup.get(accountId, catalogId, namespaceId, name);
    }

    List<RelationRef> list(
        String accountId,
        String catalogId,
        String namespaceId,
        int limit,
        String token,
        StringBuilder nextOut) {
      return listing.list(accountId, catalogId, namespaceId, limit, token, nextOut);
    }

    int count(String accountId, String catalogId, String namespaceId) {
      return counter.count(accountId, catalogId, namespaceId);
    }
  }

  @FunctionalInterface
  private interface RelationLookup {
    Optional<RelationRef> get(String accountId, String catalogId, String namespaceId, String name);
  }

  @FunctionalInterface
  private interface RelationListing {
    List<RelationRef> list(
        String accountId,
        String catalogId,
        String namespaceId,
        int limit,
        String token,
        StringBuilder nextOut);
  }

  @FunctionalInterface
  private interface RelationCount {
    int count(String accountId, String catalogId, String namespaceId);
  }
}
