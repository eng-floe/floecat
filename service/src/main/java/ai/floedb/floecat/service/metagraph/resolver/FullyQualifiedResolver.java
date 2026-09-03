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
  private final TableRepository tableRepository;
  private final ViewRepository viewRepository;

  public FullyQualifiedResolver(
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
  // Table/View lists (no pagination)
  // ----------------------------------------------------------------------

  public ResolveResult resolveTableList(
      String cid, String accountId, List<NameRef> names, int limit, String pageToken) {

    validateListToken(cid, pageToken);

    if (names == null || names.isEmpty()) {
      return new ResolveResult(List.of(), 0, "");
    }

    int max = Math.min(names.size(), normalizeLimit(limit));
    List<QualifiedRelation> out = new ArrayList<>(max);
    var memo =
        new ScopeMemo(
            name -> catalogByName(cid, accountId, name),
            (catalog, path) -> namespaceByPath(cid, accountId, catalog, path));

    for (int i = 0; i < max; i++) {
      var tblEntry = resolveTableEntry(memo, cid, accountId, names.get(i));
      if (tblEntry.isPresent()) {
        out.add(tblEntry.get());
      }
    }

    return new ResolveResult(out, out.size(), "");
  }

  public ResolveResult resolveViewList(
      String cid, String accountId, List<NameRef> names, int limit, String pageToken) {

    validateListToken(cid, pageToken);

    if (names == null || names.isEmpty()) {
      return new ResolveResult(List.of(), 0, "");
    }

    int max = Math.min(names.size(), normalizeLimit(limit));
    List<QualifiedRelation> out = new ArrayList<>(max);
    var memo =
        new ScopeMemo(
            name -> catalogByName(cid, accountId, name),
            (catalog, path) -> namespaceByPath(cid, accountId, catalog, path));

    for (int i = 0; i < max; i++) {
      var viewEntry = resolveViewEntry(memo, cid, accountId, names.get(i));
      if (viewEntry.isPresent()) {
        out.add(viewEntry.get());
      }
    }
    return new ResolveResult(out, out.size(), "");
  }

  // ----------------------------------------------------------------------
  // Prefix resolution: list tables/views under a namespace prefix
  // ----------------------------------------------------------------------

  public ResolveResult resolveTablesByPrefix(
      String cid, String accountId, NameRef prefix, int limit, String token) {

    Optional<CatalogRef> catalogOpt = catalogByName(cid, accountId, prefix.getCatalog());
    if (catalogOpt.isEmpty()) {
      return new ResolveResult(List.of(), 0, "");
    }
    CatalogRef catalog = catalogOpt.get();

    List<String> nsPath = namespacePath(prefix);
    Optional<NamespaceRef> nsOpt = namespaceByPath(cid, accountId, catalog, nsPath);
    if (nsOpt.isEmpty()) {
      return new ResolveResult(List.of(), 0, "");
    }
    NamespaceRef ns = nsOpt.get();

    StringBuilder next = new StringBuilder();

    List<RelationRef> entries = listTables(cid, accountId, catalog, ns, limit, token, next);
    int total = tableRepository.count(accountId, catalog.id().getId(), ns.id().getId());

    List<QualifiedRelation> out = new ArrayList<>(entries.size());

    for (RelationRef table : entries) {
      ResourceId tableId = requireCanonicalTableId(table.id());
      NameRef fq =
          NameRef.newBuilder()
              .setCatalog(catalog.name())
              .addAllPath(ns.pathSegments())
              .setName(table.name())
              .setResourceId(tableId)
              .build();
      out.add(new QualifiedRelation(fq, tableId));
    }

    return new ResolveResult(out, total, next.toString());
  }

  public ResolveResult resolveViewsByPrefix(
      String cid, String accountId, NameRef prefix, int limit, String token) {

    Optional<CatalogRef> catalogOpt = catalogByName(cid, accountId, prefix.getCatalog());
    if (catalogOpt.isEmpty()) {
      return new ResolveResult(List.of(), 0, "");
    }
    CatalogRef catalog = catalogOpt.get();

    List<String> nsPath = namespacePath(prefix);
    Optional<NamespaceRef> nsOpt = namespaceByPath(cid, accountId, catalog, nsPath);
    if (nsOpt.isEmpty()) {
      return new ResolveResult(List.of(), 0, "");
    }
    NamespaceRef ns = nsOpt.get();

    StringBuilder next = new StringBuilder();

    List<RelationRef> entries = listViews(cid, accountId, catalog, ns, limit, token, next);
    int total = viewRepository.count(accountId, catalog.id().getId(), ns.id().getId());

    List<QualifiedRelation> out = new ArrayList<>(entries.size());

    for (RelationRef view : entries) {
      NameRef fq =
          NameRef.newBuilder()
              .setCatalog(catalog.name())
              .addAllPath(ns.pathSegments())
              .setName(view.name())
              .setResourceId(view.id())
              .build();
      out.add(new QualifiedRelation(fq, view.id()));
    }

    return new ResolveResult(out, total, next.toString());
  }

  /**
   * Counts user tables under a prefix without fetching any rows. Unlike {@link
   * #resolveTablesByPrefix}, this skips the row listing entirely — callers that only need the total
   * (e.g. combined system+user counts on a system-phase page) should use this rather than a one-row
   * probe whose rows and cursor are discarded.
   */
  public int countTablesByPrefix(String cid, String accountId, NameRef prefix) {
    Optional<CatalogRef> catalogOpt = catalogByName(cid, accountId, prefix.getCatalog());
    if (catalogOpt.isEmpty()) {
      return 0;
    }
    CatalogRef catalog = catalogOpt.get();
    Optional<NamespaceRef> nsOpt = namespaceByPath(cid, accountId, catalog, namespacePath(prefix));
    if (nsOpt.isEmpty()) {
      return 0;
    }
    return tableRepository.count(accountId, catalog.id().getId(), nsOpt.get().id().getId());
  }

  /**
   * Counts user views under a prefix without fetching any rows. See {@link #countTablesByPrefix}.
   */
  public int countViewsByPrefix(String cid, String accountId, NameRef prefix) {
    Optional<CatalogRef> catalogOpt = catalogByName(cid, accountId, prefix.getCatalog());
    if (catalogOpt.isEmpty()) {
      return 0;
    }
    CatalogRef catalog = catalogOpt.get();
    Optional<NamespaceRef> nsOpt = namespaceByPath(cid, accountId, catalog, namespacePath(prefix));
    if (nsOpt.isEmpty()) {
      return 0;
    }
    return viewRepository.count(accountId, catalog.id().getId(), nsOpt.get().id().getId());
  }

  // ----------------------------------------------------------------------
  // Internal helpers (canonical entry resolution)
  // ----------------------------------------------------------------------

  private Optional<QualifiedRelation> resolveTableEntry(
      ScopeMemo memo, String cid, String accountId, NameRef ref) {

    validateNameRef(cid, ref);
    validateRelationName(cid, ref, "table");

    Optional<CatalogRef> catalogOpt = memo.catalog(ref.getCatalog());
    if (catalogOpt.isEmpty()) {
      return Optional.empty();
    }
    CatalogRef catalog = catalogOpt.get();

    Optional<NamespaceRef> nsOpt = memo.namespace(catalog, ref.getPathList());
    if (nsOpt.isEmpty()) {
      return Optional.empty();
    }
    NamespaceRef ns = nsOpt.get();

    return tableRepository
        .getRefByName(accountId, catalog.id().getId(), ns.id().getId(), ref.getName())
        .map(
            table -> {
              ResourceId tableId = requireCanonicalTableId(table.id());
              NameRef canonical =
                  NameRef.newBuilder()
                      .setCatalog(catalog.name())
                      .addAllPath(ns.pathSegments())
                      .setName(table.name())
                      .setResourceId(tableId)
                      .build();
              return new QualifiedRelation(canonical, tableId);
            });
  }

  private Optional<QualifiedRelation> resolveViewEntry(
      ScopeMemo memo, String cid, String accountId, NameRef ref) {

    validateNameRef(cid, ref);
    validateRelationName(cid, ref, "view");

    Optional<CatalogRef> catalogOpt = memo.catalog(ref.getCatalog());
    if (catalogOpt.isEmpty()) {
      return Optional.empty();
    }
    CatalogRef catalog = catalogOpt.get();

    Optional<NamespaceRef> nsOpt = memo.namespace(catalog, ref.getPathList());
    if (nsOpt.isEmpty()) {
      return Optional.empty();
    }
    NamespaceRef ns = nsOpt.get();

    return viewRepository
        .getRefByName(accountId, catalog.id().getId(), ns.id().getId(), ref.getName())
        .map(
            view -> {
              NameRef canonical =
                  NameRef.newBuilder()
                      .setCatalog(catalog.name())
                      .addAllPath(ns.pathSegments())
                      .setName(view.name())
                      .setResourceId(view.id())
                      .build();
              return new QualifiedRelation(canonical, view.id());
            });
  }

  // ----------------------------------------------------------------------
  // Repository calls
  // ----------------------------------------------------------------------

  private Optional<CatalogRef> catalogByName(String cid, String accountId, String name) {
    return catalogRepository.getRefByName(accountId, name);
  }

  private Optional<NamespaceRef> namespaceByPath(
      String cid, String accountId, CatalogRef catalog, List<String> path) {

    return namespaceRepository.getRefByPath(accountId, catalog.id().getId(), path);
  }

  private List<RelationRef> listTables(
      String cid,
      String accountId,
      CatalogRef catalog,
      NamespaceRef ns,
      int limit,
      String token,
      StringBuilder nextOut) {

    try {
      return tableRepository.listRefs(
          accountId, catalog.id().getId(), ns.id().getId(), normalizeLimit(limit), token, nextOut);
    } catch (IllegalArgumentException ex) {
      throw GrpcErrors.invalidArgument(
          cid, GeneratedErrorMessages.MessageKey.PAGE_TOKEN_INVALID, Map.of("page_token", token));
    }
  }

  private List<RelationRef> listViews(
      String cid,
      String accountId,
      CatalogRef catalog,
      NamespaceRef ns,
      int limit,
      String token,
      StringBuilder nextOut) {

    try {
      return viewRepository.listRefs(
          accountId, catalog.id().getId(), ns.id().getId(), normalizeLimit(limit), token, nextOut);
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

  private void validateRelationName(String cid, NameRef ref, String type) {
    if (ref.getName().isBlank()) {
      throw GrpcErrors.invalidArgument(
          cid, relationNameMissingKey(type), Map.of("name", ref.getName()));
    }
  }

  private GeneratedErrorMessages.MessageKey relationNameMissingKey(String type) {
    return switch (type) {
      case "table" -> GeneratedErrorMessages.MessageKey.TABLE_NAME_MISSING;
      case "view" -> GeneratedErrorMessages.MessageKey.VIEW_NAME_MISSING;
      default -> GeneratedErrorMessages.MessageKey.FIELD;
    };
  }

  private ResourceId requireCanonicalTableId(ResourceId tableId) {
    if (tableId == null
        || tableId.getId().isBlank()
        || tableId.getAccountId().isBlank()
        || tableId.getKind() != ResourceKind.RK_TABLE) {
      throw new IllegalStateException("non-canonical table resource id in fq resolver");
    }
    return tableId;
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
}
