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

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.error.impl.GeneratedErrorMessages;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
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

    for (int i = 0; i < max; i++) {
      var tblEntry = resolveTableEntry(cid, accountId, names.get(i));
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

    for (int i = 0; i < max; i++) {
      var viewEntry = resolveViewEntry(cid, accountId, names.get(i));
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

    Optional<Catalog> catalogOpt = catalogByName(cid, accountId, prefix.getCatalog());
    if (catalogOpt.isEmpty()) {
      return new ResolveResult(List.of(), 0, "");
    }
    Catalog catalog = catalogOpt.get();

    List<String> nsPath = namespacePath(prefix);
    Optional<Namespace> nsOpt = namespaceByPath(cid, accountId, catalog, nsPath);
    if (nsOpt.isEmpty()) {
      return new ResolveResult(List.of(), 0, "");
    }
    Namespace ns = nsOpt.get();

    StringBuilder next = new StringBuilder();

    List<Table> entries = listTables(cid, accountId, catalog, ns, limit, token, next);
    int total =
        tableRepository.count(
            accountId, catalog.getResourceId().getId(), ns.getResourceId().getId());

    List<QualifiedRelation> out = new ArrayList<>(entries.size());

    for (Table t : entries) {
      NameRef fq =
          NameRef.newBuilder()
              .setCatalog(catalog.getDisplayName())
              .addAllPath(nsPath)
              .setName(t.getDisplayName())
              .setResourceId(t.getResourceId())
              .build();
      out.add(new QualifiedRelation(fq, t.getResourceId()));
    }

    return new ResolveResult(out, total, next.toString());
  }

  public ResolveResult resolveViewsByPrefix(
      String cid, String accountId, NameRef prefix, int limit, String token) {

    Optional<Catalog> catalogOpt = catalogByName(cid, accountId, prefix.getCatalog());
    if (catalogOpt.isEmpty()) {
      return new ResolveResult(List.of(), 0, "");
    }
    Catalog catalog = catalogOpt.get();

    List<String> nsPath = namespacePath(prefix);
    Optional<Namespace> nsOpt = namespaceByPath(cid, accountId, catalog, nsPath);
    if (nsOpt.isEmpty()) {
      return new ResolveResult(List.of(), 0, "");
    }
    Namespace ns = nsOpt.get();

    StringBuilder next = new StringBuilder();

    List<View> entries = listViews(cid, accountId, catalog, ns, limit, token, next);
    int total =
        viewRepository.count(
            accountId, catalog.getResourceId().getId(), ns.getResourceId().getId());

    List<QualifiedRelation> out = new ArrayList<>(entries.size());

    for (View v : entries) {
      NameRef fq =
          NameRef.newBuilder()
              .setCatalog(catalog.getDisplayName())
              .addAllPath(nsPath)
              .setName(v.getDisplayName())
              .setResourceId(v.getResourceId())
              .build();
      out.add(new QualifiedRelation(fq, v.getResourceId()));
    }

    return new ResolveResult(out, total, next.toString());
  }

  // ----------------------------------------------------------------------
  // Internal helpers (canonical entry resolution)
  // ----------------------------------------------------------------------

  private Optional<QualifiedRelation> resolveTableEntry(String cid, String accountId, NameRef ref) {

    validateNameRef(cid, ref);
    validateRelationName(cid, ref, "table");

    Optional<Catalog> catalogOpt = catalogByName(cid, accountId, ref.getCatalog());
    if (catalogOpt.isEmpty()) {
      return Optional.empty();
    }
    Catalog catalog = catalogOpt.get();

    Optional<Namespace> nsOpt = namespaceByPath(cid, accountId, catalog, ref.getPathList());
    if (nsOpt.isEmpty()) {
      return Optional.empty();
    }
    Namespace ns = nsOpt.get();

    return tableRepository
        .getByName(
            accountId, catalog.getResourceId().getId(), ns.getResourceId().getId(), ref.getName())
        .map(
            t -> {
              NameRef canonical =
                  NameRef.newBuilder()
                      .setCatalog(catalog.getDisplayName())
                      .addAllPath(namespacePath(ns))
                      .setName(t.getDisplayName())
                      .setResourceId(t.getResourceId())
                      .build();
              return new QualifiedRelation(canonical, t.getResourceId());
            });
  }

  private Optional<QualifiedRelation> resolveViewEntry(String cid, String accountId, NameRef ref) {

    validateNameRef(cid, ref);
    validateRelationName(cid, ref, "view");

    Optional<Catalog> catalogOpt = catalogByName(cid, accountId, ref.getCatalog());
    if (catalogOpt.isEmpty()) {
      return Optional.empty();
    }
    Catalog catalog = catalogOpt.get();

    Optional<Namespace> nsOpt = namespaceByPath(cid, accountId, catalog, ref.getPathList());
    if (nsOpt.isEmpty()) {
      return Optional.empty();
    }
    Namespace ns = nsOpt.get();

    return viewRepository
        .getByName(
            accountId, catalog.getResourceId().getId(), ns.getResourceId().getId(), ref.getName())
        .map(
            v -> {
              NameRef canonical =
                  NameRef.newBuilder()
                      .setCatalog(catalog.getDisplayName())
                      .addAllPath(namespacePath(ns))
                      .setName(v.getDisplayName())
                      .setResourceId(v.getResourceId())
                      .build();
              return new QualifiedRelation(canonical, v.getResourceId());
            });
  }

  // ----------------------------------------------------------------------
  // Repository calls
  // ----------------------------------------------------------------------

  private Optional<Catalog> catalogByName(String cid, String accountId, String name) {
    return catalogRepository.getByName(accountId, name);
  }

  private Optional<Namespace> namespaceByPath(
      String cid, String accountId, Catalog catalog, List<String> path) {

    return namespaceRepository.getByPath(accountId, catalog.getResourceId().getId(), path);
  }

  private List<Table> listTables(
      String cid,
      String accountId,
      Catalog catalog,
      Namespace ns,
      int limit,
      String token,
      StringBuilder nextOut) {

    try {
      return tableRepository.list(
          accountId,
          catalog.getResourceId().getId(),
          ns.getResourceId().getId(),
          normalizeLimit(limit),
          token,
          nextOut);
    } catch (IllegalArgumentException ex) {
      throw GrpcErrors.invalidArgument(
          cid, GeneratedErrorMessages.MessageKey.PAGE_TOKEN_INVALID, Map.of("page_token", token));
    }
  }

  private List<View> listViews(
      String cid,
      String accountId,
      Catalog catalog,
      Namespace ns,
      int limit,
      String token,
      StringBuilder nextOut) {

    try {
      return viewRepository.list(
          accountId,
          catalog.getResourceId().getId(),
          ns.getResourceId().getId(),
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

  private List<String> namespacePath(Namespace ns) {
    List<String> out = new ArrayList<>(ns.getParentsList());
    if (!ns.getDisplayName().isBlank()) {
      out.add(ns.getDisplayName());
    }
    return out;
  }

  public record QualifiedRelation(NameRef name, ResourceId resourceId) {}

  public record ResolveResult(List<QualifiedRelation> relations, int totalSize, String nextToken) {}
}
