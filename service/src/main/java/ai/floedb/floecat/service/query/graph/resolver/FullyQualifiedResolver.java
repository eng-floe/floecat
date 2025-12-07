package ai.floedb.floecat.service.query.graph.resolver;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.graph.MetadataGraph.QualifiedRelation;
import ai.floedb.floecat.service.query.graph.MetadataGraph.ResolveResult;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implements the Directory ResolveFQ semantics for tables and views.
 *
 * <p>The resolver handles both list selectors (no pagination) and namespace prefix selectors
 * (pagination tokens / counts). MetadataGraph uses this helper so the public façade stays small.
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

  public ResolveResult resolveTableList(
      String correlationId, String tenantId, List<NameRef> names, int limit, String pageToken) {
    validateListToken(correlationId, pageToken);
    if (names == null || names.isEmpty()) {
      return new ResolveResult(List.of(), 0, "");
    }
    int normalizedLimit = normalizeLimit(limit);
    int end = Math.min(names.size(), normalizedLimit);
    List<QualifiedRelation> relations = new ArrayList<>(end);
    for (int i = 0; i < end; i++) {
      relations.add(resolveTableEntry(correlationId, tenantId, names.get(i)));
    }
    return new ResolveResult(relations, names.size(), "");
  }

  public ResolveResult resolveViewList(
      String correlationId, String tenantId, List<NameRef> names, int limit, String pageToken) {
    validateListToken(correlationId, pageToken);
    if (names == null || names.isEmpty()) {
      return new ResolveResult(List.of(), 0, "");
    }
    int normalizedLimit = normalizeLimit(limit);
    int end = Math.min(names.size(), normalizedLimit);
    List<QualifiedRelation> relations = new ArrayList<>(end);
    for (int i = 0; i < end; i++) {
      relations.add(resolveViewEntry(correlationId, tenantId, names.get(i)));
    }
    return new ResolveResult(relations, names.size(), "");
  }

  public ResolveResult resolveTablesByPrefix(
      String correlationId, String tenantId, NameRef prefix, int limit, String token) {
    Catalog catalog = catalogByName(correlationId, tenantId, prefix.getCatalog());
    List<String> namespacePath = namespacePathSegments(prefix);
    Namespace namespace = namespaceByPath(correlationId, tenantId, catalog, namespacePath);

    StringBuilder nextOut = new StringBuilder();
    List<Table> entries =
        listTables(correlationId, tenantId, catalog, namespace, limit, token, nextOut);
    int total =
        tableRepository.count(
            tenantId, catalog.getResourceId().getId(), namespace.getResourceId().getId());

    List<QualifiedRelation> relations = new ArrayList<>(entries.size());
    for (Table table : entries) {
      NameRef name =
          NameRef.newBuilder()
              .setCatalog(catalog.getDisplayName())
              .addAllPath(namespacePath)
              .setName(table.getDisplayName())
              .setResourceId(table.getResourceId())
              .build();
      relations.add(new QualifiedRelation(name, table.getResourceId()));
    }

    return new ResolveResult(relations, total, nextOut.toString());
  }

  public ResolveResult resolveViewsByPrefix(
      String correlationId, String tenantId, NameRef prefix, int limit, String token) {
    Catalog catalog = catalogByName(correlationId, tenantId, prefix.getCatalog());
    List<String> namespacePath = namespacePathSegments(prefix);
    Namespace namespace = namespaceByPath(correlationId, tenantId, catalog, namespacePath);

    StringBuilder nextOut = new StringBuilder();
    List<View> entries =
        listViews(correlationId, tenantId, catalog, namespace, limit, token, nextOut);
    int total =
        viewRepository.count(
            tenantId, catalog.getResourceId().getId(), namespace.getResourceId().getId());

    List<QualifiedRelation> relations = new ArrayList<>(entries.size());
    for (View view : entries) {
      NameRef name =
          NameRef.newBuilder()
              .setCatalog(catalog.getDisplayName())
              .addAllPath(namespacePath)
              .setName(view.getDisplayName())
              .setResourceId(view.getResourceId())
              .build();
      relations.add(new QualifiedRelation(name, view.getResourceId()));
    }

    return new ResolveResult(relations, total, nextOut.toString());
  }

  private QualifiedRelation resolveTableEntry(String correlationId, String tenantId, NameRef ref) {
    try {
      validateNameRef(correlationId, ref);
      validateRelationName(correlationId, ref, "table");
      Catalog catalog = catalogByName(correlationId, tenantId, ref.getCatalog());
      Namespace namespace = namespaceByPath(correlationId, tenantId, catalog, ref.getPathList());
      return tableRepository
          .getByName(
              tenantId,
              catalog.getResourceId().getId(),
              namespace.getResourceId().getId(),
              ref.getName())
          .map(
              table -> {
                NameRef canonical =
                    NameRef.newBuilder()
                        .setCatalog(catalog.getDisplayName())
                        .addAllPath(namespacePathSegments(namespace))
                        .setName(table.getDisplayName())
                        .setResourceId(table.getResourceId())
                        .build();
                return new QualifiedRelation(canonical, table.getResourceId());
              })
          .orElseGet(() -> new QualifiedRelation(ref, ResourceId.getDefaultInstance()));
    } catch (Throwable t) {
      return new QualifiedRelation(ref, ResourceId.getDefaultInstance());
    }
  }

  private QualifiedRelation resolveViewEntry(String correlationId, String tenantId, NameRef ref) {
    try {
      validateNameRef(correlationId, ref);
      validateRelationName(correlationId, ref, "view");
      Catalog catalog = catalogByName(correlationId, tenantId, ref.getCatalog());
      Namespace namespace = namespaceByPath(correlationId, tenantId, catalog, ref.getPathList());
      return viewRepository
          .getByName(
              tenantId,
              catalog.getResourceId().getId(),
              namespace.getResourceId().getId(),
              ref.getName())
          .map(
              view -> {
                NameRef canonical =
                    NameRef.newBuilder()
                        .setCatalog(catalog.getDisplayName())
                        .addAllPath(namespacePathSegments(namespace))
                        .setName(view.getDisplayName())
                        .setResourceId(view.getResourceId())
                        .build();
                return new QualifiedRelation(canonical, view.getResourceId());
              })
          .orElseGet(() -> new QualifiedRelation(ref, ResourceId.getDefaultInstance()));
    } catch (Throwable t) {
      return new QualifiedRelation(ref, ResourceId.getDefaultInstance());
    }
  }

  private List<Table> listTables(
      String correlationId,
      String tenantId,
      Catalog catalog,
      Namespace namespace,
      int limit,
      String token,
      StringBuilder nextOut) {
    try {
      return tableRepository.list(
          tenantId,
          catalog.getResourceId().getId(),
          namespace.getResourceId().getId(),
          Math.max(1, limit),
          token,
          nextOut);
    } catch (IllegalArgumentException badToken) {
      throw GrpcErrors.invalidArgument(
          correlationId, "page_token.invalid", Map.of("page_token", token));
    }
  }

  private List<View> listViews(
      String correlationId,
      String tenantId,
      Catalog catalog,
      Namespace namespace,
      int limit,
      String token,
      StringBuilder nextOut) {
    try {
      return viewRepository.list(
          tenantId,
          catalog.getResourceId().getId(),
          namespace.getResourceId().getId(),
          Math.max(1, limit),
          token,
          nextOut);
    } catch (IllegalArgumentException badToken) {
      throw GrpcErrors.invalidArgument(
          correlationId, "page_token.invalid", Map.of("page_token", token));
    }
  }

  private Catalog catalogByName(String correlationId, String tenantId, String catalogName) {
    return catalogRepository
        .getByName(tenantId, catalogName)
        .orElseThrow(
            () -> GrpcErrors.notFound(correlationId, "catalog", Map.of("id", catalogName)));
  }

  private Namespace namespaceByPath(
      String correlationId, String tenantId, Catalog catalog, List<String> pathSegments) {
    return namespaceRepository
        .getByPath(tenantId, catalog.getResourceId().getId(), pathSegments)
        .orElseThrow(
            () ->
                GrpcErrors.notFound(
                    correlationId,
                    "namespace.by_path_missing",
                    Map.of(
                        "catalog_id", catalog.getResourceId().getId(),
                        "path", String.join(".", pathSegments))));
  }

  private void validateListToken(String correlationId, String token) {
    if (token != null && !token.isBlank()) {
      throw GrpcErrors.invalidArgument(
          correlationId, "page_token.invalid", Map.of("page_token", token));
    }
  }

  private int normalizeLimit(int limit) {
    return Math.max(1, limit > 0 ? limit : 50);
  }

  private void validateNameRef(String correlationId, NameRef ref) {
    if (ref == null || ref.getCatalog().isBlank()) {
      throw GrpcErrors.invalidArgument(correlationId, "catalog.missing", Map.of());
    }
  }

  private void validateRelationName(String correlationId, NameRef ref, String type) {
    if (ref.getName().isBlank()) {
      throw GrpcErrors.invalidArgument(
          correlationId, type + ".name.missing", Map.of("name", ref.getName()));
    }
  }

  private List<String> namespacePathSegments(NameRef ref) {
    List<String> segments = new ArrayList<>(ref.getPathList());
    if (ref.getName() != null && !ref.getName().isBlank()) {
      segments.add(ref.getName());
    }
    return segments;
  }

  private List<String> namespacePathSegments(Namespace namespace) {
    List<String> segments = new ArrayList<>(namespace.getParentsList());
    if (!namespace.getDisplayName().isBlank()) {
      segments.add(namespace.getDisplayName());
    }
    return segments;
  }
}
