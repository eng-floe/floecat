package ai.floedb.floecat.service.query.graph.resolver;

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
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Repository-backed name resolution helpers.
 *
 * <p>MetadataGraph delegates catalog/namespace/table/view name lookups to this class so the public
 * API remains focused on cache orchestration and RPC plumbing.
 */
public class NameResolver {

  public record ResolvedRelation(ResourceId resourceId, NameRef canonicalName) {}

  private final CatalogRepository catalogRepository;
  private final NamespaceRepository namespaceRepository;
  private final TableRepository tableRepository;
  private final ViewRepository viewRepository;

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

  public ResourceId resolveCatalogId(String correlationId, String accountId, String catalogName) {
    return catalogRepository
        .getByName(accountId, catalogName)
        .map(Catalog::getResourceId)
        .orElseThrow(
            () -> GrpcErrors.notFound(correlationId, "catalog", Map.of("id", catalogName)));
  }

  public ResourceId resolveNamespaceId(String correlationId, String accountId, NameRef ref) {
    Catalog catalog = catalogByName(correlationId, accountId, ref.getCatalog());
    List<String> fullPath = namespacePath(ref);

    return namespaceRepository
        .getByPath(accountId, catalog.getResourceId().getId(), fullPath)
        .map(Namespace::getResourceId)
        .orElseThrow(
            () ->
                GrpcErrors.notFound(
                    correlationId,
                    "namespace.by_path_missing",
                    Map.of(
                        "catalog_id",
                        catalog.getResourceId().getId(),
                        "path",
                        String.join(".", fullPath))));
  }

  public ResourceId resolveTableId(String correlationId, String accountId, NameRef ref) {
    Catalog catalog = catalogByName(correlationId, accountId, ref.getCatalog());
    Namespace namespace = namespaceByPath(correlationId, accountId, catalog, ref.getPathList());

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
                    correlationId,
                    "table.by_name_missing",
                    Map.of(
                        "catalog", ref.getCatalog(),
                        "path", String.join(".", ref.getPathList()),
                        "name", ref.getName())));
  }

  public ResourceId resolveViewId(String correlationId, String accountId, NameRef ref) {
    Catalog catalog = catalogByName(correlationId, accountId, ref.getCatalog());
    Namespace namespace = namespaceByPath(correlationId, accountId, catalog, ref.getPathList());

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
                    correlationId,
                    "view.by_name_missing",
                    Map.of(
                        "catalog", ref.getCatalog(),
                        "path", String.join(".", ref.getPathList()),
                        "name", ref.getName())));
  }

  public Optional<ResolvedRelation> resolveTableRelation(String accountId, NameRef ref) {
    if (!isValidCatalog(ref) || !hasName(ref)) {
      return Optional.empty();
    }
    var catalogOpt = catalogRepository.getByName(accountId, ref.getCatalog());
    if (catalogOpt.isEmpty()) {
      return Optional.empty();
    }
    final Catalog catalog = catalogOpt.get();
    var namespaceOpt =
        namespaceRepository.getByPath(
            accountId, catalog.getResourceId().getId(), ref.getPathList());
    if (namespaceOpt.isEmpty()) {
      return Optional.empty();
    }
    final Namespace namespace = namespaceOpt.get();
    return tableRepository
        .getByName(
            accountId,
            catalog.getResourceId().getId(),
            namespace.getResourceId().getId(),
            ref.getName())
        .map(
            table ->
                new ResolvedRelation(
                    table.getResourceId(),
                    canonicalName(
                        catalog, namespace, table.getDisplayName(), table.getResourceId())));
  }

  public Optional<ResolvedRelation> resolveViewRelation(String accountId, NameRef ref) {
    if (!isValidCatalog(ref) || !hasName(ref)) {
      return Optional.empty();
    }
    var catalogOpt = catalogRepository.getByName(accountId, ref.getCatalog());
    if (catalogOpt.isEmpty()) {
      return Optional.empty();
    }
    final Catalog catalog = catalogOpt.get();
    var namespaceOpt =
        namespaceRepository.getByPath(
            accountId, catalog.getResourceId().getId(), ref.getPathList());
    if (namespaceOpt.isEmpty()) {
      return Optional.empty();
    }
    final Namespace namespace = namespaceOpt.get();
    return viewRepository
        .getByName(
            accountId,
            catalog.getResourceId().getId(),
            namespace.getResourceId().getId(),
            ref.getName())
        .map(
            view ->
                new ResolvedRelation(
                    view.getResourceId(),
                    canonicalName(
                        catalog, namespace, view.getDisplayName(), view.getResourceId())));
  }

  private Catalog catalogByName(String correlationId, String accountId, String catalogName) {
    return catalogRepository
        .getByName(accountId, catalogName)
        .orElseThrow(
            () -> GrpcErrors.notFound(correlationId, "catalog", Map.of("id", catalogName)));
  }

  private Namespace namespaceByPath(
      String correlationId, String accountId, Catalog catalog, List<String> pathSegments) {
    return namespaceByPath(correlationId, accountId, catalog, pathSegments, null);
  }

  private Namespace namespaceByPath(
      String correlationId, String accountId, Catalog catalog, List<String> parents, String name) {
    List<String> segments = new java.util.ArrayList<>(parents);
    if (name != null && !name.isBlank()) {
      segments.add(name);
    }
    return namespaceRepository
        .getByPath(accountId, catalog.getResourceId().getId(), segments)
        .orElseThrow(
            () ->
                GrpcErrors.notFound(
                    correlationId,
                    "namespace.by_path_missing",
                    Map.of(
                        "catalog_id", catalog.getResourceId().getId(),
                        "path", String.join(".", segments))));
  }

  private List<String> namespacePath(NameRef ref) {
    List<String> fullPath = new java.util.ArrayList<>(ref.getPathList());
    if (!ref.getName().isBlank()) {
      fullPath.add(ref.getName());
    }
    return fullPath;
  }

  private boolean isValidCatalog(NameRef ref) {
    return ref.getCatalog() != null && !ref.getCatalog().isBlank();
  }

  private boolean hasName(NameRef ref) {
    return ref.getName() != null && !ref.getName().isBlank();
  }

  private NameRef canonicalName(
      Catalog catalog, Namespace namespace, String displayName, ResourceId id) {
    List<String> pathSegments = new java.util.ArrayList<>(namespace.getParentsList());
    if (!namespace.getDisplayName().isBlank()) {
      pathSegments.add(namespace.getDisplayName());
    }
    return NameRef.newBuilder()
        .setCatalog(catalog.getDisplayName())
        .addAllPath(pathSegments)
        .setName(displayName)
        .setResourceId(id)
        .build();
  }
}
