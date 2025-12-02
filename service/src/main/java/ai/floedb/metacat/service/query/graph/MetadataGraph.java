package ai.floedb.metacat.service.query.graph;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.catalog.rpc.UpstreamRef;
import ai.floedb.metacat.catalog.rpc.View;
import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.query.rpc.SchemaColumn;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;
import ai.floedb.metacat.service.repo.impl.NamespaceRepository;
import ai.floedb.metacat.service.repo.impl.TableRepository;
import ai.floedb.metacat.service.repo.impl.ViewRepository;
import ai.floedb.metacat.storage.errors.StorageNotFoundException;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * MetadataGraph orchestrates cached access to {@link RelationNode}s.
 *
 * <p>This class encapsulates cache lifecycle, repository lookups, and mutation metadata so that
 * subsequent query/runtime layers can reason about a consistent view of catalogs, namespaces,
 * tables, and views without issuing redundant storage calls.
 */
@ApplicationScoped
public class MetadataGraph {

  private static final Map<EngineKey, EngineHint> NO_ENGINE_HINTS = Map.of();

  private final CatalogRepository catalogRepository;
  private final NamespaceRepository namespaceRepository;
  private final TableRepository tableRepository;
  private final ViewRepository viewRepository;

  private final Cache<GraphCacheKey, RelationNode> nodeCache;

  @Inject
  public MetadataGraph(
      CatalogRepository catalogRepository,
      NamespaceRepository namespaceRepository,
      TableRepository tableRepository,
      ViewRepository viewRepository) {
    this.catalogRepository = catalogRepository;
    this.namespaceRepository = namespaceRepository;
    this.tableRepository = tableRepository;
    this.viewRepository = viewRepository;
    this.nodeCache =
        Caffeine.newBuilder().maximumSize(50_000).expireAfterAccess(Duration.ofMinutes(15)).build();
  }

  /**
   * Resolve a resource into its cached {@link RelationNode}.
   *
   * @param id resource identifier (must include kind + tenant)
   * @return optional node (empty when resource is missing or not yet supported)
   */
  public Optional<RelationNode> resolve(ResourceId id) {
    Optional<MutationMeta> metaOpt = mutationMeta(id);
    if (metaOpt.isEmpty()) {
      return Optional.empty();
    }
    MutationMeta meta = metaOpt.get();
    GraphCacheKey key = new GraphCacheKey(id, meta.getPointerVersion());
    RelationNode cached = nodeCache.getIfPresent(key);
    if (cached != null) {
      return Optional.of(cached);
    }
    Optional<RelationNode> loaded = loadNode(id, meta);
    loaded.ifPresent(node -> nodeCache.put(key, node));
    return loaded;
  }

  /** Convenience wrapper returning a {@link CatalogNode}. */
  public Optional<CatalogNode> catalog(ResourceId id) {
    return resolve(id).filter(CatalogNode.class::isInstance).map(CatalogNode.class::cast);
  }

  /** Convenience wrapper returning a {@link NamespaceNode}. */
  public Optional<NamespaceNode> namespace(ResourceId id) {
    return resolve(id).filter(NamespaceNode.class::isInstance).map(NamespaceNode.class::cast);
  }

  /** Convenience wrapper returning a {@link TableNode}. */
  public Optional<TableNode> table(ResourceId id) {
    return resolve(id).filter(TableNode.class::isInstance).map(TableNode.class::cast);
  }

  /** Convenience wrapper returning a {@link ViewNode}. */
  public Optional<ViewNode> view(ResourceId id) {
    return resolve(id).filter(ViewNode.class::isInstance).map(ViewNode.class::cast);
  }

  /**
   * Evict every cached version of the provided resource.
   *
   * <p>Updaters should call this after successful mutations to avoid serving stale metadata.
   */
  public void invalidate(ResourceId id) {
    nodeCache.asMap().keySet().removeIf(key -> key.id().equals(id));
  }

  private Optional<RelationNode> loadNode(ResourceId id, MutationMeta meta) {
    return switch (id.getKind()) {
      case RK_CATALOG -> loadCatalog(id, meta);
      case RK_NAMESPACE -> loadNamespace(id, meta);
      case RK_TABLE -> loadTable(id, meta);
      case RK_VIEW -> loadView(id, meta);
      default -> Optional.empty();
    };
  }

  private Optional<RelationNode> loadCatalog(ResourceId id, MutationMeta meta) {
    return catalogRepository.getById(id).map(catalog -> toCatalogNode(catalog, meta));
  }

  private Optional<RelationNode> loadNamespace(ResourceId id, MutationMeta meta) {
    return namespaceRepository.getById(id).map(namespace -> toNamespaceNode(namespace, meta));
  }

  private Optional<RelationNode> loadTable(ResourceId id, MutationMeta meta) {
    return tableRepository.getById(id).map(table -> toTableNode(table, meta));
  }

  private Optional<RelationNode> loadView(ResourceId id, MutationMeta meta) {
    return viewRepository.getById(id).map(view -> toViewNode(view, meta));
  }

  private Optional<MutationMeta> mutationMeta(ResourceId id) {
    try {
      ResourceKind kind = id.getKind();
      return switch (kind) {
        case RK_CATALOG -> Optional.of(catalogRepository.metaForSafe(id));
        case RK_NAMESPACE -> Optional.of(namespaceRepository.metaForSafe(id));
        case RK_TABLE -> Optional.of(tableRepository.metaForSafe(id));
        case RK_VIEW -> Optional.of(viewRepository.metaForSafe(id));
        default -> Optional.empty();
      };
    } catch (StorageNotFoundException snf) {
      return Optional.empty();
    }
  }

  private static Instant toInstant(Timestamp ts) {
    if (ts == null) {
      return Instant.EPOCH;
    }
    return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
  }

  private CatalogNode toCatalogNode(Catalog catalog, MutationMeta meta) {
    return new CatalogNode(
        catalog.getResourceId(),
        meta.getPointerVersion(),
        toInstant(meta.getUpdatedAt()),
        catalog.getDisplayName(),
        catalog.getPropertiesMap(),
        catalog.hasConnectorRef() ? Optional.of(catalog.getConnectorRef()) : Optional.empty(),
        catalog.hasPolicyRef() ? Optional.of(catalog.getPolicyRef()) : Optional.empty(),
        Optional.empty(),
        NO_ENGINE_HINTS);
  }

  private NamespaceNode toNamespaceNode(Namespace namespace, MutationMeta meta) {
    return new NamespaceNode(
        namespace.getResourceId(),
        meta.getPointerVersion(),
        toInstant(meta.getUpdatedAt()),
        namespace.getCatalogId(),
        namespace.getParentsList(),
        namespace.getDisplayName(),
        namespace.getPropertiesMap(),
        Optional.empty(),
        NO_ENGINE_HINTS);
  }

  private TableNode toTableNode(Table table, MutationMeta meta) {
    UpstreamRef upstream =
        table.hasUpstream() ? table.getUpstream() : UpstreamRef.getDefaultInstance();
    TableFormat format = upstream.getFormat();
    return new TableNode(
        table.getResourceId(),
        meta.getPointerVersion(),
        toInstant(meta.getUpdatedAt()),
        table.getCatalogId(),
        table.getNamespaceId(),
        table.getDisplayName(),
        format,
        table.getSchemaJson(),
        table.getPropertiesMap(),
        upstream.getPartitionKeysList(),
        upstream.getFieldIdByPathMap(),
        Optional.<SnapshotRef>empty(),
        Optional.<SnapshotRef>empty(),
        Optional.empty(),
        Optional.empty(),
        List.of(),
        NO_ENGINE_HINTS);
  }

  private ViewNode toViewNode(View view, MutationMeta meta) {
    return new ViewNode(
        view.getResourceId(),
        meta.getPointerVersion(),
        toInstant(meta.getUpdatedAt()),
        view.getCatalogId(),
        view.getNamespaceId(),
        view.getDisplayName(),
        view.getSql(),
        "",
        List.<SchemaColumn>of(),
        List.<ResourceId>of(),
        List.of(),
        view.getPropertiesMap(),
        Optional.empty(),
        NO_ENGINE_HINTS);
  }
}
