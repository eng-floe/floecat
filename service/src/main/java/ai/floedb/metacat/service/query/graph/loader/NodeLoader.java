package ai.floedb.metacat.service.query.graph.loader;

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
import ai.floedb.metacat.service.query.graph.model.CatalogNode;
import ai.floedb.metacat.service.query.graph.model.EngineHint;
import ai.floedb.metacat.service.query.graph.model.EngineKey;
import ai.floedb.metacat.service.query.graph.model.NamespaceNode;
import ai.floedb.metacat.service.query.graph.model.RelationNode;
import ai.floedb.metacat.service.query.graph.model.TableNode;
import ai.floedb.metacat.service.query.graph.model.ViewNode;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;
import ai.floedb.metacat.service.repo.impl.NamespaceRepository;
import ai.floedb.metacat.service.repo.impl.TableRepository;
import ai.floedb.metacat.service.repo.impl.ViewRepository;
import ai.floedb.metacat.storage.errors.StorageNotFoundException;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Responsible for materialising immutable relation nodes from repository metadata.
 *
 * <p>MetadataGraph relies on this helper for both pointer metadata (`metaForSafe`) and the actual
 * protobuf → node conversions.
 */
public class NodeLoader {

  private static final Map<EngineKey, EngineHint> NO_ENGINE_HINTS = Map.of();

  private final CatalogRepository catalogRepository;
  private final NamespaceRepository namespaceRepository;
  private final TableRepository tableRepository;
  private final ViewRepository viewRepository;

  public NodeLoader(
      CatalogRepository catalogRepository,
      NamespaceRepository namespaceRepository,
      TableRepository tableRepository,
      ViewRepository viewRepository) {
    this.catalogRepository = catalogRepository;
    this.namespaceRepository = namespaceRepository;
    this.tableRepository = tableRepository;
    this.viewRepository = viewRepository;
  }

  /** Loads the mutation metadata for the provided resource. */
  public Optional<MutationMeta> mutationMeta(ResourceId id) {
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

  /** Rehydrates the relation node for the provided metadata snapshot. */
  public Optional<RelationNode> load(ResourceId id, MutationMeta meta) {
    return switch (id.getKind()) {
      case RK_CATALOG -> catalogRepository.getById(id).map(catalog -> toCatalogNode(catalog, meta));
      case RK_NAMESPACE ->
          namespaceRepository.getById(id).map(namespace -> toNamespaceNode(namespace, meta));
      case RK_TABLE -> tableRepository.getById(id).map(table -> toTableNode(table, meta));
      case RK_VIEW -> viewRepository.getById(id).map(view -> toViewNode(view, meta));
      default -> Optional.empty();
    };
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

  private static Instant toInstant(Timestamp ts) {
    if (ts == null) {
      return Instant.EPOCH;
    }
    return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
  }
}
