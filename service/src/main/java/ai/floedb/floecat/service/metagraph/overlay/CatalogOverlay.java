package ai.floedb.floecat.service.metagraph.overlay;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectGraphView;
import com.google.protobuf.Timestamp;
import java.util.List;
import java.util.Optional;

/**
 * Shared overlay between metadata/system objects that exposes the graph operations needed by
 * scanners, planners, and traversal helpers.
 *
 * <p>This interface unifies the MetadataGraph view and the builtin graph so callers can depend on a
 * single entry point and do not need to mix ad-hoc resolver code.
 */
public interface CatalogOverlay extends SystemObjectGraphView {

  /** Resolves any graph node for the given resource + engine context. */
  Optional<GraphNode> resolve(ResourceId id, String engineKind, String engineVersion);

  /**
   * Lists every relation under the requested catalog (namespaces, tables, views, plus system
   * objects).
   */
  List<GraphNode> listRelations(ResourceId catalogId, String engineKind, String engineVersion);

  /** Lists namespaces owned by the requested catalog. */
  List<NamespaceNode> listNamespaces(ResourceId catalogId, String engineKind, String engineVersion);

  /** Lists relations that live inside the given namespace. */
  List<GraphNode> listRelationsInNamespace(
      ResourceId catalogId, ResourceId namespaceId, String engineKind, String engineVersion);

  ResourceId resolveCatalog(String correlationId, String name);

  ResourceId resolveNamespace(String correlationId, NameRef ref);

  ResourceId resolveTable(String correlationId, NameRef ref);

  ResourceId resolveView(String correlationId, NameRef ref);

  ResourceId resolveName(String correlationId, NameRef ref);

  SnapshotPin snapshotPinFor(
      String correlationId,
      ResourceId tableId,
      SnapshotRef override,
      Optional<Timestamp> asOfDefault);

  ResolveResult resolveTables(String correlationId, List<NameRef> items, int limit, String token);

  ResolveResult resolveTables(String correlationId, NameRef prefix, int limit, String token);

  ResolveResult resolveViews(String correlationId, List<NameRef> items, int limit, String token);

  ResolveResult resolveViews(String correlationId, NameRef prefix, int limit, String token);

  Optional<NameRef> namespaceName(ResourceId id);

  Optional<NameRef> tableName(ResourceId id);

  Optional<NameRef> viewName(ResourceId id);

  Optional<CatalogNode> catalog(ResourceId id);

  SchemaResolution schemaFor(String correlationId, ResourceId tableId, SnapshotRef snapshot);

  /**
   * Simplified result returned by the overlay whenever caller requests a paged list of tables or
   * views.
   */
  record ResolveResult(List<QualifiedRelation> relations, int totalSize, String nextToken) {}

  record QualifiedRelation(NameRef name, ResourceId resourceId) {}

  record SchemaResolution(TableNode table, String schemaJson) {}
}
