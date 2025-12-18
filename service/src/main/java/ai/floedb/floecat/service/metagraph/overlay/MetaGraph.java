package ai.floedb.floecat.service.metagraph.overlay;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.service.context.EngineContextProvider;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.metagraph.overlay.systemobjects.SystemGraph;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.query.resolver.LogicalSchemaMapper;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.spi.scanner.CatalogOverlay;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

@ApplicationScoped
public final class MetaGraph implements CatalogOverlay {

  private final UserGraph userGraph;
  private final LogicalSchemaMapper schemaMapper;
  private final SystemGraph systemGraph;
  private final EngineContextProvider engine;

  /**
   * Constructs a MetaGraph that merges system and user graph overlays.
   *
   * @param userGraph the user-defined graph overlay for mutable catalog objects
   * @param schemaMapper mapper for converting between logical and physical schemas
   * @param systemGraph the system graph overlay for immutable built-in catalog objects
   * @param engine provider for current engine context (kind and version)
   */
  @Inject
  public MetaGraph(
      UserGraph userGraph,
      LogicalSchemaMapper schemaMapper,
      SystemGraph systemGraph,
      EngineContextProvider engine) {
    this.userGraph = userGraph;
    this.schemaMapper = schemaMapper;
    this.systemGraph = systemGraph;
    this.engine = engine;
  }

  /**
   * Resolves a graph node by ID, checking system graph first, then user graph.
   *
   * <p>System objects take precedence over user objects with the same ID, ensuring built-in catalog
   * objects like pg_class are always visible.
   *
   * @param id the resource ID to resolve
   * @return the resolved graph node, or empty if not found
   */
  @Override
  public Optional<GraphNode> resolve(ResourceId id) {
    Optional<GraphNode> sys = systemGraph.resolve(id, engine.engineKind(), engine.engineVersion());
    if (sys.isPresent()) {
      return sys;
    }
    return userGraph.resolve(id);
  }

  /**
   * Lists all relations (tables and views) in the specified catalog.
   *
   * <p>Merges system relations and user relations unconditionally, ensuring system objects are
   * globally visible alongside user objects.
   *
   * @param catalogId the catalog to list relations from
   * @return list of all relations in the catalog
   */
  @Override
  public List<GraphNode> listRelations(ResourceId catalogId) {
    return mergeLists(
        () -> systemGraph.listRelations(catalogId, engine.engineKind(), engine.engineVersion()),
        () -> userGraph.listRelations(catalogId));
  }

  /**
   * Lists all relations (tables and views) in the specified namespace.
   *
   * <p>Merges system relations and user relations unconditionally, ensuring system objects are
   * globally visible alongside user objects.
   *
   * @param catalogId the catalog containing the namespace
   * @param namespaceId the namespace to list relations from
   * @return list of all relations in the namespace
   */
  @Override
  public List<GraphNode> listRelationsInNamespace(ResourceId catalogId, ResourceId namespaceId) {
    return mergeLists(
        () ->
            systemGraph.listRelationsInNamespace(
                catalogId, namespaceId, engine.engineKind(), engine.engineVersion()),
        () -> userGraph.listRelationsInNamespace(catalogId, namespaceId));
  }

  /**
   * Lists all functions in the specified namespace.
   *
   * <p>Merges system functions and user functions unconditionally.
   *
   * @param catalogId the catalog containing the namespace (unused for functions)
   * @param namespaceId the namespace to list functions from
   * @return list of all functions in the namespace
   */
  @Override
  public List<FunctionNode> listFunctions(ResourceId catalogId, ResourceId namespaceId) {
    return mergeLists(
        () -> systemGraph.listFunctions(namespaceId, engine.engineKind(), engine.engineVersion()),
        () -> userGraph.listFunctions(namespaceId));
  }

  /**
   * Lists all types in the specified catalog.
   *
   * <p>Merges system types and user types unconditionally.
   *
   * @param catalogId the catalog to list types from
   * @return list of all types in the catalog
   */
  @Override
  public List<TypeNode> listTypes(ResourceId catalogId) {
    return mergeLists(
        () -> systemGraph.listTypes(catalogId, engine.engineKind(), engine.engineVersion()),
        () -> userGraph.listTypes(catalogId));
  }

  @Override
  public List<NamespaceNode> listNamespaces(ResourceId catalogId) {
    return mergeLists(
        () -> systemGraph.listNamespaces(catalogId, engine.engineKind(), engine.engineVersion()),
        () -> userGraph.listNamespaces(catalogId));
  }

  /**
   * Resolves a catalog by name.
   *
   * <p>Catalogs are only user-defined; system catalogs are implicit.
   *
   * @param correlationId correlation ID for error reporting
   * @param name the catalog name to resolve
   * @return the resolved catalog resource ID
   * @throws GrpcErrors if the catalog cannot be resolved
   */
  @Override
  public ResourceId resolveCatalog(String correlationId, String name) {
    return userGraph.resolveCatalog(correlationId, name);
  }

  /**
   * Resolves a namespace by name reference.
   *
   * <p>Uses system-first resolution with ambiguity checking. If both system and user graphs contain
   * a namespace with the same name, an error is thrown.
   *
   * @param correlationId correlation ID for error reporting
   * @param ref the name reference to resolve
   * @return the resolved namespace resource ID
   * @throws GrpcErrors if the namespace is ambiguous or not found
   */
  @Override
  public ResourceId resolveNamespace(String correlationId, NameRef ref) {
    return resolveWithAmbiguityCheck(
        correlationId,
        ref,
        () -> systemGraph.resolveNamespace(ref, engine.engineKind(), engine.engineVersion()),
        () -> userGraph.tryResolveNamespace(correlationId, ref),
        "namespace");
  }

  /**
   * Resolves a table by name reference.
   *
   * <p>Uses system-first resolution with ambiguity checking. If both system and user graphs contain
   * a table with the same name, an error is thrown.
   *
   * @param correlationId correlation ID for error reporting
   * @param ref the name reference to resolve
   * @return the resolved table resource ID
   * @throws GrpcErrors if the table is ambiguous or not found
   */
  @Override
  public ResourceId resolveTable(String correlationId, NameRef ref) {
    return resolveWithAmbiguityCheck(
        correlationId,
        ref,
        () -> systemGraph.resolveTable(ref, engine.engineKind(), engine.engineVersion()),
        () -> userGraph.tryResolveTable(correlationId, ref),
        "table");
  }

  /**
   * Resolves a view by name reference.
   *
   * <p>Uses system-first resolution with ambiguity checking. If both system and user graphs contain
   * a view with the same name, an error is thrown.
   *
   * @param correlationId correlation ID for error reporting
   * @param ref the name reference to resolve
   * @return the resolved view resource ID
   * @throws GrpcErrors if the view is ambiguous or not found
   */
  @Override
  public ResourceId resolveView(String correlationId, NameRef ref) {
    return resolveWithAmbiguityCheck(
        correlationId,
        ref,
        () -> systemGraph.resolveView(ref, engine.engineKind(), engine.engineVersion()),
        () -> userGraph.tryResolveView(correlationId, ref),
        "view");
  }

  /**
   * Resolves a relation (table or view) by name reference.
   *
   * <p>Uses system-first resolution with ambiguity checking. If both system and user graphs contain
   * relations with the same name, an error is thrown.
   *
   * @param correlationId correlation ID for error reporting
   * @param ref the name reference to resolve
   * @return the resolved relation resource ID
   * @throws GrpcErrors if the relation is ambiguous or not found
   */
  @Override
  public ResourceId resolveName(String correlationId, NameRef ref) {
    return resolveWithAmbiguityCheck(
        correlationId,
        ref,
        () -> systemGraph.resolveName(ref, engine.engineKind(), engine.engineVersion()),
        () -> userGraph.tryResolveName(correlationId, ref),
        "table");
  }

  /**
   * Gets the snapshot pin for a table.
   *
   * <p>System tables don't require snapshot pins as they are immutable. Returns null for system
   * tables, otherwise delegates to user graph.
   *
   * @param correlationId correlation ID for error reporting
   * @param tableId the table resource ID
   * @param override explicit snapshot override, if any
   * @param asOfDefault default timestamp for time travel queries
   * @return the snapshot pin, or null for system tables
   */
  @Override
  public SnapshotPin snapshotPinFor(
      String correlationId,
      ResourceId tableId,
      SnapshotRef override,
      Optional<Timestamp> asOfDefault) {
    // System objects don't need snapshot pins
    if (resolve(tableId).filter(SystemTableNode.class::isInstance).isPresent()) {
      return null;
    }
    return userGraph.snapshotPinFor(correlationId, tableId, override, asOfDefault);
  }

  /**
   * Resolves tables by prefix for auto-completion.
   *
   * <p>Only searches user-defined tables as system tables are not typically auto-completed in
   * queries.
   *
   * @param correlationId correlation ID for error reporting
   * @param items list of name references to resolve
   * @param limit maximum number of results to return
   * @param token pagination token for continuing results
   * @return resolve result containing matching tables
   */
  @Override
  public ResolveResult resolveTables(
      String correlationId, List<NameRef> items, int limit, String token) {
    return toResolveResult(userGraph.resolveTables(correlationId, items, limit, token));
  }

  /**
   * Resolves tables by prefix for auto-completion.
   *
   * <p>Only searches user-defined tables as system tables are not typically auto-completed in
   * queries.
   *
   * @param correlationId correlation ID for error reporting
   * @param prefix the name prefix to match
   * @param limit maximum number of results to return
   * @param token pagination token for continuing results
   * @return resolve result containing matching tables
   */
  @Override
  public ResolveResult resolveTables(
      String correlationId, NameRef prefix, int limit, String token) {
    return toResolveResult(userGraph.resolveTables(correlationId, prefix, limit, token));
  }

  /**
   * Resolves views by prefix for auto-completion.
   *
   * <p>Only searches user-defined views as system views are not typically auto-completed in
   * queries.
   *
   * @param correlationId correlation ID for error reporting
   * @param items list of name references to resolve
   * @param limit maximum number of results to return
   * @param token pagination token for continuing results
   * @return resolve result containing matching views
   */
  @Override
  public ResolveResult resolveViews(
      String correlationId, List<NameRef> items, int limit, String token) {
    return toResolveResult(userGraph.resolveViews(correlationId, items, limit, token));
  }

  /**
   * Resolves views by prefix for auto-completion.
   *
   * <p>Only searches user-defined views as system views are not typically auto-completed in
   * queries.
   *
   * @param correlationId correlation ID for error reporting
   * @param prefix the name prefix to match
   * @param limit maximum number of results to return
   * @param token pagination token for continuing results
   * @return resolve result containing matching views
   */
  @Override
  public ResolveResult resolveViews(String correlationId, NameRef prefix, int limit, String token) {
    return toResolveResult(userGraph.resolveViews(correlationId, prefix, limit, token));
  }

  /**
   * Gets the fully qualified name of a namespace by its resource ID.
   *
   * <p>Tries user graph first, then system graph for reverse lookup.
   *
   * @param id the namespace resource ID
   * @return the fully qualified name reference, or empty if not found
   */
  @Override
  public Optional<NameRef> namespaceName(ResourceId id) {
    Optional<NameRef> user = userGraph.namespaceName(id);
    if (user.isPresent()) {
      return user;
    }
    return systemGraph.namespaceName(id, engine.engineKind(), engine.engineVersion());
  }

  /**
   * Gets the fully qualified name of a table by its resource ID.
   *
   * <p>Tries user graph first, then system graph for reverse lookup.
   *
   * @param id the table resource ID
   * @return the fully qualified name reference, or empty if not found
   */
  @Override
  public Optional<NameRef> tableName(ResourceId id) {
    Optional<NameRef> user = userGraph.tableName(id);
    if (user.isPresent()) {
      return user;
    }
    return systemGraph.tableName(id, engine.engineKind(), engine.engineVersion());
  }

  /**
   * Gets the fully qualified name of a view by its resource ID.
   *
   * <p>Tries user graph first, then system graph for reverse lookup.
   *
   * @param id the view resource ID
   * @return the fully qualified name reference, or empty if not found
   */
  @Override
  public Optional<NameRef> viewName(ResourceId id) {
    Optional<NameRef> user = userGraph.viewName(id);
    if (user.isPresent()) {
      return user;
    }
    return systemGraph.viewName(id, engine.engineKind(), engine.engineVersion());
  }

  /**
   * Resolves a catalog node by its resource ID.
   *
   * <p>Catalogs are only user-defined; system catalogs are implicit.
   *
   * @param id the catalog resource ID
   * @return the catalog node, or empty if not found
   */
  @Override
  public Optional<CatalogNode> catalog(ResourceId id) {
    Optional<CatalogNode> user = userGraph.catalog(id);
    if (user.isPresent()) {
      return user;
    }
    // System catalogs are implicit, no need to handle
    return Optional.empty();
  }

  /**
   * Gets the schema resolution for a table at a specific snapshot.
   *
   * <p>Delegates to user graph for schema resolution as system schemas are handled differently.
   *
   * @param correlationId correlation ID for error reporting
   * @param tableId the table resource ID
   * @param snapshot the snapshot reference
   * @return the schema resolution, or null if not found
   */
  @Override
  public SchemaResolution schemaFor(
      String correlationId, ResourceId tableId, SnapshotRef snapshot) {
    UserGraph.SchemaResolution delegate = userGraph.schemaFor(correlationId, tableId, snapshot);
    if (delegate == null) {
      return null;
    }
    return new SchemaResolution(delegate.table(), delegate.schemaJson());
  }

  /**
   * Merges two lists of items, combining system items first, then user items.
   *
   * <p>This ensures system objects are globally visible alongside user objects without any
   * filtering or deduplication.
   *
   * @param systemSupplier supplier for system items
   * @param userSupplier supplier for user items
   * @return merged list with system items followed by user items
   * @param <T> the type of items being merged
   */
  private <T> List<T> mergeLists(Supplier<List<T>> systemSupplier, Supplier<List<T>> userSupplier) {
    List<T> result = new ArrayList<>();
    result.addAll(systemSupplier.get());
    result.addAll(userSupplier.get());
    return result;
  }

  /**
   * Resolves a name reference with system-first precedence and ambiguity checking.
   *
   * <p>If both system and user graphs resolve the name, throws an ambiguity error. If neither
   * resolves it, throws an unresolved error. Otherwise returns the resolved resource ID.
   *
   * @param correlationId correlation ID for error reporting
   * @param ref the name reference to resolve
   * @param systemResolver supplier for system graph resolution
   * @param userResolver supplier for user graph resolution
   * @param notFoundMessageKey message key for not found errors
   * @return the resolved resource ID
   * @throws GrpcErrors if the name is ambiguous or not found
   */
  private ResourceId resolveWithAmbiguityCheck(
      String correlationId,
      NameRef ref,
      Supplier<Optional<ResourceId>> systemResolver,
      Supplier<Optional<ResourceId>> userResolver,
      String notFoundMessageKey) {
    Optional<ResourceId> sys = systemResolver.get();
    Optional<ResourceId> user = userResolver.get();
    if (sys.isPresent() && user.isPresent()) {
      throw GrpcErrors.invalidArgument(
          correlationId, "query.input.ambiguous", Map.of("name", ref.toString()));
    }
    if (sys.isPresent()) {
      return sys.get();
    }
    if (user.isPresent()) {
      return user.get();
    }
    throw GrpcErrors.notFound(correlationId, notFoundMessageKey, Map.of("id", ref.toString()));
  }

  @Override
  public List<SchemaColumn> tableSchema(ResourceId tableId) {
    return resolve(tableId)
        .filter(TableNode.class::isInstance)
        .map(TableNode.class::cast)
        .map(this::schemaForTable)
        .orElse(List.of());
  }

  private List<SchemaColumn> schemaForTable(TableNode table) {
    if (table instanceof UserTableNode ut) {
      return schemaMapper.map(ut).getColumnsList();
    }
    if (table instanceof SystemTableNode st) {
      return st.columns();
    }
    return List.of();
  }

  /**
   * Converts a UserGraph resolve result to a CatalogOverlay resolve result.
   *
   * @param delegate the user graph resolve result
   * @return the catalog overlay resolve result
   */
  private ResolveResult toResolveResult(UserGraph.ResolveResult delegate) {
    List<CatalogOverlay.QualifiedRelation> relations =
        delegate.relations().stream()
            .map(rel -> new CatalogOverlay.QualifiedRelation(rel.name(), rel.resourceId()))
            .toList();
    return new ResolveResult(relations, delegate.totalSize(), delegate.nextToken());
  }
}
