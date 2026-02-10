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

package ai.floedb.floecat.service.metagraph.overlay;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.connector.common.resolver.LogicalSchemaMapper;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.service.context.EngineContextProvider;
import ai.floedb.floecat.service.error.impl.GeneratedErrorMessages;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.metagraph.overlay.systemobjects.SystemCatalogTranslator;
import ai.floedb.floecat.service.metagraph.overlay.systemobjects.SystemGraph;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.spi.scanner.CatalogOverlay;
import ai.floedb.floecat.systemcatalog.util.EngineContext;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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

  private EngineContext engineContext() {
    return engine.isPresent() ? engine.engineContext() : EngineContext.empty();
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
    EngineContext ctx = engineContext();
    Optional<GraphNode> sys = systemGraph.resolve(id, ctx);
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
    EngineContext ctx = engineContext();
    return mergeLists(
        () -> systemGraph.listRelations(catalogId, ctx), () -> userGraph.listRelations(catalogId));
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
    EngineContext ctx = engineContext();
    return mergeLists(
        () -> systemGraph.listRelationsInNamespace(catalogId, namespaceId, ctx),
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
    EngineContext ctx = engineContext();
    return mergeLists(
        () -> systemGraph.listFunctions(namespaceId, ctx),
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
    EngineContext ctx = engineContext();
    return mergeLists(
        () -> systemGraph.listTypes(catalogId, ctx), () -> userGraph.listTypes(catalogId));
  }

  @Override
  public List<NamespaceNode> listNamespaces(ResourceId catalogId) {
    EngineContext ctx = engineContext();
    return mergeLists(
        () -> systemGraph.listNamespaces(catalogId, ctx),
        () -> userGraph.listNamespaces(catalogId));
  }

  /**
   * Resolves a catalog by name.
   *
   * <p>Catalogs are only user-defined; system catalogs are implicit.
   *
   * @param correlationId correlation ID for error reporting
   * @param name the catalog name to resolve
   * @return the resolved catalog resource ID, if present
   */
  @Override
  public Optional<ResourceId> resolveCatalog(String correlationId, String name) {
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
   * @return the resolved namespace resource ID, if present
   * @throws GrpcErrors if the namespace is ambiguous
   */
  @Override
  public Optional<ResourceId> resolveNamespace(String correlationId, NameRef ref) {
    EngineContext ctx = engineContext();
    return resolveWithAmbiguityCheck(
        correlationId,
        ref,
        () -> systemGraph.resolveNamespace(ref, ctx),
        () -> userGraph.resolveNamespace(correlationId, ref));
  }

  /**
   * Resolves a table by name reference.
   *
   * <p>Uses system-first resolution with ambiguity checking. If both system and user graphs contain
   * a table with the same name, an error is thrown.
   *
   * @param correlationId correlation ID for error reporting
   * @param ref the name reference to resolve
   * @return the resolved table resource ID, if present
   * @throws GrpcErrors if the table is ambiguous
   */
  @Override
  public Optional<ResourceId> resolveTable(String correlationId, NameRef ref) {
    EngineContext ctx = engineContext();
    return resolveWithAmbiguityCheck(
        correlationId,
        ref,
        () -> systemGraph.resolveTable(ref, ctx),
        () -> userGraph.resolveTable(correlationId, ref));
  }

  /**
   * Resolves a view by name reference.
   *
   * <p>Uses system-first resolution with ambiguity checking. If both system and user graphs contain
   * a view with the same name, an error is thrown.
   *
   * @param correlationId correlation ID for error reporting
   * @param ref the name reference to resolve
   * @return the resolved view resource ID, if present
   * @throws GrpcErrors if the view is ambiguous
   */
  @Override
  public Optional<ResourceId> resolveView(String correlationId, NameRef ref) {
    EngineContext ctx = engineContext();
    return resolveWithAmbiguityCheck(
        correlationId,
        ref,
        () -> systemGraph.resolveView(ref, ctx),
        () -> userGraph.resolveView(correlationId, ref));
  }

  /**
   * Resolves a relation (table or view) by name reference.
   *
   * <p>Uses system-first resolution with ambiguity checking. If both system and user graphs contain
   * relations with the same name, an error is thrown.
   *
   * @param correlationId correlation ID for error reporting
   * @param ref the name reference to resolve
   * @return the resolved relation resource ID, if present
   * @throws GrpcErrors if the relation is ambiguous
   */
  @Override
  public Optional<ResourceId> resolveName(String correlationId, NameRef ref) {
    EngineContext ctx = engineContext();
    return resolveWithAmbiguityCheck(
        correlationId,
        ref,
        () -> systemGraph.resolveName(ref, ctx),
        () -> userGraph.resolveName(correlationId, ref));
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
   * Resolves tables for an explicit list of fully-qualified name references.
   *
   * <p>Performs a full merge between system and user objects. System resolution is attempted first
   * using the current engine context (kind/version), then user resolution is attempted. If both
   * resolve to a concrete resource, the name is treated as ambiguous.
   *
   * <p>The returned {@link NameRef} for system objects is aliased back to the user-facing catalog
   * name so callers observe a "symlink" effect (e.g. user catalog "examples" shows {@code
   * information_schema.schemata}).
   */
  @Override
  public ResolveResult batchResolveTables(
      String correlationId, List<NameRef> items, int limit, String token) {

    validateListToken(correlationId, token);

    if (items == null || items.isEmpty()) {
      return new ResolveResult(List.of(), 0, "");
    }

    EngineContext ctx = engineContext();
    int max = Math.min(items.size(), normalizeLimit(limit));

    List<NameRef> subset = items.subList(0, max);
    Map<String, CatalogOverlay.QualifiedRelation> merged =
        collectSystemRelationsForNames(subset, ctx, true);

    ResolveResult userResult =
        toResolveResult(userGraph.resolveTables(correlationId, subset, max, token));

    for (CatalogOverlay.QualifiedRelation userRelation : userResult.relations()) {
      String key = canonicalName(userRelation.name());
      if (merged.containsKey(key)) {
        throw ambiguity(correlationId, userRelation.name());
      }
      merged.put(key, userRelation);
    }

    return new ResolveResult(new ArrayList<>(merged.values()), merged.size(), "");
  }

  /**
   * Resolves tables under a namespace prefix.
   *
   * <p>Performs a full merge between system and user objects. If the prefix resolves to a system
   * namespace under the current engine context, system relations are enumerated and returned first
   * (aliased back to the user-facing catalog). User relations are then appended.
   *
   * <p>This enables queries like {@code tables examples.information_schema} to surface built-in
   * engine relations while preserving user catalog naming.
   */
  @Override
  public ResolveResult listTablesByPrefix(
      String correlationId, NameRef prefix, int limit, String token) {

    EngineContext ctx = engineContext();
    int max = normalizeLimit(limit);
    String userToken = decodeUserToken(token);
    boolean firstPage = userToken.isBlank();

    List<CatalogOverlay.QualifiedRelation> system =
        firstPage ? collectSystemRelationsInNamespace(prefix, ctx, true, max) : List.of();
    ResolveResult user =
        toResolveResult(userGraph.resolveTables(correlationId, prefix, max, userToken));

    return mergePrefixResults(system, user, max);
  }

  /**
   * Resolves views for an explicit list of fully-qualified name references.
   *
   * <p>Performs a full merge between system and user objects. System resolution is attempted first
   * using the current engine context (kind/version), then user resolution is attempted. If both
   * resolve to a concrete resource, the name is treated as ambiguous.
   *
   * <p>The returned {@link NameRef} for system objects is aliased back to the user-facing catalog
   * name so callers observe a "symlink" effect.
   */
  @Override
  public ResolveResult batchResolveViews(
      String correlationId, List<NameRef> items, int limit, String token) {

    validateListToken(correlationId, token);

    if (items == null || items.isEmpty()) {
      return new ResolveResult(List.of(), 0, "");
    }

    EngineContext ctx = engineContext();
    int max = Math.min(items.size(), normalizeLimit(limit));

    List<NameRef> subset = items.subList(0, max);
    Map<String, CatalogOverlay.QualifiedRelation> merged =
        collectSystemRelationsForNames(subset, ctx, false);

    ResolveResult userResult =
        toResolveResult(userGraph.resolveViews(correlationId, subset, max, token));

    for (CatalogOverlay.QualifiedRelation userRelation : userResult.relations()) {
      String key = canonicalName(userRelation.name());
      if (merged.containsKey(key)) {
        throw ambiguity(correlationId, userRelation.name());
      }
      merged.put(key, userRelation);
    }

    return new ResolveResult(new ArrayList<>(merged.values()), merged.size(), "");
  }

  /**
   * Resolves views under a namespace prefix.
   *
   * <p>Performs a full merge between system and user objects. If the prefix resolves to a system
   * namespace under the current engine context, system relations are enumerated and returned first
   * (aliased back to the user-facing catalog). User relations are then appended.
   */
  @Override
  public ResolveResult listViewsByPrefix(
      String correlationId, NameRef prefix, int limit, String token) {

    EngineContext ctx = engineContext();
    int max = normalizeLimit(limit);
    String userToken = decodeUserToken(token);
    boolean firstPage = userToken.isBlank();

    List<CatalogOverlay.QualifiedRelation> system =
        firstPage ? collectSystemRelationsInNamespace(prefix, ctx, false, max) : List.of();
    ResolveResult user =
        toResolveResult(userGraph.resolveViews(correlationId, prefix, max, userToken));

    return mergePrefixResults(system, user, max);
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
    EngineContext ctx = engineContext();
    return systemGraph.namespaceName(id, ctx);
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
    EngineContext ctx = engineContext();
    return systemGraph.tableName(id, ctx);
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
    EngineContext ctx = engineContext();
    return systemGraph.viewName(id, ctx);
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
    EngineContext ctx = engineContext();
    return systemGraph.catalog(id, ctx);
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
   * <p>If both system and user graphs resolve the name, throws an ambiguity error. Missing names
   * simply result in an empty optional.
   *
   * @param correlationId correlation ID for error reporting
   * @param ref the name reference to resolve
   * @param systemResolver supplier for system graph resolution
   * @param userResolver supplier for user graph resolution
   * @return the resolved resource ID, if present
   * @throws GrpcErrors if the name is ambiguous
   */
  private Optional<ResourceId> resolveWithAmbiguityCheck(
      String correlationId,
      NameRef ref,
      Supplier<Optional<ResourceId>> systemResolver,
      Supplier<Optional<ResourceId>> userResolver) {
    Optional<ResourceId> sys = systemResolver.get();
    Optional<ResourceId> user = userResolver.get();
    if (sys.isPresent() && user.isPresent()) {
      throw ambiguity(correlationId, ref);
    }
    return sys.or(() -> user);
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

  /**
   * Normalizes list/prefix resolution limits.
   *
   * <p>Ensures a positive limit and applies a small default for callers that omit the limit.
   */
  private static final String USER_TOKEN_PREFIX = "u:";

  private static int normalizeLimit(int limit) {
    return Math.max(1, limit > 0 ? limit : 50);
  }

  private static String canonicalName(NameRef ref) {
    return NameRefUtil.canonical(ref);
  }

  private static String decodeUserToken(String token) {
    if (token == null || token.isBlank()) {
      return "";
    }
    if (token.startsWith(USER_TOKEN_PREFIX)) {
      return token.substring(USER_TOKEN_PREFIX.length());
    }
    return token;
  }

  private static String encodeUserToken(String token) {
    if (token == null || token.isBlank()) {
      return "";
    }
    return USER_TOKEN_PREFIX + token;
  }

  private void validateListToken(String correlationId, String token) {
    if (token != null && !token.isBlank()) {
      throw GrpcErrors.invalidArgument(
          correlationId,
          GeneratedErrorMessages.MessageKey.PAGE_TOKEN_INVALID,
          Map.of("page_token", token));
    }
  }

  private Map<String, CatalogOverlay.QualifiedRelation> collectSystemRelationsForNames(
      List<NameRef> refs, EngineContext ctx, boolean tables) {
    Map<String, CatalogOverlay.QualifiedRelation> result = new LinkedHashMap<>();
    for (NameRef ref : refs) {
      Optional<ResourceId> sysId =
          tables
              ? systemGraph.resolveTable(SystemCatalogTranslator.toSystemRelationRef(ref, ctx), ctx)
              : systemGraph.resolveView(SystemCatalogTranslator.toSystemRelationRef(ref, ctx), ctx);
      if (sysId.isEmpty()) {
        continue;
      }

      NameRef systemName =
          tables
              ? systemGraph.tableName(sysId.get(), ctx).orElse(ref)
              : systemGraph.viewName(sysId.get(), ctx).orElse(ref);
      NameRef alias = SystemCatalogTranslator.aliasToUserCatalog(ref, systemName);
      CatalogOverlay.QualifiedRelation relation =
          new CatalogOverlay.QualifiedRelation(alias, sysId.get());
      result.put(canonicalName(alias), relation);
    }
    return result;
  }

  private List<CatalogOverlay.QualifiedRelation> collectSystemRelationsInNamespace(
      NameRef prefix, EngineContext ctx, boolean tables, int max) {
    Optional<ResourceId> sysNsId =
        systemGraph.resolveNamespace(
            SystemCatalogTranslator.toSystemNamespaceRef(prefix, ctx), ctx);
    if (sysNsId.isEmpty()) {
      return List.of();
    }

    List<GraphNode> nodes =
        systemGraph.listRelationsInNamespace(ResourceId.getDefaultInstance(), sysNsId.get(), ctx);
    List<CatalogOverlay.QualifiedRelation> out = new ArrayList<>(Math.min(nodes.size(), max));

    for (GraphNode n : nodes) {
      if (out.size() >= max) {
        break;
      }
      if (tables && n instanceof TableNode t) {
        NameRef systemName =
            systemGraph.tableName(t.id(), ctx).orElse(NameRef.getDefaultInstance());
        NameRef alias =
            SystemCatalogTranslator.aliasToUserCatalog(prefix, systemName).toBuilder()
                .setResourceId(t.id())
                .build();
        out.add(new CatalogOverlay.QualifiedRelation(alias, t.id()));
      } else if (!tables && n instanceof ViewNode v) {
        NameRef systemName = systemGraph.viewName(v.id(), ctx).orElse(NameRef.getDefaultInstance());
        NameRef alias =
            SystemCatalogTranslator.aliasToUserCatalog(prefix, systemName).toBuilder()
                .setResourceId(v.id())
                .build();
        out.add(new CatalogOverlay.QualifiedRelation(alias, v.id()));
      }
    }

    return out;
  }

  private ResolveResult mergePrefixResults(
      List<CatalogOverlay.QualifiedRelation> system, ResolveResult user, int max) {
    List<CatalogOverlay.QualifiedRelation> merged = new ArrayList<>();
    Set<String> seen = new LinkedHashSet<>();

    for (CatalogOverlay.QualifiedRelation rel : system) {
      if (merged.size() >= max) {
        break;
      }
      if (seen.add(canonicalName(rel.name()))) {
        merged.add(rel);
      }
    }

    for (CatalogOverlay.QualifiedRelation rel : user.relations()) {
      if (merged.size() >= max) {
        break;
      }
      String key = canonicalName(rel.name());
      if (seen.add(key)) {
        merged.add(rel);
      }
    }

    return new ResolveResult(merged, user.totalSize(), encodeUserToken(user.nextToken()));
  }

  private RuntimeException ambiguity(String correlationId, NameRef name) {
    return GrpcErrors.invalidArgument(
        correlationId,
        GeneratedErrorMessages.MessageKey.QUERY_INPUT_AMBIGUOUS,
        Map.of("name", name.toString()));
  }
}
