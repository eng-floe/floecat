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

package ai.floedb.floecat.scanner.spi;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.scanner.utils.EngineContext;
import com.google.protobuf.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Shared overlay between metadata/system objects that exposes the graph operations needed by
 * scanners, planners, and traversal helpers.
 *
 * <p>This interface unifies the MetadataGraph view and the builtin graph so callers can depend on a
 * single entry point and do not need to mix ad-hoc resolver code.
 *
 * <p>Engine context is resolved implicitly from the request context.
 */
public interface CatalogOverlay {

  /** Resolves any graph node for the given resource. Engine context is resolved implicitly. */
  Optional<GraphNode> resolve(ResourceId id);

  /**
   * Resolves any graph node for the given resource using an explicit engine context.
   *
   * <p>Prefer this overload wherever the caller already holds the request's engine context (e.g.
   * from a {@link MetadataResolutionContext}): re-reading the engine from the request context on
   * every lookup is fragile across executor hops, and a silently empty engine makes engine-gated
   * system objects unresolvable.
   */
  default Optional<GraphNode> resolve(ResourceId id, EngineContext engineContext) {
    return resolve(id);
  }

  /**
   * Lists every relation under the requested catalog (namespaces, tables, views, plus system
   * objects). Engine context is resolved implicitly.
   */
  List<RelationNode> listRelations(ResourceId catalogId);

  /** Lists namespaces owned by the requested catalog. Engine context is resolved implicitly. */
  List<NamespaceNode> listNamespaces(ResourceId catalogId);

  /**
   * Lists relations that live inside the given namespace. Engine context is resolved implicitly.
   */
  List<RelationNode> listRelationsInNamespace(ResourceId catalogId, ResourceId namespaceId);

  /**
   * Lists only system (built-in) relations in a namespace. Default falls back to a full list and
   * filters; MetaGraph overrides this to skip the expensive user-object DynamoDB+S3 scan.
   */
  default List<RelationNode> listSystemRelationsInNamespace(
      ResourceId catalogId, ResourceId namespaceId) {
    return listRelationsInNamespace(catalogId, namespaceId).stream()
        .filter(n -> n.origin() == GraphNodeOrigin.SYSTEM)
        .toList();
  }

  /**
   * Lists only system namespaces in a catalog. Default falls back to a full list and filters;
   * MetaGraph overrides to skip the user-namespace storage scan.
   */
  default List<NamespaceNode> listSystemNamespaces(ResourceId catalogId) {
    return listNamespaces(catalogId).stream()
        .filter(n -> n.origin() == GraphNodeOrigin.SYSTEM)
        .toList();
  }

  /**
   * Whether this overlay can enumerate lightweight refs without materializing full graph nodes.
   *
   * <p>Default implementations below are correct but derive refs from full objects, so callers that
   * need a true no-hydration path should check this before relying on refs for performance.
   */
  default boolean supportsLightweightRefs() {
    return false;
  }

  /**
   * Lists namespace refs for callers that only need topology metadata. Default derives refs from
   * full namespace nodes; production overlays should override this with cache-backed pointer refs.
   */
  default List<TopologyGraph.NamespaceRef> listNamespaceRefs(ResourceId catalogId) {
    return listNamespaces(catalogId).stream()
        .map(
            ns ->
                new TopologyGraph.NamespaceRef(
                    ns.id(), ns.displayName(), ns.catalogId(), ns.pathSegments()))
        .toList();
  }

  /** Lists namespace refs whose rendered information_schema names match the supplied set. */
  default List<TopologyGraph.NamespaceRef> listNamespaceRefsByName(
      ResourceId catalogId, Set<String> names) {
    if (names == null || names.isEmpty()) {
      return List.of();
    }
    return listNamespaceRefs(catalogId).stream()
        .filter(ref -> names.contains(TopologyNames.namespaceName(ref.pathSegments(), ref.name())))
        .toList();
  }

  /**
   * Lists relation refs for callers that only need relation name/id/kind. Default derives refs from
   * full relation nodes; production overlays should override this with cache-backed pointer refs.
   */
  default List<TopologyGraph.RelationRef> listRelationRefs(
      ResourceId catalogId, ResourceId namespaceId) {
    return listRelationsInNamespace(catalogId, namespaceId).stream()
        .map(
            rel -> {
              ResourceKind kind =
                  rel.id().getKind() == ResourceKind.RK_VIEW
                      ? ResourceKind.RK_VIEW
                      : ResourceKind.RK_TABLE;
              return new TopologyGraph.RelationRef(rel.id(), rel.displayName(), kind);
            })
        .toList();
  }

  /** Lists relation refs whose names match the supplied set. */
  default List<TopologyGraph.RelationRef> listRelationRefsByName(
      ResourceId catalogId, ResourceId namespaceId, Set<String> names) {
    if (names == null || names.isEmpty()) {
      return List.of();
    }
    return listRelationRefs(catalogId, namespaceId).stream()
        .filter(ref -> names.contains(ref.name()))
        .toList();
  }

  List<FunctionNode> listFunctions(ResourceId catalogId, ResourceId namespaceId);

  List<TypeNode> listTypes(ResourceId catalogId);

  Optional<ResourceId> resolveCatalog(String correlationId, String name);

  Optional<ResourceId> resolveNamespace(String correlationId, NameRef ref);

  Optional<ResourceId> resolveTable(String correlationId, NameRef ref);

  Optional<ResourceId> resolveView(String correlationId, NameRef ref);

  Optional<ResourceId> resolveName(String correlationId, NameRef ref);

  /**
   * Batch kind-agnostic name resolution. The default loops {@link #resolveName}; overlays backed by
   * per-name storage reads should override so names sharing a catalog/namespace resolve their scope
   * once per batch instead of once per name.
   */
  default java.util.Map<NameRef, Optional<ResourceId>> resolveNames(
      String correlationId, List<NameRef> refs) {
    var out = new java.util.LinkedHashMap<NameRef, Optional<ResourceId>>(refs.size());
    for (NameRef ref : refs) {
      out.computeIfAbsent(ref, r -> resolveName(correlationId, r));
    }
    return out;
  }

  /**
   * Resolves a relation (table or view) by name reference using an explicit engine context.
   *
   * <p>Prefer this overload wherever the caller already holds the request's engine context — see
   * {@link #resolve(ResourceId, EngineContext)}.
   */
  default Optional<ResourceId> resolveName(
      String correlationId, NameRef ref, EngineContext engineContext) {
    return resolveName(correlationId, ref);
  }

  /** Resolves a system table name without involving the user graph. */
  Optional<ResourceId> resolveSystemTable(NameRef ref);

  /** Resolves a system table id back to name without involving the user graph. */
  Optional<NameRef> resolveSystemTableName(ResourceId id);

  /** Resolves a system type by namespace + type name without involving the user graph. */
  Optional<TypeNode> resolveSystemType(String namespace, String typeName);

  SnapshotPin snapshotPinFor(
      String correlationId,
      ResourceId tableId,
      SnapshotRef override,
      Optional<Timestamp> asOfDefault);

  ResolveResult batchResolveTables(
      String correlationId, List<NameRef> items, int limit, String token);

  ResolveResult listTablesByPrefix(String correlationId, NameRef prefix, int limit, String token);

  ResolveResult batchResolveViews(
      String correlationId, List<NameRef> items, int limit, String token);

  ResolveResult listViewsByPrefix(String correlationId, NameRef prefix, int limit, String token);

  Optional<NameRef> namespaceName(ResourceId id);

  Optional<NameRef> tableName(ResourceId id);

  Optional<NameRef> viewName(ResourceId id);

  Optional<CatalogNode> catalog(ResourceId id);

  SchemaResolution schemaFor(String correlationId, ResourceId tableId, SnapshotRef snapshot);

  List<SchemaColumn> tableSchema(ResourceId tableId);

  /**
   * Simplified result returned by the overlay whenever caller requests a paged list of tables or
   * views.
   */
  record ResolveResult(List<QualifiedRelation> relations, int totalSize, String nextToken) {}

  record QualifiedRelation(NameRef name, ResourceId resourceId) {}

  record SchemaResolution(UserTableNode table, String schemaJson) {}
}
