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
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.scanner.utils.CatalogContext;
import com.google.protobuf.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Shared view over metadata and system objects that exposes the graph operations needed by
 * scanners, planners, and traversal helpers.
 *
 * <p>This interface unifies the MetadataGraph view and the builtin graph so callers can depend on a
 * single entry point and do not need to mix ad-hoc resolver code.
 *
 * <p>Operations that depend on catalog selection accept the selected catalog context explicitly.
 * Lightweight reference enumeration always uses that explicit context.
 */
public interface CatalogGraphView {

  /** Resolves any graph node for the given resource using the selected catalog context. */
  Optional<GraphNode> resolve(ResourceId id, CatalogContext catalogContext);

  /**
   * Lists every relation under the requested catalog (namespaces, tables, views, plus system
   * objects).
   */
  List<RelationNode> listRelations(ResourceId catalogId, CatalogContext catalogContext);

  /** Lists namespaces owned by the requested catalog. */
  List<NamespaceNode> listNamespaces(ResourceId catalogId, CatalogContext catalogContext);

  /** Lists relations that live inside the given namespace. */
  List<RelationNode> listRelationsInNamespace(
      ResourceId catalogId, ResourceId namespaceId, CatalogContext catalogContext);

  default List<RelationNode> listSystemRelationsInNamespace(
      ResourceId catalogId, ResourceId namespaceId, CatalogContext catalogContext) {
    return listRelationsInNamespace(catalogId, namespaceId, catalogContext).stream()
        .filter(n -> n.origin() == GraphNodeOrigin.SYSTEM)
        .toList();
  }

  /** Lists only system namespaces in a catalog. */
  default List<NamespaceNode> listSystemNamespaces(
      ResourceId catalogId, CatalogContext catalogContext) {
    return listNamespaces(catalogId, catalogContext).stream()
        .filter(n -> n.origin() == GraphNodeOrigin.SYSTEM)
        .toList();
  }

  /**
   * Whether this graph view can enumerate lightweight refs without materializing full graph nodes.
   *
   * <p>Default implementations below are correct but derive refs from full objects, so callers that
   * need a true no-hydration path should check this before relying on refs for performance.
   */
  default boolean supportsLightweightRefs() {
    return false;
  }

  /**
   * Lists namespace refs for callers that only need topology metadata. Implementations should use a
   * cache-backed pointer path when available; the default derives refs from full namespace nodes.
   */
  default List<NamespaceRef> listNamespaceRefs(
      ResourceId catalogId, CatalogContext catalogContext) {
    return listNamespaces(catalogId, catalogContext).stream()
        .map(ns -> new NamespaceRef(ns.id(), ns.displayName(), ns.catalogId(), ns.pathSegments()))
        .toList();
  }

  /** Lists namespace refs whose rendered information_schema names match the supplied set. */
  default List<NamespaceRef> listNamespaceRefsByName(
      ResourceId catalogId, Set<String> names, CatalogContext catalogContext) {
    if (names == null || names.isEmpty()) {
      return List.of();
    }
    return listNamespaceRefs(catalogId, catalogContext).stream()
        .filter(ref -> names.contains(TopologyNames.namespaceName(ref.pathSegments(), ref.name())))
        .toList();
  }

  /**
   * Lists relation refs for callers that only need relation name/id/kind. Implementations should
   * use a cache-backed pointer path when available; the default derives refs from full relation
   * nodes.
   */
  default List<RelationRef> listRelationRefs(
      ResourceId catalogId, ResourceId namespaceId, CatalogContext catalogContext) {
    return listRelationsInNamespace(catalogId, namespaceId, catalogContext).stream()
        .map(
            rel -> {
              ResourceKind kind =
                  rel.id().getKind() == ResourceKind.RK_VIEW
                      ? ResourceKind.RK_VIEW
                      : ResourceKind.RK_TABLE;
              return new RelationRef(rel.id(), rel.displayName(), kind);
            })
        .toList();
  }

  /** Lists matching relation refs using the selected catalog context. */
  default List<RelationRef> listRelationRefsByName(
      ResourceId catalogId,
      ResourceId namespaceId,
      Set<String> names,
      CatalogContext catalogContext) {
    if (names == null || names.isEmpty()) {
      return List.of();
    }
    return listRelationRefs(catalogId, namespaceId, catalogContext).stream()
        .filter(ref -> names.contains(ref.name()))
        .toList();
  }

  List<FunctionNode> listFunctions(
      ResourceId catalogId, ResourceId namespaceId, CatalogContext catalogContext);

  List<TypeNode> listTypes(ResourceId catalogId, CatalogContext catalogContext);

  Optional<ResourceId> resolveCatalog(
      String correlationId, String name, CatalogContext catalogContext);

  Optional<ResourceId> resolveNamespace(
      String correlationId, NameRef ref, CatalogContext catalogContext);

  Optional<ResourceId> resolveTable(
      String correlationId, NameRef ref, CatalogContext catalogContext);

  Optional<ResourceId> resolveView(
      String correlationId, NameRef ref, CatalogContext catalogContext);

  Optional<ResourceId> resolveName(
      String correlationId, NameRef ref, CatalogContext catalogContext);

  /**
   * Batch kind-agnostic name resolution. The default loops {@link #resolveName}; graph views backed
   * by per-name storage reads should override so names sharing a catalog/namespace resolve their
   * scope once per batch instead of once per name.
   */
  default java.util.Map<NameRef, Optional<ResourceId>> resolveNames(
      String correlationId, List<NameRef> refs, CatalogContext catalogContext) {
    var out = new java.util.LinkedHashMap<NameRef, Optional<ResourceId>>(refs.size());
    for (NameRef ref : refs) {
      out.computeIfAbsent(ref, r -> resolveName(correlationId, r, catalogContext));
    }
    return out;
  }

  /**
   * Whether independent resolution callbacks may run concurrently on this graph-view instance.
   *
   * <p>The default preserves compatibility for existing implementations whose lifecycle state may
   * be tied to one request thread. Implementations backed by thread-safe services may opt in to
   * concurrent resolution. Opting in permits {@link #catalog}, {@link #resolve}, {@code
   * resolveName(s)}, and {@link #tablePinFor} callbacks to execute concurrently and off the caller
   * thread, together with graph-view schema/name callbacks used while assembling GetUserObjects
   * relations ({@link #schemaFor}, {@link #tableSchema}, {@link #tableName(ResourceId,
   * CatalogContext)}, and {@link #viewName(ResourceId, CatalogContext)}). It does not change the
   * caller-thread contract of separately injected stats or engine-decoration collaborators.
   * Implementations opting in must make the listed graph-view callbacks thread-safe and must not
   * depend on custom caller-thread state that service context propagation does not capture.
   */
  default boolean supportsConcurrentResolution() {
    return false;
  }

  /** Resolves a system table name without involving the user graph. */
  Optional<ResourceId> resolveSystemTable(NameRef ref, CatalogContext catalogContext);

  /** Resolves a system table id back to name without involving the user graph. */
  Optional<NameRef> resolveSystemTableName(ResourceId id, CatalogContext catalogContext);

  /** Resolves a system type by namespace + type name without involving the user graph. */
  Optional<TypeNode> resolveSystemType(
      String namespace, String typeName, CatalogContext catalogContext);

  /**
   * Build the coherent {@link TablePin} the query context stores and downstream reads reuse. The
   * user graph resolves every pin kind through the table's immutable root; other graph views
   * resolve the snapshot directly. Pin kind follows the request intent (explicit snapshot / as-of /
   * current) so dedupe can rank pins for the same table.
   */
  TablePin tablePinFor(
      String correlationId,
      ResourceId tableId,
      SnapshotRef override,
      Optional<Timestamp> asOfDefault,
      CatalogContext catalogContext);

  ResolveResult batchResolveTables(
      String correlationId,
      List<NameRef> items,
      int limit,
      String token,
      CatalogContext catalogContext);

  ResolveResult listTablesByPrefix(
      String correlationId, NameRef prefix, int limit, String token, CatalogContext catalogContext);

  ResolveResult batchResolveViews(
      String correlationId,
      List<NameRef> items,
      int limit,
      String token,
      CatalogContext catalogContext);

  ResolveResult listViewsByPrefix(
      String correlationId, NameRef prefix, int limit, String token, CatalogContext catalogContext);

  Optional<NameRef> namespaceName(ResourceId id, CatalogContext catalogContext);

  Optional<NameRef> tableName(ResourceId id, CatalogContext catalogContext);

  Optional<NameRef> viewName(ResourceId id, CatalogContext catalogContext);

  Optional<CatalogNode> catalog(ResourceId id, CatalogContext catalogContext);

  /**
   * Resolve the schema for a pinned query. {@code tableBlobUri} names the pinned table blob and
   * {@code snapshotBlobUri} the pinned snapshot blob, so both the table metadata and the
   * snapshot-sourced schema are read from those immutable blobs rather than the live pointers. A
   * concurrent {@code ALTER} advances the current table pointer, and an in-place {@code
   * UpdateSnapshot} can repoint the {@code (table, snapshot id)} pointer to a new snapshot blob
   * after the pin was built; reading the pinned uris cannot drift to either. Empty uris read the
   * current pointers.
   */
  SchemaResolution schemaFor(
      String correlationId,
      ResourceId tableId,
      SnapshotRef snapshot,
      String tableBlobUri,
      String snapshotBlobUri);

  default SchemaResolution schemaFor(
      String correlationId, ResourceId tableId, SnapshotRef snapshot) {
    return schemaFor(correlationId, tableId, snapshot, "", "");
  }

  List<SchemaColumn> tableSchema(ResourceId tableId, CatalogContext catalogContext);

  /**
   * Simplified result returned by the graph view whenever a caller requests a paged list of tables
   * or views.
   */
  record ResolveResult(List<QualifiedRelation> relations, int totalSize, String nextToken) {}

  record QualifiedRelation(NameRef name, ResourceId resourceId) {}

  record SchemaResolution(UserTableNode table, String schemaJson) {}

  record NamespaceRef(ResourceId id, String name, ResourceId catalogId, List<String> pathSegments) {
    public NamespaceRef(ResourceId id, String name) {
      this(id, name, null, List.of());
    }

    public NamespaceRef {
      pathSegments = pathSegments == null ? List.of() : List.copyOf(pathSegments);
    }
  }

  record RelationRef(ResourceId id, String name, ResourceKind kind) {}
}
