package ai.floedb.floecat.service.metagraph.traversal;

import ai.floedb.floecat.catalog.systemobjects.registry.SystemObjectDefinition;
import ai.floedb.floecat.catalog.systemobjects.registry.SystemObjectResolver;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.service.metagraph.loader.NodeLoader;
import ai.floedb.floecat.service.metagraph.resolver.NameResolver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * ============================================================================ RelationLister
 * ============================================================================
 *
 * <p>Graph-level traversal: catalog → namespaces → tables / views → system objects
 *
 * <p>MUST remain free of: ✓ caching ✓ snapshot logic ✓ repository access ✓ system-object building
 * logic (delegated to SystemObjectResolver)
 *
 * <p>The only inputs to this class are: - NameResolver for ID enumeration - NodeLoader for
 * materialization - SystemObjectResolver for sysobject nodes
 */
@ApplicationScoped
public final class RelationLister {

  private final NameResolver names;
  private final NodeLoader nodes;
  private final SystemObjectResolver sysObjects;

  @Inject
  public RelationLister(NameResolver names, NodeLoader nodes, SystemObjectResolver sysObjects) {
    this.names = names;
    this.nodes = nodes;
    this.sysObjects = sysObjects;
  }

  /**
   * Enumerates all relations under a *catalog*: - namespaces (immediate children) - tables - views
   * - system objects
   *
   * <p>This is the canonical "graph walk" for the catalog root.
   */
  public List<GraphNode> listRelations(
      ResourceId catalogId, String engineKind, String engineVersion) {

    List<GraphNode> out = new ArrayList<>(128);

    final String accountId = catalogId.getAccountId();
    final String cat = catalogId.getId();

    // ------------------------------------------------------------------
    // 1. Namespaces (full nodes)
    // ------------------------------------------------------------------
    var nsIds = names.listNamespaces(accountId, cat);
    for (ResourceId nsId : nsIds) {
      nodes.namespace(nsId).ifPresent(out::add);
    }

    // ------------------------------------------------------------------
    // 2. Tables (all tables across all namespaces)
    // ------------------------------------------------------------------
    var tblIds = names.listTableIds(accountId, cat);
    for (ResourceId tblId : tblIds) {
      nodes.table(tblId).ifPresent(out::add);
    }

    // ------------------------------------------------------------------
    // 3. Views
    // ------------------------------------------------------------------
    var viewIds = names.listViewIds(accountId, cat);
    for (ResourceId viewId : viewIds) {
      nodes.view(viewId).ifPresent(out::add);
    }

    // ------------------------------------------------------------------
    // 4. System Objects (not tied to namespaces)
    // ------------------------------------------------------------------
    for (SystemObjectDefinition def : sysObjects.definitionsFor(engineKind, engineVersion)) {
      sysObjects.buildNode(def, engineKind, engineVersion).ifPresent(out::add);
    }

    return out;
  }

  /**
   * Enumerates relations *inside a namespace*: - tables - views
   *
   * <p>Does NOT return: – system objects – nested namespaces
   */
  public List<GraphNode> listRelationsInNamespace(ResourceId catalogId, ResourceId namespaceId) {

    List<GraphNode> out = new ArrayList<>(32);

    final String accountId = catalogId.getAccountId();
    final String cat = catalogId.getId();
    final String ns = namespaceId.getId();

    // Tables
    var tblIds = names.listTableIdsInNamespace(accountId, cat, ns);
    for (ResourceId tblId : tblIds) {
      nodes.table(tblId).ifPresent(out::add);
    }

    // Views
    var viewIds = names.listViewIdsInNamespace(accountId, cat, ns);
    for (ResourceId viewId : viewIds) {
      nodes.view(viewId).ifPresent(out::add);
    }

    return out;
  }
}
