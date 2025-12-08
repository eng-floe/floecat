package ai.floedb.floecat.catalog.system_objects.registry;

import ai.floedb.floecat.catalog.common.util.NameRefUtil;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.SystemObjectNode;
import java.time.Instant;
import java.util.Map;

/**
 * Factory responsible for constructing {@link SystemObjectNode} instances from {@link
 * SystemObjectDefinition}.
 *
 * <p>This centralizes all SystemObjectNode creation rules: - resourceId formatting - catalogId /
 * namespaceId resolution - displayName mapping - static version + timestamp
 */
public class SystemObjectNodeFactory {

  /**
   * Build a SystemObjectNode for the given definition + engine settings.
   *
   * @param def the definition matched from registry
   * @param engineKind executor/planner engine kind
   * @param engineVersion executor/planner engine version
   */
  public SystemObjectNode build(
      SystemObjectDefinition def, String engineKind, String engineVersion) {

    NameRef ref = def.name();

    // Compute catalog + namespace identity
    ResourceId catalogId = catalogIdFor(engineKind);
    ResourceId namespaceId = namespaceIdFor(engineKind, ref);

    // ResourceId for the system object itself
    ResourceId id = resourceIdFor(engineKind, ref);

    return new SystemObjectNode(
        id,
        0L, // version (static)
        Instant.EPOCH, // stable timestamp
        catalogId,
        namespaceId,
        ref.getName(), // displayName
        def.columns().columns(), // SchemaColumn[]
        def.scannerId(),
        Map.of() // engineHints (none yet)
        );
  }

  // =====================================================================
  // Resource / Catalog / Namespace Helpers
  // =====================================================================

  private ResourceId resourceIdFor(String engineKind, NameRef ref) {
    return ResourceId.newBuilder()
        .setAccountId("_sysobjects")
        .setKind(ResourceKind.RK_SYSTEM_OBJECT)
        .setId(engineKind + ":" + NameRefUtil.canonical(ref))
        .build();
  }

  private ResourceId catalogIdFor(String engineKind) {
    return ResourceId.newBuilder()
        .setAccountId("_sysobjects")
        .setKind(ResourceKind.RK_CATALOG)
        .setId(engineKind)
        .build();
  }

  private ResourceId namespaceIdFor(String engineKind, NameRef ref) {
    return ResourceId.newBuilder()
        .setAccountId("_sysobjects")
        .setKind(ResourceKind.RK_NAMESPACE)
        .setId(engineKind + ":" + String.join(".", ref.getPathList()))
        .build();
  }
}
