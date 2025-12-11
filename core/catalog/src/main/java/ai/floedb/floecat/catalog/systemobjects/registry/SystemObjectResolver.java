package ai.floedb.floecat.catalog.systemobjects.registry;

import ai.floedb.floecat.catalog.common.util.NameRefUtil;
import ai.floedb.floecat.catalog.systemobjects.spi.SystemObjectProvider;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.GraphNode;
import java.util.*;

/**
 * ============================================================================ SystemObjectResolver
 * (Service Layer) ============================================================================
 *
 * <p>Central registry for: - system-object definitions (per engineKind/engineVersion) - resolving
 * from NameRef → Definition - resolving from ResourceId → Definition - building GraphNode instances
 * - providing a stable ResourceId for system objects
 *
 * <p>This class never loads data itself; it only maps definitions to scanner factories and builds
 * GraphNodes.
 */
public final class SystemObjectResolver {

  private final Map<String, Map<String, List<SystemObjectDefinition>>> byEngine = new HashMap<>();
  private final SystemObjectNodeFactory factory = new SystemObjectNodeFactory();

  /**
   * Registers system object providers.
   *
   * <p>Called once during engine bootstrap.
   */
  public void register(String engineKind, String engineVersion, SystemObjectProvider provider) {
    byEngine
        .computeIfAbsent(engineKind.toLowerCase(), k -> new HashMap<>())
        .put(engineVersion, provider.definitions());
  }

  /** Returns all definitions for the given engine settings. */
  public List<SystemObjectDefinition> definitionsFor(String kind, String version) {
    return byEngine.getOrDefault(kind.toLowerCase(), Map.of()).getOrDefault(version, List.of());
  }

  /** Resolves a system-object definition from a NameRef. */
  public Optional<SystemObjectDefinition> resolveDefinition(
      NameRef ref, String engineKind, String engineVersion) {

    String canonical = NameRefUtil.canonical(ref);

    for (SystemObjectDefinition d : definitionsFor(engineKind, engineVersion)) {
      if (d.canonicalName().equalsIgnoreCase(canonical)) {
        return Optional.of(d);
      }
    }
    return Optional.empty();
  }

  /** Resolves from resource ID → definition. */
  public Optional<SystemObjectDefinition> resolve(
      ResourceId id, String engineKind, String engineVersion) {

    if (id.getKind() != ResourceKind.RK_SYSTEM_OBJECT) return Optional.empty();

    return definitionsFor(engineKind, engineVersion).stream()
        .filter(d -> toResourceId(d, engineKind).getId().equals(id.getId()))
        .findFirst();
  }

  /** Produces the synthetic ResourceId for a given definition. */
  public ResourceId toResourceId(SystemObjectDefinition def, String engineKind) {
    return SystemObjectRegistry.resourceId(engineKind, ResourceKind.RK_SYSTEM_OBJECT, def.name());
  }

  /** Builds the GraphNode instance for a definition, if scanner exists. */
  public Optional<GraphNode> buildNode(
      SystemObjectDefinition def, String engineKind, String engineVersion) {

    return Optional.of(factory.build(def, engineKind, engineVersion));
  }
}
