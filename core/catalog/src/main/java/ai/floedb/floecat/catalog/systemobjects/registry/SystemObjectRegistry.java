package ai.floedb.floecat.catalog.systemobjects.registry;

import ai.floedb.floecat.catalog.common.engine.EngineSpecificMatcher;
import ai.floedb.floecat.catalog.common.util.NameRefUtil;
import ai.floedb.floecat.catalog.systemobjects.spi.SystemObjectProvider;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Merges system-object definitions from all discovered providers.
 *
 * <p>Exactly parallels BuiltinDefinitionRegistry: - multiple providers (builtin + plugin) -
 * filtered by EngineSpecific rules - last-provider-wins override model
 */
public class SystemObjectRegistry {

  private final List<SystemObjectProvider> providers;

  public SystemObjectRegistry(List<SystemObjectProvider> providers) {
    this.providers = List.copyOf(providers);
  }

  public Optional<SystemObjectDefinition> resolveDefinition(
      NameRef name, String engineKind, String version) {

    String canonical = NameRefUtil.canonical(name);

    return providers.stream()
        .flatMap(provider -> filteredDefinitions(provider, engineKind, version))
        .filter(def -> def.canonicalName().equalsIgnoreCase(canonical))
        .reduce((a, b) -> b); // last-wins
  }

  public Map<String, SystemObjectDefinition> definitionsFor(String engineKind, String version) {

    return providers.stream()
        .flatMap(provider -> filteredDefinitions(provider, engineKind, version))
        .collect(
            Collectors.toMap(
                SystemObjectDefinition::canonicalName, def -> def, (a, b) -> b // last provider wins
                ));
  }

  /** INTERNAL unified filter: provider-level + engine-specific filtering */
  private Stream<SystemObjectDefinition> filteredDefinitions(
      SystemObjectProvider provider, String engineKind, String version) {

    // Check provider-level support
    if (!provider.supportsEngine(engineKind, version)) {
      return Stream.empty();
    }

    return provider.definitions().stream()
        .filter(def -> provider.supports(def.name(), engineKind, version))
        .filter(
            def -> EngineSpecificMatcher.matches(def.engineSpecificRules(), engineKind, version));
  }

  /** Helper to create a ResourceId for a system object */
  public static ResourceId resourceId(String engineKind, ResourceKind kind, NameRef name) {
    return ResourceId.newBuilder()
        .setAccountId("_sysobjects")
        .setKind(kind)
        .setId(engineKind + ":" + NameRefUtil.canonical(name))
        .build();
  }
}
