package ai.floedb.metacat.service.query.graph.builtin;

import ai.floedb.metacat.catalog.builtin.*;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.query.graph.model.*;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@ApplicationScoped
public class BuiltinNodeRegistry {

  private final BuiltinDefinitionRegistry definitionRegistry;
  private final ConcurrentMap<VersionKey, BuiltinNodes> cache = new ConcurrentHashMap<>();

  @Inject
  public BuiltinNodeRegistry(BuiltinDefinitionRegistry definitionRegistry) {
    this.definitionRegistry = Objects.requireNonNull(definitionRegistry);
  }

  private static final BuiltinCatalogData EMPTY_CATALOG =
      new BuiltinCatalogData(List.of(), List.of(), List.of(), List.of(), List.of(), List.of());

  private static final BuiltinNodes EMPTY_NODES =
      new BuiltinNodes(
          "",
          "",
          "",
          List.of(),
          List.of(),
          List.of(),
          List.of(),
          List.of(),
          List.of(),
          EMPTY_CATALOG);

  public BuiltinNodes nodesFor(String engineKind, String engineVersion) {
    if (engineKind == null || engineKind.isBlank()) return EMPTY_NODES;
    if (engineVersion == null || engineVersion.isBlank()) return EMPTY_NODES;
    VersionKey key = new VersionKey(engineKind, engineVersion);
    return cache.computeIfAbsent(key, k -> buildNodes(k.engineKind(), k.engineVersion()));
  }

  private BuiltinNodes buildNodes(String engineKind, String engineVersion) {
    BuiltinEngineCatalog catalog = definitionRegistry.catalog(engineKind);
    long version = versionFromFingerprint(catalog.fingerprint());

    // --- Functions ---
    List<BuiltinFunctionDef> functionDefs =
        catalog.functions().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .map(def -> withFunctionRules(def, engineKind, engineVersion))
            .toList();
    List<BuiltinFunctionNode> functionNodes =
        functionDefs.stream().map(def -> toFunctionNode(engineVersion, version, def)).toList();

    // --- Operators ---
    List<BuiltinOperatorDef> operatorDefs =
        catalog.operators().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .map(def -> withOperatorRules(def, engineKind, engineVersion))
            .toList();
    List<BuiltinOperatorNode> operatorNodes =
        operatorDefs.stream().map(def -> toOperatorNode(engineVersion, version, def)).toList();

    // --- Types ---
    List<BuiltinTypeDef> typeDefs =
        catalog.types().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .map(def -> withTypeRules(def, engineKind, engineVersion))
            .toList();
    List<BuiltinTypeNode> typeNodes =
        typeDefs.stream().map(def -> toTypeNode(engineVersion, version, def)).toList();

    // --- Casts ---
    List<BuiltinCastDef> castDefs =
        catalog.casts().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .map(def -> withCastRules(def, engineKind, engineVersion))
            .toList();
    List<BuiltinCastNode> castNodes =
        castDefs.stream().map(def -> toCastNode(engineVersion, version, def)).toList();

    // --- Collations ---
    List<BuiltinCollationDef> collationDefs =
        catalog.collations().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .map(def -> withCollationRules(def, engineKind, engineVersion))
            .toList();
    List<BuiltinCollationNode> collationNodes =
        collationDefs.stream().map(def -> toCollationNode(engineVersion, version, def)).toList();

    // --- Aggregates ---
    List<BuiltinAggregateDef> aggregateDefs =
        catalog.aggregates().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .map(def -> withAggregateRules(def, engineKind, engineVersion))
            .toList();
    List<BuiltinAggregateNode> aggregateNodes =
        aggregateDefs.stream().map(def -> toAggregateNode(engineVersion, version, def)).toList();

    return new BuiltinNodes(
        engineKind,
        engineVersion,
        catalog.fingerprint(),
        functionNodes,
        operatorNodes,
        typeNodes,
        castNodes,
        collationNodes,
        aggregateNodes,
        new BuiltinCatalogData(
            functionDefs, operatorDefs, typeDefs, castDefs, collationDefs, aggregateDefs));
  }

  // =======================================================================
  // Node Builders (ALL UPDATED!)
  // =======================================================================

  private BuiltinFunctionNode toFunctionNode(
      String engineVersion, long version, BuiltinFunctionDef def) {
    return new BuiltinFunctionNode(
        resourceId(engineVersion, ResourceKind.RK_FUNCTION_VALUE, def.name()),
        version,
        Instant.EPOCH,
        engineVersion,
        def.name(),
        def.argumentTypes(),
        def.returnType(),
        def.isAggregate(),
        def.isWindow(),
        Map.of());
  }

  private BuiltinOperatorNode toOperatorNode(
      String engineVersion, long version, BuiltinOperatorDef def) {
    return new BuiltinOperatorNode(
        resourceId(engineVersion, ResourceKind.RK_OPERATOR_VALUE, def.name()),
        version,
        Instant.EPOCH,
        engineVersion,
        def.name(),
        def.leftType(),
        def.rightType(),
        def.returnType(),
        def.isCommutative(),
        def.isAssociative(),
        Map.of());
  }

  private BuiltinTypeNode toTypeNode(String engineVersion, long version, BuiltinTypeDef def) {
    return new BuiltinTypeNode(
        resourceId(engineVersion, ResourceKind.RK_TYPE_VALUE, def.name()),
        version,
        Instant.EPOCH,
        engineVersion,
        def.name(),
        def.category(),
        def.array(),
        def.elementType(),
        Map.of());
  }

  private BuiltinCastNode toCastNode(String engineVersion, long version, BuiltinCastDef def) {
    return new BuiltinCastNode(
        resourceId(
            engineVersion, ResourceKind.RK_CAST_VALUE, def.sourceType() + "->" + def.targetType()),
        version,
        Instant.EPOCH,
        engineVersion,
        def.sourceType(),
        def.targetType(),
        def.method().wireValue(),
        Map.of());
  }

  private BuiltinCollationNode toCollationNode(
      String engineVersion, long version, BuiltinCollationDef def) {
    return new BuiltinCollationNode(
        resourceId(engineVersion, ResourceKind.RK_COLLATION_VALUE, def.name()),
        version,
        Instant.EPOCH,
        engineVersion,
        def.name(),
        def.locale(),
        Map.of());
  }

  private BuiltinAggregateNode toAggregateNode(
      String engineVersion, long version, BuiltinAggregateDef def) {
    return new BuiltinAggregateNode(
        resourceId(engineVersion, ResourceKind.RK_AGGREGATE_VALUE, def.name()),
        version,
        Instant.EPOCH,
        engineVersion,
        def.name(),
        def.argumentTypes(),
        def.stateType(),
        def.returnType(),
        Map.of());
  }

  // =======================================================================
  // Rule applications
  // =======================================================================

  private BuiltinFunctionDef withFunctionRules(
      BuiltinFunctionDef def, String engineKind, String engineVersion) {
    List<EngineSpecificRule> matched =
        matchingRules(def.engineSpecific(), engineKind, engineVersion);
    return new BuiltinFunctionDef(
        def.name(),
        def.argumentTypes(),
        def.returnType(),
        def.isAggregate(),
        def.isWindow(),
        matched);
  }

  private BuiltinOperatorDef withOperatorRules(
      BuiltinOperatorDef def, String engineKind, String engineVersion) {
    List<EngineSpecificRule> matched =
        matchingRules(def.engineSpecific(), engineKind, engineVersion);
    return new BuiltinOperatorDef(
        def.name(),
        def.leftType(),
        def.rightType(),
        def.returnType(),
        def.isCommutative(),
        def.isAssociative(),
        matched);
  }

  private BuiltinTypeDef withTypeRules(
      BuiltinTypeDef def, String engineKind, String engineVersion) {
    List<EngineSpecificRule> matched =
        matchingRules(def.engineSpecific(), engineKind, engineVersion);
    return new BuiltinTypeDef(def.name(), def.category(), def.array(), def.elementType(), matched);
  }

  private BuiltinCastDef withCastRules(
      BuiltinCastDef def, String engineKind, String engineVersion) {
    return new BuiltinCastDef(
        def.sourceType(),
        def.targetType(),
        def.method(),
        matchingRules(def.engineSpecific(), engineKind, engineVersion));
  }

  private BuiltinCollationDef withCollationRules(
      BuiltinCollationDef def, String engineKind, String engineVersion) {
    return new BuiltinCollationDef(
        def.name(), def.locale(), matchingRules(def.engineSpecific(), engineKind, engineVersion));
  }

  private BuiltinAggregateDef withAggregateRules(
      BuiltinAggregateDef def, String engineKind, String engineVersion) {
    return new BuiltinAggregateDef(
        def.name(),
        def.argumentTypes(),
        def.stateType(),
        def.returnType(),
        matchingRules(def.engineSpecific(), engineKind, engineVersion));
  }

  // =======================================================================

  private ResourceId resourceId(String engineVersion, int kindValue, String name) {
    return ResourceId.newBuilder()
        .setTenantId("_builtin")
        .setKindValue(kindValue)
        .setId(engineVersion + ":" + name)
        .build();
  }

  private static List<EngineSpecificRule> matchingRules(
      List<EngineSpecificRule> rules, String engineKind, String engineVersion) {
    if (rules == null || rules.isEmpty()) return List.of();
    return EngineSpecificMatcher.matchedRules(rules, engineKind, engineVersion);
  }

  private static boolean matches(
      List<EngineSpecificRule> rules, String engineKind, String engineVersion) {
    return EngineSpecificMatcher.matches(rules, engineKind, engineVersion);
  }

  private static long versionFromFingerprint(String fingerprint) {
    if (fingerprint == null || fingerprint.isBlank()) return 0L;
    String prefix = fingerprint.length() >= 16 ? fingerprint.substring(0, 16) : fingerprint;
    try {
      return Long.parseUnsignedLong(prefix, 16);
    } catch (NumberFormatException e) {
      return prefix.hashCode();
    }
  }

  public record BuiltinNodes(
      String engineKind,
      String engineVersion,
      String fingerprint,
      List<BuiltinFunctionNode> functions,
      List<BuiltinOperatorNode> operators,
      List<BuiltinTypeNode> types,
      List<BuiltinCastNode> casts,
      List<BuiltinCollationNode> collations,
      List<BuiltinAggregateNode> aggregates,
      BuiltinCatalogData catalogData) {

    public BuiltinNodes {
      functions = List.copyOf(functions);
      operators = List.copyOf(operators);
      types = List.copyOf(types);
      casts = List.copyOf(casts);
      collations = List.copyOf(collations);
      aggregates = List.copyOf(aggregates);
      catalogData = Objects.requireNonNull(catalogData);
    }

    public BuiltinCatalogData toCatalogData() {
      return catalogData;
    }
  }

  private record VersionKey(String engineKind, String engineVersion) {}
}
