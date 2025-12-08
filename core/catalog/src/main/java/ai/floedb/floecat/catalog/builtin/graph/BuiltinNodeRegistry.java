package ai.floedb.floecat.catalog.builtin.graph;

import ai.floedb.floecat.catalog.builtin.def.BuiltinAggregateDef;
import ai.floedb.floecat.catalog.builtin.def.BuiltinCastDef;
import ai.floedb.floecat.catalog.builtin.def.BuiltinCollationDef;
import ai.floedb.floecat.catalog.builtin.def.BuiltinFunctionDef;
import ai.floedb.floecat.catalog.builtin.def.BuiltinOperatorDef;
import ai.floedb.floecat.catalog.builtin.def.BuiltinTypeDef;
import ai.floedb.floecat.catalog.builtin.graph.model.*;
import ai.floedb.floecat.catalog.builtin.registry.BuiltinCatalogData;
import ai.floedb.floecat.catalog.builtin.registry.BuiltinDefinitionRegistry;
import ai.floedb.floecat.catalog.builtin.registry.BuiltinEngineCatalog;
import ai.floedb.floecat.catalog.common.engine.EngineSpecificMatcher;
import ai.floedb.floecat.catalog.common.engine.EngineSpecificRule;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class BuiltinNodeRegistry {

  private final BuiltinDefinitionRegistry definitionRegistry;
  private final ConcurrentMap<VersionKey, BuiltinNodes> cache = new ConcurrentHashMap<>();

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
        functionDefs.stream().map(def -> toFunctionNode(engineKind, version, def)).toList();

    // --- Operators ---
    List<BuiltinOperatorDef> operatorDefs =
        catalog.operators().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .map(def -> withOperatorRules(def, engineKind, engineVersion))
            .toList();
    List<BuiltinOperatorNode> operatorNodes =
        operatorDefs.stream().map(def -> toOperatorNode(engineKind, version, def)).toList();

    // --- Types ---
    List<BuiltinTypeDef> typeDefs =
        catalog.types().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .map(def -> withTypeRules(def, engineKind, engineVersion))
            .toList();
    List<BuiltinTypeNode> typeNodes =
        typeDefs.stream().map(def -> toTypeNode(engineKind, version, def)).toList();

    // --- Casts ---
    List<BuiltinCastDef> castDefs =
        catalog.casts().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .map(def -> withCastRules(def, engineKind, engineVersion))
            .toList();
    List<BuiltinCastNode> castNodes =
        castDefs.stream().map(def -> toCastNode(engineKind, version, def)).toList();

    // --- Collations ---
    List<BuiltinCollationDef> collationDefs =
        catalog.collations().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .map(def -> withCollationRules(def, engineKind, engineVersion))
            .toList();
    List<BuiltinCollationNode> collationNodes =
        collationDefs.stream().map(def -> toCollationNode(engineKind, version, def)).toList();

    // --- Aggregates ---
    List<BuiltinAggregateDef> aggregateDefs =
        catalog.aggregates().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .map(def -> withAggregateRules(def, engineKind, engineVersion))
            .toList();
    List<BuiltinAggregateNode> aggregateNodes =
        aggregateDefs.stream().map(def -> toAggregateNode(engineKind, version, def)).toList();

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

    List<EngineSpecificRule> matched =
        matchingRules(def.engineSpecific(), engineKind, engineVersion);

    return new BuiltinCastDef(
        def.name(), def.sourceType(), def.targetType(), def.method(), matched);
  }

  private BuiltinCollationDef withCollationRules(
      BuiltinCollationDef def, String engineKind, String engineVersion) {

    List<EngineSpecificRule> matched =
        matchingRules(def.engineSpecific(), engineKind, engineVersion);

    return new BuiltinCollationDef(def.name(), def.locale(), matched);
  }

  private BuiltinAggregateDef withAggregateRules(
      BuiltinAggregateDef def, String engineKind, String engineVersion) {

    List<EngineSpecificRule> matched =
        matchingRules(def.engineSpecific(), engineKind, engineVersion);

    return new BuiltinAggregateDef(
        def.name(), def.argumentTypes(), def.stateType(), def.returnType(), matched);
  }

  // =======================================================================
  // Node Builders
  // =======================================================================

  private BuiltinFunctionNode toFunctionNode(
      String engineKind, long version, BuiltinFunctionDef def) {

    return new BuiltinFunctionNode(
        resourceId(engineKind, ResourceKind.RK_FUNCTION, def.name()),
        version,
        Instant.EPOCH,
        engineKind,
        safeName(def.name()),
        def.argumentTypes().stream()
            .map(arg -> resourceId(engineKind, ResourceKind.RK_TYPE, arg))
            .toList(),
        resourceId(engineKind, ResourceKind.RK_TYPE, def.returnType()),
        def.isAggregate(),
        def.isWindow(),
        Map.of());
  }

  private BuiltinOperatorNode toOperatorNode(
      String engineKind, long version, BuiltinOperatorDef def) {

    return new BuiltinOperatorNode(
        resourceId(engineKind, ResourceKind.RK_OPERATOR, def.name()),
        version,
        Instant.EPOCH,
        engineKind,
        safeName(def.name()),
        resourceId(engineKind, ResourceKind.RK_TYPE, def.leftType()),
        resourceId(engineKind, ResourceKind.RK_TYPE, def.rightType()),
        resourceId(engineKind, ResourceKind.RK_TYPE, def.returnType()),
        def.isCommutative(),
        def.isAssociative(),
        Map.of());
  }

  private BuiltinTypeNode toTypeNode(String engineKind, long version, BuiltinTypeDef def) {
    return new BuiltinTypeNode(
        resourceId(engineKind, ResourceKind.RK_TYPE, def.name()),
        version,
        Instant.EPOCH,
        engineKind,
        safeName(def.name()),
        def.category(),
        def.array(),
        def.elementType() == null
            ? null
            : resourceId(engineKind, ResourceKind.RK_TYPE, def.elementType()),
        Map.of());
  }

  private BuiltinCastNode toCastNode(String engineKind, long version, BuiltinCastDef def) {
    return new BuiltinCastNode(
        resourceId(engineKind, ResourceKind.RK_CAST, def.name()),
        version,
        Instant.EPOCH,
        engineKind,
        resourceId(engineKind, ResourceKind.RK_TYPE, def.sourceType()),
        resourceId(engineKind, ResourceKind.RK_TYPE, def.targetType()),
        def.method().wireValue(),
        Map.of());
  }

  private BuiltinCollationNode toCollationNode(
      String engineKind, long version, BuiltinCollationDef def) {

    return new BuiltinCollationNode(
        resourceId(engineKind, ResourceKind.RK_COLLATION, def.name()),
        version,
        Instant.EPOCH,
        engineKind,
        safeName(def.name()),
        def.locale(),
        Map.of());
  }

  private BuiltinAggregateNode toAggregateNode(
      String engineKind, long version, BuiltinAggregateDef def) {

    return new BuiltinAggregateNode(
        resourceId(engineKind, ResourceKind.RK_AGGREGATE, def.name()),
        version,
        Instant.EPOCH,
        engineKind,
        safeName(def.name()),
        def.argumentTypes().stream()
            .map(arg -> resourceId(engineKind, ResourceKind.RK_TYPE, arg))
            .toList(),
        resourceId(engineKind, ResourceKind.RK_TYPE, def.stateType()),
        resourceId(engineKind, ResourceKind.RK_TYPE, def.returnType()),
        Map.of());
  }

  // =======================================================================
  // Helpers
  // =======================================================================

  public static ResourceId resourceId(String engineKind, ResourceKind kind, NameRef name) {
    return ResourceId.newBuilder()
        .setAccountId("_builtin")
        .setKind(kind)
        .setId(engineKind + ":" + safeName(name))
        .build();
  }

  public static String safeName(NameRef ref) {
    if (ref == null) return "";
    String path = String.join(".", ref.getPathList());
    return path.isEmpty() ? ref.getName() : path + "." + ref.getName();
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
