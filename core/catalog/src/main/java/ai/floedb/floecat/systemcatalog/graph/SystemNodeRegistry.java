package ai.floedb.floecat.systemcatalog.graph;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.AggregateNode;
import ai.floedb.floecat.metagraph.model.CastNode;
import ai.floedb.floecat.metagraph.model.CollationNode;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.OperatorNode;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.systemcatalog.def.SystemAggregateDef;
import ai.floedb.floecat.systemcatalog.def.SystemCastDef;
import ai.floedb.floecat.systemcatalog.def.SystemCollationDef;
import ai.floedb.floecat.systemcatalog.def.SystemFunctionDef;
import ai.floedb.floecat.systemcatalog.def.SystemNamespaceDef;
import ai.floedb.floecat.systemcatalog.def.SystemOperatorDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.def.SystemTypeDef;
import ai.floedb.floecat.systemcatalog.def.SystemViewDef;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificMatcher;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemDefinitionRegistry;
import ai.floedb.floecat.systemcatalog.registry.SystemEngineCatalog;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Materializes and caches engine-specific system catalog nodes.
 *
 * <p>This registry takes declarative {@link SystemCatalogData} from the {@link
 * SystemDefinitionRegistry}, filters it by engine kind and version, applies engine-specific rules,
 * and builds immutable {@link GraphNode} instances with stable {@code _system} {@link ResourceId}s.
 *
 * <p>Results are cached per {@code (engineKind, engineVersion)} pair to avoid repeated filtering
 * and node construction.
 *
 * <p>This registry is the authoritative source of system-level graph nodes (functions, types,
 * operators, casts, namespaces, tables, views) used by the catalog overlay.
 */
public class SystemNodeRegistry {

  private final SystemDefinitionRegistry definitionRegistry;

  /*
   * Immutable cache of materialized system nodes.
   * Entries are never evicted or mutated; a fresh registry instance
   * should be created for test isolation or controlled reloads.
   */
  private final ConcurrentMap<VersionKey, BuiltinNodes> cache = new ConcurrentHashMap<>();

  public SystemNodeRegistry(SystemDefinitionRegistry definitionRegistry) {
    this.definitionRegistry = Objects.requireNonNull(definitionRegistry);
  }

  private static final SystemCatalogData EMPTY_CATALOG = SystemCatalogData.empty();

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

  public List<String> engineKinds() {
    return definitionRegistry.engineKinds();
  }

  public BuiltinNodes nodesFor(String engineKind, String engineVersion) {
    String normalizedKind = engineKind == null ? "" : engineKind.toLowerCase(Locale.ROOT);
    String normalizedVersion = engineVersion == null ? "" : engineVersion;
    VersionKey key = new VersionKey(normalizedKind, normalizedVersion);
    return cache.computeIfAbsent(key, k -> buildNodes(k.engineKind(), k.engineVersion()));
  }

  private BuiltinNodes buildNodes(String engineKind, String engineVersion) {
    SystemEngineCatalog catalog = definitionRegistry.catalog(engineKind);
    long version = versionFromFingerprint(catalog.fingerprint());

    // --- Functions ---
    List<SystemFunctionDef> functionDefs =
        catalog.functions().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .map(def -> withFunctionRules(def, engineKind, engineVersion))
            .toList();
    List<FunctionNode> functionNodes =
        functionDefs.stream().map(def -> toFunctionNode(engineKind, version, def)).toList();

    // --- Operators ---
    List<SystemOperatorDef> operatorDefs =
        catalog.operators().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .map(def -> withOperatorRules(def, engineKind, engineVersion))
            .toList();
    List<OperatorNode> operatorNodes =
        operatorDefs.stream().map(def -> toOperatorNode(engineKind, version, def)).toList();

    // --- Types ---
    List<SystemTypeDef> typeDefs =
        catalog.types().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .map(def -> withTypeRules(def, engineKind, engineVersion))
            .toList();
    List<TypeNode> typeNodes =
        typeDefs.stream().map(def -> toTypeNode(engineKind, version, def)).toList();

    // --- Casts ---
    List<SystemCastDef> castDefs =
        catalog.casts().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .map(def -> withCastRules(def, engineKind, engineVersion))
            .toList();
    List<CastNode> castNodes =
        castDefs.stream().map(def -> toCastNode(engineKind, version, def)).toList();

    // --- Collations ---
    List<SystemCollationDef> collationDefs =
        catalog.collations().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .map(def -> withCollationRules(def, engineKind, engineVersion))
            .toList();
    List<CollationNode> collationNodes =
        collationDefs.stream().map(def -> toCollationNode(engineKind, version, def)).toList();

    // --- Aggregates ---
    List<SystemAggregateDef> aggregateDefs =
        catalog.aggregates().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .map(def -> withAggregateRules(def, engineKind, engineVersion))
            .toList();
    List<AggregateNode> aggregateNodes =
        aggregateDefs.stream().map(def -> toAggregateNode(engineKind, version, def)).toList();

    // --- Namespaces ---
    List<SystemNamespaceDef> namespaceDefs =
        catalog.namespaces().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .map(def -> withNamespaceRules(def, engineKind, engineVersion))
            .toList();

    // --- Tables ---
    List<SystemTableDef> tableDefs =
        catalog.tables().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .map(def -> withTableRules(def, engineKind, engineVersion))
            .toList();

    // --- Views ---
    List<SystemViewDef> viewDefs =
        catalog.views().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .map(def -> withViewRules(def, engineKind, engineVersion))
            .toList();

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
        new SystemCatalogData(
            functionDefs,
            operatorDefs,
            typeDefs,
            castDefs,
            collationDefs,
            aggregateDefs,
            namespaceDefs,
            tableDefs,
            viewDefs));
  }

  // =======================================================================
  // Rule applications
  // =======================================================================

  private SystemFunctionDef withFunctionRules(
      SystemFunctionDef def, String engineKind, String engineVersion) {

    List<EngineSpecificRule> matched =
        matchingRules(def.engineSpecific(), engineKind, engineVersion);

    return new SystemFunctionDef(
        def.name(),
        def.argumentTypes(),
        def.returnType(),
        def.isAggregate(),
        def.isWindow(),
        matched);
  }

  private SystemOperatorDef withOperatorRules(
      SystemOperatorDef def, String engineKind, String engineVersion) {

    List<EngineSpecificRule> matched =
        matchingRules(def.engineSpecific(), engineKind, engineVersion);

    return new SystemOperatorDef(
        def.name(),
        def.leftType(),
        def.rightType(),
        def.returnType(),
        def.isCommutative(),
        def.isAssociative(),
        matched);
  }

  private SystemTypeDef withTypeRules(SystemTypeDef def, String engineKind, String engineVersion) {

    List<EngineSpecificRule> matched =
        matchingRules(def.engineSpecific(), engineKind, engineVersion);

    return new SystemTypeDef(def.name(), def.category(), def.array(), def.elementType(), matched);
  }

  private SystemCastDef withCastRules(SystemCastDef def, String engineKind, String engineVersion) {

    List<EngineSpecificRule> matched =
        matchingRules(def.engineSpecific(), engineKind, engineVersion);

    return new SystemCastDef(def.name(), def.sourceType(), def.targetType(), def.method(), matched);
  }

  private SystemCollationDef withCollationRules(
      SystemCollationDef def, String engineKind, String engineVersion) {

    List<EngineSpecificRule> matched =
        matchingRules(def.engineSpecific(), engineKind, engineVersion);

    return new SystemCollationDef(def.name(), def.locale(), matched);
  }

  private SystemAggregateDef withAggregateRules(
      SystemAggregateDef def, String engineKind, String engineVersion) {

    List<EngineSpecificRule> matched =
        matchingRules(def.engineSpecific(), engineKind, engineVersion);

    return new SystemAggregateDef(
        def.name(), def.argumentTypes(), def.stateType(), def.returnType(), matched);
  }

  private SystemNamespaceDef withNamespaceRules(
      SystemNamespaceDef def, String engineKind, String engineVersion) {

    List<EngineSpecificRule> matched =
        matchingRules(def.engineSpecific(), engineKind, engineVersion);

    return new SystemNamespaceDef(def.name(), def.displayName(), matched);
  }

  private SystemTableDef withTableRules(
      SystemTableDef def, String engineKind, String engineVersion) {

    List<EngineSpecificRule> matched =
        matchingRules(def.engineSpecific(), engineKind, engineVersion);

    return new SystemTableDef(
        def.name(), def.displayName(), def.columns(), def.backendKind(), def.scannerId(), matched);
  }

  private SystemViewDef withViewRules(SystemViewDef def, String engineKind, String engineVersion) {

    List<EngineSpecificRule> matched =
        matchingRules(def.engineSpecific(), engineKind, engineVersion);

    return new SystemViewDef(
        def.name(), def.displayName(), def.sql(), def.dialect(), def.columns(), matched);
  }

  // =======================================================================
  // Node Builders
  // =======================================================================

  private FunctionNode toFunctionNode(String engineKind, long version, SystemFunctionDef def) {

    return new FunctionNode(
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

  private OperatorNode toOperatorNode(String engineKind, long version, SystemOperatorDef def) {

    return new OperatorNode(
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

  private TypeNode toTypeNode(String engineKind, long version, SystemTypeDef def) {
    return new TypeNode(
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

  private CastNode toCastNode(String engineKind, long version, SystemCastDef def) {
    return new CastNode(
        resourceId(engineKind, ResourceKind.RK_CAST, def.name()),
        version,
        Instant.EPOCH,
        engineKind,
        resourceId(engineKind, ResourceKind.RK_TYPE, def.sourceType()),
        resourceId(engineKind, ResourceKind.RK_TYPE, def.targetType()),
        def.method().wireValue(),
        Map.of());
  }

  private CollationNode toCollationNode(String engineKind, long version, SystemCollationDef def) {

    return new CollationNode(
        resourceId(engineKind, ResourceKind.RK_COLLATION, def.name()),
        version,
        Instant.EPOCH,
        engineKind,
        safeName(def.name()),
        def.locale(),
        Map.of());
  }

  private AggregateNode toAggregateNode(String engineKind, long version, SystemAggregateDef def) {

    return new AggregateNode(
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
        .setAccountId("_system")
        .setKind(kind)
        .setId(engineKind + ":" + safeName(name))
        .build();
  }

  /**
   * Builds a display-friendly (case-preserving) identifier for graph nodes and ResourceIds.
   *
   * <p>This deliberately keeps the original casing so `_system:floe-demo.pg_catalog.pg_fn` matches
   * what planners expect. For maps/overrides we use {@link
   * ai.floedb.floecat.systemcatalog.util.NameRefUtil#canonical}.
   */
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
      List<FunctionNode> functions,
      List<OperatorNode> operators,
      List<TypeNode> types,
      List<CastNode> casts,
      List<CollationNode> collations,
      List<AggregateNode> aggregates,
      SystemCatalogData catalogData) {

    public BuiltinNodes {
      functions = List.copyOf(functions);
      operators = List.copyOf(operators);
      types = List.copyOf(types);
      casts = List.copyOf(casts);
      collations = List.copyOf(collations);
      aggregates = List.copyOf(aggregates);
      catalogData = Objects.requireNonNull(catalogData);
    }

    public SystemCatalogData toCatalogData() {
      return catalogData;
    }
  }

  private record VersionKey(String engineKind, String engineVersion) {}
}
