package ai.floedb.metacat.service.query.graph.builtin;

import ai.floedb.metacat.catalog.builtin.BuiltinAggregateDef;
import ai.floedb.metacat.catalog.builtin.BuiltinCastDef;
import ai.floedb.metacat.catalog.builtin.BuiltinCatalogData;
import ai.floedb.metacat.catalog.builtin.BuiltinCollationDef;
import ai.floedb.metacat.catalog.builtin.BuiltinDefinitionRegistry;
import ai.floedb.metacat.catalog.builtin.BuiltinEngineCatalog;
import ai.floedb.metacat.catalog.builtin.BuiltinFunctionDef;
import ai.floedb.metacat.catalog.builtin.BuiltinOperatorDef;
import ai.floedb.metacat.catalog.builtin.BuiltinTypeDef;
import ai.floedb.metacat.catalog.builtin.EngineSpecificRule;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.query.graph.model.BuiltinAggregateNode;
import ai.floedb.metacat.service.query.graph.model.BuiltinCastNode;
import ai.floedb.metacat.service.query.graph.model.BuiltinCollationNode;
import ai.floedb.metacat.service.query.graph.model.BuiltinFunctionNode;
import ai.floedb.metacat.service.query.graph.model.BuiltinOperatorNode;
import ai.floedb.metacat.service.query.graph.model.BuiltinTypeNode;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Materializes builtin relation nodes from the parsed builtin catalog definitions. */
@ApplicationScoped
public class BuiltinNodeRegistry {

  private final BuiltinDefinitionRegistry definitionRegistry;
  private final ConcurrentMap<VersionKey, BuiltinNodes> cache = new ConcurrentHashMap<>();

  @Inject
  public BuiltinNodeRegistry(BuiltinDefinitionRegistry definitionRegistry) {
    this.definitionRegistry = Objects.requireNonNull(definitionRegistry, "definitionRegistry");
  }

  private static final BuiltinCatalogData EMPTY_CATALOG =
      new BuiltinCatalogData("", List.of(), List.of(), List.of(), List.of(), List.of(), List.of());

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

  /** Returns the cached builtin nodes for the provided engine version. */
  public BuiltinNodes nodesFor(String engineKind, String engineVersion) {
    if (engineVersion == null || engineVersion.isBlank()) {
      return EMPTY_NODES;
    }
    if (engineKind == null || engineKind.isBlank()) {
      return EMPTY_NODES;
    }
    VersionKey key = new VersionKey(engineKind, engineVersion);
    return cache.computeIfAbsent(key, k -> buildNodes(k.engineKind(), k.engineVersion()));
  }

  private BuiltinNodes buildNodes(String engineKind, String engineVersion) {
    BuiltinEngineCatalog catalog = definitionRegistry.catalog(engineVersion);
    long version = versionFromFingerprint(catalog.fingerprint());
    var functionDefs =
        catalog.functions().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .toList();
    var functions =
        functionDefs.stream().map(def -> toFunctionNode(engineVersion, version, def)).toList();
    var operatorDefs =
        catalog.operators().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .toList();
    var operators =
        operatorDefs.stream().map(def -> toOperatorNode(engineVersion, version, def)).toList();
    var typeDefs =
        catalog.types().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .toList();
    var types = typeDefs.stream().map(def -> toTypeNode(engineVersion, version, def)).toList();
    var castDefs =
        catalog.casts().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .toList();
    var casts = castDefs.stream().map(def -> toCastNode(engineVersion, version, def)).toList();
    var collationDefs =
        catalog.collations().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .toList();
    var collations =
        collationDefs.stream().map(def -> toCollationNode(engineVersion, version, def)).toList();
    var aggregateDefs =
        catalog.aggregates().stream()
            .filter(def -> matches(def.engineSpecific(), engineKind, engineVersion))
            .toList();
    var aggregates =
        aggregateDefs.stream().map(def -> toAggregateNode(engineVersion, version, def)).toList();

    var catalogData =
        new BuiltinCatalogData(
            engineVersion,
            functionDefs,
            operatorDefs,
            typeDefs,
            castDefs,
            collationDefs,
            aggregateDefs);
    return new BuiltinNodes(
        engineKind,
        engineVersion,
        catalog.fingerprint(),
        functions,
        operators,
        types,
        casts,
        collations,
        aggregates,
        catalogData);
  }

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
        def.aggregate(),
        def.window(),
        def.strict(),
        def.immutable(),
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
        def.functionName(),
        Map.of());
  }

  private BuiltinTypeNode toTypeNode(String engineVersion, long version, BuiltinTypeDef def) {
    return new BuiltinTypeNode(
        resourceId(engineVersion, ResourceKind.RK_TYPE_VALUE, def.name()),
        version,
        Instant.EPOCH,
        engineVersion,
        def.name(),
        def.oid(),
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
        def.method().name().toLowerCase(),
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
        def.stateFunction(),
        def.finalFunction(),
        Map.of());
  }

  private ResourceId resourceId(String engineVersion, int kindValue, String name) {
    return ResourceId.newBuilder()
        .setTenantId("_builtin")
        .setKindValue(kindValue)
        .setId(engineVersion + ":" + name)
        .build();
  }

  private static long versionFromFingerprint(String fingerprint) {
    if (fingerprint == null || fingerprint.isBlank()) {
      return 0L;
    }
    String prefix = fingerprint.length() >= 16 ? fingerprint.substring(0, 16) : fingerprint;
    try {
      return Long.parseUnsignedLong(prefix, 16);
    } catch (NumberFormatException ex) {
      return prefix.hashCode();
    }
  }

  private static boolean matches(
      List<EngineSpecificRule> rules, String engineKind, String engineVersion) {
    if (rules == null || rules.isEmpty()) {
      return true;
    }
    for (EngineSpecificRule rule : rules) {
      if (rule == null) {
        continue;
      }
      if (rule.hasEngineKind()
          && !rule.engineKind().equalsIgnoreCase(engineKind == null ? "" : engineKind)) {
        continue;
      }
      if (rule.hasMinVersion() && compareVersions(engineVersion, rule.minVersion()) < 0) {
        continue;
      }
      if (rule.hasMaxVersion() && compareVersions(engineVersion, rule.maxVersion()) > 0) {
        continue;
      }
      return true;
    }
    return false;
  }

  private static int compareVersions(String left, String right) {
    if (left == null || left.isBlank()) {
      left = "0";
    }
    if (right == null || right.isBlank()) {
      right = "0";
    }
    if (isSemantic(left) && isSemantic(right)) {
      return compareSemantic(left, right);
    }
    return left.compareToIgnoreCase(right);
  }

  private static boolean isSemantic(String value) {
    return value.matches("[0-9]+(\\.[0-9]+)*");
  }

  private static int compareSemantic(String left, String right) {
    String[] leftParts = left.split("\\.");
    String[] rightParts = right.split("\\.");
    int length = Math.max(leftParts.length, rightParts.length);
    for (int i = 0; i < length; i++) {
      int leftVal = i < leftParts.length ? Integer.parseInt(leftParts[i]) : 0;
      int rightVal = i < rightParts.length ? Integer.parseInt(rightParts[i]) : 0;
      if (leftVal != rightVal) {
        return Integer.compare(leftVal, rightVal);
      }
    }
    return 0;
  }

  /** Immutable bundle of builtin relation nodes for an engine version. */
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
      catalogData = Objects.requireNonNull(catalogData, "catalogData");
    }

    public BuiltinCatalogData toCatalogData() {
      return catalogData;
    }
  }

  private record VersionKey(String engineKind, String engineVersion) {}
}
