package ai.floedb.metacat.service.query.graph.hint;

import ai.floedb.metacat.catalog.builtin.BuiltinAggregateDef;
import ai.floedb.metacat.catalog.builtin.BuiltinCastDef;
import ai.floedb.metacat.catalog.builtin.BuiltinDefinitionRegistry;
import ai.floedb.metacat.catalog.builtin.BuiltinEngineCatalog;
import ai.floedb.metacat.catalog.builtin.BuiltinFunctionDef;
import ai.floedb.metacat.catalog.builtin.BuiltinOperatorDef;
import ai.floedb.metacat.catalog.builtin.BuiltinTypeDef;
import ai.floedb.metacat.catalog.builtin.EngineSpecificRule;
import ai.floedb.metacat.service.query.graph.model.BuiltinAggregateNode;
import ai.floedb.metacat.service.query.graph.model.BuiltinCastNode;
import ai.floedb.metacat.service.query.graph.model.BuiltinCollationNode;
import ai.floedb.metacat.service.query.graph.model.BuiltinFunctionNode;
import ai.floedb.metacat.service.query.graph.model.BuiltinOperatorNode;
import ai.floedb.metacat.service.query.graph.model.BuiltinTypeNode;
import ai.floedb.metacat.service.query.graph.model.EngineHint;
import ai.floedb.metacat.service.query.graph.model.EngineKey;
import ai.floedb.metacat.service.query.graph.model.RelationNode;
import ai.floedb.metacat.service.query.graph.model.RelationNodeKind;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Publishes per-object builtin metadata as engine hints. */
@ApplicationScoped
public class BuiltinCatalogHintProvider implements EngineHintProvider {

  public static final String HINT_TYPE = "builtin.catalog.properties";

  private static final Set<RelationNodeKind> SUPPORTED_KINDS =
      EnumSet.of(
          RelationNodeKind.BUILTIN_FUNCTION,
          RelationNodeKind.BUILTIN_OPERATOR,
          RelationNodeKind.BUILTIN_TYPE,
          RelationNodeKind.BUILTIN_CAST,
          RelationNodeKind.BUILTIN_COLLATION,
          RelationNodeKind.BUILTIN_AGGREGATE);

  private final BuiltinDefinitionRegistry definitionRegistry;

  @Inject
  public BuiltinCatalogHintProvider(BuiltinDefinitionRegistry definitionRegistry) {
    this.definitionRegistry = definitionRegistry;
  }

  @Override
  public boolean supports(RelationNodeKind kind, String hintType) {
    return HINT_TYPE.equals(hintType) && SUPPORTED_KINDS.contains(kind);
  }

  @Override
  public boolean isAvailable(EngineKey engineKey) {
    return engineKey != null
        && engineKey.engineVersion() != null
        && !engineKey.engineVersion().isBlank();
  }

  @Override
  public String fingerprint(RelationNode node, EngineKey engineKey, String hintType) {
    Map<String, String> properties = propertiesFor(node, engineKey);
    String base =
        node.id().getId()
            + ":"
            + node.version()
            + ":"
            + engineKey.engineKind()
            + ":"
            + engineKey.engineVersion();
    if (properties.isEmpty()) {
      return base;
    }
    return base + ":" + properties.hashCode();
  }

  @Override
  public EngineHint compute(
      RelationNode node, EngineKey engineKey, String hintType, String correlationId) {
    Map<String, String> properties = propertiesFor(node, engineKey);
    byte[] payload = toJsonPayload(properties);
    return new EngineHint(
        "application/json",
        payload,
        payload.length,
        Map.of("entries", Integer.toString(properties.size())));
  }

  private Map<String, String> propertiesFor(RelationNode node, EngineKey engineKey) {
    if (engineKey == null
        || engineKey.engineVersion() == null
        || engineKey.engineVersion().isBlank()
        || !SUPPORTED_KINDS.contains(node.kind())) {
      return Map.of();
    }

    String nodeEngineVersion = engineVersionFor(node);
    if (nodeEngineVersion == null || nodeEngineVersion.isBlank()) {
      return Map.of();
    }

    BuiltinEngineCatalog catalog;
    try {
      catalog = definitionRegistry.catalog(nodeEngineVersion);
    } catch (RuntimeException ex) {
      return Map.of();
    }

    return switch (node.kind()) {
      case BUILTIN_FUNCTION ->
          ruleForFunction((BuiltinFunctionNode) node, catalog, engineKey)
              .map(EngineSpecificRule::properties)
              .orElse(Map.of());
      case BUILTIN_OPERATOR ->
          ruleForOperator((BuiltinOperatorNode) node, catalog, engineKey)
              .map(EngineSpecificRule::properties)
              .orElse(Map.of());
      case BUILTIN_TYPE ->
          ruleForType((BuiltinTypeNode) node, catalog, engineKey)
              .map(EngineSpecificRule::properties)
              .orElse(Map.of());
      case BUILTIN_CAST ->
          ruleForCast((BuiltinCastNode) node, catalog, engineKey)
              .map(EngineSpecificRule::properties)
              .orElse(Map.of());
      case BUILTIN_COLLATION ->
          ruleForCollation((BuiltinCollationNode) node, catalog, engineKey)
              .map(EngineSpecificRule::properties)
              .orElse(Map.of());
      case BUILTIN_AGGREGATE ->
          ruleForAggregate((BuiltinAggregateNode) node, catalog, engineKey)
              .map(EngineSpecificRule::properties)
              .orElse(Map.of());
      default -> Map.of();
    };
  }

  private static String engineVersionFor(RelationNode node) {
    if (node instanceof BuiltinFunctionNode fn) {
      return fn.engineVersion();
    }
    if (node instanceof BuiltinOperatorNode op) {
      return op.engineVersion();
    }
    if (node instanceof BuiltinTypeNode type) {
      return type.engineVersion();
    }
    if (node instanceof BuiltinCastNode cast) {
      return cast.engineVersion();
    }
    if (node instanceof BuiltinCollationNode coll) {
      return coll.engineVersion();
    }
    if (node instanceof BuiltinAggregateNode agg) {
      return agg.engineVersion();
    }
    return null;
  }

  private Optional<EngineSpecificRule> ruleForFunction(
      BuiltinFunctionNode node, BuiltinEngineCatalog catalog, EngineKey engineKey) {
    return catalog.functions().stream()
        .filter(def -> functionMatches(node, def))
        .findFirst()
        .flatMap(def -> selectRule(def.engineSpecific(), engineKey));
  }

  private Optional<EngineSpecificRule> ruleForOperator(
      BuiltinOperatorNode node, BuiltinEngineCatalog catalog, EngineKey engineKey) {
    return catalog.operators().stream()
        .filter(def -> operatorMatches(node, def))
        .findFirst()
        .flatMap(def -> selectRule(def.engineSpecific(), engineKey));
  }

  private Optional<EngineSpecificRule> ruleForType(
      BuiltinTypeNode node, BuiltinEngineCatalog catalog, EngineKey engineKey) {
    return catalog.types().stream()
        .filter(def -> typeMatches(node, def))
        .findFirst()
        .flatMap(def -> selectRule(def.engineSpecific(), engineKey));
  }

  private Optional<EngineSpecificRule> ruleForCast(
      BuiltinCastNode node, BuiltinEngineCatalog catalog, EngineKey engineKey) {
    return catalog.casts().stream()
        .filter(def -> castMatches(node, def))
        .findFirst()
        .flatMap(def -> selectRule(def.engineSpecific(), engineKey));
  }

  private Optional<EngineSpecificRule> ruleForCollation(
      BuiltinCollationNode node, BuiltinEngineCatalog catalog, EngineKey engineKey) {
    return catalog.collations().stream()
        .filter(def -> collationMatches(node, def.name()))
        .findFirst()
        .flatMap(def -> selectRule(def.engineSpecific(), engineKey));
  }

  private Optional<EngineSpecificRule> ruleForAggregate(
      BuiltinAggregateNode node, BuiltinEngineCatalog catalog, EngineKey engineKey) {
    return catalog.aggregates().stream()
        .filter(def -> aggregateMatches(node, def))
        .findFirst()
        .flatMap(def -> selectRule(def.engineSpecific(), engineKey));
  }

  private Optional<EngineSpecificRule> selectRule(
      List<EngineSpecificRule> rules, EngineKey engineKey) {
    if (rules == null || rules.isEmpty()) {
      return Optional.empty();
    }
    for (EngineSpecificRule rule : rules) {
      if (ruleMatches(rule, engineKey.engineKind(), engineKey.engineVersion())) {
        return Optional.of(rule);
      }
    }
    return Optional.empty();
  }

  private static boolean functionMatches(BuiltinFunctionNode node, BuiltinFunctionDef def) {
    return def.name().equals(node.name())
        && def.argumentTypes().equals(node.argumentTypes())
        && def.returnType().equals(node.returnType())
        && def.aggregate() == node.aggregate()
        && def.window() == node.window();
  }

  private static boolean operatorMatches(BuiltinOperatorNode node, BuiltinOperatorDef def) {
    return def.name().equals(node.name())
        && equals(def.leftType(), node.leftType())
        && equals(def.rightType(), node.rightType())
        && equals(def.functionName(), node.functionName());
  }

  private static boolean typeMatches(BuiltinTypeNode node, BuiltinTypeDef def) {
    return def.name().equals(node.name())
        && equals(def.category(), node.category())
        && (def.oid() == null ? node.oid() == null : def.oid().equals(node.oid()))
        && equals(def.elementType(), node.elementType());
  }

  private static boolean castMatches(BuiltinCastNode node, BuiltinCastDef def) {
    return equals(def.sourceType(), node.sourceType())
        && equals(def.targetType(), node.targetType())
        && (def.method() == null
            || equals(def.method().name().toLowerCase(), node.method().toLowerCase()));
  }

  private static boolean collationMatches(BuiltinCollationNode node, String name) {
    return equals(name, node.name());
  }

  private static boolean aggregateMatches(BuiltinAggregateNode node, BuiltinAggregateDef def) {
    return def.name().equals(node.name())
        && def.argumentTypes().equals(node.argumentTypes())
        && equals(def.returnType(), node.returnType());
  }

  private static boolean equals(String left, String right) {
    if (left == null) {
      return right == null;
    }
    return left.equals(right);
  }

  private static boolean ruleMatches(
      EngineSpecificRule rule, String engineKind, String engineVersion) {
    if (rule == null) {
      return false;
    }
    if (rule.hasEngineKind()
        && (engineKind == null || !rule.engineKind().equalsIgnoreCase(engineKind))) {
      return false;
    }
    if (rule.hasMinVersion() && compareVersions(engineVersion, rule.minVersion()) < 0) {
      return false;
    }
    if (rule.hasMaxVersion() && compareVersions(engineVersion, rule.maxVersion()) > 0) {
      return false;
    }
    return true;
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

  private static byte[] toJsonPayload(Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return "{}".getBytes(StandardCharsets.UTF_8);
    }
    String body =
        properties.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(entry -> "\"" + escape(entry.getKey()) + "\":\"" + escape(entry.getValue()) + "\"")
            .collect(Collectors.joining(",", "{", "}"));
    return body.getBytes(StandardCharsets.UTF_8);
  }

  private static String escape(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
