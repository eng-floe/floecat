package ai.floedb.metacat.service.query.graph.hint;

import ai.floedb.metacat.catalog.builtin.*;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.query.graph.builtin.BuiltinNodeRegistry;
import ai.floedb.metacat.service.query.graph.builtin.EngineSpecificMatcher;
import ai.floedb.metacat.service.query.graph.model.*;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.*;
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

    BuiltinEngineCatalog catalog;
    try {
      catalog = definitionRegistry.catalog(engineKey.engineKind());
    } catch (RuntimeException ex) {
      return Map.of();
    }

    return switch (node.kind()) {
      case BUILTIN_FUNCTION ->
          ruleForFunction((BuiltinFunctionNode) node, catalog, engineKey)
              .map(BuiltinCatalogHintProvider::flattenProperties)
              .orElse(Map.of());
      case BUILTIN_OPERATOR ->
          ruleForOperator((BuiltinOperatorNode) node, catalog, engineKey)
              .map(BuiltinCatalogHintProvider::flattenProperties)
              .orElse(Map.of());
      case BUILTIN_TYPE ->
          ruleForType((BuiltinTypeNode) node, catalog, engineKey)
              .map(BuiltinCatalogHintProvider::flattenProperties)
              .orElse(Map.of());
      case BUILTIN_CAST ->
          ruleForCast((BuiltinCastNode) node, catalog, engineKey)
              .map(BuiltinCatalogHintProvider::flattenProperties)
              .orElse(Map.of());
      case BUILTIN_COLLATION ->
          ruleForCollation((BuiltinCollationNode) node, catalog, engineKey)
              .map(BuiltinCatalogHintProvider::flattenProperties)
              .orElse(Map.of());
      case BUILTIN_AGGREGATE ->
          ruleForAggregate((BuiltinAggregateNode) node, catalog, engineKey)
              .map(BuiltinCatalogHintProvider::flattenProperties)
              .orElse(Map.of());
      default -> Map.of();
    };
  }

  private Optional<EngineSpecificRule> ruleForFunction(
      BuiltinFunctionNode node, BuiltinEngineCatalog catalog, EngineKey engineKey) {
    return catalog.functions().stream()
        .filter(def -> functionMatches(node, def, engineKey.engineKind()))
        .findFirst()
        .flatMap(def -> selectRule(def.engineSpecific(), engineKey));
  }

  private Optional<EngineSpecificRule> ruleForOperator(
      BuiltinOperatorNode node, BuiltinEngineCatalog catalog, EngineKey engineKey) {
    return catalog.operators().stream()
        .filter(def -> operatorMatches(node, def, engineKey.engineKind()))
        .findFirst()
        .flatMap(def -> selectRule(def.engineSpecific(), engineKey));
  }

  private Optional<EngineSpecificRule> ruleForType(
      BuiltinTypeNode node, BuiltinEngineCatalog catalog, EngineKey engineKey) {
    return catalog.types().stream()
        .filter(def -> typeMatches(node, def, engineKey.engineKind()))
        .findFirst()
        .flatMap(def -> selectRule(def.engineSpecific(), engineKey));
  }

  private Optional<EngineSpecificRule> ruleForCast(
      BuiltinCastNode node, BuiltinEngineCatalog catalog, EngineKey engineKey) {
    return catalog.casts().stream()
        .filter(def -> castMatches(node, def, engineKey.engineKind()))
        .findFirst()
        .flatMap(def -> selectRule(def.engineSpecific(), engineKey));
  }

  private Optional<EngineSpecificRule> ruleForCollation(
      BuiltinCollationNode node, BuiltinEngineCatalog catalog, EngineKey engineKey) {
    return catalog.collations().stream()
        .filter(def -> collationMatches(node, def, engineKey.engineKind()))
        .findFirst()
        .flatMap(def -> selectRule(def.engineSpecific(), engineKey));
  }

  private Optional<EngineSpecificRule> ruleForAggregate(
      BuiltinAggregateNode node, BuiltinEngineCatalog catalog, EngineKey engineKey) {
    return catalog.aggregates().stream()
        .filter(def -> aggregateMatches(node, def, engineKey.engineKind()))
        .findFirst()
        .flatMap(def -> selectRule(def.engineSpecific(), engineKey));
  }

  private Optional<EngineSpecificRule> selectRule(
      List<EngineSpecificRule> rules, EngineKey engineKey) {
    return EngineSpecificMatcher.selectRule(
        rules, engineKey.engineKind(), engineKey.engineVersion());
  }

  private static boolean functionMatches(
      BuiltinFunctionNode node, BuiltinFunctionDef def, String engineKind) {
    return node.id().equals(rid(engineKind, ResourceKind.RK_FUNCTION, def.name()))
        && node.argumentTypes()
            .equals(
                def.argumentTypes().stream()
                    .map(n -> rid(engineKind, ResourceKind.RK_TYPE, n))
                    .toList())
        && node.returnType().equals(rid(engineKind, ResourceKind.RK_TYPE, def.returnType()))
        && def.isAggregate() == node.aggregate()
        && def.isWindow() == node.window();
  }

  private static boolean operatorMatches(
      BuiltinOperatorNode node, BuiltinOperatorDef def, String engineKind) {
    return node.id().equals(rid(engineKind, ResourceKind.RK_OPERATOR, def.name()))
        && node.leftType().equals(rid(engineKind, ResourceKind.RK_TYPE, def.leftType()))
        && node.rightType().equals(rid(engineKind, ResourceKind.RK_TYPE, def.rightType()))
        && node.returnType().equals(rid(engineKind, ResourceKind.RK_TYPE, def.returnType()))
        && def.isCommutative() == node.commutative()
        && def.isAssociative() == node.associative();
  }

  private static boolean typeMatches(BuiltinTypeNode node, BuiltinTypeDef def, String engineKind) {
    ResourceId elemRid =
        def.elementType() == null ? null : rid(engineKind, ResourceKind.RK_TYPE, def.elementType());

    return node.id().equals(rid(engineKind, ResourceKind.RK_TYPE, def.name()))
        && safeEquals(def.category(), node.category())
        && Objects.equals(elemRid, node.elementType());
  }

  private static boolean castMatches(BuiltinCastNode node, BuiltinCastDef def, String engineKind) {
    return node.id().equals(rid(engineKind, ResourceKind.RK_CAST, def.name()))
        && node.sourceType().equals(rid(engineKind, ResourceKind.RK_TYPE, def.sourceType()))
        && node.targetType().equals(rid(engineKind, ResourceKind.RK_TYPE, def.targetType()))
        && (def.method() == null
            || def.method().name().toLowerCase().equals(node.method().toLowerCase()));
  }

  private static boolean collationMatches(
      BuiltinCollationNode node, BuiltinCollationDef def, String engineKind) {
    return node.id().equals(rid(engineKind, ResourceKind.RK_COLLATION, def.name()));
  }

  private static boolean aggregateMatches(
      BuiltinAggregateNode node, BuiltinAggregateDef def, String engineKind) {
    return node.id().equals(rid(engineKind, ResourceKind.RK_AGGREGATE, def.name()))
        && node.argumentTypes()
            .equals(
                def.argumentTypes().stream()
                    .map(n -> rid(engineKind, ResourceKind.RK_TYPE, n))
                    .toList())
        && node.returnType().equals(rid(engineKind, ResourceKind.RK_TYPE, def.returnType()));
  }

  private static boolean safeEquals(String left, String right) {
    if (left == null) return right == null;
    return left.equals(right);
  }

  // -------------------------------------------------------------------------
  //  PROTO REFLECTION EXTRACTOR
  // -------------------------------------------------------------------------

  private static Map<String, String> extractProtoFields(Message proto) {
    Map<String, String> map = new LinkedHashMap<>();

    for (Map.Entry<FieldDescriptor, Object> entry : proto.getAllFields().entrySet()) {
      String key = entry.getKey().getName();
      Object value = entry.getValue();

      if (value instanceof List<?> list) {
        String joined = list.stream().map(Object::toString).collect(Collectors.joining(","));
        map.put(key, joined);
      } else {
        map.put(key, value.toString());
      }
    }

    return map;
  }

  private static Map<String, String> flattenProperties(EngineSpecificRule rule) {
    if (rule == null) return Map.of();

    Map<String, String> props = new LinkedHashMap<>(rule.properties());

    if (rule.floeFunction() != null) {
      props.putAll(extractProtoFields(rule.floeFunction()));
    }
    if (rule.floeType() != null) {
      props.putAll(extractProtoFields(rule.floeType()));
    }
    if (rule.floeOperator() != null) {
      props.putAll(extractProtoFields(rule.floeOperator()));
    }
    if (rule.floeCast() != null) {
      props.putAll(extractProtoFields(rule.floeCast()));
    }
    if (rule.floeAggregate() != null) {
      props.putAll(extractProtoFields(rule.floeAggregate()));
    }
    if (rule.floeCollation() != null) {
      props.putAll(extractProtoFields(rule.floeCollation()));
    }

    return props.isEmpty() ? Map.of() : Map.copyOf(props);
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

  private static ResourceId rid(String engineKind, ResourceKind kind, NameRef ref) {
    return BuiltinNodeRegistry.resourceId(engineKind, kind, ref);
  }
}
