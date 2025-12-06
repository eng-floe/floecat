package ai.floedb.metacat.service.query.graph.hint;

import ai.floedb.metacat.catalog.builtin.BuiltinAggregateDef;
import ai.floedb.metacat.catalog.builtin.BuiltinCastDef;
import ai.floedb.metacat.catalog.builtin.BuiltinDef;
import ai.floedb.metacat.catalog.builtin.BuiltinFunctionDef;
import ai.floedb.metacat.catalog.builtin.BuiltinOperatorDef;
import ai.floedb.metacat.catalog.builtin.EngineSpecificRule;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.query.graph.builtin.BuiltinNodeRegistry;
import ai.floedb.metacat.service.query.graph.model.*;
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

  private final BuiltinNodeRegistry nodeRegistry;

  @Inject
  public BuiltinCatalogHintProvider(BuiltinNodeRegistry nodeRegistry) {
    this.nodeRegistry = nodeRegistry;
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
    // The fingerprint MUST change whenever the matched rules change
    Map<String, String> props = propertiesFor(node, engineKey);

    String base =
        node.id().getId()
            + ":"
            + node.version()
            + ":"
            + engineKey.engineKind()
            + ":"
            + engineKey.engineVersion();

    if (props.isEmpty()) {
      return base;
    }
    return base + ":" + props.hashCode();
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

  /**
   * Reads the filtered & rule-applied BuiltinDefs from BuiltinNodeRegistry, avoids re-matching
   * against raw catalog definitions.
   */
  private Map<String, String> propertiesFor(RelationNode node, EngineKey engineKey) {
    if (!SUPPORTED_KINDS.contains(node.kind())) return Map.of();

    var nodes = nodeRegistry.nodesFor(engineKey.engineKind(), engineKey.engineVersion());

    // Step 1: get matching BuiltinDef list based on kind
    List<? extends BuiltinDef> defs =
        switch (node.kind()) {
          case BUILTIN_FUNCTION -> nodes.catalogData().functions();
          case BUILTIN_OPERATOR -> nodes.catalogData().operators();
          case BUILTIN_TYPE -> nodes.catalogData().types();
          case BUILTIN_CAST -> nodes.catalogData().casts();
          case BUILTIN_COLLATION -> nodes.catalogData().collations();
          case BUILTIN_AGGREGATE -> nodes.catalogData().aggregates();
          default -> List.of();
        };

    // Step 2: fast filter by ResourceId
    String engineKind = engineKey.engineKind();

    // Collapse List<? extends BuiltinDef> → List<BuiltinDef> (safe: all elements are BuiltinDef)
    @SuppressWarnings("unchecked")
    List<BuiltinDef> candidates =
        (List<BuiltinDef>)
            (List<?>)
                defs.stream()
                    .filter(
                        def ->
                            node.id()
                                .equals(
                                    BuiltinNodeRegistry.resourceId(
                                        engineKind, def.kind(), def.name())))
                    .toList();

    if (candidates.isEmpty()) return Map.of();

    // Step 3: pick the correct entry based on signature when needed
    EngineSpecificRule rule = null;

    for (BuiltinDef def : candidates) {
      rule =
          switch (def.kind()) {
            case RK_FUNCTION ->
                matchFunction((BuiltinFunctionDef) def, (BuiltinFunctionNode) node, engineKind);

            case RK_AGGREGATE ->
                matchAggregate((BuiltinAggregateDef) def, (BuiltinAggregateNode) node, engineKind);

            case RK_OPERATOR ->
                matchOperator((BuiltinOperatorDef) def, (BuiltinOperatorNode) node, engineKind);

            case RK_CAST -> matchCast((BuiltinCastDef) def, (BuiltinCastNode) node, engineKind);

            case RK_TYPE, RK_COLLATION -> matchSingleRule(def);

            default -> null;
          };

      if (rule != null) break;
    }

    if (rule == null) return Map.of();
    return flatten(rule);
  }

  private static ResourceId id(String engineKind, ResourceKind kind, NameRef ref) {
    return BuiltinNodeRegistry.resourceId(engineKind, kind, ref);
  }

  // ────────────────────────────────────────────────
  //   Flatten rule properties
  // ────────────────────────────────────────────────

  private static Map<String, String> flatten(EngineSpecificRule rule) {
    Map<String, String> props = new LinkedHashMap<>(rule.properties());

    if (rule.floeFunction() != null) props.putAll(extract(rule.floeFunction()));
    if (rule.floeType() != null) props.putAll(extract(rule.floeType()));
    if (rule.floeOperator() != null) props.putAll(extract(rule.floeOperator()));
    if (rule.floeCast() != null) props.putAll(extract(rule.floeCast()));
    if (rule.floeAggregate() != null) props.putAll(extract(rule.floeAggregate()));
    if (rule.floeCollation() != null) props.putAll(extract(rule.floeCollation()));

    return props;
  }

  private static Map<String, String> extract(Message proto) {
    Map<String, String> out = new LinkedHashMap<>();
    for (var e : proto.getAllFields().entrySet()) {
      String k = e.getKey().getName();
      Object v = e.getValue();
      if (v instanceof List<?> list) {
        out.put(k, list.stream().map(Object::toString).collect(Collectors.joining(",")));
      } else {
        out.put(k, v.toString());
      }
    }
    return out;
  }

  private static byte[] toJsonPayload(Map<String, String> props) {
    if (props.isEmpty()) return "{}".getBytes(StandardCharsets.UTF_8);

    String json =
        props.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(e -> "\"" + escape(e.getKey()) + "\":\"" + escape(e.getValue()) + "\"")
            .collect(Collectors.joining(",", "{", "}"));

    return json.getBytes(StandardCharsets.UTF_8);
  }

  private static String escape(String s) {
    return s.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  /** Signature match for functions */
  private EngineSpecificRule matchFunction(
      BuiltinFunctionDef def, BuiltinFunctionNode fn, String engineKind) {
    var argIds =
        def.argumentTypes().stream()
            .map(a -> BuiltinNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, a))
            .toList();
    boolean argsMatch = argIds.equals(fn.argumentTypes());

    var retId = BuiltinNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, def.returnType());
    boolean returnMatch = retId.equals(fn.returnType());

    return (argsMatch && returnMatch)
        ? def.engineSpecific().stream().findFirst().orElse(null)
        : null;
  }

  /** Signature match for aggregates */
  private EngineSpecificRule matchAggregate(
      BuiltinAggregateDef def, BuiltinAggregateNode an, String engineKind) {
    var argIds =
        def.argumentTypes().stream()
            .map(a -> BuiltinNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, a))
            .toList();
    boolean argsMatch = argIds.equals(an.argumentTypes());

    var retId = BuiltinNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, def.returnType());
    boolean returnMatch = retId.equals(an.returnType());

    return (argsMatch && returnMatch)
        ? def.engineSpecific().stream().findFirst().orElse(null)
        : null;
  }

  /** Signature match for operators */
  private EngineSpecificRule matchOperator(
      BuiltinOperatorDef def, BuiltinOperatorNode on, String engineKind) {
    boolean leftMatch =
        BuiltinNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, def.leftType())
            .equals(on.leftType());

    boolean rightMatch =
        BuiltinNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, def.rightType())
            .equals(on.rightType());

    boolean returnMatch =
        BuiltinNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, def.returnType())
            .equals(on.returnType());

    return (leftMatch && rightMatch && returnMatch)
        ? def.engineSpecific().stream().findFirst().orElse(null)
        : null;
  }

  /** Signature match for casts */
  private EngineSpecificRule matchCast(BuiltinCastDef def, BuiltinCastNode cn, String engineKind) {
    boolean sourceMatch =
        BuiltinNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, def.sourceType())
            .equals(cn.sourceType());

    boolean targetMatch =
        BuiltinNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, def.targetType())
            .equals(cn.targetType());

    boolean methodMatch = def.method().wireValue().equalsIgnoreCase(cn.method());

    return (sourceMatch && targetMatch && methodMatch)
        ? def.engineSpecific().stream().findFirst().orElse(null)
        : null;
  }

  /** Default single-rule match (types, collations) */
  private EngineSpecificRule matchSingleRule(BuiltinDef def) {
    return switch (def.kind()) {
      case RK_TYPE -> def.engineSpecific().stream().findFirst().orElse(null);
      case RK_COLLATION -> def.engineSpecific().stream().findFirst().orElse(null);
      default -> null;
    };
  }
}
