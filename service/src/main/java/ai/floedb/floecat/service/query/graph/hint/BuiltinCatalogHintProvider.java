package ai.floedb.floecat.service.query.graph.hint;

import ai.floedb.floecat.catalog.builtin.*;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.query.graph.builtin.BuiltinNodeRegistry;
import ai.floedb.floecat.service.query.graph.model.*;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.*;

/* Publishes per-object builtin metadata as engine hints. */
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
    EngineSpecificRule r = matchedRule(node, engineKey);

    String base =
        node.id().getId()
            + ":"
            + node.version()
            + ":"
            + engineKey.engineKind()
            + ":"
            + engineKey.engineVersion();

    return (r == null) ? base : base + ":" + r.hashCode();
  }

  @Override
  public EngineHint compute(
      RelationNode node, EngineKey engineKey, String hintType, String correlationId) {
    EngineSpecificRule rule = matchedRule(node, engineKey);

    if (rule == null) {
      return new EngineHint(
          "", // empty content-type
          new byte[0], // empty payload
          0,
          Map.of() // empty metadata
          );
    }

    String contentType = rule.payloadType();
    byte[] payload = rule.extensionPayload();
    Map<String, String> meta = rule.properties();

    return new EngineHint(
        (contentType == null) ? "" : contentType,
        payload == null ? new byte[0] : payload,
        payload == null ? 0 : payload.length,
        meta == null ? Map.of() : meta);
  }

  /** Determines the matching rule for the provided node. */
  private EngineSpecificRule matchedRule(RelationNode node, EngineKey engineKey) {
    if (!SUPPORTED_KINDS.contains(node.kind())) {
      return null;
    }

    var nodes = nodeRegistry.nodesFor(engineKey.engineKind(), engineKey.engineVersion());

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

    String engineKind = engineKey.engineKind();

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

    if (candidates.isEmpty()) {
      return null;
    }

    for (BuiltinDef def : candidates) {
      EngineSpecificRule r =
          switch (def.kind()) {
            case RK_FUNCTION ->
                matchFunction((BuiltinFunctionDef) def, (BuiltinFunctionNode) node, engineKind);

            case RK_AGGREGATE ->
                matchAggregate((BuiltinAggregateDef) def, (BuiltinAggregateNode) node, engineKind);

            case RK_OPERATOR ->
                matchOperator((BuiltinOperatorDef) def, (BuiltinOperatorNode) node, engineKind);

            case RK_CAST -> matchCast((BuiltinCastDef) def, (BuiltinCastNode) node, engineKind);

            case RK_TYPE, RK_COLLATION -> def.engineSpecific().stream().findFirst().orElse(null);

            default -> null;
          };

      if (r != null) return r;
    }

    return null;
  }

  private EngineSpecificRule matchFunction(
      BuiltinFunctionDef def, BuiltinFunctionNode fn, String engineKind) {
    boolean argsMatch =
        def.argumentTypes().stream()
            .map(a -> BuiltinNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, a))
            .toList()
            .equals(fn.argumentTypes());

    boolean retMatch =
        BuiltinNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, def.returnType())
            .equals(fn.returnType());

    return (argsMatch && retMatch) ? def.engineSpecific().stream().findFirst().orElse(null) : null;
  }

  private EngineSpecificRule matchAggregate(
      BuiltinAggregateDef def, BuiltinAggregateNode an, String engineKind) {
    boolean argsMatch =
        def.argumentTypes().stream()
            .map(a -> BuiltinNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, a))
            .toList()
            .equals(an.argumentTypes());

    boolean retMatch =
        BuiltinNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, def.returnType())
            .equals(an.returnType());

    return (argsMatch && retMatch) ? def.engineSpecific().stream().findFirst().orElse(null) : null;
  }

  private EngineSpecificRule matchOperator(
      BuiltinOperatorDef def, BuiltinOperatorNode on, String engineKind) {
    boolean leftMatch =
        BuiltinNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, def.leftType())
            .equals(on.leftType());

    boolean rightMatch =
        BuiltinNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, def.rightType())
            .equals(on.rightType());

    boolean retMatch =
        BuiltinNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, def.returnType())
            .equals(on.returnType());

    return (leftMatch && rightMatch && retMatch)
        ? def.engineSpecific().stream().findFirst().orElse(null)
        : null;
  }

  private EngineSpecificRule matchCast(BuiltinCastDef def, BuiltinCastNode cn, String engineKind) {
    boolean srcMatch =
        BuiltinNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, def.sourceType())
            .equals(cn.sourceType());

    boolean dstMatch =
        BuiltinNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, def.targetType())
            .equals(cn.targetType());

    boolean methodMatch = def.method().wireValue().equalsIgnoreCase(cn.method());

    return (srcMatch && dstMatch && methodMatch)
        ? def.engineSpecific().stream().findFirst().orElse(null)
        : null;
  }
}
