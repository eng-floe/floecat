package ai.floedb.floecat.systemcatalog.hint;

import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.hint.EngineHintProvider;
import ai.floedb.floecat.metagraph.model.*;
import ai.floedb.floecat.systemcatalog.def.*;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import java.util.*;

/* Publishes per-object builtin metadata as engine hints. */
public class SystemCatalogHintProvider implements EngineHintProvider {

  public static final String HINT_TYPE = "builtin.systemcatalog.properties";

  private static final Set<GraphNodeKind> SUPPORTED_KINDS =
      EnumSet.of(
          GraphNodeKind.FUNCTION,
          GraphNodeKind.OPERATOR,
          GraphNodeKind.TYPE,
          GraphNodeKind.CAST,
          GraphNodeKind.COLLATION,
          GraphNodeKind.AGGREGATE);

  private final SystemNodeRegistry nodeRegistry;

  public SystemCatalogHintProvider(SystemNodeRegistry nodeRegistry) {
    this.nodeRegistry = nodeRegistry;
  }

  @Override
  public boolean supports(GraphNodeKind kind, String hintType) {
    return HINT_TYPE.equals(hintType) && SUPPORTED_KINDS.contains(kind);
  }

  @Override
  public boolean isAvailable(EngineKey engineKey) {
    return engineKey != null
        && engineKey.engineVersion() != null
        && !engineKey.engineVersion().isBlank();
  }

  @Override
  public String fingerprint(GraphNode node, EngineKey engineKey, String hintType) {
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
      GraphNode node, EngineKey engineKey, String hintType, String correlationId) {
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
  private EngineSpecificRule matchedRule(GraphNode node, EngineKey engineKey) {
    if (!SUPPORTED_KINDS.contains(node.kind())) {
      return null;
    }

    var nodes = nodeRegistry.nodesFor(engineKey.engineKind(), engineKey.engineVersion());

    List<? extends SystemObjectDef> defs =
        switch (node.kind()) {
          case FUNCTION -> nodes.catalogData().functions();
          case OPERATOR -> nodes.catalogData().operators();
          case TYPE -> nodes.catalogData().types();
          case CAST -> nodes.catalogData().casts();
          case COLLATION -> nodes.catalogData().collations();
          case AGGREGATE -> nodes.catalogData().aggregates();
          default -> List.of();
        };

    String engineKind = engineKey.engineKind();

    @SuppressWarnings("unchecked")
    List<SystemObjectDef> candidates =
        (List<SystemObjectDef>)
            (List<?>)
                defs.stream()
                    .filter(
                        def ->
                            node.id()
                                .equals(
                                    SystemNodeRegistry.resourceId(
                                        engineKind, def.kind(), def.name())))
                    .toList();

    if (candidates.isEmpty()) {
      return null;
    }

    for (SystemObjectDef def : candidates) {
      EngineSpecificRule r =
          switch (def.kind()) {
            case RK_FUNCTION ->
                matchFunction((SystemFunctionDef) def, (FunctionNode) node, engineKind);

            case RK_AGGREGATE ->
                matchAggregate((SystemAggregateDef) def, (AggregateNode) node, engineKind);

            case RK_OPERATOR ->
                matchOperator((SystemOperatorDef) def, (OperatorNode) node, engineKind);

            case RK_CAST -> matchCast((SystemCastDef) def, (CastNode) node, engineKind);

            case RK_TYPE, RK_COLLATION -> def.engineSpecific().stream().findFirst().orElse(null);

            default -> null;
          };

      if (r != null) return r;
    }

    return null;
  }

  private EngineSpecificRule matchFunction(
      SystemFunctionDef def, FunctionNode fn, String engineKind) {
    boolean argsMatch =
        def.argumentTypes().stream()
            .map(a -> SystemNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, a))
            .toList()
            .equals(fn.argumentTypes());

    boolean retMatch =
        SystemNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, def.returnType())
            .equals(fn.returnType());

    return (argsMatch && retMatch) ? def.engineSpecific().stream().findFirst().orElse(null) : null;
  }

  private EngineSpecificRule matchAggregate(
      SystemAggregateDef def, AggregateNode an, String engineKind) {
    boolean argsMatch =
        def.argumentTypes().stream()
            .map(a -> SystemNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, a))
            .toList()
            .equals(an.argumentTypes());

    boolean retMatch =
        SystemNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, def.returnType())
            .equals(an.returnType());

    return (argsMatch && retMatch) ? def.engineSpecific().stream().findFirst().orElse(null) : null;
  }

  private EngineSpecificRule matchOperator(
      SystemOperatorDef def, OperatorNode on, String engineKind) {
    boolean leftMatch =
        SystemNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, def.leftType())
            .equals(on.leftType());

    boolean rightMatch =
        SystemNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, def.rightType())
            .equals(on.rightType());

    boolean retMatch =
        SystemNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, def.returnType())
            .equals(on.returnType());

    return (leftMatch && rightMatch && retMatch)
        ? def.engineSpecific().stream().findFirst().orElse(null)
        : null;
  }

  private EngineSpecificRule matchCast(SystemCastDef def, CastNode cn, String engineKind) {
    boolean srcMatch =
        SystemNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, def.sourceType())
            .equals(cn.sourceType());

    boolean dstMatch =
        SystemNodeRegistry.resourceId(engineKind, ResourceKind.RK_TYPE, def.targetType())
            .equals(cn.targetType());

    boolean methodMatch = def.method().wireValue().equalsIgnoreCase(cn.method());

    return (srcMatch && dstMatch && methodMatch)
        ? def.engineSpecific().stream().findFirst().orElse(null)
        : null;
  }
}
