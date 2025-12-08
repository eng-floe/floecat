package ai.floedb.floecat.catalog.builtin.def;

import ai.floedb.floecat.catalog.common.engine.EngineSpecificRule;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceKind;
import java.util.List;
import java.util.Objects;

public record BuiltinOperatorDef(
    NameRef name,
    NameRef leftType,
    NameRef rightType,
    NameRef returnType,
    boolean isCommutative,
    boolean isAssociative,
    List<EngineSpecificRule> engineSpecific)
    implements BuiltinDef {

  public BuiltinOperatorDef {
    name = Objects.requireNonNull(name, "name");
    leftType = Objects.requireNonNull(leftType, "leftType");
    rightType = Objects.requireNonNull(rightType, "rightType");
    returnType = Objects.requireNonNull(returnType, "returnType");
    engineSpecific = List.copyOf(engineSpecific == null ? List.of() : engineSpecific);
  }

  @Override
  public ResourceKind kind() {
    return ResourceKind.RK_OPERATOR;
  }
}
