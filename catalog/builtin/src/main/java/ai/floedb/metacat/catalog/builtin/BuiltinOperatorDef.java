package ai.floedb.metacat.catalog.builtin;

import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceKind;
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
