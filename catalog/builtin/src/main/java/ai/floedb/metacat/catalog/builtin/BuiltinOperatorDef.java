package ai.floedb.metacat.catalog.builtin;

import java.util.Objects;

public record BuiltinOperatorDef(
    String name, String leftType, String rightType, String functionName) {

  public BuiltinOperatorDef {
    name = Objects.requireNonNull(name, "name");
    leftType = Objects.requireNonNull(leftType, "leftType");
    rightType = Objects.requireNonNull(rightType, "rightType");
    functionName = Objects.requireNonNull(functionName, "functionName");
  }
}
