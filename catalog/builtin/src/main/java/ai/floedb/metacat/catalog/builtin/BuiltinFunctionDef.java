package ai.floedb.metacat.catalog.builtin;

import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceKind;
import java.util.List;
import java.util.Objects;

public record BuiltinFunctionDef(
    NameRef name,
    List<NameRef> argumentTypes,
    NameRef returnType,
    boolean isAggregate,
    boolean isWindow,
    List<EngineSpecificRule> engineSpecific)
    implements BuiltinDef {

  public BuiltinFunctionDef {
    name = Objects.requireNonNull(name, "name");
    argumentTypes = List.copyOf(argumentTypes == null ? List.of() : argumentTypes);
    returnType = Objects.requireNonNull(returnType, "returnType");
    engineSpecific = List.copyOf(engineSpecific == null ? List.of() : engineSpecific);
  }

  @Override
  public ResourceKind kind() {
    return ResourceKind.RK_FUNCTION;
  }
}
