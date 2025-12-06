package ai.floedb.metacat.catalog.builtin;

import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceKind;
import java.util.List;
import java.util.Objects;

public record BuiltinAggregateDef(
    NameRef name,
    List<NameRef> argumentTypes,
    NameRef stateType,
    NameRef returnType,
    List<EngineSpecificRule> engineSpecific)
    implements BuiltinDef {

  public BuiltinAggregateDef {
    name = Objects.requireNonNull(name, "name");
    argumentTypes = List.copyOf(argumentTypes == null ? List.of() : argumentTypes);
    stateType = Objects.requireNonNull(stateType, "stateType");
    returnType = Objects.requireNonNull(returnType, "returnType");
    engineSpecific = List.copyOf(engineSpecific == null ? List.of() : engineSpecific);
  }

  @Override
  public ResourceKind kind() {
    return ResourceKind.RK_AGGREGATE;
  }
}
