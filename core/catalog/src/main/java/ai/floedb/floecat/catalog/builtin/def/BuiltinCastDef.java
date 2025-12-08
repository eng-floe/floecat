package ai.floedb.floecat.catalog.builtin.def;

import ai.floedb.floecat.catalog.common.engine.EngineSpecificRule;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceKind;
import java.util.List;
import java.util.Objects;

public record BuiltinCastDef(
    NameRef name,
    NameRef sourceType,
    NameRef targetType,
    BuiltinCastMethod method,
    List<EngineSpecificRule> engineSpecific)
    implements BuiltinDef {

  public BuiltinCastDef {
    name = Objects.requireNonNull(name, "name");
    sourceType = Objects.requireNonNull(sourceType, "sourceType");
    targetType = Objects.requireNonNull(targetType, "targetType");
    method = Objects.requireNonNull(method, "method");
    engineSpecific = List.copyOf(engineSpecific == null ? List.of() : engineSpecific);
  }

  @Override
  public ResourceKind kind() {
    return ResourceKind.RK_CAST;
  }
}
