package ai.floedb.floecat.systemcatalog.def;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import java.util.List;
import java.util.Objects;

public record SystemCastDef(
    NameRef name,
    NameRef sourceType,
    NameRef targetType,
    SystemCastMethod method,
    List<EngineSpecificRule> engineSpecific)
    implements SystemObjectDef {

  public SystemCastDef {
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
