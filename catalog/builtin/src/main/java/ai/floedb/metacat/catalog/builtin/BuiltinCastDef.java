package ai.floedb.metacat.catalog.builtin;

import ai.floedb.metacat.common.rpc.NameRef;
import java.util.List;
import java.util.Objects;

public record BuiltinCastDef(
    NameRef name,
    NameRef sourceType,
    NameRef targetType,
    BuiltinCastMethod method,
    List<EngineSpecificRule> engineSpecific) {

  public BuiltinCastDef {
    name = Objects.requireNonNull(name, "name");
    sourceType = Objects.requireNonNull(sourceType, "sourceType");
    targetType = Objects.requireNonNull(targetType, "targetType");
    method = Objects.requireNonNull(method, "method");
    engineSpecific = List.copyOf(engineSpecific == null ? List.of() : engineSpecific);
  }
}
