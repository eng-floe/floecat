package ai.floedb.metacat.catalog.builtin;

import java.util.List;
import java.util.Objects;

public record BuiltinCastDef(
    String sourceType,
    String targetType,
    BuiltinCastMethod method,
    List<EngineSpecificRule> engineSpecific) {

  public BuiltinCastDef {
    sourceType = Objects.requireNonNull(sourceType, "sourceType");
    targetType = Objects.requireNonNull(targetType, "targetType");
    method = Objects.requireNonNull(method, "method");
    engineSpecific = List.copyOf(engineSpecific == null ? List.of() : engineSpecific);
  }
}
