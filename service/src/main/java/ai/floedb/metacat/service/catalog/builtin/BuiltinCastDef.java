package ai.floedb.metacat.service.catalog.builtin;

import java.util.Objects;

public record BuiltinCastDef(String sourceType, String targetType, BuiltinCastMethod method) {

  public BuiltinCastDef {
    sourceType = Objects.requireNonNull(sourceType, "sourceType");
    targetType = Objects.requireNonNull(targetType, "targetType");
    method = Objects.requireNonNull(method, "method");
  }
}
