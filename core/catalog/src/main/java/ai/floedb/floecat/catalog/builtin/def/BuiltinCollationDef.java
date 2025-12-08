package ai.floedb.floecat.catalog.builtin.def;

import ai.floedb.floecat.catalog.common.engine.EngineSpecificRule;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceKind;
import java.util.List;
import java.util.Objects;

public record BuiltinCollationDef(
    NameRef name, String locale, List<EngineSpecificRule> engineSpecific) implements BuiltinDef {

  public BuiltinCollationDef {
    name = Objects.requireNonNull(name, "name");
    locale = Objects.requireNonNull(locale, "locale");
    engineSpecific = List.copyOf(engineSpecific == null ? List.of() : engineSpecific);
  }

  @Override
  public ResourceKind kind() {
    return ResourceKind.RK_COLLATION;
  }
}
