package ai.floedb.metacat.catalog.builtin;

import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceKind;
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
