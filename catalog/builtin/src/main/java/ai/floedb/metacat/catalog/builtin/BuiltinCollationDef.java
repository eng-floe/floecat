package ai.floedb.metacat.catalog.builtin;

import ai.floedb.metacat.common.rpc.NameRef;
import java.util.List;
import java.util.Objects;

public record BuiltinCollationDef(
    NameRef name, String locale, List<EngineSpecificRule> engineSpecific) {

  public BuiltinCollationDef {
    name = Objects.requireNonNull(name, "name");
    locale = Objects.requireNonNull(locale, "locale");
    engineSpecific = List.copyOf(engineSpecific == null ? List.of() : engineSpecific);
  }
}
