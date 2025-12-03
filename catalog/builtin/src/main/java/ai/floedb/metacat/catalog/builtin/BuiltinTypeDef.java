package ai.floedb.metacat.catalog.builtin;

import java.util.List;
import java.util.Objects;

public record BuiltinTypeDef(
    String name,
    Integer oid,
    String category,
    boolean array,
    String elementType,
    List<EngineSpecificRule> engineSpecific) {

  public BuiltinTypeDef {
    name = Objects.requireNonNull(name, "name");
    category = category == null ? "" : category;
    if (!array) {
      elementType = null;
    }
    engineSpecific = List.copyOf(engineSpecific == null ? List.of() : engineSpecific);
  }
}
