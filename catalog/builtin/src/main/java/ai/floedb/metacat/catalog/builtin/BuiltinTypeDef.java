package ai.floedb.metacat.catalog.builtin;

import ai.floedb.metacat.common.rpc.NameRef;
import java.util.List;
import java.util.Objects;

public record BuiltinTypeDef(
    NameRef name,
    String category,
    boolean array,
    NameRef elementType,
    List<EngineSpecificRule> engineSpecific) {

  public BuiltinTypeDef {
    name = Objects.requireNonNull(name, "name");
    category = category == null ? "" : category.trim();

    // Normalize elementType
    if (!array || elementType == null || elementType.getName().isBlank()) {
      elementType = null;
    }

    engineSpecific = List.copyOf(engineSpecific == null ? List.of() : engineSpecific);
  }
}
