package ai.floedb.metacat.catalog.builtin;

import java.util.List;
import java.util.Objects;

public record BuiltinTypeDef(
    String name,
    String category,
    boolean array,
    String elementType,
    List<EngineSpecificRule> engineSpecific) {

  public BuiltinTypeDef {
    name = Objects.requireNonNull(name, "name");
    category = category == null ? "" : category.trim();

    // Normalize elementType
    if (!array || elementType == null || elementType.isBlank()) {
      elementType = null;
    }

    engineSpecific = List.copyOf(engineSpecific == null ? List.of() : engineSpecific);
  }
}
