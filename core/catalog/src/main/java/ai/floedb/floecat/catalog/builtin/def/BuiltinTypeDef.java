package ai.floedb.floecat.catalog.builtin.def;

import ai.floedb.floecat.catalog.common.engine.EngineSpecificRule;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceKind;
import java.util.List;
import java.util.Objects;

public record BuiltinTypeDef(
    NameRef name,
    String category,
    boolean array,
    NameRef elementType,
    List<EngineSpecificRule> engineSpecific)
    implements BuiltinDef {

  public BuiltinTypeDef {
    name = Objects.requireNonNull(name, "name");
    category = category == null ? "" : category.trim();

    // Normalize elementType
    if (!array || elementType == null || elementType.getName().isBlank()) {
      elementType = null;
    }

    engineSpecific = List.copyOf(engineSpecific == null ? List.of() : engineSpecific);
  }

  @Override
  public ResourceKind kind() {
    return ResourceKind.RK_TYPE;
  }
}
