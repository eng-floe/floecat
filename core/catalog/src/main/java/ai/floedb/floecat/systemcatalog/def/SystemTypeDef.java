package ai.floedb.floecat.systemcatalog.def;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import java.util.List;
import java.util.Objects;

public record SystemTypeDef(
    NameRef name,
    String category,
    boolean array,
    NameRef elementType,
    List<EngineSpecificRule> engineSpecific)
    implements SystemObjectDef {

  public SystemTypeDef {
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
