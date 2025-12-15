package ai.floedb.floecat.systemcatalog.def;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import java.util.List;
import java.util.Objects;

public record SystemNamespaceDef(
    NameRef name, String displayName, List<EngineSpecificRule> engineSpecific)
    implements SystemObjectDef {

  public SystemNamespaceDef {
    name = Objects.requireNonNull(name, "name");
    displayName = displayName == null ? "" : displayName;
    engineSpecific = List.copyOf(engineSpecific == null ? List.of() : engineSpecific);
  }

  @Override
  public ResourceKind kind() {
    return ResourceKind.RK_NAMESPACE;
  }
}
