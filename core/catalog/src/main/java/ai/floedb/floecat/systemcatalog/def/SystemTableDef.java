package ai.floedb.floecat.systemcatalog.def;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.TableBackendKind;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import java.util.List;
import java.util.Objects;

public record SystemTableDef(
    NameRef name,
    String displayName,
    List<SchemaColumn> columns,
    TableBackendKind backendKind,
    String scannerId,
    List<EngineSpecificRule> engineSpecific)
    implements SystemObjectDef {

  public SystemTableDef {
    name = Objects.requireNonNull(name, "name");
    columns = List.copyOf(Objects.requireNonNull(columns, "columns"));
    backendKind = Objects.requireNonNull(backendKind, "backendKind");
    scannerId = Objects.requireNonNull(scannerId, "scannerId");
    displayName = displayName == null ? "" : displayName;
    engineSpecific = List.copyOf(engineSpecific == null ? List.of() : engineSpecific);
  }

  @Override
  public ResourceKind kind() {
    return ResourceKind.RK_TABLE;
  }
}
