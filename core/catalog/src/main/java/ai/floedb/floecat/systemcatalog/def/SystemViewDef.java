package ai.floedb.floecat.systemcatalog.def;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import java.util.List;
import java.util.Objects;

public record SystemViewDef(
    NameRef name,
    String displayName,
    String sql,
    String dialect,
    List<SchemaColumn> outputColumns,
    List<EngineSpecificRule> engineSpecific)
    implements SystemObjectDef {

  public SystemViewDef {
    name = Objects.requireNonNull(name, "name");
    displayName = displayName == null ? "" : displayName;
    sql = sql == null ? "" : sql;
    dialect = dialect == null ? "" : dialect;
    outputColumns = List.copyOf(outputColumns);
    engineSpecific = List.copyOf(engineSpecific == null ? List.of() : engineSpecific);
  }

  public List<SchemaColumn> columns() {
    return outputColumns;
  }

  @Override
  public ResourceKind kind() {
    return ResourceKind.RK_VIEW;
  }
}
