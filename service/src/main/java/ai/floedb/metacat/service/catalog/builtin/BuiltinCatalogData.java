package ai.floedb.metacat.service.catalog.builtin;

import java.util.List;

public record BuiltinCatalogData(
    String version,
    List<BuiltinFunctionDef> functions,
    List<BuiltinOperatorDef> operators,
    List<BuiltinTypeDef> types,
    List<BuiltinCastDef> casts,
    List<BuiltinCollationDef> collations,
    List<BuiltinAggregateDef> aggregates) {

  public BuiltinCatalogData {
    version = version == null ? "" : version;
    functions = copy(functions);
    operators = copy(operators);
    types = copy(types);
    casts = copy(casts);
    collations = copy(collations);
    aggregates = copy(aggregates);
  }

  private static <T> List<T> copy(List<T> values) {
    return List.copyOf(values == null ? List.of() : values);
  }
}
