package ai.floedb.floecat.catalog.builtin;

import java.util.List;

/** Immutable representation of a builtin catalog after parsing from protobuf. */
public record BuiltinCatalogData(
    List<BuiltinFunctionDef> functions,
    List<BuiltinOperatorDef> operators,
    List<BuiltinTypeDef> types,
    List<BuiltinCastDef> casts,
    List<BuiltinCollationDef> collations,
    List<BuiltinAggregateDef> aggregates) {

  public BuiltinCatalogData {
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
