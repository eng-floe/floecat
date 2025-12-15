package ai.floedb.floecat.systemcatalog.registry;

import ai.floedb.floecat.systemcatalog.def.*;
import java.util.List;

/** Immutable representation of a builtin catalog after parsing from protobuf. */
public record SystemCatalogData(
    List<SystemFunctionDef> functions,
    List<SystemOperatorDef> operators,
    List<SystemTypeDef> types,
    List<SystemCastDef> casts,
    List<SystemCollationDef> collations,
    List<SystemAggregateDef> aggregates,
    List<SystemNamespaceDef> namespaces,
    List<SystemTableDef> tables,
    List<SystemViewDef> views) {

  public SystemCatalogData {
    functions = copy(functions);
    operators = copy(operators);
    types = copy(types);
    casts = copy(casts);
    collations = copy(collations);
    aggregates = copy(aggregates);
    namespaces = copy(namespaces);
    tables = copy(tables);
    views = copy(views);
  }

  private static <T> List<T> copy(List<T> values) {
    return List.copyOf(values == null ? List.of() : values);
  }

  public static SystemCatalogData empty() {
    return new SystemCatalogData(
        List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of(),
        List.of());
  }
}
