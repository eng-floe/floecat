package ai.floedb.metacat.catalog.builtin;

import java.util.List;
import java.util.Objects;

public record BuiltinAggregateDef(
    String name,
    List<String> argumentTypes,
    String stateType,
    String returnType,
    String stateFunction,
    String finalFunction) {

  public BuiltinAggregateDef {
    name = Objects.requireNonNull(name, "name");
    argumentTypes = List.copyOf(argumentTypes == null ? List.of() : argumentTypes);
    stateType = Objects.requireNonNull(stateType, "stateType");
    returnType = Objects.requireNonNull(returnType, "returnType");
  }
}
