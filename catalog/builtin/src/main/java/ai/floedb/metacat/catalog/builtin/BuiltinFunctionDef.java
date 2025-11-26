package ai.floedb.metacat.catalog.builtin;

import java.util.List;
import java.util.Objects;

public record BuiltinFunctionDef(
    String name,
    List<String> argumentTypes,
    String returnType,
    boolean aggregate,
    boolean window,
    boolean strict,
    boolean immutable) {

  public BuiltinFunctionDef {
    name = Objects.requireNonNull(name, "name");
    argumentTypes = List.copyOf(argumentTypes == null ? List.of() : argumentTypes);
    returnType = Objects.requireNonNull(returnType, "returnType");
  }
}
