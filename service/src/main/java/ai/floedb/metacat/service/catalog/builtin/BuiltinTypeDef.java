package ai.floedb.metacat.service.catalog.builtin;

import java.util.Objects;

public record BuiltinTypeDef(
    String name, Integer oid, String category, boolean array, String elementType) {

  public BuiltinTypeDef {
    name = Objects.requireNonNull(name, "name");
    category = category == null ? "" : category;
    if (!array) {
      elementType = null;
    }
  }
}
