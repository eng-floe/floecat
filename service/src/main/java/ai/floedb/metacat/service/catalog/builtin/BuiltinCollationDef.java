package ai.floedb.metacat.service.catalog.builtin;

import java.util.Objects;

public record BuiltinCollationDef(String name, String locale) {

  public BuiltinCollationDef {
    name = Objects.requireNonNull(name, "name");
    locale = Objects.requireNonNull(locale, "locale");
  }
}
