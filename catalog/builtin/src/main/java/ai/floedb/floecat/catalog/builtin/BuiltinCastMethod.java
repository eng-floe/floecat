package ai.floedb.floecat.catalog.builtin;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum BuiltinCastMethod {
  IMPLICIT("implicit"),
  ASSIGNMENT("assignment"),
  EXPLICIT("explicit");

  private static final Map<String, BuiltinCastMethod> BY_WIRE =
      Arrays.stream(values())
          .collect(Collectors.toUnmodifiableMap(BuiltinCastMethod::wireValue, Function.identity()));

  private final String wireValue;

  BuiltinCastMethod(String wireValue) {
    this.wireValue = wireValue;
  }

  public String wireValue() {
    return wireValue;
  }

  public static BuiltinCastMethod fromWireValue(String value) {
    if (value == null || value.isBlank()) {
      return EXPLICIT;
    }
    var normalized = value.trim().toLowerCase(Locale.ROOT);
    var method = BY_WIRE.get(normalized);
    if (method == null) {
      throw new IllegalArgumentException("Unknown cast method: " + value);
    }
    return method;
  }
}
