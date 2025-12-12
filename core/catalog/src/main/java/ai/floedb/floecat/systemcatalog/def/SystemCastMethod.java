package ai.floedb.floecat.systemcatalog.def;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum SystemCastMethod {
  IMPLICIT("implicit"),
  ASSIGNMENT("assignment"),
  EXPLICIT("explicit");

  private static final Map<String, SystemCastMethod> BY_WIRE =
      Arrays.stream(values())
          .collect(Collectors.toUnmodifiableMap(SystemCastMethod::wireValue, Function.identity()));

  private final String wireValue;

  SystemCastMethod(String wireValue) {
    this.wireValue = wireValue;
  }

  public String wireValue() {
    return wireValue;
  }

  public static SystemCastMethod fromWireValue(String value) {
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
