package ai.floedb.floecat.telemetry.profiling;

import java.util.Locale;

public enum ProfilingMode {
  JFR("jfr");

  private final String value;

  ProfilingMode(String value) {
    this.value = value;
  }

  public String value() {
    return value;
  }

  public static ProfilingMode from(String raw) {
    String normalized = normalize(raw);
    return JFR.value.equals(normalized) ? JFR : null;
  }

  public static String normalize(String raw) {
    if (raw == null) {
      return JFR.value;
    }
    String trimmed = raw.trim().toLowerCase(Locale.ROOT);
    return trimmed.isEmpty() ? JFR.value : trimmed;
  }
}
