package ai.floedb.floecat.gateway.iceberg.rest.services.resolution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class NamespacePaths {
  private static final char DELIMITER = 0x1F;

  private NamespacePaths() {}

  public static List<String> split(String namespace) {
    if (namespace == null || namespace.isBlank()) {
      return List.of();
    }

    String normalized = normalizeDelimiter(namespace);
    if (normalized.indexOf(DELIMITER) >= 0) {
      return splitOnDelimiter(normalized, DELIMITER);
    }
    return List.of(normalized);
  }

  public static String encode(List<String> parts) {
    if (parts == null || parts.isEmpty()) {
      return "";
    }
    return String.join(String.valueOf(DELIMITER), parts);
  }

  private static List<String> splitOnDelimiter(String text, char delimiter) {
    String[] raw = text.split(String.valueOf(delimiter), -1);
    return new ArrayList<>(Arrays.asList(raw));
  }

  private static String normalizeDelimiter(String value) {
    if (value.indexOf('%') >= 0) {
      return value
          .replace("%1F", String.valueOf(DELIMITER))
          .replace("%1f", String.valueOf(DELIMITER));
    }
    return value;
  }
}
