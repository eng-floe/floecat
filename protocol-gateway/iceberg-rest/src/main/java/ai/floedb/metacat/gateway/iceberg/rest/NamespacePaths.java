package ai.floedb.metacat.gateway.iceberg.rest;

import java.util.Arrays;
import java.util.List;

/**
 * Utility to parse Iceberg namespace identifiers (dot-delimited) into path parts.
 */
public final class NamespacePaths {
  private NamespacePaths() {}

  public static List<String> split(String namespace) {
    if (namespace == null || namespace.isBlank()) {
      return List.of();
    }
    return Arrays.asList(namespace.split("\\."));
  }
}
