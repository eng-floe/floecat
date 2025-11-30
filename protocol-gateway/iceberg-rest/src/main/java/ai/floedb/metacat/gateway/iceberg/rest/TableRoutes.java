package ai.floedb.metacat.gateway.iceberg.rest;

import java.util.List;

/**
 * Helpers for parsing table identifiers from REST path components.
 */
public final class TableRoutes {
  private TableRoutes() {}

  public static List<String> namespaceParts(String ns) {
    return NamespacePaths.split(ns);
  }
}
