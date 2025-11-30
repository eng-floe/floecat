package ai.floedb.metacat.gateway.iceberg.rest;

import java.util.Map;

/** Minimal DTOs for Iceberg table requests. */
public final class TableRequests {
  private TableRequests() {}

  public record Create(String name, String schemaJson, Map<String, String> properties) {}

  public record Update(
      String name, String namespace, String schemaJson, Map<String, String> properties) {}

  public record Commit(
      String name, String namespace, String schemaJson, Map<String, String> properties) {}
}
