package ai.floedb.metacat.gateway.iceberg.rest;

import java.util.Map;

/**
 * Minimal DTOs for Iceberg view requests.
 */
public final class ViewRequests {
  private ViewRequests() {}

  public record Create(String name, String sql, Map<String, String> properties) {}

  public record Update(String name, String namespace, String sql, Map<String, String> properties) {}

  public record Commit(String namespace, Map<String, String> summary, String sql) {}
}
