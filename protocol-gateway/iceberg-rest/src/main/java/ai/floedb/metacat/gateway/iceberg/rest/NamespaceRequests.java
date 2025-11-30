package ai.floedb.metacat.gateway.iceberg.rest;

import java.util.Map;

/**
 * Minimal DTOs for Iceberg namespace requests.
 */
public final class NamespaceRequests {
  private NamespaceRequests() {}

  public record Create(String namespace, String description, Map<String, String> properties, String policyRef) {}

  public record Update(String description, Map<String, String> properties, String policyRef) {}
}
