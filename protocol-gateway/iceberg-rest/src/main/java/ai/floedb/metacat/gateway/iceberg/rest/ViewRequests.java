package ai.floedb.metacat.gateway.iceberg.rest;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import java.util.Map;

/** Minimal DTOs for Iceberg view requests. */
public final class ViewRequests {
  private ViewRequests() {}

  public record Create(String name, String sql, Map<String, String> properties) {}

  public record Update(
      String name,
      @JsonDeserialize(using = NamespaceListDeserializer.class) List<String> namespace,
      String sql,
      Map<String, String> properties) {}

  public record Commit(
      @JsonDeserialize(using = NamespaceListDeserializer.class) List<String> namespace,
      Map<String, String> summary,
      String sql) {}
}
