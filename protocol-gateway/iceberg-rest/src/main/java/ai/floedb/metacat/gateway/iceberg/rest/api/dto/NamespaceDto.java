package ai.floedb.metacat.gateway.iceberg.rest.api.dto;

import java.util.List;
import java.util.Map;

public record NamespaceDto(
    String id,
    String displayName,
    List<String> parents,
    String description,
    Map<String, String> properties) {}
