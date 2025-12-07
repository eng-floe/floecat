package ai.floedb.floecat.gateway.iceberg.rest.api.dto;

import java.util.List;

public record NamespacePropertiesResponse(
    List<String> updated, List<String> removed, List<String> missing) {}
