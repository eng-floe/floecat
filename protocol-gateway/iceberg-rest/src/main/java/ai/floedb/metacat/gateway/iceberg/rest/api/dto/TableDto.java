package ai.floedb.metacat.gateway.iceberg.rest.api.dto;

import java.util.Map;

public record TableDto(
    String id,
    String displayName,
    String catalogId,
    String namespaceId,
    String schemaJson,
    Map<String, String> properties) {}
