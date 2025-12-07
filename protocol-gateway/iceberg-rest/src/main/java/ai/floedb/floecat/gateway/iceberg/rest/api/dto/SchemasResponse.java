package ai.floedb.floecat.gateway.iceberg.rest.api.dto;

import java.util.List;

public record SchemasResponse(List<SchemaHistoryDto> schemas, PageDto page) {}
