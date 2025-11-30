package ai.floedb.metacat.gateway.iceberg.rest;

import java.util.List;

public record SchemasResponse(List<SchemaHistoryDto> schemas, PageDto page) {}
