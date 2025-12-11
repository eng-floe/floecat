package ai.floedb.floecat.gateway.iceberg.rest.api.dto;

import java.util.List;

public record ViewListResponse(List<TableIdentifierDto> identifiers, String nextPageToken) {}
