package ai.floedb.floecat.gateway.iceberg.rest.api.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record ViewListResponse(
    List<TableIdentifierDto> identifiers, @JsonProperty("next-page-token") String nextPageToken) {}
