package ai.floedb.floecat.gateway.iceberg.rest.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record TableListResponseDto(
    @JsonProperty("identifiers") List<TableIdentifierDto> identifiers,
    @JsonProperty("next-page-token") String nextPageToken) {}
