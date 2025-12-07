package ai.floedb.floecat.gateway.iceberg.rest.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record FileScanTaskDto(
    @JsonProperty("data-file") ContentFileDto dataFile,
    @JsonProperty("delete-file-references") List<Integer> deleteFileReferences,
    @JsonProperty("residual-filter") Object residualFilter) {}
