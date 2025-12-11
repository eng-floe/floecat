package ai.floedb.floecat.gateway.iceberg.rest.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record TablePlanTasksResponseDto(
    @JsonProperty("plan-tasks") List<String> planTasks,
    @JsonProperty("file-scan-tasks") List<FileScanTaskDto> fileScanTasks,
    @JsonProperty("delete-files") List<ContentFileDto> deleteFiles) {}
