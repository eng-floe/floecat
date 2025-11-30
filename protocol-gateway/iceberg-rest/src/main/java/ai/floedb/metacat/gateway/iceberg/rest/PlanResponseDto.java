package ai.floedb.metacat.gateway.iceberg.rest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record PlanResponseDto(
    String status,
    @JsonProperty("plan-id") String planId,
    @JsonProperty("plan-tasks") List<Object> planTasks,
    @JsonProperty("file-scan-tasks") List<FileScanTaskDto> fileScanTasks,
    @JsonProperty("delete-files") List<ContentFileDto> deleteFiles,
    @JsonProperty("storage-credentials") List<Object> storageCredentials) {}

@JsonInclude(JsonInclude.Include.NON_NULL)
record FileScanTaskDto(
    @JsonProperty("data-file") ContentFileDto dataFile,
    @JsonProperty("delete-file-references") List<Integer> deleteFileReferences,
    @JsonProperty("residual-filter") Object residualFilter) {}

@JsonInclude(JsonInclude.Include.NON_NULL)
record ContentFileDto(
    String content,
    @JsonProperty("file-path") String filePath,
    @JsonProperty("file-format") String fileFormat,
    @JsonProperty("spec-id") Integer specId,
    @JsonProperty("partition") List<Object> partition,
    @JsonProperty("file-size-in-bytes") Long fileSizeInBytes,
    @JsonProperty("record-count") Long recordCount,
    @JsonProperty("key-metadata") String keyMetadata,
    @JsonProperty("split-offsets") List<Long> splitOffsets,
    @JsonProperty("sort-order-id") Integer sortOrderId,
    @JsonProperty("equality-ids") List<Integer> equalityIds) {}
