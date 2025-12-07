package ai.floedb.floecat.gateway.iceberg.rest.api.dto;

import java.util.List;

public record PartitionSpecsResponse(List<PartitionSpecHistoryDto> specs, PageDto page) {}
