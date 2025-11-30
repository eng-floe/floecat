package ai.floedb.metacat.gateway.iceberg.rest;

import java.util.List;

public record PartitionSpecsResponse(List<PartitionSpecHistoryDto> specs, PageDto page) {}
