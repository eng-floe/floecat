package ai.floedb.floecat.gateway.iceberg.rest.api.dto;

import java.util.List;

public record SnapshotsResponse(List<SnapshotDto> snapshots, PageDto page) {}
