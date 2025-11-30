package ai.floedb.metacat.gateway.iceberg.rest;

import java.util.List;

public record SnapshotsResponse(List<SnapshotDto> snapshots, PageDto page) {}
