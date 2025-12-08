package ai.floedb.floecat.gateway.iceberg.rest.api.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record SnapshotsResponse(@JsonProperty("entries") List<SnapshotDto> entries, PageDto page) {

  @JsonProperty("snapshots")
  public List<SnapshotDto> snapshots() {
    return entries;
  }
}
