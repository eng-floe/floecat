package ai.floedb.floecat.gateway.iceberg.rest.services.catalog;

import ai.floedb.floecat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.SnapshotClient;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public final class SnapshotLister {

  private SnapshotLister() {}

  public enum Mode {
    ALL,
    REFS
  }

  public static List<Snapshot> fetchSnapshots(
      SnapshotClient snapshotClient, ResourceId tableId, Mode mode, IcebergMetadata metadata) {
    try {
      var resp =
          snapshotClient.listSnapshots(
              ListSnapshotsRequest.newBuilder().setTableId(tableId).build());
      List<Snapshot> snapshots = resp.getSnapshotsList();
      if (mode == Mode.REFS) {
        if (metadata == null || metadata.getRefsCount() == 0) {
          return List.of();
        }
        Set<Long> refIds =
            metadata.getRefsMap().values().stream()
                .map(IcebergRef::getSnapshotId)
                .collect(Collectors.toSet());
        return snapshots.stream()
            .filter(s -> refIds.contains(s.getSnapshotId()))
            .collect(Collectors.toList());
      }
      return snapshots;
    } catch (io.grpc.StatusRuntimeException e) {
      return List.of();
    }
  }
}
