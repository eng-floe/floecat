package ai.floedb.floecat.gateway.iceberg.rest.services.catalog;

import ai.floedb.floecat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.SnapshotClient;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef;
import java.util.ArrayList;
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
      List<Snapshot> snapshots = fetchAllSnapshots(snapshotClient, tableId);
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

  private static List<Snapshot> fetchAllSnapshots(
      SnapshotClient snapshotClient, ResourceId tableId) {
    List<Snapshot> out = new ArrayList<>();
    String token = "";
    while (true) {
      ListSnapshotsRequest.Builder request = ListSnapshotsRequest.newBuilder().setTableId(tableId);
      request.setPage(PageRequest.newBuilder().setPageSize(1000).setPageToken(token).build());
      ListSnapshotsResponse resp = snapshotClient.listSnapshots(request.build());
      if (resp == null) {
        break;
      }
      out.addAll(resp.getSnapshotsList());
      if (!resp.hasPage()) {
        break;
      }
      String nextToken = resp.getPage().getNextPageToken();
      if (nextToken == null || nextToken.isBlank() || nextToken.equals(token)) {
        break;
      }
      token = nextToken;
    }
    return out;
  }
}
