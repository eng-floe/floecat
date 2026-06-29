/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.gateway.iceberg.rest.services.catalog;

import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.gateway.iceberg.rest.common.RefPropertyUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import io.grpc.StatusRuntimeException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class SnapshotLister {

  private SnapshotLister() {}

  public enum Mode {
    ALL,
    REFS
  }

  public static List<Snapshot> fetchSnapshots(
      GrpcServiceFacade snapshotClient, Table table, Mode mode) {
    try {
      ResourceId tableId = table == null ? null : table.getResourceId();
      List<Snapshot> snapshots = fetchAllSnapshots(snapshotClient, tableId);
      if (mode == Mode.REFS) {
        Set<Long> refIds = referencedSnapshotIds(snapshotClient, table, snapshots);
        if (refIds.isEmpty()) {
          return List.of();
        }
        return snapshots.stream()
            .filter(s -> refIds.contains(s.getSnapshotId()))
            .collect(Collectors.toList());
      }
      return snapshots;
    } catch (StatusRuntimeException e) {
      return List.of();
    }
  }

  private static Set<Long> referencedSnapshotIds(
      GrpcServiceFacade snapshotClient, Table table, List<Snapshot> snapshots) {
    if (table == null) {
      return Set.of();
    }
    Set<Long> refHeads =
        RefPropertyUtil.decode(table.getPropertiesMap().get(RefPropertyUtil.PROPERTY_KEY))
            .values()
            .stream()
            .map(ref -> TableMappingUtil.asLong(ref.get("snapshot-id")))
            .filter(id -> id != null && id >= 0L)
            .collect(Collectors.toSet());
    Long currentSnapshotId = currentSnapshotId(snapshotClient, table.getResourceId());
    if (currentSnapshotId != null && currentSnapshotId >= 0L) {
      refHeads.add(currentSnapshotId);
    }
    return reachableSnapshotIds(refHeads, snapshots);
  }

  private static Set<Long> reachableSnapshotIds(Set<Long> refHeads, List<Snapshot> snapshots) {
    if (refHeads == null || refHeads.isEmpty() || snapshots == null || snapshots.isEmpty()) {
      return Set.of();
    }
    Map<Long, Snapshot> byId = new HashMap<>();
    for (Snapshot snapshot : snapshots) {
      if (snapshot != null && snapshot.getSnapshotId() >= 0L) {
        byId.put(snapshot.getSnapshotId(), snapshot);
      }
    }
    Set<Long> reachable = new HashSet<>();
    ArrayDeque<Long> pending = new ArrayDeque<>(refHeads);
    while (!pending.isEmpty()) {
      Long id = pending.removeFirst();
      if (id == null || !reachable.add(id)) {
        continue;
      }
      Snapshot snapshot = byId.get(id);
      if (snapshot != null && snapshot.hasParentSnapshotId()) {
        pending.addLast(snapshot.getParentSnapshotId());
      }
    }
    return reachable;
  }

  private static Long currentSnapshotId(GrpcServiceFacade snapshotClient, ResourceId tableId) {
    if (snapshotClient == null || tableId == null) {
      return null;
    }
    try {
      var request =
          GetSnapshotRequest.newBuilder()
              .setTableId(tableId)
              .setSnapshot(SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT))
              .build();
      var response = snapshotClient.getSnapshot(request);
      if (response == null
          || !response.hasSnapshot()
          || response.getSnapshot().getSnapshotId() < 0) {
        return null;
      }
      return response.getSnapshot().getSnapshotId();
    } catch (StatusRuntimeException e) {
      return null;
    }
  }

  private static List<Snapshot> fetchAllSnapshots(
      GrpcServiceFacade snapshotClient, ResourceId tableId) {
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
